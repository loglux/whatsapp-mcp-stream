import crypto from "crypto";
import { log } from "../utils/logger.js";
import { setBoundedMapEntry } from "../utils/bounded-map.js";
import { StoreService } from "../providers/store/store-service.js";

export interface IdempotencyOptions {
  sendDedupWindowMs?: number;
  idempotencyTtlMs?: number;
  recentSendRequestsMax?: number;
}

const DEFAULT_DEDUP_WINDOW_MS = 45000;
const DEFAULT_IDEMPOTENCY_TTL_MS = 86400000;
const DEFAULT_RECENT_SEND_REQUESTS_MAX = 500;

export class IdempotencyManager {
  private readonly sendDedupWindowMs: number;
  private readonly idempotencyTtlMs: number;
  private readonly recentSendRequestsMax: number;
  private recentSendRequests = new Map<
    string,
    { timestamp: number; result: any; messageId: string | null }
  >();
  private inFlightSendRequests = new Map<string, Promise<any>>();

  constructor(
    private readonly storeService: StoreService | null,
    options: IdempotencyOptions = {},
  ) {
    this.sendDedupWindowMs = Math.max(
      0,
      options.sendDedupWindowMs ?? DEFAULT_DEDUP_WINDOW_MS,
    );
    this.idempotencyTtlMs = Math.max(
      60000,
      options.idempotencyTtlMs ?? DEFAULT_IDEMPOTENCY_TTL_MS,
    );
    this.recentSendRequestsMax = Math.max(
      1,
      options.recentSendRequestsMax ?? DEFAULT_RECENT_SEND_REQUESTS_MAX,
    );
  }

  buildSendDedupKey(jid: string, message: string): string {
    return `${jid}\n${message}`;
  }

  buildRequestFingerprint(jid: string, message: string): string {
    return crypto
      .createHash("sha256")
      .update(this.buildSendDedupKey(jid, message))
      .digest("hex");
  }

  private markDeduplicated(result: any, messageId?: string | null): any {
    return {
      ...(result || {}),
      __deduplicated: true,
      __originalMessageId:
        messageId ||
        (result?.key?.remoteJid && result?.key?.id
          ? `${result.key.remoteJid}:${result.key.id}`
          : null),
    };
  }

  getRecentSendResult(jid: string, message: string): any | null {
    if (!this.sendDedupWindowMs) return null;
    const key = this.buildSendDedupKey(jid, message);
    const existing = this.recentSendRequests.get(key);
    if (!existing) return null;
    if (Date.now() - existing.timestamp > this.sendDedupWindowMs) {
      this.recentSendRequests.delete(key);
      return null;
    }
    return {
      ...(existing.result || {}),
      __deduplicated: true,
      __originalMessageId: existing.messageId,
    };
  }

  rememberSendResult(jid: string, message: string, result: any): void {
    if (!this.sendDedupWindowMs) return;
    const messageId =
      result?.key?.remoteJid && result?.key?.id
        ? `${result.key.remoteJid}:${result.key.id}`
        : null;
    const key = this.buildSendDedupKey(jid, message);
    setBoundedMapEntry(
      this.recentSendRequests,
      key,
      { timestamp: Date.now(), result, messageId },
      this.recentSendRequestsMax,
    );
  }

  getStoredIdempotentResult(
    operation: string,
    idempotencyKey: string,
    requestFingerprint: string,
  ): any | null {
    if (!this.storeService) return null;
    const existing = this.storeService.getIdempotencyRecord(idempotencyKey);
    if (!existing) return null;
    if (existing.expires_at <= Date.now()) {
      return null;
    }
    if (existing.operation !== operation) {
      throw new Error(
        `idempotency_key was already used for ${existing.operation}, not ${operation}`,
      );
    }
    if (existing.request_fingerprint !== requestFingerprint) {
      throw new Error(
        `idempotency_key was already used with different ${operation} parameters`,
      );
    }
    try {
      return this.markDeduplicated(
        JSON.parse(existing.response_json),
        existing.message_id,
      );
    } catch (error) {
      log.warn(
        { err: error, idempotencyKey },
        "Failed to parse stored idempotent send result",
      );
      return null;
    }
  }

  persistIdempotentResult(
    operation: string,
    idempotencyKey: string,
    jid: string,
    requestFingerprint: string,
    result: any,
  ): void {
    if (!this.storeService) return;
    const now = Date.now();
    const messageId =
      result?.key?.remoteJid && result?.key?.id
        ? `${result.key.remoteJid}:${result.key.id}`
        : null;
    this.storeService.upsertIdempotencyRecord({
      key: idempotencyKey,
      operation,
      scope_jid: jid,
      request_fingerprint: requestFingerprint,
      response_json: JSON.stringify(result || {}),
      message_id: messageId,
      created_at: now,
      expires_at: now + this.idempotencyTtlMs,
    });
  }

  async executeIdempotent<T>(
    operation: string,
    requestFingerprint: string,
    action: () => Promise<T>,
    options?: { idempotencyKey?: string | null; scopeJid?: string | null },
  ): Promise<T | any> {
    const idempotencyKey = options?.idempotencyKey?.trim() || null;
    if (!idempotencyKey) {
      return await action();
    }

    const stored = this.getStoredIdempotentResult(
      operation,
      idempotencyKey,
      requestFingerprint,
    );
    if (stored) {
      log.warn(
        {
          operation,
          idempotencyKey,
          scopeJid: options?.scopeJid || null,
          messageId: stored.__originalMessageId,
        },
        "Returned stored idempotent operation result",
      );
      return stored;
    }

    const inFlightKey = `idempotency:${operation}:${idempotencyKey}`;
    const inFlight = this.inFlightSendRequests.get(inFlightKey);
    if (inFlight) {
      log.warn(
        { operation, idempotencyKey, scopeJid: options?.scopeJid || null },
        "Joined in-flight idempotent operation",
      );
      const result = await inFlight;
      return this.markDeduplicated(result);
    }

    const opPromise = Promise.resolve(action());
    this.inFlightSendRequests.set(inFlightKey, opPromise);
    try {
      const result = await opPromise;
      this.persistIdempotentResult(
        operation,
        idempotencyKey,
        options?.scopeJid || "",
        requestFingerprint,
        result,
      );
      return result;
    } finally {
      this.inFlightSendRequests.delete(inFlightKey);
    }
  }

  async executeSendWithDedup(
    key: string,
    jid: string,
    message: string,
    operation: () => Promise<any>,
  ): Promise<any> {
    const inFlight = this.inFlightSendRequests.get(key);
    if (inFlight) {
      log.warn({ jid }, "Joined in-flight duplicate WhatsApp send request");
      const result = await inFlight;
      return this.markDeduplicated(result);
    }

    const sendPromise = (async () => {
      const result = await operation();
      this.rememberSendResult(jid, message, result);
      return result;
    })();

    this.inFlightSendRequests.set(key, sendPromise);
    try {
      return await sendPromise;
    } finally {
      this.inFlightSendRequests.delete(key);
    }
  }
}
