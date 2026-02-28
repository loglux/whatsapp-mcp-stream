import fs from "fs";
import path from "path";
import crypto from "crypto";
import axios from "axios";
import { fileTypeFromBuffer } from "file-type";
import { log } from "../utils/logger.js";
import { mapContact, mapMessage, mapStoredMessage } from "../core/mappers.js";
import {
  ResolvedContact,
  SimpleChat,
  SimpleContact,
  SimpleMessage,
} from "../core/types.js";
import {
  StoredChat,
  StoredContact,
  StoredGroupMeta,
  StoredGroupParticipant,
  StoredMessage,
  StoredMedia,
} from "../storage/message-store.js";
import { StoreService } from "../providers/store/store-service.js";
import {
  BaileysClient,
  BaileysLogEvent,
} from "../providers/wa/baileys-client.js";
import { WhatsAppSync } from "./whatsapp-sync.js";

import {
  ALL_WA_PATCH_NAMES,
  DisconnectReason,
  downloadContentFromMessage,
  jidNormalizedUser,
} from "baileys";

export interface DownloadedMedia {
  data: Buffer;
  mimetype: string;
  filename?: string;
  filesize?: number;
}

export class WhatsAppService {
  private latestQrCode: string | null = null;
  private isAuthenticatedFlag = false;
  private isReadyFlag = false;
  private initializing: Promise<void> | null = null;
  private reconnecting = false;
  private messageIndex = new Map<string, any>();
  private messagesByChat = new Map<string, any[]>();
  private messageKeyIndex = new Map<string, any>();
  private sessionReplaced = false;
  private lastDisconnectReason: string | null = null;
  private lastDisconnectAt: number | null = null;
  private lifecycleLock: Promise<void> = Promise.resolve();
  private reconnectAttempts = 0;
  private readonly maxMessageIndexSize = Math.max(
    1000,
    Number(process.env.WA_MESSAGE_INDEX_MAX || 20000) || 20000,
  );
  private readonly maxMessageKeyIndexSize = Math.max(
    1000,
    Number(process.env.WA_MESSAGE_KEY_INDEX_MAX || 20000) || 20000,
  );
  private sessionDir: string;
  private storeService: StoreService | null = null;
  private client: BaileysClient;
  private sync: WhatsAppSync;
  private readonly eventLogEnabled =
    (process.env.WA_EVENT_LOG || "").toLowerCase() === "1";
  private readonly eventStreamEnabled =
    (process.env.WA_EVENT_STREAM || "").toLowerCase() === "1";
  private readonly eventStreamPath =
    process.env.WA_EVENT_STREAM_PATH || "/app/wa-events.log";
  private eventStreamWriter: fs.WriteStream | null = null;
  private readonly resyncReconnectEnabled =
    (process.env.WA_RESYNC_RECONNECT || "1").toLowerCase() === "1";
  private readonly resyncReconnectDelayMs = Number(
    process.env.WA_RESYNC_RECONNECT_DELAY_MS || 15000,
  );
  private syncRecoveryInProgress = false;
  private syncRecoveryAttempts = 0;
  private lastSyncFailureAt: number | null = null;
  private lastRecoveryAt: number | null = null;
  private lastRecoveryReason: string | null = null;
  private readonly syncRecoveryCooldownMs = Math.max(
    30000,
    Number(process.env.WA_SYNC_RECOVERY_COOLDOWN_MS || 300000) || 300000,
  );
  private readonly syncRecoveryWindowMs = Math.max(
    60000,
    Number(process.env.WA_SYNC_RECOVERY_WINDOW_MS || 900000) || 900000,
  );
  private readonly syncSoftRecoveryLimit = Math.max(
    1,
    Number(process.env.WA_SYNC_SOFT_RECOVERY_LIMIT || 2) || 2,
  );
  private readonly readinessGraceMs = Math.max(
    30000,
    Number(process.env.WA_READINESS_GRACE_MS || 180000) || 180000,
  );
  private readonly sendDedupWindowMs = Math.max(
    0,
    Number(process.env.WA_SEND_DEDUP_WINDOW_MS || 45000) || 45000,
  );
  private readonly idempotencyTtlMs = Math.max(
    60000,
    Number(process.env.WA_IDEMPOTENCY_TTL_MS || 86400000) || 86400000,
  );
  private recentSendRequests = new Map<
    string,
    { timestamp: number; result: any; messageId: string | null }
  >();
  private inFlightSendRequests = new Map<string, Promise<any>>();

  constructor() {
    const baseDir =
      process.env.SESSION_DIR ||
      path.join(process.cwd(), "whatsapp-sessions", "baileys");
    this.sessionDir = baseDir;
    this.client = new BaileysClient(this.sessionDir, (event) =>
      this.handleBaileysLogEvent(event),
    );
    this.initStoreService();
    this.storeService?.deleteExpiredIdempotencyRecords();
    this.sync = new WhatsAppSync(
      this.storeService,
      () => this.getSocketOptional(),
      (chat: any) => this.normalizeChatRecord(chat),
      (msg: any) => this.trackMessage(msg),
      (chat: any) => this.upsertStoredChat(chat),
      (msg: any) => this.upsertStoredMessage(msg),
      () => this.getChatCount(),
    );
  }

  private isAutoDownloadEnabled(): boolean {
    const value = (process.env.AUTO_DOWNLOAD_MEDIA || "").toLowerCase();
    return value === "true" || value === "1";
  }

  private getAutoDownloadMaxBytes(): number | null {
    const raw = process.env.AUTO_DOWNLOAD_MAX_MB;
    if (!raw) return null;
    const mb = Number(raw);
    if (!Number.isFinite(mb) || mb <= 0) return null;
    return mb * 1024 * 1024;
  }

  private getMediaPayload(message: any): {
    type: "image" | "video" | "audio" | "document" | "sticker";
    content: any;
  } | null {
    if (!message) return null;
    if (message.imageMessage) {
      return { type: "image", content: message.imageMessage };
    }
    if (message.videoMessage) {
      return { type: "video", content: message.videoMessage };
    }
    if (message.audioMessage) {
      return { type: "audio", content: message.audioMessage };
    }
    if (message.documentMessage) {
      return { type: "document", content: message.documentMessage };
    }
    if (message.stickerMessage) {
      return { type: "sticker", content: message.stickerMessage };
    }
    return null;
  }

  private async maybeAutoDownloadMedia(msg: any): Promise<void> {
    if (!this.isAutoDownloadEnabled()) return;
    if (!msg?.message) return;
    const jid = msg?.key?.remoteJid;
    if (jid === "status@broadcast") return;

    const payload = this.getMediaPayload(msg.message);
    if (!payload) return;

    const messageId = this.serializeMessageId(msg);
    if (this.storeService) {
      const existing = this.storeService.getMediaByMessageId(messageId);
      if (existing) return;
    }

    const maxBytes = this.getAutoDownloadMaxBytes();
    const rawSize =
      payload.content?.fileLength ??
      payload.content?.fileSize ??
      payload.content?.fileLength?.toNumber?.() ??
      payload.content?.fileSize?.toNumber?.();
    const size = rawSize ? Number(rawSize) : null;
    if (maxBytes && size && size > maxBytes) {
      log.info(
        { messageId, size, maxBytes },
        "Skipping auto-download: media too large",
      );
      return;
    }

    try {
      await this.downloadMedia(messageId);
    } catch (error) {
      log.warn({ err: error, messageId }, "Auto-download media failed");
    }
  }

  private initStoreService(): void {
    if (this.storeService) return;
    const dbPath =
      process.env.DB_PATH || path.join(this.sessionDir, "store.sqlite");
    this.storeService = new StoreService(dbPath);
  }

  private handleBaileysLogEvent(event: BaileysLogEvent): void {
    const message = String(event.message || "");
    const payloadText =
      event.payload && typeof event.payload === "object"
        ? JSON.stringify(event.payload)
        : "";
    const combined = `${message} ${payloadText}`.toLowerCase();

    if (
      combined.includes("failed to sync state from version") ||
      (combined.includes("failed to find key") &&
        combined.includes("decode mutation"))
    ) {
      this.scheduleSyncRecovery(message);
    }
  }

  private scheduleSyncRecovery(reason: string): void {
    const now = Date.now();
    if (
      this.lastSyncFailureAt &&
      now - this.lastSyncFailureAt > this.syncRecoveryWindowMs
    ) {
      this.syncRecoveryAttempts = 0;
    }
    this.lastSyncFailureAt = now;
    this.syncRecoveryAttempts += 1;

    if (this.syncRecoveryInProgress) {
      log.warn(
        { reason, attempts: this.syncRecoveryAttempts },
        "App state recovery already in progress",
      );
      return;
    }

    if (this.lastRecoveryAt && now - this.lastRecoveryAt < this.syncRecoveryCooldownMs) {
      log.warn(
        {
          reason,
          attempts: this.syncRecoveryAttempts,
          cooldownMs: this.syncRecoveryCooldownMs,
        },
        "App state recovery skipped due to cooldown",
      );
      return;
    }

    this.syncRecoveryInProgress = true;
    this.lastRecoveryAt = now;
    this.lastRecoveryReason = reason;

    const strategy =
      this.syncRecoveryAttempts <= this.syncSoftRecoveryLimit
        ? "force-resync"
        : "restart";

    setTimeout(() => {
      void (async () => {
        try {
          if (strategy === "force-resync") {
            log.warn(
              { reason, attempts: this.syncRecoveryAttempts },
              "Detected corrupted WhatsApp app state; forcing resync",
            );
            await this.forceResync();
          } else {
            log.warn(
              { reason, attempts: this.syncRecoveryAttempts },
              "Detected repeated app state corruption; restarting WhatsApp client",
            );
            await this.restart();
          }
        } catch (error) {
          log.error(
            { err: error, reason, strategy, attempts: this.syncRecoveryAttempts },
            "Automatic app state recovery failed",
          );
        } finally {
          this.syncRecoveryInProgress = false;
        }
      })();
    }, 0);
  }

  private normalizeJid(jid: string): string {
    if (!jid) return jid;
    if (typeof jidNormalizedUser === "function") {
      return jidNormalizedUser(jid);
    }
    return jid;
  }

  private isLidJid(jid: string): boolean {
    return Boolean(jid && String(jid).endsWith("@lid"));
  }

  private isPnJid(jid: string): boolean {
    return Boolean(jid && String(jid).endsWith("@s.whatsapp.net"));
  }

  private normalizePnNumber(value: string | null | undefined): string | null {
    if (!value) return null;
    const cleaned = String(value).replace(/[^\d]/g, "");
    return cleaned || null;
  }

  private storeLidMapping(lidJid: string, pnJid: string | null): void {
    if (!this.storeService || !lidJid) return;
    const pnNumber = pnJid ? this.normalizePnNumber(pnJid) : null;
    this.storeService.upsertLidMapping(lidJid, pnJid, pnNumber);
  }

  private storeLidMappingFromPair(a?: string, b?: string): void {
    const first = a || "";
    const second = b || "";
    if (!first || !second) return;
    if (this.isLidJid(first) && this.isPnJid(second)) {
      this.storeLidMapping(first, second);
    } else if (this.isPnJid(first) && this.isLidJid(second)) {
      this.storeLidMapping(second, first);
    }
  }

  private storeLidMappingFromKey(key: any): void {
    if (!key) return;
    this.storeLidMappingFromPair(key.remoteJid, key.remoteJidAlt);
    this.storeLidMappingFromPair(key.participant, key.participantAlt);
  }

  private storeLidMappingFromContact(contact: any): void {
    if (!contact) return;
    const id = contact?.id || contact?.jid || null;
    const lid =
      contact?.lid ||
      (id && this.isLidJid(id) ? id : null) ||
      (contact?.lid?.user ? contact.lid.user : null);
    const pn =
      contact?.phoneNumber?.user ||
      contact?.phoneNumber?.number ||
      contact?.phoneNumber ||
      (id && this.isPnJid(id) ? id : null);
    if (lid && pn) {
      this.storeLidMapping(lid, this.isPnJid(pn) ? pn : `${pn}@s.whatsapp.net`);
    }
  }

  private resolveLookupJid(jid: string): string {
    const normalized = this.normalizeJid(jid);
    if (!this.storeService || !normalized) return normalized;
    const direct = this.storeService.getChatById(normalized);
    if (direct) return normalized;
    const lidFromPn = this.storeService.getLidForPn(normalized);
    if (lidFromPn) return lidFromPn;
    const pnNumber = this.normalizePnNumber(normalized);
    if (pnNumber) {
      const lid = this.storeService.getLidForPn(pnNumber);
      if (lid) return lid;
    }
    return normalized;
  }

  private getRelatedJids(jid: string): string[] {
    const normalized = this.normalizeJid(jid);
    if (!normalized) return [];
    if (!this.storeService) return [normalized];
    const related = new Set<string>();
    related.add(normalized);
    if (this.isLidJid(normalized)) {
      const mapped = this.storeService.getPnForLid(normalized);
      if (mapped?.pnJid) related.add(mapped.pnJid);
    } else {
      const lidFromPn = this.storeService.getLidForPn(normalized);
      if (lidFromPn) {
        related.add(lidFromPn);
      } else {
        const pnNumber = this.normalizePnNumber(normalized);
        if (pnNumber) {
          const lid = this.storeService.getLidForPn(pnNumber);
          if (lid) related.add(lid);
        }
      }
    }
    return Array.from(related);
  }

  private resolveCanonicalChatId(jid: string): string {
    const normalized = this.normalizeJid(jid);
    if (!this.storeService || !normalized) return normalized;
    if (this.isLidJid(normalized)) {
      const mapped = this.storeService.getPnForLid(normalized);
      if (mapped?.pnJid) return mapped.pnJid;
    }
    return normalized;
  }

  private serializeMessageId(msg: any): string {
    const jid = msg?.key?.remoteJid || msg?.key?.remoteJidAlt || "unknown";
    const id = msg?.key?.id || "unknown";
    return `${jid}:${id}`;
  }

  private serializeKeyId(key: any): string {
    const jid = key?.remoteJid || key?.remoteJidAlt || "unknown";
    const id = key?.id || "unknown";
    return `${jid}:${id}`;
  }

  private upsertStoredMessage(msg: any): void {
    if (!this.storeService) return;
    const mapped = mapMessage(msg, this.serializeMessageId.bind(this));
    const record: StoredMessage = {
      id: mapped.id,
      chat_jid: mapped.to,
      from: mapped.from,
      to: mapped.to,
      timestamp: mapped.timestamp,
      from_me: mapped.fromMe ? 1 : 0,
      body: mapped.body || "",
      has_media: mapped.hasMedia ? 1 : 0,
      type: mapped.type,
    };
    this.storeService.upsertMessage(record);
  }

  private updateStoredMessageContent(msg: any): void {
    if (!this.storeService) return;
    const mapped = mapMessage(msg, this.serializeMessageId.bind(this));
    const changes = this.storeService.updateMessageContent(
      mapped.id,
      mapped.body || "",
      mapped.hasMedia ? 1 : 0,
      mapped.type,
    );
    if (changes === 0) {
      this.upsertStoredMessage(msg);
    }
  }

  private upsertStoredChat(chat: any): void {
    if (!this.storeService) return;
    const id = chat?.id || chat?.jid;
    if (!id) return;
    const rawTs = chat?.conversationTimestamp;
    const tsValue =
      typeof rawTs === "number"
        ? rawTs
        : typeof rawTs?.toNumber === "function"
          ? rawTs.toNumber()
          : Number(rawTs || 0);
    const record: StoredChat = {
      id,
      name: chat?.name || chat?.subject || id,
      is_group: String(id).endsWith("@g.us") ? 1 : 0,
      unread_count: chat?.unreadCount || 0,
      timestamp: tsValue * 1000,
    };
    this.storeService.upsertChat(record);
  }

  private upsertStoredContact(contact: any): void {
    if (!this.storeService || !contact) return;
    const jid = contact?.id || contact?.jid;
    if (!jid) return;
    const mapped = mapContact(contact);
    const record: StoredContact = {
      jid,
      name: mapped.name,
      pushname: mapped.pushname,
      number: mapped.number || null,
      is_group: mapped.isGroup ? 1 : 0,
      is_my_contact: mapped.isMyContact ? 1 : 0,
      updated_at: Date.now(),
    };
    this.storeService.upsertContact(record);
  }

  private getContactSummary(jid: string): {
    name: string | null;
    pushname: string | null;
    number: string | null;
    is_my_contact: boolean | null;
  } | null {
    if (!this.storeService || !jid) return null;
    let contact = this.storeService.getContactById(jid);
    if (!contact && this.isLidJid(jid)) {
      const mapped = this.storeService.getPnForLid(jid);
      if (mapped?.pnJid) {
        contact = this.storeService.getContactById(mapped.pnJid);
      }
    }
    if (!contact && this.isPnJid(jid)) {
      const lid = this.storeService.getLidForPn(jid);
      if (lid) {
        contact = this.storeService.getContactById(lid);
      }
    }
    if (!contact) return null;
    return {
      name: contact.name || null,
      pushname: contact.pushname || null,
      number: contact.number || null,
      is_my_contact: contact.is_my_contact ? true : false,
    };
  }

  private getBestContactName(jid: string): string | null {
    const summary = this.getContactSummary(jid);
    if (!summary) return null;
    const candidates = [summary.name, summary.pushname, summary.number];
    for (const candidate of candidates) {
      if (this.isUsableDisplayName(candidate, jid)) {
        return candidate!;
      }
    }
    return null;
  }

  private getBestChatName(
    related: string[],
    canonicalId: string,
    storedName: string | null | undefined,
  ): string | null {
    if (this.isUsableDisplayName(storedName, canonicalId)) return storedName!;
    for (const jid of related) {
      const name = this.getBestContactName(jid);
      if (name) return name;
    }
    const fallback = this.getBestContactName(canonicalId);
    if (fallback) return fallback;
    if (this.isPnJid(canonicalId)) {
      const pn = this.normalizePnNumber(canonicalId);
      if (pn) return pn;
    }
    return this.isUsableDisplayName(storedName, canonicalId) ? storedName! : null;
  }

  private isUsableDisplayName(
    value: string | null | undefined,
    canonicalId?: string,
  ): boolean {
    if (!value) return false;
    const trimmed = String(value).trim();
    if (!trimmed) return false;
    if (trimmed === canonicalId) return false;
    if (this.isLidJid(trimmed)) return false;
    return true;
  }

  private buildSimpleContact(
    contact: any,
    canonicalId?: string,
  ): SimpleContact {
    const jid = contact?.jid || contact?.id || "";
    const id = canonicalId || jid;
    const number =
      contact?.number ||
      this.normalizePnNumber(jid) ||
      this.normalizePnNumber(canonicalId || "") ||
      "";
    return {
      id,
      name: contact?.name || null,
      pushname: contact?.pushname || null,
      isMe: false,
      isUser: true,
      isGroup: Boolean(contact?.is_group),
      isWAContact: true,
      isMyContact: Boolean(contact?.is_my_contact),
      number,
    };
  }

  private mergeContacts(contacts: any[]): SimpleContact[] {
    const merged = new Map<string, SimpleContact>();
    for (const contact of contacts) {
      const jid = contact?.jid || contact?.id || "";
      if (!jid) continue;
      const canonicalId = this.resolveCanonicalChatId(jid);
      const entry = this.buildSimpleContact(contact, canonicalId);
      const existing = merged.get(canonicalId);
      if (!existing) {
        merged.set(canonicalId, entry);
        continue;
      }
      const name =
        existing.name && existing.name !== existing.id
          ? existing.name
          : entry.name && entry.name !== entry.id
            ? entry.name
            : existing.name || entry.name;
      const pushname = existing.pushname || entry.pushname;
      const number = existing.number || entry.number;
      merged.set(canonicalId, {
        ...existing,
        name,
        pushname,
        number,
        isMyContact: existing.isMyContact || entry.isMyContact,
      });
    }
    return Array.from(merged.values());
  }

  private persistGroupMetadata(metadata: any): void {
    if (!this.storeService || !metadata) return;
    const jid = metadata?.id;
    if (!jid) return;
    const record: StoredGroupMeta = {
      jid,
      subject: metadata?.subject || null,
      owner: metadata?.owner || metadata?.ownerPn || null,
      subject_owner: metadata?.subjectOwner || metadata?.subjectOwnerPn || null,
      size: typeof metadata?.size === "number" ? metadata.size : null,
      creation:
        typeof metadata?.creation === "number" ? metadata.creation : null,
      desc: metadata?.desc || null,
      updated_at: Date.now(),
    };
    this.storeService.upsertGroupMeta(record);

    const participants = Array.isArray(metadata?.participants)
      ? metadata.participants.map((p: any) => ({
          group_jid: jid,
          participant_jid: p?.id || "",
          admin: p?.admin || null,
          updated_at: Date.now(),
        }))
      : [];
    if (participants.length) {
      this.storeService.replaceGroupParticipants(jid, participants);
    }
  }

  private getCachedGroupInfo(groupJid: string): any | null {
    if (!this.storeService) return null;
    const meta = this.storeService.getGroupMeta(groupJid);
    if (!meta) return null;
    const participants = this.storeService.listGroupParticipants(groupJid);
    return {
      id: meta.jid,
      subject: meta.subject,
      owner: meta.owner,
      subjectOwner: meta.subject_owner,
      size: meta.size,
      creation: meta.creation,
      desc: meta.desc,
      participants: participants.map((p: StoredGroupParticipant) => {
        const contact = this.getContactSummary(p.participant_jid);
        return {
          id: p.participant_jid,
          admin: p.admin,
          ...contact,
        };
      }),
    };
  }

  private normalizeChatRecord(chat: any): any | null {
    if (!chat) return null;
    if (!chat.id && chat.jid) {
      return { ...chat, id: chat.jid };
    }
    return chat;
  }

  private trackMessage(msg: any): void {
    const id = this.serializeMessageId(msg);
    this.setBoundedMapEntry(
      this.messageIndex,
      id,
      msg,
      this.maxMessageIndexSize,
    );
    const keyId = msg?.key?.id;
    if (keyId) {
      this.setBoundedMapEntry(
        this.messageKeyIndex,
        keyId,
        msg,
        this.maxMessageKeyIndexSize,
      );
    }
    const jid = msg?.key?.remoteJid;
    if (!jid) return;
    this.storeLidMappingFromKey(msg?.key);
    const list = this.messagesByChat.get(jid) || [];
    list.push(msg);
    if (list.length > 500) {
      list.splice(0, list.length - 500);
    }
    this.messagesByChat.set(jid, list);
  }

  private setBoundedMapEntry<K, V>(
    map: Map<K, V>,
    key: K,
    value: V,
    maxSize: number,
  ): void {
    if (map.has(key)) {
      map.delete(key);
    }
    map.set(key, value);
    while (map.size > maxSize) {
      const oldestKey = map.keys().next().value;
      if (oldestKey === undefined) break;
      map.delete(oldestKey);
    }
  }

  private buildSendDedupKey(jid: string, message: string): string {
    return `${jid}\n${message}`;
  }

  private buildRequestFingerprint(jid: string, message: string): string {
    return crypto
      .createHash("sha256")
      .update(this.buildSendDedupKey(jid, message))
      .digest("hex");
  }

  private getRecentSendResult(jid: string, message: string): any | null {
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

  private rememberSendResult(jid: string, message: string, result: any): void {
    if (!this.sendDedupWindowMs) return;
    const messageId =
      result?.key?.remoteJid && result?.key?.id
        ? `${result.key.remoteJid}:${result.key.id}`
        : null;
    const key = this.buildSendDedupKey(jid, message);
    this.setBoundedMapEntry(
      this.recentSendRequests,
      key,
      { timestamp: Date.now(), result, messageId },
      500,
    );
  }

  private markDeduplicatedResult(result: any, messageId?: string | null): any {
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

  private getStoredIdempotentResult(
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
      return this.markDeduplicatedResult(
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

  private persistIdempotentResult(
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

  async executeIdempotentOperation<T>(
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
      return this.markDeduplicatedResult(result);
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

  private async executeSendWithDedup(
    key: string,
    jid: string,
    message: string,
    operation: () => Promise<any>,
  ): Promise<any> {
    const inFlight = this.inFlightSendRequests.get(key);
    if (inFlight) {
      log.warn({ jid }, "Joined in-flight duplicate WhatsApp send request");
      const result = await inFlight;
      return this.markDeduplicatedResult(result);
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

  private async buildMediaMessage(
    buffer: Buffer,
    mimetype: string,
    filename?: string,
    caption?: string,
    asAudioMessage = false,
  ): Promise<any> {
    if (mimetype.startsWith("image/")) {
      return { image: buffer, mimetype, caption };
    }
    if (mimetype.startsWith("video/")) {
      return { video: buffer, mimetype, caption };
    }
    if (mimetype.startsWith("audio/")) {
      return { audio: buffer, mimetype, ptt: asAudioMessage };
    }
    return {
      document: buffer,
      mimetype,
      fileName: filename || "file",
      caption,
    };
  }

  async initialize(): Promise<void> {
    await this.withLifecycleLock(async () => {
      if (this.initializing) {
        await this.initializing;
        return;
      }

      this.initStoreService();

      this.initializing = (async () => {
        await this.client.initialize(async (key: any) => {
          const cached = key?.id ? this.messageKeyIndex.get(key.id) : undefined;
          return cached?.message;
        });

        const sock = this.client.getSocket();
        log.info(
          {
            eventLogEnabled: this.eventLogEnabled,
            eventStreamEnabled: this.eventStreamEnabled,
            eventStreamPath: this.eventStreamPath,
          },
          "WhatsApp event debug config",
        );

        if (this.eventLogEnabled || this.eventStreamEnabled) {
          if (this.eventStreamEnabled) {
            try {
              fs.writeFileSync(this.eventStreamPath, "", { flag: "a" });
            } catch (err) {
              log.warn({ err }, "Failed to touch WhatsApp event stream file");
            }
          }
          const originalEmit = sock.ev.emit.bind(sock.ev);
          sock.ev.emit = ((event: string, ...args: any[]) => {
            if (this.eventStreamEnabled) {
              this.writeEventStream(event, args);
            }
            if (this.eventLogEnabled) {
              const summary = this.summarizeEventPayload(event, args);
              log.info({ event, ...summary }, "WhatsApp event (raw)");
            }
            return originalEmit(event, ...args);
          }) as typeof sock.ev.emit;
        }

        if (this.eventStreamEnabled) {
          try {
            this.eventStreamWriter = fs.createWriteStream(
              this.eventStreamPath,
              { flags: "a" },
            );
            log.info(
              { path: this.eventStreamPath },
              "WhatsApp event stream capture enabled",
            );
          } catch (err) {
            log.warn({ err }, "Failed to enable WhatsApp event stream capture");
          }
        }

        sock.ev.on("connection.update", (update: any) => {
          const { connection, lastDisconnect, qr } = update;

          if (connection) {
            log.info({ connection }, "WhatsApp connection update");
          }
          this.logEvent("connection.update", {
            connection,
            hasQr: Boolean(qr),
            isOnline: update?.isOnline,
            receivedPendingNotifications: update?.receivedPendingNotifications,
            statusCode: lastDisconnect?.error?.output?.statusCode,
            reason: lastDisconnect?.error?.message,
          });

          if (qr) {
            this.latestQrCode = qr;
            log.info("QR code received.");
          }

          if (connection === "open") {
            this.isAuthenticatedFlag = true;
            this.isReadyFlag = true;
            this.latestQrCode = null;
            this.sessionReplaced = false;
            this.lastDisconnectReason = null;
            this.lastDisconnectAt = null;
            this.reconnectAttempts = 0;
            log.info("WhatsApp connection opened.");
            this.sync.scheduleWarmup(() => this.forceResync());
          }

          if (connection === "close") {
            this.isReadyFlag = false;
            this.isAuthenticatedFlag = false;
            const statusCode = lastDisconnect?.error?.output?.statusCode;
            const reason = lastDisconnect?.error?.message;
            const reasonText = reason ? String(reason) : "";
            this.lastDisconnectReason =
              reasonText ||
              (typeof statusCode === "number" ? String(statusCode) : null);
            this.lastDisconnectAt = Date.now();
            if (
              statusCode === DisconnectReason.connectionReplaced ||
              reasonText.toLowerCase().includes("conflict") ||
              reasonText.toLowerCase().includes("replaced")
            ) {
              this.sessionReplaced = true;
            }
            log.warn({ statusCode, reason }, "WhatsApp connection closed.");
            if (statusCode === DisconnectReason.loggedOut) {
              log.warn("WhatsApp logged out. Clearing session.");
              try {
                fs.rmSync(this.sessionDir, { recursive: true, force: true });
              } catch (_error) {
                // Ignore
              }
              this.reconnect().catch((err) =>
                log.error({ err }, "Failed to reconnect WhatsApp"),
              );
            } else if (
              statusCode === 401 ||
              reason?.includes("Connection Failure")
            ) {
              log.warn(
                "WhatsApp connection failed. Resetting session and reconnecting.",
              );
              try {
                fs.rmSync(this.sessionDir, { recursive: true, force: true });
              } catch (_error) {
                // Ignore
              }
              this.reconnect().catch((err) =>
                log.error({ err }, "Failed to reconnect WhatsApp"),
              );
            } else {
              this.reconnect().catch((err) =>
                log.error({ err }, "Failed to reconnect WhatsApp"),
              );
            }
          }
        });

        sock.ev.on("messages.upsert", (ev: any) => {
          this.logEvent("messages.upsert", {
            type: ev?.type,
            count: Array.isArray(ev?.messages) ? ev.messages.length : 0,
            jids: Array.isArray(ev?.messages)
              ? ev.messages.map((m: any) => m?.key?.remoteJid).filter(Boolean)
              : [],
            ids: Array.isArray(ev?.messages)
              ? ev.messages.map((m: any) => m?.key?.id).filter(Boolean)
              : [],
          });
          if (!ev?.messages) return;
          for (const msg of ev.messages) {
            this.trackMessage(msg);
            this.upsertStoredMessage(msg);
            const jid = msg?.key?.remoteJid;
            if (jid) {
              this.upsertStoredChat({
                id: jid,
                conversationTimestamp: msg?.messageTimestamp,
              });
            }
            void this.maybeAutoDownloadMedia(msg);
          }
        });

        sock.ev.on("messages.update", (updates: any[]) => {
          this.logEvent("messages.update", {
            count: Array.isArray(updates) ? updates.length : 0,
          });
          if (!updates) return;
          for (const update of updates) {
            const key =
              update?.key?.remoteJid && update?.key?.id
                ? `${update.key.remoteJid}:${update.key.id}`
                : null;
            if (key && this.messageIndex.has(key)) {
              const existing = this.messageIndex.get(key);
              const merged = {
                ...existing,
                ...update.update,
                message: {
                  ...(existing.message || {}),
                  ...(update.update?.message || {}),
                },
              };
              this.setBoundedMapEntry(
                this.messageIndex,
                key,
                merged,
                this.maxMessageIndexSize,
              );
              if (update.update?.message) {
                this.updateStoredMessageContent(merged);
              }
            } else if (update?.update?.message && update?.key?.remoteJid) {
              const synthetic = {
                key: update.key,
                message: update.update.message,
                messageTimestamp:
                  update.update?.messageTimestamp ??
                  update.update?.timestamp ??
                  0,
              };
              this.updateStoredMessageContent(synthetic);
            }
          }
        });

        sock.ev.on("messages.media-update", (updates: any[]) => {
          this.logEvent("messages.media-update", {
            count: Array.isArray(updates) ? updates.length : 0,
          });
          if (!updates) return;
          for (const update of updates) {
            const key = update?.key;
            const msgId = this.serializeKeyId(key);
            if (this.messageIndex.has(msgId)) {
              const existing = this.messageIndex.get(msgId);
              const merged = {
                ...existing,
                media: {
                  ...(existing.media || {}),
                  ...(update.media || {}),
                },
              };
              this.setBoundedMapEntry(
                this.messageIndex,
                msgId,
                merged,
                this.maxMessageIndexSize,
              );
            }
          }
        });

        sock.ev.on("messages.reaction", (updates: any[]) => {
          this.logEvent("messages.reaction", {
            count: Array.isArray(updates) ? updates.length : 0,
          });
          if (!updates) return;
          for (const update of updates) {
            const key = update?.key;
            const msgId = this.serializeKeyId(key);
            if (this.messageIndex.has(msgId)) {
              const existing = this.messageIndex.get(msgId);
              const reaction = update?.reaction;
              const merged = {
                ...existing,
                reactions: reaction
                  ? [...(existing.reactions || []), reaction]
                  : existing.reactions,
              };
              this.setBoundedMapEntry(
                this.messageIndex,
                msgId,
                merged,
                this.maxMessageIndexSize,
              );
            }
            if (this.storeService) {
              try {
                this.storeService.insertMessageReaction(
                  msgId,
                  JSON.stringify(update),
                );
              } catch (error) {
                log.warn({ err: error }, "Failed to persist message reaction");
              }
            }
          }
        });

        sock.ev.on("message-receipt.update", (updates: any[]) => {
          this.logEvent("message-receipt.update", {
            count: Array.isArray(updates) ? updates.length : 0,
          });
          if (!updates) return;
          for (const update of updates) {
            const key = update?.key;
            const msgId = this.serializeKeyId(key);
            if (this.messageIndex.has(msgId)) {
              const existing = this.messageIndex.get(msgId);
              const receipts = update?.receipt;
              const merged = {
                ...existing,
                receipts: receipts
                  ? [...(existing.receipts || []), receipts]
                  : existing.receipts,
              };
              this.setBoundedMapEntry(
                this.messageIndex,
                msgId,
                merged,
                this.maxMessageIndexSize,
              );
            }
            if (this.storeService) {
              try {
                this.storeService.insertMessageReceipt(
                  msgId,
                  JSON.stringify(update),
                );
              } catch (error) {
                log.warn({ err: error }, "Failed to persist message receipt");
              }
            }
          }
        });

        sock.ev.on("messages.delete", (payload: any) => {
          this.logEvent("messages.delete", {
            all: Boolean(payload?.all),
            jid: payload?.jid,
            count: Array.isArray(payload?.keys) ? payload.keys.length : 0,
          });
          if (!payload) return;
          if (payload.all && payload.jid) {
            if (this.storeService) {
              this.storeService.deleteMessagesByChat(payload.jid);
            }
            this.messagesByChat.delete(payload.jid);
            return;
          }
          const keys = payload.keys || [];
          for (const key of keys) {
            const jid = key?.remoteJid;
            const id = key?.id;
            if (!jid || !id) continue;
            const msgId = `${jid}:${id}`;
            this.messageIndex.delete(msgId);
            this.messageKeyIndex.delete(id);
            const list = this.messagesByChat.get(jid);
            if (list?.length) {
              const filtered = list.filter((msg: any) => msg?.key?.id !== id);
              this.messagesByChat.set(jid, filtered);
            }
            if (this.storeService) {
              this.storeService.deleteMessageById(msgId);
            }
          }
        });

        sock.ev.on("chats.set", (payload: any) => {
          this.logEvent("chats.set", {
            count: Array.isArray(payload?.chats) ? payload.chats.length : 0,
          });
          this.sync.handleChatsSet(payload);
        });

        sock.ev.on("messages.set", (payload: any) => {
          this.logEvent("messages.set", {
            count: Array.isArray(payload?.messages)
              ? payload.messages.length
              : 0,
          });
          this.sync.handleMessagesSet(payload);
        });

        sock.ev.on("chats.upsert", (payload: any) => {
          this.logEvent("chats.upsert", {
            count: Array.isArray(payload) ? payload.length : 0,
          });
          this.sync.handleChatsUpsert(payload);
        });

        sock.ev.on("chats.update", (payload: any) => {
          this.logEvent("chats.update", {
            count: Array.isArray(payload) ? payload.length : 0,
          });
          this.sync.handleChatsUpdate(payload);
        });

        sock.ev.on("contacts.upsert", (payload: any) => {
          this.logEvent("contacts.upsert", {
            count: Array.isArray(payload) ? payload.length : 0,
          });
          if (payload && Array.isArray(payload)) {
            log.info({ count: payload.length }, "Contacts upsert");
            for (const contact of payload) {
              this.upsertStoredContact(contact);
              this.storeLidMappingFromContact(contact);
            }
          }
        });

        sock.ev.on("contacts.update", (payload: any) => {
          this.logEvent("contacts.update", {
            count: Array.isArray(payload) ? payload.length : 0,
          });
          if (payload && Array.isArray(payload)) {
            log.info({ count: payload.length }, "Contacts update");
            for (const contact of payload) {
              this.upsertStoredContact(contact);
              this.storeLidMappingFromContact(contact);
            }
          }
        });

        sock.ev.on("lid-mapping.update", (payload: any) => {
          this.logEvent("lid-mapping.update", {
            type: Array.isArray(payload) ? "array" : typeof payload,
          });
          const mappings = Array.isArray(payload) ? payload : [payload];
          for (const item of mappings) {
            if (!item) continue;
            const lid = item?.lid || item?.lidJid || item?.jid || null;
            const pn = item?.pn || item?.pnJid || item?.phoneNumber || null;
            if (lid && pn) {
              this.storeLidMapping(
                String(lid),
                this.isPnJid(String(pn)) ? String(pn) : `${pn}@s.whatsapp.net`,
              );
            }
          }
        });

        sock.ev.on("messaging-history.set", (payload: any) => {
          this.logEvent("messaging-history.set", {
            chats: Array.isArray(payload?.chats) ? payload.chats.length : 0,
            contacts: Array.isArray(payload?.contacts)
              ? payload.contacts.length
              : 0,
            messages: Array.isArray(payload?.messages)
              ? payload.messages.length
              : 0,
            isLatest: payload?.isLatest,
            progress: payload?.progress,
            syncType: payload?.syncType,
          });
          this.sync.handleMessagingHistorySet(payload);
          const contacts = payload?.contacts;
          if (contacts && Array.isArray(contacts)) {
            for (const contact of contacts) {
              this.upsertStoredContact(contact);
              this.storeLidMappingFromContact(contact);
            }
          }
        });
      })().finally(() => {
        this.initializing = null;
      });

      await this.initializing;
    });
  }

  private async reconnect(): Promise<void> {
    if (this.reconnecting) {
      return;
    }
    this.reconnecting = true;
    try {
      const attempt = Math.min(this.reconnectAttempts, 6);
      const delayMs = Math.min(30000, 1000 * Math.pow(2, attempt));
      this.reconnectAttempts += 1;
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      await this.withLifecycleLock(async () => {
        await this.destroyInternal();
        await this.initialize();
      });
    } catch (error) {
      log.error({ err: error }, "Reconnect failed");
    } finally {
      this.reconnecting = false;
    }
  }

  async forceResync(): Promise<void> {
    await this.withLifecycleLock(async () => {
      await this.sync.forceResync(async () => {
        const sock = this.getSocketOptional();
        if (sock?.authState?.keys?.set) {
          const resetMap: Record<string, null> = {};
          for (const name of ALL_WA_PATCH_NAMES) {
            resetMap[name] = null;
          }
          await sock.authState.keys.set({
            "app-state-sync-version": resetMap,
          });
          log.info("Force resync: reset app state versions");
        }
        if (sock?.authState?.creds) {
          sock.authState.creds.accountSyncCounter = 0;
          sock.ev.emit("creds.update", { accountSyncCounter: 0 });
          log.info("Force resync: reset account sync counter");
        }
      });
      await this.destroyInternal();
      await this.initialize();
      this.ensureReconnectAfterResync();
    });
  }

  async destroy(): Promise<void> {
    await this.withLifecycleLock(async () => {
      await this.destroyInternal();
    });
  }

  async restart(): Promise<void> {
    await this.withLifecycleLock(async () => {
      await this.destroyInternal();
      await this.initialize();
    });
  }

  async logout(): Promise<void> {
    await this.withLifecycleLock(async () => {
      await this.destroyInternal();
      try {
        fs.rmSync(this.sessionDir, { recursive: true, force: true });
      } catch (_error) {
        // Ignore
      }
    });
  }

  private ensureReconnectAfterResync(): void {
    if (!this.resyncReconnectEnabled) {
      return;
    }
    if (!this.resyncReconnectDelayMs || this.resyncReconnectDelayMs <= 0) {
      return;
    }
    setTimeout(() => {
      if (this.isReadyFlag) return;
      log.warn(
        { delayMs: this.resyncReconnectDelayMs },
        "Force resync did not reach ready state; reconnecting",
      );
      this.reconnect();
    }, this.resyncReconnectDelayMs);
  }

  isAuthenticated(): boolean {
    return this.isAuthenticatedFlag;
  }

  isReady(): boolean {
    return this.isReadyFlag;
  }

  getLatestQrCode(): string | null {
    return this.latestQrCode;
  }

  getSyncStats(): {
    chatCount: number;
    messageCount: number;
    lastHistorySyncAt: number | null;
    lastChatsSyncAt: number | null;
    lastMessagesSyncAt: number | null;
    warmupAttempts: number;
    warmupInProgress: boolean;
  } {
    const chatCount = this.getChatCount();
    const messageCount = this.messageIndex.size;
    const syncStats = this.sync.getStats();
    return {
      chatCount,
      messageCount,
      lastHistorySyncAt: syncStats.lastHistorySyncAt,
      lastChatsSyncAt: syncStats.lastChatsSyncAt,
      lastMessagesSyncAt: syncStats.lastMessagesSyncAt,
      warmupAttempts: syncStats.warmupAttempts,
      warmupInProgress: syncStats.warmupInProgress,
    };
  }

  getHealthStatus(): {
    ok: boolean;
    reason: string;
    ready: boolean;
    authenticated: boolean;
    chatCount: number;
    lastDisconnectAt: number | null;
    lastRecoveryAt: number | null;
    syncRecoveryInProgress: boolean;
  } {
    const now = Date.now();
    const chatCount = this.getChatCount();
    const recoveringRecently =
      this.syncRecoveryInProgress ||
      (this.lastRecoveryAt !== null &&
        now - this.lastRecoveryAt <= this.readinessGraceMs);
    const disconnectedRecently =
      this.lastDisconnectAt !== null &&
      now - this.lastDisconnectAt <= this.readinessGraceMs;

    if (this.isReadyFlag) {
      return {
        ok: true,
        reason: "ready",
        ready: this.isReadyFlag,
        authenticated: this.isAuthenticatedFlag,
        chatCount,
        lastDisconnectAt: this.lastDisconnectAt,
        lastRecoveryAt: this.lastRecoveryAt,
        syncRecoveryInProgress: this.syncRecoveryInProgress,
      };
    }

    if (recoveringRecently) {
      return {
        ok: true,
        reason: "recovering",
        ready: this.isReadyFlag,
        authenticated: this.isAuthenticatedFlag,
        chatCount,
        lastDisconnectAt: this.lastDisconnectAt,
        lastRecoveryAt: this.lastRecoveryAt,
        syncRecoveryInProgress: this.syncRecoveryInProgress,
      };
    }

    if (!this.isAuthenticatedFlag && this.latestQrCode) {
      return {
        ok: true,
        reason: "awaiting-qr",
        ready: this.isReadyFlag,
        authenticated: this.isAuthenticatedFlag,
        chatCount,
        lastDisconnectAt: this.lastDisconnectAt,
        lastRecoveryAt: this.lastRecoveryAt,
        syncRecoveryInProgress: this.syncRecoveryInProgress,
      };
    }

    if (disconnectedRecently) {
      return {
        ok: true,
        reason: "recent-disconnect",
        ready: this.isReadyFlag,
        authenticated: this.isAuthenticatedFlag,
        chatCount,
        lastDisconnectAt: this.lastDisconnectAt,
        lastRecoveryAt: this.lastRecoveryAt,
        syncRecoveryInProgress: this.syncRecoveryInProgress,
      };
    }

    return {
      ok: false,
      reason: "not-ready",
      ready: this.isReadyFlag,
      authenticated: this.isAuthenticatedFlag,
      chatCount,
      lastDisconnectAt: this.lastDisconnectAt,
      lastRecoveryAt: this.lastRecoveryAt,
      syncRecoveryInProgress: this.syncRecoveryInProgress,
    };
  }

  private async withLifecycleLock<T>(fn: () => Promise<T>): Promise<T> {
    let release: () => void;
    const next = new Promise<void>((resolve) => {
      release = resolve;
    });
    const current = this.lifecycleLock;
    this.lifecycleLock = current.then(() => next);
    await current;
    try {
      return await fn();
    } finally {
      release!();
    }
  }

  private async destroyInternal(): Promise<void> {
    await this.client.destroy();
    if (this.eventStreamWriter) {
      try {
        this.eventStreamWriter.end();
      } catch (_error) {
        // ignore
      }
      this.eventStreamWriter = null;
    }
    this.sync.clearWarmupTimer();
    this.isAuthenticatedFlag = false;
    this.isReadyFlag = false;
    this.latestQrCode = null;
  }

  private logEvent(event: string, payload: Record<string, unknown>): void {
    if (!this.eventLogEnabled) return;
    log.info({ event, ...payload }, "WhatsApp event");
  }

  private summarizeEventPayload(
    event: string,
    args: any[],
  ): Record<string, unknown> {
    const payload = args.length === 1 ? args[0] : args;
    if (Array.isArray(payload)) {
      const itemTypes = Array.from(
        new Set(
          payload.map((item) => (Array.isArray(item) ? "array" : typeof item)),
        ),
      ).slice(0, 5);
      return {
        payloadType: "array",
        arrayLength: payload.length,
        itemTypes,
      };
    }
    if (payload && typeof payload === "object") {
      return {
        payloadType: "object",
        keys: Object.keys(payload).slice(0, 25),
      };
    }
    return {
      payloadType: typeof payload,
      hasPayload: Boolean(payload),
      event,
    };
  }

  private writeEventStream(event: string, args: any[]): void {
    if (!this.eventStreamEnabled || !this.eventStreamWriter) return;
    const payload = args.length === 1 ? args[0] : args;
    try {
      const line = JSON.stringify(
        { ts: Date.now(), event, payload },
        (_key, value) => {
          if (typeof value === "bigint") return value.toString();
          if (value instanceof Buffer) {
            return { __type: "Buffer", length: value.length };
          }
          if (value instanceof Uint8Array) {
            return { __type: "Uint8Array", length: value.length };
          }
          return value;
        },
      );
      this.eventStreamWriter.write(`${line}\n`);
    } catch (err) {
      log.warn({ err, event }, "Failed to write WhatsApp event stream");
    }
  }

  getConnectionInfo(): {
    sessionReplaced: boolean;
    lastDisconnectReason: string | null;
    lastDisconnectAt: number | null;
    lastRecoveryReason: string | null;
    lastRecoveryAt: number | null;
    syncRecoveryAttempts: number;
    syncRecoveryInProgress: boolean;
  } {
    return {
      sessionReplaced: this.sessionReplaced,
      lastDisconnectReason: this.lastDisconnectReason,
      lastDisconnectAt: this.lastDisconnectAt,
      lastRecoveryReason: this.lastRecoveryReason,
      lastRecoveryAt: this.lastRecoveryAt,
      syncRecoveryAttempts: this.syncRecoveryAttempts,
      syncRecoveryInProgress: this.syncRecoveryInProgress,
    };
  }

  getMessageStoreStats(): {
    chats: number;
    messages: number;
    media: number;
    contacts: number;
  } | null {
    if (!this.storeService) return null;
    return this.storeService.stats();
  }

  private getChatCount(): number {
    if (this.storeService) {
      const stats = this.storeService.stats();
      if (stats) return stats.chats;
    }
    return this.messagesByChat.size;
  }

  async runWarmup(): Promise<{ chatCount: number; messageCount: number }> {
    return this.sync.runWarmup(
      () => this.messageIndex.size,
      () => this.forceResync(),
    );
  }

  getSocket(): any {
    return this.client.getSocket();
  }

  private getSocketOptional(): any | null {
    return this.client.getSocketOptional();
  }

  async listChats(
    limit = 20,
    includeLastMessage = true,
    includeSystemMessages = false,
  ): Promise<SimpleChat[]> {
    if (this.storeService) {
      const expandedLimit = Math.max(limit * 5, 50);
      const stored = this.storeService.listChats(expandedLimit);
      if (stored.length > 0) {
        const merged = new Map<string, SimpleChat>();
        for (const chat of stored) {
          if (chat.id === "status@broadcast") {
            continue;
          }
          const canonicalId = this.resolveCanonicalChatId(chat.id);
          const related = this.getRelatedJids(chat.id);
          const lastMessage = includeLastMessage
            ? this.getLastMessageForChat(chat.id)
            : undefined;
          const fallbackName = this.getBestChatName(
            related,
            canonicalId,
            chat.name,
          );
          const entry: SimpleChat = {
            id: canonicalId,
            name: fallbackName || chat.name,
            isGroup: Boolean(chat.is_group),
            unreadCount: chat.unread_count || 0,
            timestamp: chat.timestamp || 0,
            lastMessage,
          };
          const existing = merged.get(canonicalId);
          if (!existing) {
            merged.set(canonicalId, entry);
            continue;
          }
          const existingTs = existing.timestamp || 0;
          const entryTs = entry.timestamp || 0;
          const bestMessage =
            (existing.lastMessage?.timestamp || 0) >=
            (entry.lastMessage?.timestamp || 0)
              ? existing.lastMessage
              : entry.lastMessage;
          merged.set(canonicalId, {
            ...existing,
            name:
              existing.name && existing.name !== existing.id
                ? existing.name
                : entry.name,
            unreadCount: Math.max(existing.unreadCount, entry.unreadCount),
            timestamp: Math.max(existingTs, entryTs),
            lastMessage: bestMessage,
          });
        }
        const mapped = Array.from(merged.values()).filter(
          (chat) =>
            includeSystemMessages ||
            chat.lastMessage?.type !== "protocolMessage",
        );
        mapped.sort((a, b) => b.timestamp - a.timestamp);
        return mapped.slice(0, limit);
      }
    }

    await this.sync.warmup(() => this.forceResync());
    const chatIds = Array.from(this.messagesByChat.keys());
    if (chatIds.length > 0) {
      const merged = new Map<string, SimpleChat>();
      for (const jid of chatIds) {
        if (jid === "status@broadcast") {
          continue;
        }
        const canonicalId = this.resolveCanonicalChatId(jid);
        const lastMessage = includeLastMessage
          ? this.getLastMessageForChat(jid)
          : undefined;
        const entry: SimpleChat = {
          id: canonicalId,
          name: canonicalId,
          isGroup: canonicalId.endsWith("@g.us"),
          unreadCount: 0,
          timestamp: lastMessage?.timestamp || 0,
          lastMessage,
        };
        const existing = merged.get(canonicalId);
        if (!existing) {
          merged.set(canonicalId, entry);
          continue;
        }
        const bestMessage =
          (existing.lastMessage?.timestamp || 0) >=
          (entry.lastMessage?.timestamp || 0)
            ? existing.lastMessage
            : entry.lastMessage;
        merged.set(canonicalId, {
          ...existing,
          timestamp: Math.max(existing.timestamp, entry.timestamp),
          lastMessage: bestMessage,
        });
      }
      const mapped = Array.from(merged.values()).filter(
        (chat) =>
          includeSystemMessages || chat.lastMessage?.type !== "protocolMessage",
      );
      mapped.sort((a, b) => b.timestamp - a.timestamp);
      return mapped.slice(0, limit);
    }

    const mapped = chatIds
      .map((jid) => ({
        id: jid,
        name: jid,
        isGroup: jid.endsWith("@g.us"),
        unreadCount: 0,
        timestamp: 0,
        lastMessage: includeLastMessage
          ? this.getLastMessageForChat(jid)
          : undefined,
      }))
      .filter(
        (chat) =>
          includeSystemMessages || chat.lastMessage?.type !== "protocolMessage",
      );
    return mapped.slice(0, limit);
  }

  async listSystemChats(limit = 20): Promise<SimpleChat[]> {
    const chats = await this.listChats(limit * 5, true, true);
    const system = chats.filter(
      (chat) => chat.lastMessage?.type === "protocolMessage",
    );
    return system.slice(0, limit);
  }

  async listGroups(
    limit = 20,
    includeLastMessage = true,
  ): Promise<SimpleChat[]> {
    const chats = await this.listChats(limit * 5, includeLastMessage);
    const groups = chats.filter((chat) => chat.isGroup);
    return groups.slice(0, limit);
  }

  async getChatById(jid: string): Promise<SimpleChat | null> {
    if (this.storeService) {
      const related = this.getRelatedJids(jid);
      const merged = new Map<string, SimpleChat>();
      for (const entry of related) {
        const stored = this.storeService.getChatById(entry);
        if (!stored) continue;
        const canonicalId = this.resolveCanonicalChatId(stored.id);
        const name = this.getBestChatName(related, canonicalId, stored.name);
        const candidate: SimpleChat = {
          id: canonicalId,
          name: name || stored.name,
          isGroup: Boolean(stored.is_group),
          unreadCount: stored.unread_count || 0,
          timestamp: stored.timestamp || 0,
          lastMessage: this.getLastMessageForChat(canonicalId),
        };
        const existing = merged.get(canonicalId);
        if (!existing) {
          merged.set(canonicalId, candidate);
          continue;
        }
        const existingTs = existing.timestamp || 0;
        const candidateTs = candidate.timestamp || 0;
        merged.set(canonicalId, {
          ...existing,
          name:
            existing.name && existing.name !== existing.id
              ? existing.name
              : candidate.name,
          unreadCount: Math.max(existing.unreadCount, candidate.unreadCount),
          timestamp: Math.max(existingTs, candidateTs),
          lastMessage:
            (existing.lastMessage?.timestamp || 0) >=
            (candidate.lastMessage?.timestamp || 0)
              ? existing.lastMessage
              : candidate.lastMessage,
        });
      }
      const mergedList = Array.from(merged.values());
      if (mergedList.length === 0) return null;
      mergedList.sort((a, b) => b.timestamp - a.timestamp);
      return mergedList[0];
    }

    return null;
  }

  async getMessages(jid: string, limit = 50): Promise<SimpleMessage[]> {
    const related = this.getRelatedJids(jid);
    const perLimit = related.length > 1 ? Math.max(limit * 2, 100) : limit;
    const fromMemory = related.flatMap((entry) =>
      (this.messagesByChat.get(entry) || []).map((msg) =>
        mapMessage(msg, this.serializeMessageId.bind(this)),
      ),
    );
    const store = this.storeService;
    const fromDb = store
      ? related.flatMap((entry) =>
          store
            .listMessages(entry, perLimit)
            .map((msg) => mapStoredMessage(msg)),
        )
      : [];

    const merged = new Map<string, SimpleMessage>();
    for (const msg of fromDb) merged.set(msg.id, msg);
    for (const msg of fromMemory) merged.set(msg.id, msg);

    const combined = Array.from(merged.values()).sort(
      (a, b) => a.timestamp - b.timestamp,
    );
    return combined.slice(-limit);
  }

  async exportChat(
    jid: string,
    includeMedia: boolean,
  ): Promise<{
    chat: SimpleChat | null;
    messages: SimpleMessage[];
    media: StoredMedia[];
  }> {
    if (!this.storeService) {
      return { chat: null, messages: [], media: [] };
    }
    const normalized = this.resolveLookupJid(jid);
    const chat = await this.getChatById(normalized);
    const messages = this.storeService
      .listMessagesAll(normalized)
      .map((msg) => mapStoredMessage(msg));
    const media = includeMedia
      ? this.storeService.listMediaByChat(normalized)
      : [];
    return { chat, messages, media };
  }

  async searchMessages(
    query: string,
    limit = 20,
    chatId?: string,
  ): Promise<SimpleMessage[]> {
    const q = query.toLowerCase();
    const results: SimpleMessage[] = [];

    const pushIfMatch = (msg: SimpleMessage) => {
      if (msg.body && msg.body.toLowerCase().includes(q)) {
        results.push(msg);
      }
    };

    const searchRawList = (msgs: any[]) => {
      for (const msg of msgs) {
        pushIfMatch(mapMessage(msg, this.serializeMessageId.bind(this)));
      }
    };

    if (chatId) {
      const normalized = this.resolveLookupJid(chatId);
      const list = this.messagesByChat.get(normalized) || [];
      if (list.length > 0) {
        searchRawList(list);
      }
      if (this.storeService) {
        const stored = this.storeService.listMessages(
          normalized,
          Math.max(50, limit * 5),
        );
        for (const msg of stored) {
          pushIfMatch(mapStoredMessage(msg));
        }
      }
    } else {
      const all = Array.from(this.messageIndex.values());
      searchRawList(all);
    }

    if (results.length === 0 && this.storeService && !chatId) {
      const stored = this.storeService.searchMessages(query, limit);
      return stored.map((msg) => mapStoredMessage(msg));
    }

    results.sort((a, b) => b.timestamp - a.timestamp);
    return results.slice(0, limit);
  }

  async getMessageById(messageId: string): Promise<SimpleMessage | null> {
    const msg = this.messageIndex.get(messageId);
    if (msg) {
      return mapMessage(msg, this.serializeMessageId.bind(this));
    }
    if (this.storeService) {
      const stored = this.storeService.getMessageById(messageId);
      if (stored) {
        return mapStoredMessage(stored);
      }
    }
    return null;
  }

  async getProfilePicUrl(jid: string): Promise<string | null> {
    const socket = this.getSocket();
    try {
      const normalized = this.resolveLookupJid(jid);
      const url = await socket.profilePictureUrl(normalized, "image");
      return url || null;
    } catch (_error) {
      return null;
    }
  }

  async getGroupInfo(groupJid: string): Promise<any> {
    const socket = this.getSocket();
    const normalized = this.resolveLookupJid(groupJid);
    try {
      const metadata = await socket.groupMetadata(normalized);
      this.persistGroupMetadata(metadata);
      const participants = Array.isArray(metadata?.participants)
        ? metadata.participants.map((p: any) => {
            const jid = this.normalizeJid(p?.id || "");
            if (p?.id && (p?.pn || p?.pnJid || p?.phoneNumber)) {
              const pn = p?.pn || p?.pnJid || p?.phoneNumber;
              this.storeLidMappingFromPair(String(p.id), String(pn));
            }
            const contact = jid ? this.getContactSummary(jid) : null;
            return {
              ...p,
              ...(contact || {}),
            };
          })
        : metadata?.participants;
      return {
        ...metadata,
        participants,
      };
    } catch (error: any) {
      if (this.storeService) {
        const cached = this.getCachedGroupInfo(normalized);
        if (cached) {
          return { ...cached, source: "cached" };
        }
      }
      throw error;
    }
  }

  async sendMessage(jid: string, message: string): Promise<any> {
    return this.sendMessageWithOptions(jid, message);
  }

  async sendMessageWithOptions(
    jid: string,
    message: string,
    options?: { idempotencyKey?: string | null },
  ): Promise<any> {
    const socket = this.getSocket();
    const normalized = this.resolveLookupJid(jid);
    const isGroup = normalized.endsWith("@g.us");
    const dedupKey = this.buildSendDedupKey(normalized, message);
    const idempotencyKey = options?.idempotencyKey?.trim() || null;
    const requestFingerprint = this.buildRequestFingerprint(normalized, message);
    if (idempotencyKey) {
      const stored = this.getStoredIdempotentResult(
        "send_message",
        idempotencyKey,
        requestFingerprint,
      );
      if (stored) {
        log.warn(
          { jid: normalized, idempotencyKey, messageId: stored.__originalMessageId },
          "Returned stored idempotent WhatsApp send result",
        );
        return stored;
      }
    }
    const duplicate = this.getRecentSendResult(normalized, message);
    if (duplicate) {
      log.warn(
        { jid: normalized, messageId: duplicate.__originalMessageId },
        "Suppressed duplicate WhatsApp send request",
      );
      return duplicate;
    }
    const operationKey = idempotencyKey || dedupKey;
    try {
      const result = await this.executeSendWithDedup(
        operationKey,
        normalized,
        message,
        () => socket.sendMessage(normalized, { text: message }),
      );
      if (idempotencyKey) {
        this.persistIdempotentResult(
          "send_message",
          idempotencyKey,
          normalized,
          requestFingerprint,
          result,
        );
      }
      return result;
    } catch (error: any) {
      const msg = String(error?.message || error);
      const statusCode = error?.output?.statusCode;
      if (
        isGroup &&
        (msg.toLowerCase().includes("forbidden") || statusCode === 403)
      ) {
        try {
          await socket.groupMetadata(normalized);
        } catch (metaErr) {
          log.warn(
            { err: metaErr, jid: normalized },
            "Failed to refresh group metadata before retry",
          );
        }
        await new Promise((resolve) => setTimeout(resolve, 2000));
        try {
          const result = await this.executeSendWithDedup(
            operationKey,
            normalized,
            message,
            () => socket.sendMessage(normalized, { text: message }),
          );
          if (idempotencyKey) {
            this.persistIdempotentResult(
              "send_message",
              idempotencyKey,
              normalized,
              requestFingerprint,
              result,
            );
          }
          return result;
        } catch (retryErr: any) {
          log.error(
            { err: retryErr, jid: normalized },
            "Failed to send WhatsApp message after retry",
          );
          throw retryErr;
        }
      }

      log.error(
        { err: error, jid: normalized },
        "Failed to send WhatsApp message",
      );
      throw error;
    }
  }

  async sendMedia(
    jid: string,
    input: string,
    caption?: string,
    asAudioMessage = false,
    options?: { idempotencyKey?: string | null; requestFingerprint?: string },
  ): Promise<any> {
    let buffer: Buffer;
    let mimetype = "application/octet-stream";
    let filename: string | undefined;

    if (input.startsWith("http://") || input.startsWith("https://")) {
      const resp = await axios.get(input, { responseType: "arraybuffer" });
      buffer = Buffer.from(resp.data);
      const detected = await fileTypeFromBuffer(buffer);
      if (detected) {
        mimetype = detected.mime;
        filename = `file.${detected.ext}`;
      }
    } else {
      buffer = fs.readFileSync(input);
      const detected = await fileTypeFromBuffer(buffer);
      if (detected) {
        mimetype = detected.mime;
        filename = path.basename(input);
      }
    }

    const content = await this.buildMediaMessage(
      buffer,
      mimetype,
      filename,
      caption,
      asAudioMessage,
    );
    const normalized = this.resolveLookupJid(jid);
    const send = () => this.getSocket().sendMessage(normalized, content);
    if (options?.idempotencyKey) {
      return await this.executeIdempotentOperation(
        "send_media",
        options.requestFingerprint ||
          this.buildRequestFingerprint(
            normalized,
            JSON.stringify({
              input,
              caption: caption || null,
              asAudioMessage,
              mimetype,
              filename: filename || null,
            }),
          ),
        send,
        { idempotencyKey: options.idempotencyKey, scopeJid: normalized },
      );
    }
    return await send();
  }

  async sendMediaFromBase64(
    jid: string,
    base64: string,
    mimeType: string,
    filename?: string,
    caption?: string,
    asAudioMessage = false,
    options?: { idempotencyKey?: string | null; requestFingerprint?: string },
  ): Promise<any> {
    const buffer = Buffer.from(base64, "base64");
    const content = await this.buildMediaMessage(
      buffer,
      mimeType,
      filename,
      caption,
      asAudioMessage,
    );
    const normalized = this.resolveLookupJid(jid);
    const send = () => this.getSocket().sendMessage(normalized, content);
    if (options?.idempotencyKey) {
      return await this.executeIdempotentOperation(
        "send_media",
        options.requestFingerprint ||
          this.buildRequestFingerprint(
            normalized,
            JSON.stringify({
              base64,
              mimeType,
              filename: filename || null,
              caption: caption || null,
              asAudioMessage,
            }),
          ),
        send,
        { idempotencyKey: options.idempotencyKey, scopeJid: normalized },
      );
    }
    return await send();
  }

  async downloadMedia(messageId: string): Promise<DownloadedMedia | null> {
    let msg = this.messageIndex.get(messageId);
    if (!msg?.message) {
      const parts = messageId.split(":");
      if (parts.length >= 2) {
        const jid = parts[0];
        const keyId = parts.slice(1).join(":");
        const list = this.messagesByChat.get(jid) || [];
        msg = list.find((m: any) => m?.key?.id === keyId);
        if (msg) {
          this.trackMessage(msg);
        }
      }
    }

    if (!msg?.message) return null;

    const message = msg.message;
    let content: any;
    let type: "image" | "video" | "audio" | "document" | "sticker" | null =
      null;

    if (message.imageMessage) {
      content = message.imageMessage;
      type = "image";
    } else if (message.videoMessage) {
      content = message.videoMessage;
      type = "video";
    } else if (message.audioMessage) {
      content = message.audioMessage;
      type = "audio";
    } else if (message.documentMessage) {
      content = message.documentMessage;
      type = "document";
    } else if (message.stickerMessage) {
      content = message.stickerMessage;
      type = "sticker";
    }

    if (!content || !type) return null;

    const stream = await downloadContentFromMessage(content, type);
    const chunks: Buffer[] = [];
    for await (const chunk of stream) {
      chunks.push(Buffer.from(chunk));
    }
    const data = Buffer.concat(chunks);

    if (this.storeService) {
      const existing = this.storeService.getMediaByMessageId(messageId);
      if (!existing) {
        const mediaDir =
          process.env.MEDIA_DIR || path.join(process.cwd(), "media");
        fs.mkdirSync(mediaDir, { recursive: true });
        const safeName = (content.fileName || `media_${Date.now()}`).replace(
          /[^a-zA-Z0-9._-]/g,
          "_",
        );
        const ext = path.extname(safeName) || "";
        const base = ext ? safeName.replace(ext, "") : safeName;
        const filename = `${base}_${Date.now()}${ext || ""}`;
        const filePath = path.join(mediaDir, filename);
        try {
          fs.writeFileSync(filePath, data);
          const record: StoredMedia = {
            message_id: messageId,
            chat_jid: msg?.key?.remoteJid || "",
            file_path: filePath,
            filename,
            mimetype: content.mimetype || "application/octet-stream",
            size: data.length,
          };
          this.storeService.upsertMedia(record);
        } catch (error) {
          log.warn({ err: error }, "Failed to persist media file");
        }
      }
    }

    return {
      data,
      mimetype: content.mimetype || "application/octet-stream",
      filename: content.fileName,
      filesize: content.fileLength ? Number(content.fileLength) : undefined,
    };
  }

  async searchContacts(query: string): Promise<SimpleContact[]> {
    if (this.storeService) {
      const stored = this.storeService.searchContacts(query, 50);
      return this.mergeContacts(stored);
    }

    return [];
  }

  async resolveContacts(query: string, limit = 5): Promise<ResolvedContact[]> {
    const text = query.trim().toLowerCase();
    if (!text) return [];

    const digits = text.replace(/[^\d]/g, "");
    const hasDigits = digits.length >= 6;

    const contacts = this.storeService
      ? this.mergeContacts(this.storeService.listContacts(200))
      : [];
    const scored = contacts.map((contact) => {
      const mapped = mapContact(contact);
      const name = (mapped.name || "").toLowerCase();
      const push = (mapped.pushname || "").toLowerCase();
      const id = String(mapped.id || "").toLowerCase();
      const number = String(mapped.number || "").replace(/[^\d]/g, "");

      let score = 0;
      let matchedBy: ResolvedContact["matchedBy"] = "id";

      if (hasDigits) {
        if (number === digits) {
          score = 100;
          matchedBy = "number";
        } else if (number.startsWith(digits)) {
          score = 90;
          matchedBy = "number";
        } else if (number.includes(digits)) {
          score = 70;
          matchedBy = "number";
        } else if (id.includes(digits)) {
          score = 60;
          matchedBy = "id";
        }
      }

      if (text && !hasDigits) {
        if (name === text) {
          score = 100;
          matchedBy = "name";
        } else if (push === text) {
          score = 95;
          matchedBy = "pushname";
        } else if (name.startsWith(text)) {
          score = Math.max(score, 80);
          matchedBy = "name";
        } else if (push.startsWith(text)) {
          score = Math.max(score, 75);
          matchedBy = "pushname";
        } else if (name.includes(text)) {
          score = Math.max(score, 60);
          matchedBy = "name";
        } else if (push.includes(text)) {
          score = Math.max(score, 55);
          matchedBy = "pushname";
        } else if (id.includes(text)) {
          score = Math.max(score, 50);
          matchedBy = "id";
        }
      }

      if (hasDigits && text && score === 0) {
        if (name.includes(text) || push.includes(text) || id.includes(text)) {
          score = 50;
          matchedBy = name.includes(text)
            ? "name"
            : push.includes(text)
              ? "pushname"
              : "id";
        }
      }

      return { ...mapped, matchedBy, score } as ResolvedContact;
    });

    return scored
      .filter((c) => c.score > 0)
      .sort(
        (a, b) =>
          b.score - a.score ||
          String(a.name || "").localeCompare(String(b.name || "")),
      )
      .slice(0, limit);
  }

  async getContactById(jid: string): Promise<SimpleContact | null> {
    if (this.storeService) {
      let contact = this.storeService.getContactById(jid);
      if (!contact && this.isPnJid(jid)) {
        const lid = this.storeService.getLidForPn(jid);
        if (lid) {
          contact = this.storeService.getContactById(lid);
        }
      }
      if (!contact && this.isLidJid(jid)) {
        const mapped = this.storeService.getPnForLid(jid);
        if (mapped?.pnJid) {
          contact = this.storeService.getContactById(mapped.pnJid);
        }
      }
      if (!contact) return null;
      const canonicalId = this.resolveCanonicalChatId(contact.jid);
      const mapped = this.buildSimpleContact(contact, canonicalId);
      if (!mapped.number && this.isLidJid(contact.jid)) {
        const pn = this.storeService.getPnForLid(contact.jid);
        if (pn?.pnNumber) {
          mapped.number = pn.pnNumber;
        }
      }
      return mapped;
    }

    return null;
  }

  private getLastMessageForChat(jid: string): SimpleMessage | undefined {
    const candidates: SimpleMessage[] = [];
    const canonicalId = this.resolveCanonicalChatId(jid);
    if (this.storeService) {
      const related = this.getRelatedJids(jid);
      for (const entry of related) {
        const stored = this.storeService.listMessages(entry, 1);
        if (stored.length > 0) {
          candidates.push(this.normalizeSimpleMessageId(mapStoredMessage(stored[0]), canonicalId));
        }
      }
    }
    const related = this.getRelatedJids(jid);
    for (const entry of related) {
      const list = this.messagesByChat.get(entry) || [];
      const last = list[list.length - 1];
      if (last) {
        candidates.push(
          this.normalizeSimpleMessageId(
            mapMessage(last, this.serializeMessageId.bind(this)),
            canonicalId,
          ),
        );
      }
    }
    if (candidates.length === 0) return undefined;
    return candidates.reduce((best, curr) =>
      (best.timestamp || 0) >= (curr.timestamp || 0) ? best : curr,
    );
  }

  private normalizeSimpleMessageId(
    message: SimpleMessage,
    canonicalChatId: string,
  ): SimpleMessage {
    if (!message?.id || !canonicalChatId) return message;
    const idx = message.id.indexOf(":");
    if (idx <= 0) return message;
    const rawChatId = message.id.slice(0, idx);
    if (rawChatId === canonicalChatId) return message;
    if (this.resolveCanonicalChatId(rawChatId) !== canonicalChatId) return message;
    return {
      ...message,
      id: `${canonicalChatId}:${message.id.slice(idx + 1)}`,
      to: canonicalChatId,
    };
  }

  private hasDirectChatForParticipant(jid: string): boolean {
    if (!this.storeService || !jid) return false;
    const related = this.getRelatedJids(jid);
    for (const entry of related) {
      const chat = this.storeService.getChatById(entry);
      if (chat && !chat.is_group) {
        return true;
      }
    }
    return false;
  }

  private isParticipantInContacts(jid: string): boolean {
    if (!this.storeService || !jid) return false;
    const related = this.getRelatedJids(jid);
    for (const entry of related) {
      const contact = this.storeService.getContactById(entry);
      if (contact) return true;
    }
    return false;
  }

  private async getGroupParticipantJids(
    groupJid: string,
    refreshGroupInfo: boolean,
  ): Promise<string[]> {
    if (!groupJid) return [];

    if (refreshGroupInfo) {
      const info = await this.getGroupInfo(groupJid);
      const participants = Array.isArray(info?.participants)
        ? info.participants
        : [];
      return participants
        .map((p: any) => String(p?.id || "").trim())
        .filter(Boolean);
    }

    if (this.storeService) {
      const cached = this.storeService.listGroupParticipants(groupJid);
      if (cached.length > 0) {
        return cached
          .map((p) => String(p.participant_jid || "").trim())
          .filter(Boolean);
      }
    }
    return [];
  }

  private buildMemberDisplay(canonicalId: string): {
    name: string | null;
    pushname: string | null;
    number: string | null;
  } {
    const summary = this.getContactSummary(canonicalId);
    return {
      name: summary?.name || null,
      pushname: summary?.pushname || null,
      number: summary?.number || this.normalizePnNumber(canonicalId) || null,
    };
  }

  private async buildGroupAuditMatrix(
    groupLimit = 200,
    refreshGroupInfo = false,
  ): Promise<{
    groupsProcessed: number;
    members: Array<{
      canonicalId: string;
      ids: string[];
      name: string | null;
      pushname: string | null;
      number: string | null;
      groupCount: number;
      groups: Array<{ id: string; name: string }>;
      hasDirectChat: boolean;
      inContacts: boolean;
    }>;
  }> {
    let groups: SimpleChat[] = [];
    try {
      groups = await this.listGroups(groupLimit, false);
    } catch (error) {
      log.warn(
        { err: error, groupLimit, refreshGroupInfo },
        "Group audit: failed to list groups",
      );
      return {
        groupsProcessed: 0,
        members: [],
      };
    }
    const memberMap = new Map<
      string,
      {
        canonicalId: string;
        ids: Set<string>;
        groups: Array<{ id: string; name: string }>;
      }
    >();

    for (const group of groups) {
      const groupId = String(group.id || "").trim();
      if (!groupId.endsWith("@g.us")) continue;
      let participants: string[] = [];
      try {
        participants = await this.getGroupParticipantJids(
          groupId,
          refreshGroupInfo,
        );
      } catch (error) {
        log.warn(
          { err: error, groupId, refreshGroupInfo },
          "Group audit: failed to load participants",
        );
        continue;
      }
      for (const participantRaw of participants) {
        const participant = this.normalizeJid(participantRaw);
        if (!participant) continue;
        const canonicalId = this.resolveCanonicalChatId(participant);
        const existing = memberMap.get(canonicalId) || {
          canonicalId,
          ids: new Set<string>(),
          groups: [],
        };
        existing.ids.add(participant);
        if (!existing.groups.some((g) => g.id === groupId)) {
          existing.groups.push({
            id: groupId,
            name: group.name || groupId,
          });
        }
        memberMap.set(canonicalId, existing);
      }
    }

    const members = Array.from(memberMap.values())
      .map((entry) => {
        const profile = this.buildMemberDisplay(entry.canonicalId);
        return {
          canonicalId: entry.canonicalId,
          ids: Array.from(entry.ids.values()).sort(),
          name: profile.name,
          pushname: profile.pushname,
          number: profile.number,
          groupCount: entry.groups.length,
          groups: entry.groups.sort((a, b) => a.name.localeCompare(b.name)),
          hasDirectChat: this.hasDirectChatForParticipant(entry.canonicalId),
          inContacts: this.isParticipantInContacts(entry.canonicalId),
        };
      })
      .sort(
        (a, b) =>
          b.groupCount - a.groupCount ||
          String(a.name || "").localeCompare(String(b.name || "")),
      );

    return {
      groupsProcessed: groups.length,
      members,
    };
  }

  async analyzeGroupOverlaps(
    groupLimit = 200,
    refreshGroupInfo = false,
    minSharedGroups = 2,
  ): Promise<{
    groupsProcessed: number;
    totalMembers: number;
    overlaps: Array<{
      canonicalId: string;
      ids: string[];
      name: string | null;
      pushname: string | null;
      number: string | null;
      groupCount: number;
      groups: Array<{ id: string; name: string }>;
      hasDirectChat: boolean;
      inContacts: boolean;
    }>;
  }> {
    const matrix = await this.buildGroupAuditMatrix(
      groupLimit,
      refreshGroupInfo,
    );
    const threshold = Math.max(2, minSharedGroups);
    const overlaps = matrix.members.filter((m) => m.groupCount >= threshold);
    return {
      groupsProcessed: matrix.groupsProcessed,
      totalMembers: matrix.members.length,
      overlaps,
    };
  }

  async findMembersWithoutDirectChat(
    groupLimit = 200,
    refreshGroupInfo = false,
    minSharedGroups = 1,
  ): Promise<{
    groupsProcessed: number;
    totalMembers: number;
    members: Array<{
      canonicalId: string;
      ids: string[];
      name: string | null;
      pushname: string | null;
      number: string | null;
      groupCount: number;
      groups: Array<{ id: string; name: string }>;
      hasDirectChat: boolean;
      inContacts: boolean;
    }>;
  }> {
    const matrix = await this.buildGroupAuditMatrix(
      groupLimit,
      refreshGroupInfo,
    );
    const threshold = Math.max(1, minSharedGroups);
    const members = matrix.members.filter(
      (m) => !m.hasDirectChat && m.groupCount >= threshold,
    );
    return {
      groupsProcessed: matrix.groupsProcessed,
      totalMembers: matrix.members.length,
      members,
    };
  }

  async findMembersNotInContacts(
    groupLimit = 200,
    refreshGroupInfo = false,
    minSharedGroups = 1,
  ): Promise<{
    groupsProcessed: number;
    totalMembers: number;
    members: Array<{
      canonicalId: string;
      ids: string[];
      name: string | null;
      pushname: string | null;
      number: string | null;
      groupCount: number;
      groups: Array<{ id: string; name: string }>;
      hasDirectChat: boolean;
      inContacts: boolean;
    }>;
  }> {
    const matrix = await this.buildGroupAuditMatrix(
      groupLimit,
      refreshGroupInfo,
    );
    const threshold = Math.max(1, minSharedGroups);
    const members = matrix.members.filter(
      (m) => !m.inContacts && m.groupCount >= threshold,
    );
    return {
      groupsProcessed: matrix.groupsProcessed,
      totalMembers: matrix.members.length,
      members,
    };
  }

  async runGroupAudit(
    groupLimit = 200,
    refreshGroupInfo = false,
    overlapMinSharedGroups = 2,
    minSharedGroups = 1,
  ): Promise<{
    summary: {
      groupsProcessed: number;
      totalMembers: number;
      overlapCount: number;
      withoutDirectChatCount: number;
      notInContactsCount: number;
    };
    overlaps: Array<{
      canonicalId: string;
      ids: string[];
      name: string | null;
      pushname: string | null;
      number: string | null;
      groupCount: number;
      groups: Array<{ id: string; name: string }>;
      hasDirectChat: boolean;
      inContacts: boolean;
    }>;
    withoutDirectChat: Array<{
      canonicalId: string;
      ids: string[];
      name: string | null;
      pushname: string | null;
      number: string | null;
      groupCount: number;
      groups: Array<{ id: string; name: string }>;
      hasDirectChat: boolean;
      inContacts: boolean;
    }>;
    notInContacts: Array<{
      canonicalId: string;
      ids: string[];
      name: string | null;
      pushname: string | null;
      number: string | null;
      groupCount: number;
      groups: Array<{ id: string; name: string }>;
      hasDirectChat: boolean;
      inContacts: boolean;
    }>;
  }> {
    const matrix = await this.buildGroupAuditMatrix(
      groupLimit,
      refreshGroupInfo,
    );
    const overlapThreshold = Math.max(2, overlapMinSharedGroups);
    const baseThreshold = Math.max(1, minSharedGroups);
    const overlaps = matrix.members.filter(
      (m) => m.groupCount >= overlapThreshold,
    );
    const withoutDirectChat = matrix.members.filter(
      (m) => !m.hasDirectChat && m.groupCount >= baseThreshold,
    );
    const notInContacts = matrix.members.filter(
      (m) => !m.inContacts && m.groupCount >= baseThreshold,
    );

    return {
      summary: {
        groupsProcessed: matrix.groupsProcessed,
        totalMembers: matrix.members.length,
        overlapCount: overlaps.length,
        withoutDirectChatCount: withoutDirectChat.length,
        notInContactsCount: notInContacts.length,
      },
      overlaps,
      withoutDirectChat,
      notInContacts,
    };
  }
}
