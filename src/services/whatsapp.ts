import fs from "fs";
import path from "path";
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
  AdminChat,
  StoredGroupParticipant,
  StoredMedia,
  StoredMessage,
} from "../storage/message-store.js";
import { StoreService } from "../providers/store/store-service.js";
import {
  BaileysClient,
  BaileysLogEvent,
} from "../providers/wa/baileys-client.js";
import { WhatsAppSync } from "./whatsapp-sync.js";
import { GroupAuditEngine } from "./group-audit.js";
import { IdempotencyManager } from "./idempotency-manager.js";
import { MessageIndexStore } from "./message-index-store.js";
import { JidResolver } from "./jid-resolver.js";
import { RecoveryManager } from "./recovery-manager.js";
import { withTimeout } from "../utils/with-timeout.js";
import { AutoDownloadManager } from "./auto-download-manager.js";
import { StorageWriter } from "./storage-writer.js";
import { ContactPresenter } from "./contact-presenter.js";

import {
  ALL_WA_PATCH_NAMES,
  DisconnectReason,
  downloadContentFromMessage,
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
  private ownJid: string | null = null;
  private legacySenderBackfilled = false;
  private initializing: Promise<void> | null = null;
  private readonly initializeTimeoutMs: number = (() => {
    const raw = process.env.WA_INITIALIZE_TIMEOUT_MS;
    if (raw === undefined || raw === "") return 120_000;
    const n = Number(raw);
    return Number.isFinite(n) ? Math.max(0, n) : 120_000;
  })();
  private readonly messageIndexStore: MessageIndexStore;
  private lifecycleLock: Promise<void> = Promise.resolve();
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
  private groupAudit: GroupAuditEngine;
  private idempotency: IdempotencyManager;
  private jidResolver: JidResolver;
  private recovery: RecoveryManager;
  private autoDownload!: AutoDownloadManager;
  private storage!: StorageWriter;
  private contactPresenter!: ContactPresenter;

  constructor() {
    const baseDir =
      process.env.SESSION_DIR ||
      path.join(process.cwd(), "whatsapp-sessions", "baileys");
    this.sessionDir = baseDir;
    this.messageIndexStore = new MessageIndexStore({
      maxIndexSize: Math.max(
        1000,
        Number(process.env.WA_MESSAGE_INDEX_MAX) || 20000,
      ),
      maxKeyIndexSize: Math.max(
        1000,
        Number(process.env.WA_MESSAGE_KEY_INDEX_MAX) || 20000,
      ),
    });
    this.client = new BaileysClient(this.sessionDir, (event) =>
      this.handleBaileysLogEvent(event),
    );
    this.initStoreService();
    this.storeService?.deleteExpiredIdempotencyRecords();
    this.jidResolver = new JidResolver(this.storeService);
    this.storage = new StorageWriter({
      storeService: this.storeService,
      getOwnJid: () => this.ownJid,
    });
    this.contactPresenter = new ContactPresenter({
      storeService: this.storeService,
      jidResolver: this.jidResolver,
    });
    const autoDownloadMaxMbRaw = Number(process.env.AUTO_DOWNLOAD_MAX_MB);
    this.autoDownload = new AutoDownloadManager(
      {
        storeService: this.storeService,
        downloadMedia: (messageId: string) => this.downloadMedia(messageId),
      },
      {
        enabled: ["true", "1"].includes(
          (process.env.AUTO_DOWNLOAD_MEDIA || "").toLowerCase(),
        ),
        maxBytes:
          Number.isFinite(autoDownloadMaxMbRaw) && autoDownloadMaxMbRaw > 0
            ? autoDownloadMaxMbRaw * 1024 * 1024
            : null,
        concurrency: Number(process.env.WA_AUTO_DOWNLOAD_CONCURRENCY) || 3,
        maxQueued: Number(process.env.WA_AUTO_DOWNLOAD_QUEUE_MAX) || 200,
      },
    );
    this.sync = new WhatsAppSync(
      this.storeService,
      () => this.getSocketOptional(),
      (chat: any) => this.normalizeChatRecord(chat),
      (msg: any) => this.trackMessage(msg),
      (chat: any) => this.storage.upsertChat(chat),
      (msg: any) => this.storage.upsertMessage(msg),
      () => this.getChatCount(),
    );
    this.groupAudit = new GroupAuditEngine({
      storeService: this.storeService,
      listGroups: (limit, refresh) => this.listGroups(limit, refresh),
      getGroupInfo: (jid) => this.getGroupInfo(jid),
      resolveCanonicalChatId: (jid) =>
        this.jidResolver.resolveCanonicalChatId(jid),
      getRelatedJids: (jid) => this.jidResolver.getRelatedJids(jid),
      normalizeJid: (jid) => JidResolver.normalizeJid(jid),
      normalizePnNumber: (jid) => JidResolver.normalizePnNumber(jid),
      getContactSummary: (jid) => this.contactPresenter.getContactSummary(jid),
    });
    this.idempotency = new IdempotencyManager(this.storeService, {
      sendDedupWindowMs: Number(process.env.WA_SEND_DEDUP_WINDOW_MS) || 45000,
      idempotencyTtlMs: Number(process.env.WA_IDEMPOTENCY_TTL_MS) || 86400000,
    });
    this.recovery = new RecoveryManager(
      {
        withLifecycleLock: (fn) => this.withLifecycleLock(fn),
        destroyInternal: () => this.destroyInternal(),
        initializeWithinLifecycleLock: () =>
          this.initializeWithinLifecycleLock(),
        forceResync: () => this.forceResync(),
        restart: () => this.restart(),
        isReady: () => this.isReadyFlag,
      },
      {
        syncRecoveryCooldownMs: Math.max(
          30000,
          Number(process.env.WA_SYNC_RECOVERY_COOLDOWN_MS) || 300000,
        ),
        syncRecoveryWindowMs: Math.max(
          60000,
          Number(process.env.WA_SYNC_RECOVERY_WINDOW_MS) || 900000,
        ),
        syncSoftRecoveryLimit: Math.max(
          1,
          Number(process.env.WA_SYNC_SOFT_RECOVERY_LIMIT) || 2,
        ),
        disconnectRecoveryDelayMs: Math.max(
          5000,
          Number(process.env.WA_DISCONNECT_RECOVERY_DELAY_MS) || 30000,
        ),
        disconnectRecoveryRestartCodes: String(
          process.env.WA_DISCONNECT_RECOVERY_RESTART_CODES || "428",
        )
          .split(",")
          .map((value) => Number(value.trim()))
          .filter((value) => Number.isFinite(value)),
        resyncReconnectEnabled:
          (process.env.WA_RESYNC_RECONNECT || "1").toLowerCase() === "1",
        resyncReconnectDelayMs: Number(
          process.env.WA_RESYNC_RECONNECT_DELAY_MS || 15000,
        ),
        readinessGraceMs: Math.max(
          30000,
          Number(process.env.WA_READINESS_GRACE_MS) || 180000,
        ),
      },
    );
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
      this.recovery.scheduleSyncRecovery(message);
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
        const contact = this.contactPresenter.getContactSummary(
          p.participant_jid,
        );
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
    this.messageIndexStore.add(msg);
    this.jidResolver.storeLidMappingFromKey(msg?.key);
  }

  executeIdempotentOperation<T>(
    operation: string,
    requestFingerprint: string,
    action: () => Promise<T>,
    options?: { idempotencyKey?: string | null; scopeJid?: string | null },
  ): Promise<T | any> {
    return this.idempotency.executeIdempotent(
      operation,
      requestFingerprint,
      action,
      options,
    );
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

  private async waitForInitializing(): Promise<void> {
    if (!this.initializing) return;
    await withTimeout(
      this.initializing,
      this.initializeTimeoutMs,
      "WhatsApp initialize",
    );
  }

  private async initializeWithinLifecycleLock(): Promise<void> {
    if (this.initializing) {
      log.info(
        "Initialize requested while another initialize is already running",
      );
      await this.waitForInitializing();
      return;
    }

    this.initStoreService();

    this.initializing = (async () => {
      log.info("Initializing WhatsApp client");
      await this.client.initialize(async (key: any) => {
        const cached = key?.id
          ? this.messageIndexStore.getByKeyId(key.id)
          : undefined;
        return cached?.message;
      });

      const sock = this.client.getSocket();
      log.info("WhatsApp client initialized; wiring socket events");
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
          this.eventStreamWriter = fs.createWriteStream(this.eventStreamPath, {
            flags: "a",
          });
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
          const userId = sock?.user?.id;
          if (typeof userId === "string" && userId.length > 0) {
            this.ownJid = JidResolver.normalizeJid(userId);
            if (!this.legacySenderBackfilled && this.storeService) {
              const changed = this.storeService.backfillLegacySender(
                this.ownJid,
              );
              this.legacySenderBackfilled = true;
              if (changed > 0) {
                log.info(
                  { updated: changed, ownJid: this.ownJid },
                  "Backfilled legacy 'me' sender entries to real JID",
                );
              }
            }
          }
          this.recovery.markConnectionOpen();
          log.info("WhatsApp connection opened.");
          this.sync.scheduleWarmup(() => this.forceResync());
        }

        if (connection === "close") {
          this.isReadyFlag = false;
          this.isAuthenticatedFlag = false;
          const statusCode = lastDisconnect?.error?.output?.statusCode;
          const reason = lastDisconnect?.error?.message;
          const reasonText = reason ? String(reason) : "";
          this.recovery.markDisconnect(
            reasonText ||
              (typeof statusCode === "number" ? String(statusCode) : null),
          );
          if (
            statusCode === DisconnectReason.connectionReplaced ||
            reasonText.toLowerCase().includes("conflict") ||
            reasonText.toLowerCase().includes("replaced")
          ) {
            this.recovery.markSessionReplaced();
          }
          log.warn({ statusCode, reason }, "WhatsApp connection closed.");
          this.recovery.scheduleDisconnectRecovery(statusCode, reasonText);
          if (statusCode === DisconnectReason.loggedOut) {
            log.warn("WhatsApp logged out. Clearing session.");
            try {
              fs.rmSync(this.sessionDir, { recursive: true, force: true });
            } catch (_error) {
              // Ignore
            }
            this.recovery
              .reconnect()
              .catch((err) =>
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
            this.recovery
              .reconnect()
              .catch((err) =>
                log.error({ err }, "Failed to reconnect WhatsApp"),
              );
          } else {
            this.recovery
              .reconnect()
              .catch((err) =>
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
          this.storage.upsertMessage(msg);
          const jid = msg?.key?.remoteJid;
          if (jid) {
            this.storage.upsertChat({
              id: jid,
              conversationTimestamp: msg?.messageTimestamp,
            });
          }
          this.autoDownload.maybeProcess(msg);
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
          const merged = key
            ? this.messageIndexStore.updateById(key, (existing) => ({
                ...existing,
                ...update.update,
                message: {
                  ...(existing.message || {}),
                  ...(update.update?.message || {}),
                },
              }))
            : undefined;
          if (merged) {
            if (update.update?.message) {
              this.storage.updateMessageContent(merged);
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
            this.storage.updateMessageContent(synthetic);
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
          const msgId = MessageIndexStore.serializeKeyId(key);
          this.messageIndexStore.updateById(msgId, (existing) => ({
            ...existing,
            media: {
              ...(existing.media || {}),
              ...(update.media || {}),
            },
          }));
        }
      });

      sock.ev.on("messages.reaction", (updates: any[]) => {
        this.logEvent("messages.reaction", {
          count: Array.isArray(updates) ? updates.length : 0,
        });
        if (!updates) return;
        for (const update of updates) {
          const key = update?.key;
          const msgId = MessageIndexStore.serializeKeyId(key);
          const reaction = update?.reaction;
          this.messageIndexStore.updateById(msgId, (existing) => ({
            ...existing,
            reactions: reaction
              ? [...(existing.reactions || []), reaction]
              : existing.reactions,
          }));
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
          const msgId = MessageIndexStore.serializeKeyId(key);
          const receipts = update?.receipt;
          this.messageIndexStore.updateById(msgId, (existing) => ({
            ...existing,
            receipts: receipts
              ? [...(existing.receipts || []), receipts]
              : existing.receipts,
          }));
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
          this.messageIndexStore.deleteByChat(payload.jid);
          return;
        }
        const keys = payload.keys || [];
        for (const key of keys) {
          const jid = key?.remoteJid;
          const id = key?.id;
          if (!jid || !id) continue;
          const msgId = `${jid}:${id}`;
          this.messageIndexStore.deleteByKey(key);
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
          count: Array.isArray(payload?.messages) ? payload.messages.length : 0,
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
            this.storage.upsertContact(contact);
            this.jidResolver.storeLidMappingFromContact(contact);
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
            this.storage.upsertContact(contact);
            this.jidResolver.storeLidMappingFromContact(contact);
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
            this.jidResolver.storeLidMapping(
              String(lid),
              JidResolver.isPnJid(String(pn))
                ? String(pn)
                : `${pn}@s.whatsapp.net`,
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
            this.storage.upsertContact(contact);
            this.jidResolver.storeLidMappingFromContact(contact);
          }
        }
      });
    })().finally(() => {
      this.initializing = null;
      log.info("Initialize flow settled");
    });

    await this.waitForInitializing();
  }

  async initialize(): Promise<void> {
    await this.withLifecycleLock(async () => {
      await this.initializeWithinLifecycleLock();
    });
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
      await this.initializeWithinLifecycleLock();
      this.recovery.ensureReconnectAfterResync();
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
      await this.initializeWithinLifecycleLock();
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

  isAuthenticated(): boolean {
    return this.isAuthenticatedFlag;
  }

  isReady(): boolean {
    return this.isReadyFlag;
  }

  getLatestQrCode(): string | null {
    return this.latestQrCode;
  }

  getOwnJid(): string | null {
    return this.ownJid;
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
    const messageCount = this.messageIndexStore.messageCount;
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
    const stats = this.recovery.getStats();
    const graceMs = this.recovery.readinessGraceMs;
    const recoveringRecently =
      stats.syncRecoveryInProgress ||
      (stats.lastRecoveryAt !== null && now - stats.lastRecoveryAt <= graceMs);
    const disconnectedRecently =
      stats.lastDisconnectAt !== null &&
      now - stats.lastDisconnectAt <= graceMs;

    const base = {
      ready: this.isReadyFlag,
      authenticated: this.isAuthenticatedFlag,
      chatCount,
      lastDisconnectAt: stats.lastDisconnectAt,
      lastRecoveryAt: stats.lastRecoveryAt,
      syncRecoveryInProgress: stats.syncRecoveryInProgress,
    };

    if (this.isReadyFlag) {
      return { ok: true, reason: "ready", ...base };
    }
    if (recoveringRecently) {
      return { ok: true, reason: "recovering", ...base };
    }
    if (!this.isAuthenticatedFlag && this.latestQrCode) {
      return { ok: true, reason: "awaiting-qr", ...base };
    }
    if (disconnectedRecently) {
      return { ok: true, reason: "recent-disconnect", ...base };
    }
    return { ok: false, reason: "not-ready", ...base };
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
    log.info("Destroying WhatsApp client internals");
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
    log.info("Finished destroying WhatsApp client internals");
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
    const stats = this.recovery.getStats();
    return {
      sessionReplaced: stats.sessionReplaced,
      lastDisconnectReason: stats.lastDisconnectReason,
      lastDisconnectAt: stats.lastDisconnectAt,
      lastRecoveryReason: stats.lastRecoveryReason,
      lastRecoveryAt: stats.lastRecoveryAt,
      syncRecoveryAttempts: stats.syncRecoveryAttempts,
      syncRecoveryInProgress: stats.syncRecoveryInProgress,
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

  getChatsPage(
    limit: number,
    offset: number,
    search: string,
  ): { chats: AdminChat[]; total: number } {
    if (!this.storeService) return { chats: [], total: 0 };
    const chats = this.storeService.listChatsForAdmin(limit, offset, search);
    const resolved = chats.map((chat) => {
      const isUnresolved =
        chat.display_name === chat.id ||
        chat.display_name.endsWith("@lid") ||
        chat.display_name.endsWith("@s.whatsapp.net");
      if (!isUnresolved) return chat;
      const related = this.jidResolver.getRelatedJids(chat.id);
      const name = this.contactPresenter.getBestChatName(
        related,
        chat.id,
        null,
      );
      return name ? { ...chat, display_name: name } : chat;
    });
    return {
      chats: resolved,
      total: this.storeService.countChatsForAdmin(search),
    };
  }

  getMessagesPage(
    jid: string,
    limit: number,
    offset: number,
  ): { messages: StoredMessage[]; total: number } {
    if (!this.storeService) return { messages: [], total: 0 };
    return {
      messages: this.storeService.listMessagesPage(jid, limit, offset),
      total: this.storeService.countMessages(jid),
    };
  }

  getAutoDownloadStats(): {
    enabled: boolean;
    inFlight: number;
    queued: number;
  } {
    return this.autoDownload.getStats();
  }

  getRecoveryStats(): ReturnType<RecoveryManager["getStats"]> {
    return this.recovery.getStats();
  }

  getMessageIndexStats(): { messageCount: number; chatCount: number } {
    return {
      messageCount: this.messageIndexStore.messageCount,
      chatCount: this.messageIndexStore.chatCount,
    };
  }

  private getChatCount(): number {
    if (this.storeService) {
      const stats = this.storeService.stats();
      if (stats) return stats.chats;
    }
    return this.messageIndexStore.chatCount;
  }

  async runWarmup(): Promise<{ chatCount: number; messageCount: number }> {
    return this.sync.runWarmup(
      () => this.messageIndexStore.messageCount,
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
          const canonicalId = this.jidResolver.resolveCanonicalChatId(chat.id);
          const related = this.jidResolver.getRelatedJids(chat.id);
          const lastMessage = includeLastMessage
            ? this.getLastMessageForChat(chat.id)
            : undefined;
          const fallbackName = this.contactPresenter.getBestChatName(
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
    const chatIds = this.messageIndexStore.chatJids();
    if (chatIds.length > 0) {
      const merged = new Map<string, SimpleChat>();
      for (const jid of chatIds) {
        if (jid === "status@broadcast") {
          continue;
        }
        const canonicalId = this.jidResolver.resolveCanonicalChatId(jid);
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
      const related = this.jidResolver.getRelatedJids(jid);
      const merged = new Map<string, SimpleChat>();
      for (const entry of related) {
        const stored = this.storeService.getChatById(entry);
        if (!stored) continue;
        const canonicalId = this.jidResolver.resolveCanonicalChatId(stored.id);
        const name = this.contactPresenter.getBestChatName(
          related,
          canonicalId,
          stored.name,
        );
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
    const related = this.jidResolver.getRelatedJids(jid);
    const perLimit = related.length > 1 ? Math.max(limit * 2, 100) : limit;
    const fromMemory = related.flatMap((entry) =>
      (this.messageIndexStore.listByChat(entry) || []).map((msg) =>
        mapMessage(msg, MessageIndexStore.serializeMessageId),
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
    const normalized = this.jidResolver.resolveLookupJid(jid);
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
        pushIfMatch(mapMessage(msg, MessageIndexStore.serializeMessageId));
      }
    };

    if (chatId) {
      const normalized = this.jidResolver.resolveLookupJid(chatId);
      const list = this.messageIndexStore.listByChat(normalized) || [];
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
      const all = Array.from(this.messageIndexStore.messages());
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
    const msg = this.messageIndexStore.getById(messageId);
    if (msg) {
      return mapMessage(msg, MessageIndexStore.serializeMessageId);
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
      const normalized = this.jidResolver.resolveLookupJid(jid);
      const url = await socket.profilePictureUrl(normalized, "image");
      return url || null;
    } catch (_error) {
      return null;
    }
  }

  async getGroupInfo(groupJid: string): Promise<any> {
    const socket = this.getSocket();
    const normalized = this.jidResolver.resolveLookupJid(groupJid);
    try {
      const metadata = await socket.groupMetadata(normalized);
      this.storage.persistGroupMetadata(metadata);
      const participants = Array.isArray(metadata?.participants)
        ? metadata.participants.map((p: any) => {
            const jid = JidResolver.normalizeJid(p?.id || "");
            if (p?.id && (p?.pn || p?.pnJid || p?.phoneNumber)) {
              const pn = p?.pn || p?.pnJid || p?.phoneNumber;
              this.jidResolver.storeLidMappingFromPair(
                String(p.id),
                String(pn),
              );
            }
            const contact = jid
              ? this.contactPresenter.getContactSummary(jid)
              : null;
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
    const normalized = this.jidResolver.resolveLookupJid(jid);
    const isGroup = normalized.endsWith("@g.us");
    const dedupKey = this.idempotency.buildSendDedupKey(normalized, message);
    const idempotencyKey = options?.idempotencyKey?.trim() || null;
    const requestFingerprint = this.idempotency.buildRequestFingerprint(
      normalized,
      message,
    );
    if (idempotencyKey) {
      const stored = this.idempotency.getStoredIdempotentResult(
        "send_message",
        idempotencyKey,
        requestFingerprint,
      );
      if (stored) {
        log.warn(
          {
            jid: normalized,
            idempotencyKey,
            messageId: stored.__originalMessageId,
          },
          "Returned stored idempotent WhatsApp send result",
        );
        return stored;
      }
    }
    const duplicate = this.idempotency.getRecentSendResult(normalized, message);
    if (duplicate) {
      log.warn(
        { jid: normalized, messageId: duplicate.__originalMessageId },
        "Suppressed duplicate WhatsApp send request",
      );
      return duplicate;
    }
    const operationKey = idempotencyKey || dedupKey;
    try {
      const result = await this.idempotency.executeSendWithDedup(
        operationKey,
        normalized,
        message,
        () => socket.sendMessage(normalized, { text: message }),
      );
      if (idempotencyKey) {
        this.idempotency.persistIdempotentResult(
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
          const result = await this.idempotency.executeSendWithDedup(
            operationKey,
            normalized,
            message,
            () => socket.sendMessage(normalized, { text: message }),
          );
          if (idempotencyKey) {
            this.idempotency.persistIdempotentResult(
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
      } else {
        try {
          const urlPathname = new URL(input).pathname;
          const urlBase = path.basename(urlPathname);
          if (urlBase && urlBase.includes(".")) filename = urlBase;
        } catch {
          // ignore URL parse errors
        }
      }
    } else {
      buffer = fs.readFileSync(input);
      filename = path.basename(input);
      const detected = await fileTypeFromBuffer(buffer);
      if (detected) {
        mimetype = detected.mime;
      } else {
        const extMimeMap: Record<string, string> = {
          pdf: "application/pdf",
          docx: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
          xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
          pptx: "application/vnd.openxmlformats-officedocument.presentationml.presentation",
          doc: "application/msword",
          xls: "application/vnd.ms-excel",
          ppt: "application/vnd.ms-powerpoint",
          txt: "text/plain",
          csv: "text/csv",
          zip: "application/zip",
          gz: "application/gzip",
        };
        const ext = path.extname(input).toLowerCase().replace(".", "");
        mimetype = extMimeMap[ext] || "application/octet-stream";
      }
    }

    const content = await this.buildMediaMessage(
      buffer,
      mimetype,
      filename,
      caption,
      asAudioMessage,
    );
    const normalized = this.jidResolver.resolveLookupJid(jid);
    const send = () => this.getSocket().sendMessage(normalized, content);
    if (options?.idempotencyKey) {
      return await this.executeIdempotentOperation(
        "send_media",
        options.requestFingerprint ||
          this.idempotency.buildRequestFingerprint(
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
    const normalized = this.jidResolver.resolveLookupJid(jid);
    const send = () => this.getSocket().sendMessage(normalized, content);
    if (options?.idempotencyKey) {
      return await this.executeIdempotentOperation(
        "send_media",
        options.requestFingerprint ||
          this.idempotency.buildRequestFingerprint(
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
    let msg = this.messageIndexStore.getById(messageId);
    if (!msg?.message) {
      const parts = messageId.split(":");
      if (parts.length >= 2) {
        const jid = parts[0];
        const keyId = parts.slice(1).join(":");
        const list = this.messageIndexStore.listByChat(jid) || [];
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
      return this.contactPresenter.mergeContacts(stored);
    }

    return [];
  }

  async resolveContacts(query: string, limit = 5): Promise<ResolvedContact[]> {
    const text = query.trim().toLowerCase();
    if (!text) return [];

    const digits = text.replace(/[^\d]/g, "");
    const hasDigits = digits.length >= 6;

    const contacts = this.storeService
      ? this.contactPresenter.mergeContacts(this.storeService.listContacts(200))
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
      if (!contact && JidResolver.isPnJid(jid)) {
        const lid = this.storeService.getLidForPn(jid);
        if (lid) {
          contact = this.storeService.getContactById(lid);
        }
      }
      if (!contact && JidResolver.isLidJid(jid)) {
        const mapped = this.storeService.getPnForLid(jid);
        if (mapped?.pnJid) {
          contact = this.storeService.getContactById(mapped.pnJid);
        }
      }
      if (!contact) return null;
      const canonicalId = this.jidResolver.resolveCanonicalChatId(contact.jid);
      const mapped = this.contactPresenter.buildSimpleContact(
        contact,
        canonicalId,
      );
      if (!mapped.number && JidResolver.isLidJid(contact.jid)) {
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
    const canonicalId = this.jidResolver.resolveCanonicalChatId(jid);
    if (this.storeService) {
      const related = this.jidResolver.getRelatedJids(jid);
      for (const entry of related) {
        const stored = this.storeService.listMessages(entry, 1);
        if (stored.length > 0) {
          candidates.push(
            this.normalizeSimpleMessageId(
              mapStoredMessage(stored[0]),
              canonicalId,
            ),
          );
        }
      }
    }
    const related = this.jidResolver.getRelatedJids(jid);
    for (const entry of related) {
      const list = this.messageIndexStore.listByChat(entry) || [];
      const last = list[list.length - 1];
      if (last) {
        candidates.push(
          this.normalizeSimpleMessageId(
            mapMessage(last, MessageIndexStore.serializeMessageId),
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
    if (this.jidResolver.resolveCanonicalChatId(rawChatId) !== canonicalChatId)
      return message;
    return {
      ...message,
      id: `${canonicalChatId}:${message.id.slice(idx + 1)}`,
      to: canonicalChatId,
    };
  }

  analyzeGroupOverlaps(
    groupLimit = 200,
    refreshGroupInfo = false,
    minSharedGroups = 2,
  ) {
    return this.groupAudit.analyzeOverlaps(
      groupLimit,
      refreshGroupInfo,
      minSharedGroups,
    );
  }

  findMembersWithoutDirectChat(
    groupLimit = 200,
    refreshGroupInfo = false,
    minSharedGroups = 1,
  ) {
    return this.groupAudit.findWithoutDirectChat(
      groupLimit,
      refreshGroupInfo,
      minSharedGroups,
    );
  }

  findMembersNotInContacts(
    groupLimit = 200,
    refreshGroupInfo = false,
    minSharedGroups = 1,
  ) {
    return this.groupAudit.findNotInContacts(
      groupLimit,
      refreshGroupInfo,
      minSharedGroups,
    );
  }

  runGroupAudit(
    groupLimit = 200,
    refreshGroupInfo = false,
    overlapMinSharedGroups = 2,
    minSharedGroups = 1,
  ) {
    return this.groupAudit.runAudit(
      groupLimit,
      refreshGroupInfo,
      overlapMinSharedGroups,
      minSharedGroups,
    );
  }
}
