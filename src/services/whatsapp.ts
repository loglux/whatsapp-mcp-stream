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
  StoredChat,
  StoredContact,
  StoredGroupMeta,
  StoredGroupParticipant,
  StoredMessage,
  StoredMedia,
} from "../storage/message-store.js";
import { StoreService } from "../providers/store/store-service.js";
import { BaileysClient } from "../providers/wa/baileys-client.js";
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

  constructor() {
    const baseDir =
      process.env.SESSION_DIR ||
      path.join(process.cwd(), "whatsapp-sessions", "baileys");
    this.sessionDir = baseDir;
    this.client = new BaileysClient(this.sessionDir);
    this.initStoreService();
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

  private initStoreService(): void {
    if (this.storeService) return;
    const dbPath =
      process.env.DB_PATH || path.join(this.sessionDir, "store.sqlite");
    this.storeService = new StoreService(dbPath);
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
    this.messageIndex.set(id, msg);
    const keyId = msg?.key?.id;
    if (keyId) {
      this.messageKeyIndex.set(keyId, msg);
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
              this.messageIndex.set(key, merged);
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
              this.messageIndex.set(msgId, merged);
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
              this.messageIndex.set(msgId, merged);
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
              this.messageIndex.set(msgId, merged);
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
  } {
    return {
      sessionReplaced: this.sessionReplaced,
      lastDisconnectReason: this.lastDisconnectReason,
      lastDisconnectAt: this.lastDisconnectAt,
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
  ): Promise<SimpleChat[]> {
    if (this.storeService) {
      const stored = this.storeService.listChats(limit);
      if (stored.length > 0) {
        const mapped = stored.map((chat) => ({
          id: chat.id,
          name: chat.name,
          isGroup: Boolean(chat.is_group),
          unreadCount: chat.unread_count || 0,
          timestamp: chat.timestamp || 0,
          lastMessage: includeLastMessage
            ? this.getLastMessageForChat(chat.id)
            : undefined,
        }));
        mapped.sort((a, b) => b.timestamp - a.timestamp);
        return mapped.slice(0, limit);
      }
    }

    await this.sync.warmup(() => this.forceResync());
    const chatIds = Array.from(this.messagesByChat.keys());
    if (chatIds.length > 0) {
      const mapped = chatIds.map((jid) => ({
        id: jid,
        name: jid,
        isGroup: jid.endsWith("@g.us"),
        unreadCount: 0,
        timestamp: 0,
        lastMessage: includeLastMessage
          ? this.getLastMessageForChat(jid)
          : undefined,
      }));
      return mapped.slice(0, limit);
    }

    const mapped = chatIds.map((jid) => ({
      id: jid,
      name: jid,
      isGroup: jid.endsWith("@g.us"),
      unreadCount: 0,
      timestamp: 0,
      lastMessage: includeLastMessage
        ? this.getLastMessageForChat(jid)
        : undefined,
    }));
    return mapped.slice(0, limit);
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
    const normalized = this.resolveLookupJid(jid);
    if (this.storeService) {
      const stored = this.storeService.getChatById(normalized);
      if (!stored) return null;
      return {
        id: stored.id,
        name: stored.name,
        isGroup: Boolean(stored.is_group),
        unreadCount: stored.unread_count || 0,
        timestamp: stored.timestamp || 0,
        lastMessage: this.getLastMessageForChat(stored.id),
      };
    }

    return null;
  }

  async getMessages(jid: string, limit = 50): Promise<SimpleMessage[]> {
    const normalized = this.resolveLookupJid(jid);
    const fromMemory = (this.messagesByChat.get(normalized) || []).map((msg) =>
      mapMessage(msg, this.serializeMessageId.bind(this)),
    );
    const fromDb = this.storeService
      ? this.storeService
          .listMessages(normalized, limit)
          .map((msg) => mapStoredMessage(msg))
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
    if (!msg) return null;
    return mapMessage(msg, this.serializeMessageId.bind(this));
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
    const socket = this.getSocket();
    const normalized = this.normalizeJid(jid);
    const isGroup = normalized.endsWith("@g.us");
    try {
      return await socket.sendMessage(normalized, { text: message });
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
          return await socket.sendMessage(normalized, { text: message });
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
    return await this.getSocket().sendMessage(jid, content);
  }

  async sendMediaFromBase64(
    jid: string,
    base64: string,
    mimeType: string,
    filename?: string,
    caption?: string,
    asAudioMessage = false,
  ): Promise<any> {
    const buffer = Buffer.from(base64, "base64");
    const content = await this.buildMediaMessage(
      buffer,
      mimeType,
      filename,
      caption,
      asAudioMessage,
    );
    return await this.getSocket().sendMessage(jid, content);
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
      return stored.map((contact) => ({
        id: contact.jid,
        name: contact.name,
        pushname: contact.pushname,
        isMe: false,
        isUser: true,
        isGroup: Boolean(contact.is_group),
        isWAContact: true,
        isMyContact: Boolean(contact.is_my_contact),
        number: contact.number || "",
      }));
    }

    return [];
  }

  async resolveContacts(query: string, limit = 5): Promise<ResolvedContact[]> {
    const text = query.trim().toLowerCase();
    if (!text) return [];

    const digits = text.replace(/[^\d]/g, "");
    const hasDigits = digits.length >= 6;

    const contacts = this.storeService
      ? this.storeService.listContacts(200).map((contact) => ({
          id: contact.jid,
          name: contact.name,
          notify: contact.pushname,
        }))
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
      return {
        id: contact.jid,
        name: contact.name,
        pushname: contact.pushname,
        isMe: false,
        isUser: true,
        isGroup: Boolean(contact.is_group),
        isWAContact: true,
        isMyContact: Boolean(contact.is_my_contact),
        number: contact.number || "",
      };
    }

    return null;
  }

  private getLastMessageForChat(jid: string): SimpleMessage | undefined {
    if (this.storeService) {
      const resolved = this.resolveLookupJid(jid);
      const stored = this.storeService.listMessages(resolved, 1);
      if (stored.length > 0) {
        return mapStoredMessage(stored[0]);
      }
    }
    const list = this.messagesByChat.get(jid) || [];
    const last = list[list.length - 1];
    if (last) {
      return mapMessage(last, this.serializeMessageId.bind(this));
    }
    return undefined;
  }
}
