import { createRequire } from "module";
import { webcrypto } from "crypto";
import fs from "fs";
import path from "path";
import axios from "axios";
import { fileTypeFromBuffer } from "file-type";
import pino from "pino";
import { log } from "../utils/logger.js";
import {
  MessageStore,
  StoredChat,
  StoredMessage,
  StoredMedia,
} from "../storage/message-store.js";

const require = createRequire(import.meta.url);
const baileys = require("@whiskeysockets/baileys");

if (!(globalThis as any).crypto) {
  (globalThis as any).crypto = webcrypto;
}

const makeWASocket = baileys.default;
const {
  DisconnectReason,
  fetchLatestBaileysVersion,
  useMultiFileAuthState,
  makeInMemoryStore,
  jidNormalizedUser,
  downloadContentFromMessage,
  ALL_WA_PATCH_NAMES,
} = baileys;

export interface SimpleContact {
  id: string;
  name: string | null;
  pushname: string | null;
  isMe: boolean;
  isUser: boolean;
  isGroup: boolean;
  isWAContact: boolean;
  isMyContact: boolean;
  number: string;
}

export interface ResolvedContact extends SimpleContact {
  matchedBy: "name" | "pushname" | "number" | "id";
  score: number;
}

export interface SimpleChat {
  id: string;
  name: string;
  isGroup: boolean;
  lastMessage?: SimpleMessage;
  unreadCount: number;
  timestamp: number;
}

export interface SimpleMessage {
  id: string;
  body: string;
  from: string;
  to: string;
  timestamp: number;
  fromMe: boolean;
  hasMedia: boolean;
  mediaKey?: string;
  type: string;
}

export interface DownloadedMedia {
  data: Buffer;
  mimetype: string;
  filename?: string;
  filesize?: number;
}

export class WhatsAppService {
  private sock: any | null = null;
  private store: any | null = null;
  private storePath: string | null = null;
  private storeFlushTimer: NodeJS.Timeout | null = null;
  private lastHistorySyncAt: number | null = null;
  private lastChatsSyncAt: number | null = null;
  private lastMessagesSyncAt: number | null = null;
  private warmupTimer: NodeJS.Timeout | null = null;
  private warmupAttempts = 0;
  private forcedResync = false;
  private latestQrCode: string | null = null;
  private isAuthenticatedFlag = false;
  private isReadyFlag = false;
  private initializing: Promise<void> | null = null;
  private reconnecting = false;
  private messageIndex = new Map<string, any>();
  private messagesByChat = new Map<string, any[]>();
  private messageKeyIndex = new Map<string, any>();
  private sessionDir: string;
  private messageStore: MessageStore | null = null;

  constructor() {
    const baseDir =
      process.env.SESSION_DIR ||
      path.join(process.cwd(), "whatsapp-sessions", "baileys");
    this.sessionDir = baseDir;
    this.initMessageStore();
  }

  private initMessageStore(): void {
    if (this.messageStore) return;
    const dbPath =
      process.env.DB_PATH || path.join(this.sessionDir, "store.sqlite");
    try {
      this.messageStore = new MessageStore(dbPath);
      log.info({ dbPath }, "Message store initialized");
    } catch (error) {
      log.warn({ err: error }, "Failed to initialize message store");
      this.messageStore = null;
    }
  }

  private normalizeJid(jid: string): string {
    if (!jid) return jid;
    if (typeof jidNormalizedUser === "function") {
      return jidNormalizedUser(jid);
    }
    return jid;
  }

  private serializeMessageId(msg: any): string {
    const jid = msg?.key?.remoteJid || "unknown";
    const id = msg?.key?.id || "unknown";
    return `${jid}:${id}`;
  }

  private extractText(message: any): string {
    if (!message) return "";
    if (message.conversation) return message.conversation;
    if (message.extendedTextMessage?.text)
      return message.extendedTextMessage.text;
    if (message.imageMessage?.caption) return message.imageMessage.caption;
    if (message.videoMessage?.caption) return message.videoMessage.caption;
    if (message.documentMessage?.caption)
      return message.documentMessage.caption;
    if (message.buttonsResponseMessage?.selectedDisplayText)
      return message.buttonsResponseMessage.selectedDisplayText;
    if (message.listResponseMessage?.title)
      return message.listResponseMessage.title;
    if (message.templateButtonReplyMessage?.selectedDisplayText)
      return message.templateButtonReplyMessage.selectedDisplayText;
    if (message.reactionMessage?.text)
      return `reacted: ${message.reactionMessage.text}`;
    return "";
  }

  private mapMessage(msg: any): SimpleMessage {
    const id = this.serializeMessageId(msg);
    const jid = msg?.key?.remoteJid || "";
    const fromMe = Boolean(msg?.key?.fromMe);
    const timestamp = Number(msg?.messageTimestamp || 0) * 1000;
    const message = msg?.message || {};
    const body = this.extractText(message);
    const hasMedia = Boolean(
      message.imageMessage ||
        message.videoMessage ||
        message.audioMessage ||
        message.documentMessage ||
        message.stickerMessage,
    );

    return {
      id,
      body,
      from: fromMe ? "me" : msg?.key?.participant || jid,
      to: jid,
      timestamp,
      fromMe,
      hasMedia,
      type: Object.keys(message)[0] || "unknown",
    };
  }

  private mapStoredMessage(msg: StoredMessage): SimpleMessage {
    return {
      id: msg.id,
      body: msg.body,
      from: msg.from,
      to: msg.to,
      timestamp: msg.timestamp,
      fromMe: Boolean(msg.from_me),
      hasMedia: Boolean(msg.has_media),
      type: msg.type || "unknown",
    };
  }

  private upsertStoredMessage(msg: any): void {
    if (!this.messageStore) return;
    const mapped = this.mapMessage(msg);
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
    this.messageStore.upsertMessage(record);
  }

  private upsertStoredChat(chat: any): void {
    if (!this.messageStore) return;
    const id = chat?.id || chat?.jid;
    if (!id) return;
    const record: StoredChat = {
      id,
      name: chat?.name || chat?.subject || id,
      is_group: String(id).endsWith("@g.us") ? 1 : 0,
      unread_count: chat?.unreadCount || 0,
      timestamp: Number(chat?.conversationTimestamp || 0) * 1000,
    };
    this.messageStore.upsertChat(record);
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
    if (this.initializing) {
      await this.initializing;
      return;
    }

    this.initMessageStore();

    this.initializing = (async () => {
      fs.mkdirSync(this.sessionDir, { recursive: true });
      const { state, saveCreds } = await useMultiFileAuthState(this.sessionDir);

      const { version } = await fetchLatestBaileysVersion();
      const logger = (pino as unknown as (opts: any) => any)({
        level: "silent",
      });

      if (typeof makeInMemoryStore === "function") {
        this.store = makeInMemoryStore({ logger });
        this.storePath =
          process.env.STORE_PATH || path.join(this.sessionDir, "store.json");
        try {
          if (
            this.storePath &&
            fs.existsSync(this.storePath) &&
            this.store?.readFromFile
          ) {
            this.store.readFromFile(this.storePath);
            log.info(
              { storePath: this.storePath },
              "Loaded WhatsApp store from disk",
            );
          }
        } catch (error) {
          log.warn({ err: error }, "Failed to load WhatsApp store from disk");
        }
        if (this.store?.writeToFile) {
          this.storeFlushTimer = setInterval(() => {
            try {
              if (this.storePath) {
                this.store.writeToFile(this.storePath);
              }
            } catch (error) {
              log.warn({ err: error }, "Failed to persist WhatsApp store");
            }
          }, 10000);
        }
      } else {
        this.store = null;
      }

      this.sock = makeWASocket({
        version,
        auth: state,
        printQRInTerminal: false,
        logger,
        syncFullHistory: true,
        browser: ["MCP", "Desktop", "1.0.0"],
        getMessage: async (key: any) => {
          const cached = key?.id ? this.messageKeyIndex.get(key.id) : undefined;
          return cached?.message;
        },
      });

      if (this.store?.bind) {
        this.store.bind(this.sock.ev);
      }

      this.sock.ev.on("creds.update", saveCreds);

      this.sock.ev.on("connection.update", (update: any) => {
        const { connection, lastDisconnect, qr } = update;

        if (connection) {
          log.info({ connection }, "WhatsApp connection update");
        }

        if (qr) {
          this.latestQrCode = qr;
          log.info("QR code received.");
        }

        if (connection === "open") {
          this.isAuthenticatedFlag = true;
          this.isReadyFlag = true;
          this.latestQrCode = null;
          log.info("WhatsApp connection opened.");
          this.scheduleWarmup();
        }

        if (connection === "close") {
          this.isReadyFlag = false;
          this.isAuthenticatedFlag = false;
          const statusCode = lastDisconnect?.error?.output?.statusCode;
          const reason = lastDisconnect?.error?.message;
          log.warn({ statusCode, reason }, "WhatsApp connection closed.");
          this.sock = null;
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

      this.sock.ev.on("messages.upsert", (ev: any) => {
        if (!ev?.messages) return;
        for (const msg of ev.messages) {
          this.trackMessage(msg);
          this.upsertStoredMessage(msg);
        }
      });

      this.sock.ev.on("messages.update", (updates: any[]) => {
        if (!updates) return;
        for (const update of updates) {
          const key =
            update?.key?.remoteJid && update?.key?.id
              ? `${update.key.remoteJid}:${update.key.id}`
              : null;
          if (key && this.messageIndex.has(key)) {
            const existing = this.messageIndex.get(key);
            this.messageIndex.set(key, { ...existing, ...update });
          }
        }
      });

      this.sock.ev.on("chats.set", (payload: any) => {
        if (payload?.chats && Array.isArray(payload.chats)) {
          const validChats = payload.chats
            .map((chat: any) => this.normalizeChatRecord(chat))
            .filter((chat: any) => chat?.id);
          if (validChats.length && this.store?.chats?.insert) {
            try {
              this.store.chats.insert(validChats);
            } catch (error) {
              log.warn({ err: error }, "Failed to insert chats into store");
            }
          }
          for (const chat of validChats) {
            this.upsertStoredChat(chat);
          }
          this.lastChatsSyncAt = Date.now();
          log.info({ count: validChats.length }, "Chats synced");
        }
      });

      this.sock.ev.on("messages.set", (payload: any) => {
        if (payload?.messages && Array.isArray(payload.messages)) {
          for (const msg of payload.messages) {
            this.trackMessage(msg);
            this.upsertStoredMessage(msg);
          }
        }
        if (payload?.messages && Array.isArray(payload.messages)) {
          this.lastMessagesSyncAt = Date.now();
        }
        if (payload?.messages && Array.isArray(payload.messages)) {
          log.info({ count: payload.messages.length }, "Messages synced");
        }
      });

      this.sock.ev.on("chats.upsert", (payload: any) => {
        if (payload && Array.isArray(payload)) {
          for (const chat of payload) {
            this.upsertStoredChat(chat);
          }
          this.lastChatsSyncAt = Date.now();
          log.info({ count: payload.length }, "Chats upsert");
        }
      });

      this.sock.ev.on("chats.update", (payload: any) => {
        if (payload && Array.isArray(payload)) {
          for (const chat of payload) {
            this.upsertStoredChat(chat);
          }
          this.lastChatsSyncAt = Date.now();
          log.info({ count: payload.length }, "Chats update");
        }
      });

      this.sock.ev.on("contacts.upsert", (payload: any) => {
        if (payload && Array.isArray(payload)) {
          log.info({ count: payload.length }, "Contacts upsert");
        }
      });

      this.sock.ev.on("contacts.update", (payload: any) => {
        if (payload && Array.isArray(payload)) {
          log.info({ count: payload.length }, "Contacts update");
        }
      });

      this.sock.ev.on("messaging-history.set", (payload: any) => {
        const { chats, contacts, messages } = payload || {};
        if (chats && Array.isArray(chats)) {
          const validChats = chats
            .map((chat: any) => this.normalizeChatRecord(chat))
            .filter((chat: any) => chat?.id);
          if (validChats.length && this.store?.chats?.insert) {
            try {
              this.store.chats.insert(validChats);
            } catch (error) {
              log.warn(
                { err: error },
                "Failed to insert history chats into store",
              );
            }
          }
          for (const chat of validChats) {
            this.upsertStoredChat(chat);
          }
        }
        if (contacts && this.store?.contacts) {
          for (const contact of contacts) {
            if (contact?.id) {
              this.store.contacts[contact.id] = contact;
            }
          }
        }
        if (messages && Array.isArray(messages)) {
          for (const msg of messages) {
            this.trackMessage(msg);
            this.upsertStoredMessage(msg);
          }
        }
        this.lastHistorySyncAt = Date.now();
        log.info(
          {
            chats: Array.isArray(chats) ? chats.length : 0,
            contacts: Array.isArray(contacts) ? contacts.length : 0,
            messages: Array.isArray(messages) ? messages.length : 0,
          },
          "Messaging history synced",
        );
      });
    })().finally(() => {
      this.initializing = null;
    });

    await this.initializing;
  }

  private async reconnect(): Promise<void> {
    if (this.reconnecting) {
      return;
    }
    this.reconnecting = true;
    try {
      await new Promise((resolve) => setTimeout(resolve, 1000));
      await this.initialize();
    } catch (error) {
      log.error({ err: error }, "Reconnect failed");
    } finally {
      this.reconnecting = false;
    }
  }

  async forceResync(): Promise<void> {
    this.forcedResync = true;
    if (this.sock?.authState?.keys?.set) {
      const resetMap: Record<string, null> = {};
      for (const name of ALL_WA_PATCH_NAMES) {
        resetMap[name] = null;
      }
      await this.sock.authState.keys.set({
        "app-state-sync-version": resetMap,
      });
      log.info("Force resync: reset app state versions");
    }
    if (this.sock?.authState?.creds) {
      this.sock.authState.creds.accountSyncCounter = 0;
      this.sock.ev.emit("creds.update", { accountSyncCounter: 0 });
      log.info("Force resync: reset account sync counter");
    }
    await this.destroy();
    await this.initialize();
  }

  async destroy(): Promise<void> {
    if (!this.sock) return;
    try {
      this.sock.end(new Error("Client destroyed"));
    } catch (_error) {
      // Ignore
    }
    if (this.storeFlushTimer) {
      clearInterval(this.storeFlushTimer);
      this.storeFlushTimer = null;
    }
    if (this.warmupTimer) {
      clearInterval(this.warmupTimer);
      this.warmupTimer = null;
    }
    this.sock = null;
    this.isAuthenticatedFlag = false;
    this.isReadyFlag = false;
    this.latestQrCode = null;
  }

  async logout(): Promise<void> {
    await this.destroy();
    try {
      fs.rmSync(this.sessionDir, { recursive: true, force: true });
    } catch (_error) {
      // Ignore
    }
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
    const chatCount = this.store?.chats?.all
      ? this.store.chats.all().length
      : this.messagesByChat.size;
    const messageCount = this.messageIndex.size;
    return {
      chatCount,
      messageCount,
      lastHistorySyncAt: this.lastHistorySyncAt,
      lastChatsSyncAt: this.lastChatsSyncAt,
      lastMessagesSyncAt: this.lastMessagesSyncAt,
      warmupAttempts: this.warmupAttempts,
      warmupInProgress: Boolean(this.warmupTimer),
    };
  }

  getMessageStoreStats(): {
    chats: number;
    messages: number;
    media: number;
  } | null {
    if (!this.messageStore) return null;
    return this.messageStore.stats();
  }

  private scheduleWarmup(): void {
    if (this.warmupTimer) return;
    this.warmupAttempts = 0;
    this.warmupTimer = setInterval(() => {
      this.warmupAttempts += 1;
      const chatCount = this.store?.chats?.all
        ? this.store.chats.all().length
        : 0;
      log.info({ attempt: this.warmupAttempts, chatCount }, "Warmup tick");
      if (chatCount > 0 || this.warmupAttempts > 5) {
        if (this.warmupTimer) {
          clearInterval(this.warmupTimer);
          this.warmupTimer = null;
        }
        return;
      }
      this.warmup().catch((err) => log.warn({ err }, "Warmup failed"));
    }, 10000);
  }

  private async warmup(): Promise<void> {
    if (!this.sock) return;
    try {
      log.info("Warmup started");
      const initialChats = this.store?.chats?.all ? this.store.chats.all() : [];
      if (
        (!initialChats || initialChats.length === 0) &&
        typeof this.sock.resyncAppState === "function"
      ) {
        try {
          if (this.sock.authState?.keys?.set) {
            const resetMap: Record<string, null> = {};
            for (const name of ALL_WA_PATCH_NAMES) {
              resetMap[name] = null;
            }
            await this.sock.authState.keys.set({
              "app-state-sync-version": resetMap,
            });
            log.info("Warmup reset app state versions");
          }
          if (this.sock.authState?.creds) {
            this.sock.authState.creds.accountSyncCounter = 0;
            this.sock.ev.emit("creds.update", { accountSyncCounter: 0 });
            log.info("Warmup reset account sync counter");
          }
          await this.sock.resyncAppState(ALL_WA_PATCH_NAMES, true);
          log.info("Warmup requested app state resync");
        } catch (error) {
          log.warn({ err: error }, "Warmup app state resync failed");
        }
      }

      const chats = this.store?.chats?.all ? this.store.chats.all() : [];
      if (chats?.length && this.store?.loadMessages) {
        const targets = chats
          .map((c: any) => c?.id || c?.jid)
          .filter((jid: string) => Boolean(jid))
          .slice(0, 10);
        for (const jid of targets) {
          const msgs = await this.store.loadMessages(jid, 20);
          if (msgs?.length) {
            msgs.forEach((msg: any) => this.trackMessage(msg));
          }
        }
      }
      const chatCount = chats?.length || 0;
      log.info(
        { chatCount, messageCount: this.messageIndex.size },
        "Warmup completed",
      );
      if (chatCount === 0 && !this.forcedResync && this.warmupAttempts >= 2) {
        log.warn("Warmup still empty, forcing resync");
        await this.forceResync();
      }
    } catch (error) {
      log.warn({ err: error }, "Warmup failed");
    }
  }

  async runWarmup(): Promise<{ chatCount: number; messageCount: number }> {
    await this.warmup();
    const chatCount = this.store?.chats?.all
      ? this.store.chats.all().length
      : 0;
    const messageCount = this.messageIndex.size;
    return { chatCount, messageCount };
  }

  getSocket(): any {
    if (!this.sock) {
      throw new Error("WhatsApp socket not initialized");
    }
    return this.sock;
  }

  async listChats(
    limit = 20,
    includeLastMessage = true,
  ): Promise<SimpleChat[]> {
    if (!this.store?.chats?.all) {
      const chatIds = Array.from(this.messagesByChat.keys());
      const chats = chatIds.map((jid) => ({ id: jid }));
      return chats.slice(0, limit).map((chat) => ({
        id: chat.id,
        name: chat.id,
        isGroup: chat.id.endsWith("@g.us"),
        unreadCount: 0,
        timestamp: 0,
      }));
    }

    const chats = this.store.chats.all();
    if ((!chats || chats.length === 0) && this.store?.loadChats) {
      try {
        const loaded = await this.store.loadChats();
        if (loaded?.length && this.store?.chats?.insert) {
          const valid = loaded
            .map((chat: any) => this.normalizeChatRecord(chat))
            .filter((chat: any) => chat?.id);
          if (valid.length) {
            try {
              this.store.chats.insert(valid);
            } catch (_error) {
              // Ignore
            }
          }
        }
      } catch (_error) {
        // Ignore
      }
    }
    const effectiveChats = this.store.chats.all();
    if (effectiveChats.length === 0) {
      await this.warmup();
    }
    const refreshedChats = this.store.chats.all();
    if (refreshedChats.length === 0 && this.messageStore) {
      const stored = this.messageStore.listChats(limit);
      return stored.map((chat) => ({
        id: chat.id,
        name: chat.name,
        isGroup: Boolean(chat.is_group),
        unreadCount: chat.unread_count || 0,
        timestamp: chat.timestamp || 0,
        lastMessage: includeLastMessage
          ? this.getLastMessageForChat(chat.id)
          : undefined,
      }));
    }
    const mapped = refreshedChats.map((chat: any) => {
      const id = chat?.id || chat?.jid;
      const name = chat?.name || chat?.subject || id;
      const lastMessage = includeLastMessage
        ? this.getLastMessageForChat(id)
        : undefined;
      return {
        id,
        name,
        isGroup: String(id).endsWith("@g.us"),
        unreadCount: chat?.unreadCount || 0,
        timestamp: Number(chat?.conversationTimestamp || 0) * 1000,
        lastMessage,
      } as SimpleChat;
    });

    mapped.sort((a: SimpleChat, b: SimpleChat) => b.timestamp - a.timestamp);
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
    const normalized = this.normalizeJid(jid);
    if (this.store?.chats?.get) {
      const chat = this.store.chats.get(normalized);
      if (!chat) return null;
      const lastMessage = this.getLastMessageForChat(normalized);
      return {
        id: normalized,
        name: chat?.name || chat?.subject || normalized,
        isGroup: normalized.endsWith("@g.us"),
        unreadCount: chat?.unreadCount || 0,
        timestamp: Number(chat?.conversationTimestamp || 0) * 1000,
        lastMessage,
      };
    }

    if (this.messageStore) {
      const stored = this.messageStore.getChatById(normalized);
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
    const normalized = this.normalizeJid(jid);
    if (this.store?.loadMessages) {
      const raw = await this.store.loadMessages(normalized, limit);
      raw.forEach((msg: any) => this.trackMessage(msg));
      if (raw.length > 0) {
        return raw.map((msg: any) => this.mapMessage(msg));
      }
    }
    const list = this.messagesByChat.get(normalized) || [];
    if (list.length > 0) {
      return list.slice(-limit).map((msg) => this.mapMessage(msg));
    }
    if (this.messageStore) {
      const stored = this.messageStore.listMessages(normalized, limit);
      return stored.map((msg) => this.mapStoredMessage(msg));
    }
    return [];
  }

  async exportChat(
    jid: string,
    includeMedia: boolean,
  ): Promise<{
    chat: SimpleChat | null;
    messages: SimpleMessage[];
    media: StoredMedia[];
  }> {
    if (!this.messageStore) {
      return { chat: null, messages: [], media: [] };
    }
    const chat = await this.getChatById(jid);
    const messages = this.messageStore
      .listMessagesAll(jid)
      .map((msg) => this.mapStoredMessage(msg));
    const media = includeMedia ? this.messageStore.listMediaByChat(jid) : [];
    return { chat, messages, media };
  }

  async searchMessages(
    query: string,
    limit = 20,
    chatId?: string,
  ): Promise<SimpleMessage[]> {
    const q = query.toLowerCase();
    const results: SimpleMessage[] = [];

    const searchList = (msgs: any[]) => {
      for (const msg of msgs) {
        const mapped = this.mapMessage(msg);
        if (mapped.body && mapped.body.toLowerCase().includes(q)) {
          results.push(mapped);
        }
      }
    };

    if (chatId) {
      const normalized = this.normalizeJid(chatId);
      if (this.store?.loadMessages) {
        const raw = await this.store.loadMessages(
          normalized,
          Math.max(50, limit * 5),
        );
        raw.forEach((msg: any) => this.trackMessage(msg));
        searchList(raw);
      } else {
        const list = this.messagesByChat.get(normalized) || [];
        searchList(list);
      }
    } else {
      const all = Array.from(this.messageIndex.values());
      searchList(all);
    }

    if (results.length === 0 && this.messageStore && !chatId) {
      const stored = this.messageStore.searchMessages(query, limit);
      return stored.map((msg) => this.mapStoredMessage(msg));
    }

    results.sort((a, b) => b.timestamp - a.timestamp);
    return results.slice(0, limit);
  }

  async getMessageById(messageId: string): Promise<SimpleMessage | null> {
    const msg = this.messageIndex.get(messageId);
    if (!msg) return null;
    return this.mapMessage(msg);
  }

  async getProfilePicUrl(jid: string): Promise<string | null> {
    const socket = this.getSocket();
    try {
      const normalized = this.normalizeJid(jid);
      const url = await socket.profilePictureUrl(normalized, "image");
      return url || null;
    } catch (_error) {
      return null;
    }
  }

  async getGroupInfo(groupJid: string): Promise<any> {
    const socket = this.getSocket();
    const normalized = this.normalizeJid(groupJid);
    const metadata = await socket.groupMetadata(normalized);
    return metadata;
  }

  async sendMessage(jid: string, message: string): Promise<any> {
    const socket = this.getSocket();
    return await socket.sendMessage(jid, { text: message });
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
        if (this.store?.loadMessages) {
          const loaded = await this.store.loadMessages(jid, 50);
          msg = loaded.find((m: any) => m?.key?.id === keyId);
        } else {
          const list = this.messagesByChat.get(jid) || [];
          msg = list.find((m: any) => m?.key?.id === keyId);
        }
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

    if (this.messageStore) {
      const existing = this.messageStore.getMediaByMessageId(messageId);
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
          this.messageStore.upsertMedia(record);
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
    const q = query.toLowerCase();
    if (!this.store?.contacts) return [];

    const contacts = Object.values(this.store.contacts) as any[];
    return contacts
      .filter((contact) => {
        const name = (contact?.name || contact?.notify || "").toLowerCase();
        const id = String(contact?.id || "").toLowerCase();
        return name.includes(q) || id.includes(q);
      })
      .map((contact) => this.mapContact(contact));
  }

  async resolveContacts(query: string, limit = 5): Promise<ResolvedContact[]> {
    const text = query.trim().toLowerCase();
    if (!text || !this.store?.contacts) return [];

    const digits = text.replace(/[^\d]/g, "");
    const hasDigits = digits.length >= 6;

    const contacts = Object.values(this.store.contacts) as any[];
    const scored = contacts.map((contact) => {
      const mapped = this.mapContact(contact);
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
    if (!this.store?.contacts) return null;
    const contact = this.store.contacts[jid];
    if (!contact) return null;
    return this.mapContact(contact);
  }

  private mapContact(contact: any): SimpleContact {
    const id = contact?.id || "";
    const number = String(id).split("@")[0] || "";
    return {
      id,
      name: contact?.name || contact?.notify || null,
      pushname: contact?.notify || null,
      isMe: false,
      isUser: true,
      isGroup: String(id).endsWith("@g.us"),
      isWAContact: true,
      isMyContact: true,
      number,
    };
  }

  private getLastMessageForChat(jid: string): SimpleMessage | undefined {
    const list = this.messagesByChat.get(jid) || [];
    const last = list[list.length - 1];
    if (last) {
      return this.mapMessage(last);
    }
    if (this.messageStore) {
      const stored = this.messageStore.listMessages(jid, 1);
      if (stored.length > 0) {
        return this.mapStoredMessage(stored[0]);
      }
    }
    return undefined;
  }
}
