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
  StoredMessage,
  StoredMedia,
} from "../storage/message-store.js";
import { StoreService } from "../providers/store/store-service.js";
import { BaileysClient } from "../providers/wa/baileys-client.js";
import { WhatsAppSync } from "./whatsapp-sync.js";

import { createRequire } from "module";

const require = createRequire(import.meta.url);
const baileys = require("@whiskeysockets/baileys");

const {
  DisconnectReason,
  jidNormalizedUser,
  downloadContentFromMessage,
  ALL_WA_PATCH_NAMES,
} = baileys;

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
  private sessionDir: string;
  private storeService: StoreService | null = null;
  private client: BaileysClient;
  private sync: WhatsAppSync;

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
      async (jid: string, limit: number) =>
        this.getStoreOptional()?.loadMessages
          ? await this.getStoreOptional().loadMessages(jid, limit)
          : [],
      () =>
        this.getStoreOptional()?.chats?.all
          ? this.getStoreOptional().chats.all()
          : [],
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

  private serializeMessageId(msg: any): string {
    const jid = msg?.key?.remoteJid || "unknown";
    const id = msg?.key?.id || "unknown";
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

  private upsertStoredChat(chat: any): void {
    if (!this.storeService) return;
    const id = chat?.id || chat?.jid;
    if (!id) return;
    const record: StoredChat = {
      id,
      name: chat?.name || chat?.subject || id,
      is_group: String(id).endsWith("@g.us") ? 1 : 0,
      unread_count: chat?.unreadCount || 0,
      timestamp: Number(chat?.conversationTimestamp || 0) * 1000,
    };
    this.storeService.upsertChat(record);
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

    this.initStoreService();

    this.initializing = (async () => {
      await this.client.initialize(async (key: any) => {
        const cached = key?.id ? this.messageKeyIndex.get(key.id) : undefined;
        return cached?.message;
      });

      const sock = this.client.getSocket();
      const store = this.client.getStoreOptional();

      sock.ev.on("connection.update", (update: any) => {
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
          this.sync.scheduleWarmup(() => this.forceResync());
        }

        if (connection === "close") {
          this.isReadyFlag = false;
          this.isAuthenticatedFlag = false;
          const statusCode = lastDisconnect?.error?.output?.statusCode;
          const reason = lastDisconnect?.error?.message;
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
        if (!ev?.messages) return;
        for (const msg of ev.messages) {
          this.trackMessage(msg);
          this.upsertStoredMessage(msg);
        }
      });

      sock.ev.on("messages.update", (updates: any[]) => {
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

      sock.ev.on("chats.set", (payload: any) => {
        this.sync.handleChatsSet(payload, store);
      });

      sock.ev.on("messages.set", (payload: any) => {
        this.sync.handleMessagesSet(payload);
      });

      sock.ev.on("chats.upsert", (payload: any) => {
        this.sync.handleChatsUpsert(payload);
      });

      sock.ev.on("chats.update", (payload: any) => {
        this.sync.handleChatsUpdate(payload);
      });

      sock.ev.on("contacts.upsert", (payload: any) => {
        if (payload && Array.isArray(payload)) {
          log.info({ count: payload.length }, "Contacts upsert");
        }
      });

      sock.ev.on("contacts.update", (payload: any) => {
        if (payload && Array.isArray(payload)) {
          log.info({ count: payload.length }, "Contacts update");
        }
      });

      sock.ev.on("messaging-history.set", (payload: any) => {
        this.sync.handleMessagingHistorySet(payload, store);
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
    await this.destroy();
    await this.initialize();
  }

  async destroy(): Promise<void> {
    await this.client.destroy();
    this.sync.clearWarmupTimer();
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
    const store = this.getStoreOptional();
    const chatCount = store?.chats?.all
      ? store.chats.all().length
      : this.messagesByChat.size;
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

  getMessageStoreStats(): {
    chats: number;
    messages: number;
    media: number;
  } | null {
    if (!this.storeService) return null;
    return this.storeService.stats();
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

  private getStoreOptional(): any | null {
    return this.client.getStoreOptional();
  }

  async listChats(
    limit = 20,
    includeLastMessage = true,
  ): Promise<SimpleChat[]> {
    const store = this.getStoreOptional();
    if (!store?.chats?.all) {
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

    const chats = store.chats.all();
    if ((!chats || chats.length === 0) && store?.loadChats) {
      try {
        const loaded = await store.loadChats();
        if (loaded?.length && store?.chats?.insert) {
          const valid = loaded
            .map((chat: any) => this.normalizeChatRecord(chat))
            .filter((chat: any) => chat?.id);
          if (valid.length) {
            try {
              store.chats.insert(valid);
            } catch (_error) {
              // Ignore
            }
          }
        }
      } catch (_error) {
        // Ignore
      }
    }
    const effectiveChats = store.chats.all();
    if (effectiveChats.length === 0) {
      await this.sync.warmup(() => this.forceResync());
    }
    const refreshedChats = store.chats.all();
    if (refreshedChats.length === 0 && this.storeService) {
      const stored = this.storeService.listChats(limit);
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
    const store = this.getStoreOptional();
    if (store?.chats?.get) {
      const chat = store.chats.get(normalized);
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
    const normalized = this.normalizeJid(jid);
    const store = this.getStoreOptional();
    if (store?.loadMessages) {
      const raw = await store.loadMessages(normalized, limit);
      raw.forEach((msg: any) => this.trackMessage(msg));
      if (raw.length > 0) {
        return raw.map((msg: any) =>
          mapMessage(msg, this.serializeMessageId.bind(this)),
        );
      }
    }
    const list = this.messagesByChat.get(normalized) || [];
    if (list.length > 0) {
      return list
        .slice(-limit)
        .map((msg) => mapMessage(msg, this.serializeMessageId.bind(this)));
    }
    if (this.storeService) {
      const stored = this.storeService.listMessages(normalized, limit);
      return stored.map((msg) => mapStoredMessage(msg));
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
    if (!this.storeService) {
      return { chat: null, messages: [], media: [] };
    }
    const chat = await this.getChatById(jid);
    const messages = this.storeService
      .listMessagesAll(jid)
      .map((msg) => mapStoredMessage(msg));
    const media = includeMedia ? this.storeService.listMediaByChat(jid) : [];
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
        const mapped = mapMessage(msg, this.serializeMessageId.bind(this));
        if (mapped.body && mapped.body.toLowerCase().includes(q)) {
          results.push(mapped);
        }
      }
    };

    if (chatId) {
      const normalized = this.normalizeJid(chatId);
      const store = this.getStoreOptional();
      if (store?.loadMessages) {
        const raw = await store.loadMessages(
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
        const store = this.getStoreOptional();
        if (store?.loadMessages) {
          const loaded = await store.loadMessages(jid, 50);
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
    const q = query.toLowerCase();
    const store = this.getStoreOptional();
    if (!store?.contacts) return [];

    const contacts = Object.values(store.contacts) as any[];
    return contacts
      .filter((contact) => {
        const name = (contact?.name || contact?.notify || "").toLowerCase();
        const id = String(contact?.id || "").toLowerCase();
        return name.includes(q) || id.includes(q);
      })
      .map((contact) => mapContact(contact));
  }

  async resolveContacts(query: string, limit = 5): Promise<ResolvedContact[]> {
    const text = query.trim().toLowerCase();
    const store = this.getStoreOptional();
    if (!text || !store?.contacts) return [];

    const digits = text.replace(/[^\d]/g, "");
    const hasDigits = digits.length >= 6;

    const contacts = Object.values(store.contacts) as any[];
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
    const store = this.getStoreOptional();
    if (!store?.contacts) return null;
    const contact = store.contacts[jid];
    if (!contact) return null;
    return mapContact(contact);
  }

  private getLastMessageForChat(jid: string): SimpleMessage | undefined {
    const list = this.messagesByChat.get(jid) || [];
    const last = list[list.length - 1];
    if (last) {
      return mapMessage(last, this.serializeMessageId.bind(this));
    }
    if (this.storeService) {
      const stored = this.storeService.listMessages(jid, 1);
      if (stored.length > 0) {
        return mapStoredMessage(stored[0]);
      }
    }
    return undefined;
  }
}
