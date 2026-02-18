import { createRequire } from 'module';
import { webcrypto } from 'crypto';
import fs from 'fs';
import path from 'path';
import axios from 'axios';
import { fileTypeFromBuffer } from 'file-type';
import pino from 'pino';
import { log } from '../utils/logger.js';

const require = createRequire(import.meta.url);
const baileys = require('@whiskeysockets/baileys');

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
  private latestQrCode: string | null = null;
  private isAuthenticatedFlag = false;
  private isReadyFlag = false;
  private initializing: Promise<void> | null = null;
  private reconnecting = false;
  private messageIndex = new Map<string, any>();
  private messagesByChat = new Map<string, any[]>();
  private messageKeyIndex = new Map<string, any>();
  private sessionDir: string;

  constructor() {
    const baseDir = process.env.SESSION_DIR || path.join(process.cwd(), 'whatsapp-sessions', 'baileys');
    this.sessionDir = baseDir;
  }

  private normalizeJid(jid: string): string {
    if (!jid) return jid;
    if (typeof jidNormalizedUser === 'function') {
      return jidNormalizedUser(jid);
    }
    return jid;
  }

  private serializeMessageId(msg: any): string {
    const jid = msg?.key?.remoteJid || 'unknown';
    const id = msg?.key?.id || 'unknown';
    return `${jid}:${id}`;
  }

  private extractText(message: any): string {
    if (!message) return '';
    if (message.conversation) return message.conversation;
    if (message.extendedTextMessage?.text) return message.extendedTextMessage.text;
    if (message.imageMessage?.caption) return message.imageMessage.caption;
    if (message.videoMessage?.caption) return message.videoMessage.caption;
    if (message.documentMessage?.caption) return message.documentMessage.caption;
    if (message.buttonsResponseMessage?.selectedDisplayText) return message.buttonsResponseMessage.selectedDisplayText;
    if (message.listResponseMessage?.title) return message.listResponseMessage.title;
    if (message.templateButtonReplyMessage?.selectedDisplayText) return message.templateButtonReplyMessage.selectedDisplayText;
    if (message.reactionMessage?.text) return `reacted: ${message.reactionMessage.text}`;
    return '';
  }

  private mapMessage(msg: any): SimpleMessage {
    const id = this.serializeMessageId(msg);
    const jid = msg?.key?.remoteJid || '';
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
      from: fromMe ? 'me' : (msg?.key?.participant || jid),
      to: jid,
      timestamp,
      fromMe,
      hasMedia,
      type: Object.keys(message)[0] || 'unknown',
    };
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
    if (mimetype.startsWith('image/')) {
      return { image: buffer, mimetype, caption };
    }
    if (mimetype.startsWith('video/')) {
      return { video: buffer, mimetype, caption };
    }
    if (mimetype.startsWith('audio/')) {
      return { audio: buffer, mimetype, ptt: asAudioMessage };
    }
    return { document: buffer, mimetype, fileName: filename || 'file', caption };
  }

  async initialize(): Promise<void> {
    if (this.initializing) {
      await this.initializing;
      return;
    }

    this.initializing = (async () => {
      fs.mkdirSync(this.sessionDir, { recursive: true });
      const { state, saveCreds } = await useMultiFileAuthState(this.sessionDir);

      const { version } = await fetchLatestBaileysVersion();
      const logger = (pino as unknown as (opts: any) => any)({ level: 'silent' });

      if (typeof makeInMemoryStore === 'function') {
        this.store = makeInMemoryStore({ logger });
      } else {
        this.store = null;
      }

      this.sock = makeWASocket({
        version,
        auth: state,
        printQRInTerminal: false,
        logger,
        syncFullHistory: true,
        browser: ['MCP', 'Desktop', '1.0.0'],
        getMessage: async (key: any) => {
          const cached = key?.id ? this.messageKeyIndex.get(key.id) : undefined;
          return cached?.message;
        },
      });

      if (this.store?.bind) {
        this.store.bind(this.sock.ev);
      }

      this.sock.ev.on('creds.update', saveCreds);

      this.sock.ev.on('connection.update', (update: any) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
          this.latestQrCode = qr;
          log.info('QR code received.');
        }

        if (connection === 'open') {
          this.isAuthenticatedFlag = true;
          this.isReadyFlag = true;
          this.latestQrCode = null;
          log.info('WhatsApp connection opened.');
        }

        if (connection === 'close') {
          this.isReadyFlag = false;
          this.isAuthenticatedFlag = false;
          const statusCode = lastDisconnect?.error?.output?.statusCode;
          const reason = lastDisconnect?.error?.message;
          log.warn({ statusCode, reason }, 'WhatsApp connection closed.');
          this.sock = null;
          if (statusCode === DisconnectReason.loggedOut) {
            log.warn('WhatsApp logged out. Clearing session.');
            try {
              fs.rmSync(this.sessionDir, { recursive: true, force: true });
            } catch (_error) {
              // Ignore
            }
            this.reconnect().catch((err) => log.error({ err }, 'Failed to reconnect WhatsApp'));
          } else if (statusCode === 401 || reason?.includes('Connection Failure')) {
            log.warn('WhatsApp connection failed. Resetting session and reconnecting.');
            try {
              fs.rmSync(this.sessionDir, { recursive: true, force: true });
            } catch (_error) {
              // Ignore
            }
            this.reconnect().catch((err) => log.error({ err }, 'Failed to reconnect WhatsApp'));
          } else {
            this.reconnect().catch((err) => log.error({ err }, 'Failed to reconnect WhatsApp'));
          }
        }
      });

      this.sock.ev.on('messages.upsert', (ev: any) => {
        if (!ev?.messages) return;
        for (const msg of ev.messages) {
          this.trackMessage(msg);
        }
      });

      this.sock.ev.on('messages.update', (updates: any[]) => {
        if (!updates) return;
        for (const update of updates) {
          const key = update?.key?.remoteJid && update?.key?.id ? `${update.key.remoteJid}:${update.key.id}` : null;
          if (key && this.messageIndex.has(key)) {
            const existing = this.messageIndex.get(key);
            this.messageIndex.set(key, { ...existing, ...update });
          }
        }
      });

      this.sock.ev.on('chats.set', (payload: any) => {
        if (payload?.chats && Array.isArray(payload.chats) && this.store?.chats?.insert) {
          this.store.chats.insert(payload.chats);
        }
      });

      this.sock.ev.on('messages.set', (payload: any) => {
        if (payload?.messages && Array.isArray(payload.messages)) {
          for (const msg of payload.messages) {
            this.trackMessage(msg);
          }
        }
      });

      this.sock.ev.on('messaging-history.set', (payload: any) => {
        const { chats, contacts, messages } = payload || {};
        if (chats && Array.isArray(chats) && this.store?.chats?.insert) {
          this.store.chats.insert(chats);
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
          }
        }
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
      log.error({ err: error }, 'Reconnect failed');
    } finally {
      this.reconnecting = false;
    }
  }

  async destroy(): Promise<void> {
    if (!this.sock) return;
    try {
      this.sock.end(new Error('Client destroyed'));
    } catch (_error) {
      // Ignore
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

  getSocket(): any {
    if (!this.sock) {
      throw new Error('WhatsApp socket not initialized');
    }
    return this.sock;
  }

  async listChats(limit = 20, includeLastMessage = true): Promise<SimpleChat[]> {
    if (!this.store?.chats?.all) {
      const chatIds = Array.from(this.messagesByChat.keys());
      const chats = chatIds.map((jid) => ({ id: jid }));
      return chats.slice(0, limit).map((chat) => ({
        id: chat.id,
        name: chat.id,
        isGroup: chat.id.endsWith('@g.us'),
        unreadCount: 0,
        timestamp: 0,
      }));
    }

    const chats = this.store.chats.all();
    if ((!chats || chats.length === 0) && this.store?.loadChats) {
      try {
        const loaded = await this.store.loadChats();
        if (loaded?.length) {
          loaded.forEach((chat: any) => {
            if (this.store?.chats?.insert) {
              this.store.chats.insert(chat);
            }
          });
        }
      } catch (_error) {
        // Ignore
      }
    }
    const effectiveChats = this.store.chats.all();
    const mapped = effectiveChats.map((chat: any) => {
      const id = chat?.id || chat?.jid;
      const name = chat?.name || chat?.subject || id;
      const lastMessage = includeLastMessage ? this.getLastMessageForChat(id) : undefined;
      return {
        id,
        name,
        isGroup: String(id).endsWith('@g.us'),
        unreadCount: chat?.unreadCount || 0,
        timestamp: Number(chat?.conversationTimestamp || 0) * 1000,
        lastMessage,
      } as SimpleChat;
    });

    mapped.sort((a: SimpleChat, b: SimpleChat) => b.timestamp - a.timestamp);
    return mapped.slice(0, limit);
  }

  async listGroups(limit = 20, includeLastMessage = true): Promise<SimpleChat[]> {
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
        isGroup: normalized.endsWith('@g.us'),
        unreadCount: chat?.unreadCount || 0,
        timestamp: Number(chat?.conversationTimestamp || 0) * 1000,
        lastMessage,
      };
    }

    return null;
  }

  async getMessages(jid: string, limit = 50): Promise<SimpleMessage[]> {
    const normalized = this.normalizeJid(jid);
    if (this.store?.loadMessages) {
      const raw = await this.store.loadMessages(normalized, limit);
      raw.forEach((msg: any) => this.trackMessage(msg));
      return raw.map((msg: any) => this.mapMessage(msg));
    }
    const list = this.messagesByChat.get(normalized) || [];
    return list.slice(-limit).map((msg) => this.mapMessage(msg));
  }

  async searchMessages(query: string, limit = 20, chatId?: string): Promise<SimpleMessage[]> {
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
        const raw = await this.store.loadMessages(normalized, Math.max(50, limit * 5));
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

    results.sort((a, b) => b.timestamp - a.timestamp);
    return results.slice(0, limit);
  }

  async getMessageById(messageId: string): Promise<SimpleMessage | null> {
    const msg = this.messageIndex.get(messageId);
    if (!msg) return null;
    return this.mapMessage(msg);
  }

  async sendMessage(jid: string, message: string): Promise<any> {
    const socket = this.getSocket();
    return await socket.sendMessage(jid, { text: message });
  }

  async sendMedia(jid: string, input: string, caption?: string, asAudioMessage = false): Promise<any> {
    let buffer: Buffer;
    let mimetype = 'application/octet-stream';
    let filename: string | undefined;

    if (input.startsWith('http://') || input.startsWith('https://')) {
      const resp = await axios.get(input, { responseType: 'arraybuffer' });
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

    const content = await this.buildMediaMessage(buffer, mimetype, filename, caption, asAudioMessage);
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
    const buffer = Buffer.from(base64, 'base64');
    const content = await this.buildMediaMessage(buffer, mimeType, filename, caption, asAudioMessage);
    return await this.getSocket().sendMessage(jid, content);
  }

  async downloadMedia(messageId: string): Promise<DownloadedMedia | null> {
    let msg = this.messageIndex.get(messageId);
    if (!msg?.message) {
      const parts = messageId.split(':');
      if (parts.length >= 2) {
        const jid = parts[0];
        const keyId = parts.slice(1).join(':');
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
    let type: 'image' | 'video' | 'audio' | 'document' | 'sticker' | null = null;

    if (message.imageMessage) {
      content = message.imageMessage;
      type = 'image';
    } else if (message.videoMessage) {
      content = message.videoMessage;
      type = 'video';
    } else if (message.audioMessage) {
      content = message.audioMessage;
      type = 'audio';
    } else if (message.documentMessage) {
      content = message.documentMessage;
      type = 'document';
    } else if (message.stickerMessage) {
      content = message.stickerMessage;
      type = 'sticker';
    }

    if (!content || !type) return null;

    const stream = await downloadContentFromMessage(content, type);
    const chunks: Buffer[] = [];
    for await (const chunk of stream) {
      chunks.push(Buffer.from(chunk));
    }
    const data = Buffer.concat(chunks);

    return {
      data,
      mimetype: content.mimetype || 'application/octet-stream',
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
        const name = (contact?.name || contact?.notify || '').toLowerCase();
        const id = String(contact?.id || '').toLowerCase();
        return name.includes(q) || id.includes(q);
      })
      .map((contact) => this.mapContact(contact));
  }

  async getContactById(jid: string): Promise<SimpleContact | null> {
    if (!this.store?.contacts) return null;
    const contact = this.store.contacts[jid];
    if (!contact) return null;
    return this.mapContact(contact);
  }

  private mapContact(contact: any): SimpleContact {
    const id = contact?.id || '';
    const number = String(id).split('@')[0] || '';
    return {
      id,
      name: contact?.name || contact?.notify || null,
      pushname: contact?.notify || null,
      isMe: false,
      isUser: true,
      isGroup: String(id).endsWith('@g.us'),
      isWAContact: true,
      isMyContact: true,
      number,
    };
  }

  private getLastMessageForChat(jid: string): SimpleMessage | undefined {
    const list = this.messagesByChat.get(jid) || [];
    const last = list[list.length - 1];
    if (!last) return undefined;
    return this.mapMessage(last);
  }
}
