import { createRequire } from 'module';
import fs from 'fs';
import path from 'path';
import { log } from '../utils/logger.js';

const require = createRequire(import.meta.url);
const Database = require('better-sqlite3');

export interface StoredChat {
  id: string;
  name: string;
  is_group: number;
  unread_count: number;
  timestamp: number;
}

export interface StoredMessage {
  id: string;
  chat_jid: string;
  from: string;
  to: string;
  timestamp: number;
  from_me: number;
  body: string;
  has_media: number;
  type: string;
}

export interface StoredMedia {
  message_id: string;
  chat_jid: string;
  file_path: string;
  filename: string;
  mimetype: string;
  size: number;
}

export class MessageStore {
  private db: any;

  constructor(dbPath: string) {
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');
    this.migrate();
  }

  private migrate(): void {
    const sql = `
      CREATE TABLE IF NOT EXISTS chats (
        id TEXT PRIMARY KEY,
        name TEXT,
        is_group INTEGER,
        unread_count INTEGER,
        timestamp INTEGER,
        updated_at INTEGER
      );

      CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY,
        chat_jid TEXT,
        sender TEXT,
        recipient TEXT,
        timestamp INTEGER,
        from_me INTEGER,
        body TEXT,
        has_media INTEGER,
        type TEXT
      );

      CREATE TABLE IF NOT EXISTS media (
        message_id TEXT PRIMARY KEY,
        chat_jid TEXT,
        file_path TEXT,
        filename TEXT,
        mimetype TEXT,
        size INTEGER
      );

      CREATE INDEX IF NOT EXISTS idx_media_chat ON media(chat_jid);

      CREATE INDEX IF NOT EXISTS idx_messages_chat_ts ON messages(chat_jid, timestamp DESC);
    `;
    this.db.exec(sql);
  }

  upsertChat(chat: StoredChat): void {
    const stmt = this.db.prepare(
      `INSERT INTO chats (id, name, is_group, unread_count, timestamp, updated_at)
       VALUES (@id, @name, @is_group, @unread_count, @timestamp, @updated_at)
       ON CONFLICT(id) DO UPDATE SET
         name=excluded.name,
         is_group=excluded.is_group,
         unread_count=excluded.unread_count,
         timestamp=CASE
           WHEN excluded.timestamp > chats.timestamp THEN excluded.timestamp
           ELSE chats.timestamp
         END,
         updated_at=excluded.updated_at`,
    );
    stmt.run({ ...chat, updated_at: Date.now() });
  }

  upsertMessage(msg: StoredMessage): void {
    const stmt = this.db.prepare(
      `INSERT OR IGNORE INTO messages
       (id, chat_jid, sender, recipient, timestamp, from_me, body, has_media, type)
       VALUES (@id, @chat_jid, @from, @to, @timestamp, @from_me, @body, @has_media, @type)`,
    );
    stmt.run(msg);
  }

  listChats(limit = 20): StoredChat[] {
    const stmt = this.db.prepare(
      `SELECT id, name, is_group, unread_count, timestamp
       FROM chats
       ORDER BY timestamp DESC
       LIMIT ?`,
    );
    return stmt.all(limit);
  }

  getChatById(jid: string): StoredChat | null {
    const stmt = this.db.prepare(
      `SELECT id, name, is_group, unread_count, timestamp FROM chats WHERE id = ?`,
    );
    return stmt.get(jid) || null;
  }

  listMessages(jid: string, limit = 50): StoredMessage[] {
    const stmt = this.db.prepare(
      `SELECT id, chat_jid, sender, recipient, timestamp, from_me, body, has_media, type
       FROM messages
       WHERE chat_jid = ?
       ORDER BY timestamp DESC
       LIMIT ?`,
    );
    return stmt.all(jid, limit);
  }

  listMessagesAll(jid: string): StoredMessage[] {
    const stmt = this.db.prepare(
      `SELECT id, chat_jid, sender, recipient, timestamp, from_me, body, has_media, type
       FROM messages
       WHERE chat_jid = ?
       ORDER BY timestamp ASC`,
    );
    return stmt.all(jid);
  }

  searchMessages(query: string, limit = 20): StoredMessage[] {
    const stmt = this.db.prepare(
      `SELECT id, chat_jid, sender, recipient, timestamp, from_me, body, has_media, type
       FROM messages
       WHERE body LIKE ?
       ORDER BY timestamp DESC
       LIMIT ?`,
    );
    return stmt.all(`%${query}%`, limit);
  }

  upsertMedia(record: StoredMedia): void {
    const stmt = this.db.prepare(
      `INSERT OR REPLACE INTO media (message_id, chat_jid, file_path, filename, mimetype, size)
       VALUES (@message_id, @chat_jid, @file_path, @filename, @mimetype, @size)`,
    );
    stmt.run(record);
  }

  getMediaByMessageId(messageId: string): StoredMedia | null {
    const stmt = this.db.prepare(
      `SELECT message_id, chat_jid, file_path, filename, mimetype, size FROM media WHERE message_id = ?`,
    );
    return stmt.get(messageId) || null;
  }

  listMediaByChat(jid: string): StoredMedia[] {
    const stmt = this.db.prepare(
      `SELECT message_id, chat_jid, file_path, filename, mimetype, size FROM media WHERE chat_jid = ?`,
    );
    return stmt.all(jid);
  }

  stats(): { chats: number; messages: number; media: number } {
    const chats = this.db.prepare('SELECT COUNT(*) as count FROM chats').get().count as number;
    const messages = this.db.prepare('SELECT COUNT(*) as count FROM messages').get().count as number;
    const media = this.db.prepare('SELECT COUNT(*) as count FROM media').get().count as number;
    return { chats, messages, media };
  }

  close(): void {
    try {
      this.db.close();
    } catch (error) {
      log.warn({ err: error }, 'Failed to close message store');
    }
  }
}
