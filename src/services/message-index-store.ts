import { setBoundedMapEntry } from "../utils/bounded-map.js";

export interface MessageIndexOptions {
  maxIndexSize?: number;
  maxKeyIndexSize?: number;
  maxPerChat?: number;
}

const DEFAULT_INDEX_SIZE = 20000;
const DEFAULT_KEY_INDEX_SIZE = 20000;
const DEFAULT_PER_CHAT = 500;

export class MessageIndexStore {
  private readonly maxIndexSize: number;
  private readonly maxKeyIndexSize: number;
  private readonly maxPerChat: number;

  private messageIndex = new Map<string, any>();
  private messageKeyIndex = new Map<string, any>();
  private messagesByChat = new Map<string, any[]>();

  constructor(options: MessageIndexOptions = {}) {
    this.maxIndexSize = Math.max(1, options.maxIndexSize ?? DEFAULT_INDEX_SIZE);
    this.maxKeyIndexSize = Math.max(
      1,
      options.maxKeyIndexSize ?? DEFAULT_KEY_INDEX_SIZE,
    );
    this.maxPerChat = Math.max(1, options.maxPerChat ?? DEFAULT_PER_CHAT);
  }

  static serializeMessageId(msg: any): string {
    const jid = msg?.key?.remoteJid || msg?.key?.remoteJidAlt || "unknown";
    const id = msg?.key?.id || "unknown";
    return `${jid}:${id}`;
  }

  static serializeKeyId(key: any): string {
    const jid = key?.remoteJid || key?.remoteJidAlt || "unknown";
    const id = key?.id || "unknown";
    return `${jid}:${id}`;
  }

  add(msg: any): void {
    const id = MessageIndexStore.serializeMessageId(msg);
    setBoundedMapEntry(this.messageIndex, id, msg, this.maxIndexSize);
    const keyId = msg?.key?.id;
    if (keyId) {
      setBoundedMapEntry(
        this.messageKeyIndex,
        keyId,
        msg,
        this.maxKeyIndexSize,
      );
    }
    const jid = msg?.key?.remoteJid;
    if (!jid) return;
    const list = this.messagesByChat.get(jid) || [];
    list.push(msg);
    if (list.length > this.maxPerChat) {
      list.splice(0, list.length - this.maxPerChat);
    }
    this.messagesByChat.set(jid, list);
  }

  getById(id: string): any | undefined {
    return this.messageIndex.get(id);
  }

  hasId(id: string): boolean {
    return this.messageIndex.has(id);
  }

  getByKeyId(keyId: string): any | undefined {
    return this.messageKeyIndex.get(keyId);
  }

  /** Merge-in-place update keyed by canonical `${remoteJid}:${id}`. Returns the merged value or undefined if the id was not indexed. */
  updateById(id: string, merger: (existing: any) => any): any | undefined {
    if (!this.messageIndex.has(id)) return undefined;
    const existing = this.messageIndex.get(id);
    const merged = merger(existing);
    setBoundedMapEntry(this.messageIndex, id, merged, this.maxIndexSize);
    return merged;
  }

  deleteByKey(
    key: { remoteJid?: string; id?: string } | null | undefined,
  ): void {
    const jid = key?.remoteJid;
    const id = key?.id;
    if (!jid || !id) return;
    const msgId = `${jid}:${id}`;
    this.messageIndex.delete(msgId);
    this.messageKeyIndex.delete(id);
    const list = this.messagesByChat.get(jid);
    if (list?.length) {
      this.messagesByChat.set(
        jid,
        list.filter((msg: any) => msg?.key?.id !== id),
      );
    }
  }

  deleteByChat(jid: string): void {
    this.messagesByChat.delete(jid);
  }

  listByChat(jid: string): any[] {
    return this.messagesByChat.get(jid) || [];
  }

  chatJids(): string[] {
    return Array.from(this.messagesByChat.keys());
  }

  messages(): IterableIterator<any> {
    return this.messageIndex.values();
  }

  get messageCount(): number {
    return this.messageIndex.size;
  }

  get chatCount(): number {
    return this.messagesByChat.size;
  }
}
