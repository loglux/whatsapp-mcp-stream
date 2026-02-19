import {
  MessageStore,
  StoredChat,
  StoredContact,
  StoredMedia,
  StoredMessage,
} from "../../storage/message-store.js";
import { log } from "../../utils/logger.js";

export class StoreService {
  private messageStore: MessageStore | null = null;

  constructor(dbPath: string) {
    this.init(dbPath);
  }

  private init(dbPath: string): void {
    try {
      this.messageStore = new MessageStore(dbPath);
      log.info({ dbPath }, "Message store initialized");
    } catch (error) {
      log.warn({ err: error }, "Failed to initialize message store");
      this.messageStore = null;
    }
  }

  isReady(): boolean {
    return Boolean(this.messageStore);
  }

  stats(): { chats: number; messages: number; media: number; contacts: number } | null {
    return this.messageStore ? this.messageStore.stats() : null;
  }

  upsertChat(chat: StoredChat): void {
    this.messageStore?.upsertChat(chat);
  }

  upsertMessage(message: StoredMessage): void {
    this.messageStore?.upsertMessage(message);
  }

  upsertMedia(media: StoredMedia): void {
    this.messageStore?.upsertMedia(media);
  }

  upsertContact(contact: StoredContact): void {
    this.messageStore?.upsertContact(contact);
  }

  listChats(limit = 20): StoredChat[] {
    return this.messageStore ? this.messageStore.listChats(limit) : [];
  }

  getChatById(jid: string): StoredChat | null {
    return this.messageStore ? this.messageStore.getChatById(jid) : null;
  }

  listMessages(jid: string, limit = 50): StoredMessage[] {
    return this.messageStore ? this.messageStore.listMessages(jid, limit) : [];
  }

  listMessagesAll(jid: string): StoredMessage[] {
    return this.messageStore ? this.messageStore.listMessagesAll(jid) : [];
  }

  searchMessages(query: string, limit = 20): StoredMessage[] {
    return this.messageStore ? this.messageStore.searchMessages(query, limit) : [];
  }

  getMediaByMessageId(messageId: string): StoredMedia | null {
    return this.messageStore ? this.messageStore.getMediaByMessageId(messageId) : null;
  }

  listMediaByChat(jid: string): StoredMedia[] {
    return this.messageStore ? this.messageStore.listMediaByChat(jid) : [];
  }

  getContactById(jid: string): StoredContact | null {
    return this.messageStore ? this.messageStore.getContactById(jid) : null;
  }

  listContacts(limit = 100): StoredContact[] {
    return this.messageStore ? this.messageStore.listContacts(limit) : [];
  }

  searchContacts(query: string, limit = 20): StoredContact[] {
    return this.messageStore ? this.messageStore.searchContacts(query, limit) : [];
  }
}
