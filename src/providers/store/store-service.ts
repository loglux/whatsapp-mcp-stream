import {
  MessageStore,
  StoredChat,
  StoredContact,
  StoredGroupMeta,
  StoredGroupParticipant,
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

  updateMessageContent(
    id: string,
    body: string,
    hasMedia: number,
    type: string,
  ): number {
    return this.messageStore
      ? this.messageStore.updateMessageContent(id, body, hasMedia, type)
      : 0;
  }

  upsertMedia(media: StoredMedia): void {
    this.messageStore?.upsertMedia(media);
  }

  upsertContact(contact: StoredContact): void {
    this.messageStore?.upsertContact(contact);
  }

  upsertLidMapping(lidJid: string, pnJid: string | null, pnNumber: string | null): void {
    this.messageStore?.upsertLidMapping({
      lid_jid: lidJid,
      pn_jid: pnJid,
      pn_number: pnNumber,
      updated_at: Date.now(),
    });
  }

  getLidForPn(pnJidOrNumber: string): string | null {
    return this.messageStore ? this.messageStore.getLidForPn(pnJidOrNumber) : null;
  }

  getPnForLid(lidJid: string): { pnJid: string | null; pnNumber: string | null } | null {
    return this.messageStore ? this.messageStore.getPnForLid(lidJid) : null;
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

  getMessageById(id: string): StoredMessage | null {
    return this.messageStore ? this.messageStore.getMessageById(id) : null;
  }

  deleteMessageById(id: string): void {
    this.messageStore?.deleteMessageById(id);
  }

  deleteMessagesByChat(jid: string): void {
    this.messageStore?.deleteMessagesByChat(jid);
  }

  insertMessageReaction(messageId: string, dataJson: string): void {
    this.messageStore?.insertMessageReaction(messageId, dataJson);
  }

  insertMessageReceipt(messageId: string, dataJson: string): void {
    this.messageStore?.insertMessageReceipt(messageId, dataJson);
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

  upsertGroupMeta(meta: StoredGroupMeta): void {
    this.messageStore?.upsertGroupMeta(meta);
  }

  getGroupMeta(jid: string): StoredGroupMeta | null {
    return this.messageStore ? this.messageStore.getGroupMeta(jid) : null;
  }

  replaceGroupParticipants(
    groupJid: string,
    participants: StoredGroupParticipant[],
  ): void {
    this.messageStore?.replaceGroupParticipants(groupJid, participants);
  }

  listGroupParticipants(jid: string): StoredGroupParticipant[] {
    return this.messageStore ? this.messageStore.listGroupParticipants(jid) : [];
  }
}
