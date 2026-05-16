import {
  mapContact,
  mapMessage,
  resolveStoredSender,
} from "../core/mappers.js";
import { MessageIndexStore } from "./message-index-store.js";
import type { StoreService } from "../providers/store/store-service.js";
import type {
  StoredChat,
  StoredContact,
  StoredGroupMeta,
  StoredGroupParticipant,
  StoredMessage,
} from "../storage/message-store.js";

export interface StorageWriterDeps {
  storeService: StoreService | null;
  /** The bot's own normalised JID, looked up dynamically (set on connection.open). */
  getOwnJid: () => string | null;
}

export class StorageWriter {
  constructor(private readonly deps: StorageWriterDeps) {}

  upsertMessage(msg: any): void {
    const store = this.deps.storeService;
    if (!store) return;
    const mapped = mapMessage(msg, MessageIndexStore.serializeMessageId);
    const record: StoredMessage = {
      id: mapped.id,
      chat_jid: mapped.to,
      from: resolveStoredSender(mapped, this.deps.getOwnJid()),
      to: mapped.to,
      timestamp: mapped.timestamp,
      from_me: mapped.fromMe ? 1 : 0,
      body: mapped.body || "",
      has_media: mapped.hasMedia ? 1 : 0,
      type: mapped.type,
    };
    store.upsertMessage(record);
  }

  updateMessageContent(msg: any): void {
    const store = this.deps.storeService;
    if (!store) return;
    const mapped = mapMessage(msg, MessageIndexStore.serializeMessageId);
    const changes = store.updateMessageContent(
      mapped.id,
      mapped.body || "",
      mapped.hasMedia ? 1 : 0,
      mapped.type,
    );
    if (changes === 0) {
      this.upsertMessage(msg);
    }
  }

  upsertChat(chat: any): void {
    const store = this.deps.storeService;
    if (!store) return;
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
    store.upsertChat(record);
  }

  upsertContact(contact: any): void {
    const store = this.deps.storeService;
    if (!store || !contact) return;
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
    store.upsertContact(record);
  }

  persistGroupMetadata(metadata: any): void {
    const store = this.deps.storeService;
    if (!store || !metadata) return;
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
    store.upsertGroupMeta(record);

    const participants: StoredGroupParticipant[] = Array.isArray(
      metadata?.participants,
    )
      ? metadata.participants.map((p: any) => ({
          group_jid: jid,
          participant_jid: p?.id || "",
          admin: p?.admin || null,
          updated_at: Date.now(),
        }))
      : [];
    if (participants.length) {
      store.replaceGroupParticipants(jid, participants);
    }
  }
}
