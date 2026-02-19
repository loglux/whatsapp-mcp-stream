import { StoredMessage } from "../storage/message-store.js";
import { SimpleContact, SimpleMessage } from "./types.js";

export function extractText(message: any): string {
  if (!message) return "";
  if (message.conversation) return message.conversation;
  if (message.extendedTextMessage?.text)
    return message.extendedTextMessage.text;
  if (message.imageMessage?.caption) return message.imageMessage.caption;
  if (message.videoMessage?.caption) return message.videoMessage.caption;
  if (message.documentMessage?.caption) return message.documentMessage.caption;
  if (message.buttonsResponseMessage?.selectedDisplayText) {
    return message.buttonsResponseMessage.selectedDisplayText;
  }
  if (message.listResponseMessage?.title)
    return message.listResponseMessage.title;
  if (message.templateButtonReplyMessage?.selectedDisplayText) {
    return message.templateButtonReplyMessage.selectedDisplayText;
  }
  if (message.reactionMessage?.text)
    return `reacted: ${message.reactionMessage.text}`;
  return "";
}

export function mapMessage(
  msg: any,
  serializeMessageId: (msg: any) => string,
): SimpleMessage {
  const id = serializeMessageId(msg);
  const jid = msg?.key?.remoteJid || "";
  const fromMe = Boolean(msg?.key?.fromMe);
  const timestamp = Number(msg?.messageTimestamp || 0) * 1000;
  const message = msg?.message || {};
  const body = extractText(message);
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

export function mapStoredMessage(msg: StoredMessage): SimpleMessage {
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

export function mapContact(contact: any): SimpleContact {
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
