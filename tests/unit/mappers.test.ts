import { describe, it, expect } from "vitest";
import {
  extractText,
  mapMessage,
  mapStoredMessage,
  mapContact,
  resolveStoredSender,
} from "../../src/core/mappers.js";
import type { StoredMessage } from "../../src/storage/message-store.js";

describe("extractText", () => {
  it("returns conversation when present", () => {
    expect(extractText({ conversation: "hello" })).toBe("hello");
  });

  it("returns extendedTextMessage.text", () => {
    expect(extractText({ extendedTextMessage: { text: "extended hi" } })).toBe(
      "extended hi",
    );
  });

  it("returns imageMessage.caption", () => {
    expect(extractText({ imageMessage: { caption: "a photo" } })).toBe(
      "a photo",
    );
  });

  it("prefixes reaction emoji with 'reacted: '", () => {
    expect(extractText({ reactionMessage: { text: "👍" } })).toBe(
      "reacted: 👍",
    );
  });

  it("returns empty string for null/empty messages", () => {
    expect(extractText(null)).toBe("");
    expect(extractText(undefined)).toBe("");
    expect(extractText({})).toBe("");
  });
});

describe("mapMessage", () => {
  const serializer = (m: { key?: { remoteJid?: string; id?: string } }) =>
    `${m.key?.remoteJid}:${m.key?.id}`;

  it("uses the provided id serializer", () => {
    const result = mapMessage(
      { key: { remoteJid: "1@s.whatsapp.net", id: "AAA" } },
      serializer,
    );
    expect(result.id).toBe("1@s.whatsapp.net:AAA");
  });

  it("sets from to 'me' when fromMe is true", () => {
    const result = mapMessage(
      {
        key: {
          remoteJid: "1@s.whatsapp.net",
          id: "AAA",
          fromMe: true,
          participant: "2@s.whatsapp.net",
        },
      },
      serializer,
    );
    expect(result.from).toBe("me");
    expect(result.fromMe).toBe(true);
  });

  it("uses participant for group messages when fromMe is false", () => {
    const result = mapMessage(
      {
        key: {
          remoteJid: "group@g.us",
          id: "AAA",
          fromMe: false,
          participant: "member@s.whatsapp.net",
        },
      },
      serializer,
    );
    expect(result.from).toBe("member@s.whatsapp.net");
    expect(result.to).toBe("group@g.us");
  });

  it("converts seconds timestamp to milliseconds", () => {
    const result = mapMessage(
      { key: { remoteJid: "x", id: "y" }, messageTimestamp: 1700000000 },
      serializer,
    );
    expect(result.timestamp).toBe(1700000000_000);
  });

  it("handles Long-like timestamp via toNumber()", () => {
    const result = mapMessage(
      {
        key: { remoteJid: "x", id: "y" },
        messageTimestamp: { toNumber: () => 1700000123 },
      },
      serializer,
    );
    expect(result.timestamp).toBe(1700000123_000);
  });

  it("flags hasMedia true for image, video, audio, document, sticker", () => {
    for (const kind of [
      "imageMessage",
      "videoMessage",
      "audioMessage",
      "documentMessage",
      "stickerMessage",
    ]) {
      const result = mapMessage(
        { key: { remoteJid: "x", id: "y" }, message: { [kind]: {} } },
        serializer,
      );
      expect(result.hasMedia, `expected hasMedia for ${kind}`).toBe(true);
      expect(result.type).toBe(kind);
    }
  });

  it("flags hasMedia false for plain text", () => {
    const result = mapMessage(
      {
        key: { remoteJid: "x", id: "y" },
        message: { conversation: "hi" },
      },
      serializer,
    );
    expect(result.hasMedia).toBe(false);
    expect(result.body).toBe("hi");
    expect(result.type).toBe("conversation");
  });
});

describe("mapStoredMessage", () => {
  it("maps a StoredMessage with declared from/to fields", () => {
    const stored: StoredMessage = {
      id: "x@y:abc",
      chat_jid: "x@y",
      from: "a@s.whatsapp.net",
      to: "x@y",
      timestamp: 123,
      from_me: 1,
      body: "hi",
      has_media: 0,
      type: "conversation",
    };
    const result = mapStoredMessage(stored);
    expect(result).toEqual({
      id: "x@y:abc",
      body: "hi",
      from: "a@s.whatsapp.net",
      to: "x@y",
      timestamp: 123,
      fromMe: true,
      hasMedia: false,
      type: "conversation",
    });
  });

  it("falls back type to 'unknown' when empty", () => {
    const stored = {
      id: "x:y",
      chat_jid: "x",
      from: "a",
      to: "x",
      timestamp: 0,
      from_me: 0,
      body: "",
      has_media: 0,
      type: "",
    } as StoredMessage;
    expect(mapStoredMessage(stored).type).toBe("unknown");
  });
});

describe("resolveStoredSender", () => {
  const incoming = {
    id: "x:y",
    body: "",
    from: "alice@s.whatsapp.net",
    to: "me",
    timestamp: 0,
    fromMe: false,
    hasMedia: false,
    type: "conversation",
  };
  const outgoing = { ...incoming, from: "me", fromMe: true };

  it("returns mapped.from verbatim for incoming messages", () => {
    expect(resolveStoredSender(incoming, "447700900111@s.whatsapp.net")).toBe(
      "alice@s.whatsapp.net",
    );
  });

  it("substitutes the bot's own JID for outgoing messages when known", () => {
    expect(resolveStoredSender(outgoing, "447700900111@s.whatsapp.net")).toBe(
      "447700900111@s.whatsapp.net",
    );
  });

  it("falls back to 'me' when ownJid is not yet known", () => {
    expect(resolveStoredSender(outgoing, null)).toBe("me");
  });
});

describe("mapContact", () => {
  it("flags group JIDs as group", () => {
    const c = mapContact({ id: "12345@g.us", name: "Group" });
    expect(c.isGroup).toBe(true);
    expect(c.isUser).toBe(false);
    expect(c.isWAContact).toBe(false);
  });

  it("derives number from JID when no phoneNumber provided", () => {
    const c = mapContact({ id: "447700900111@s.whatsapp.net" });
    expect(c.number).toBe("447700900111");
    expect(c.isGroup).toBe(false);
  });

  it("strips non-digits from phoneNumber objects", () => {
    const c = mapContact({
      id: "x@s.whatsapp.net",
      phoneNumber: { user: "+44 77 0090-0222" },
    });
    expect(c.number).toBe("447700900222");
  });

  it("prefers name over verifiedName over notify", () => {
    expect(
      mapContact({
        id: "x@s.whatsapp.net",
        name: "A",
        verifiedName: "B",
        notify: "C",
      }).name,
    ).toBe("A");
    expect(
      mapContact({ id: "x@s.whatsapp.net", verifiedName: "B", notify: "C" })
        .name,
    ).toBe("B");
    expect(mapContact({ id: "x@s.whatsapp.net", notify: "C" }).name).toBe("C");
  });
});
