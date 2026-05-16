import { describe, it, expect, vi } from "vitest";
import { StorageWriter } from "../../src/services/storage-writer.js";
import type { StoreService } from "../../src/providers/store/store-service.js";
import type {
  StoredChat,
  StoredContact,
  StoredGroupMeta,
  StoredGroupParticipant,
  StoredMessage,
} from "../../src/storage/message-store.js";

interface MockStore {
  service: StoreService;
  upsertMessage: ReturnType<typeof vi.fn<(m: StoredMessage) => void>>;
  updateMessageContent: ReturnType<
    typeof vi.fn<(...args: unknown[]) => number>
  >;
  upsertChat: ReturnType<typeof vi.fn<(c: StoredChat) => void>>;
  upsertContact: ReturnType<typeof vi.fn<(c: StoredContact) => void>>;
  upsertGroupMeta: ReturnType<typeof vi.fn<(m: StoredGroupMeta) => void>>;
  replaceGroupParticipants: ReturnType<
    typeof vi.fn<(jid: string, p: StoredGroupParticipant[]) => void>
  >;
}

function mockStore(): MockStore {
  const upsertMessage = vi.fn<(m: StoredMessage) => void>();
  const updateMessageContent = vi.fn<(...args: unknown[]) => number>();
  const upsertChat = vi.fn<(c: StoredChat) => void>();
  const upsertContact = vi.fn<(c: StoredContact) => void>();
  const upsertGroupMeta = vi.fn<(m: StoredGroupMeta) => void>();
  const replaceGroupParticipants =
    vi.fn<(jid: string, p: StoredGroupParticipant[]) => void>();
  return {
    service: {
      upsertMessage,
      updateMessageContent,
      upsertChat,
      upsertContact,
      upsertGroupMeta,
      replaceGroupParticipants,
    } as unknown as StoreService,
    upsertMessage,
    updateMessageContent,
    upsertChat,
    upsertContact,
    upsertGroupMeta,
    replaceGroupParticipants,
  };
}

function buildWriter(
  store: StoreService | null,
  ownJid: string | null = null,
): StorageWriter {
  return new StorageWriter({ storeService: store, getOwnJid: () => ownJid });
}

describe("StorageWriter.upsertMessage", () => {
  it("is a no-op when storeService is null", () => {
    const writer = buildWriter(null);
    expect(() =>
      writer.upsertMessage({
        key: { remoteJid: "x@s", id: "A" },
        message: { conversation: "hi" },
      }),
    ).not.toThrow();
  });

  it("substitutes the bot's own JID for outgoing messages", () => {
    const store = mockStore();
    const writer = buildWriter(store.service, "447@s.whatsapp.net");
    writer.upsertMessage({
      key: { remoteJid: "x@s.whatsapp.net", id: "A", fromMe: true },
      message: { conversation: "hi" },
      messageTimestamp: 100,
    });
    expect(store.upsertMessage).toHaveBeenCalledTimes(1);
    const arg = store.upsertMessage.mock.calls[0][0];
    expect(arg.from).toBe("447@s.whatsapp.net");
    expect(arg.from_me).toBe(1);
    expect(arg.body).toBe("hi");
    expect(arg.timestamp).toBe(100_000);
  });

  it("keeps the participant for incoming group messages", () => {
    const store = mockStore();
    const writer = buildWriter(store.service, "447@s.whatsapp.net");
    writer.upsertMessage({
      key: {
        remoteJid: "group@g.us",
        id: "A",
        fromMe: false,
        participant: "alice@s.whatsapp.net",
      },
      message: { conversation: "yo" },
    });
    const arg = store.upsertMessage.mock.calls[0][0];
    expect(arg.from).toBe("alice@s.whatsapp.net");
    expect(arg.from_me).toBe(0);
  });
});

describe("StorageWriter.updateMessageContent", () => {
  it("returns early when storeService is null", () => {
    const writer = buildWriter(null);
    expect(() =>
      writer.updateMessageContent({ key: { remoteJid: "x", id: "y" } }),
    ).not.toThrow();
  });

  it("does not upsert when an existing row was updated", () => {
    const store = mockStore();
    store.updateMessageContent.mockReturnValue(1);
    const writer = buildWriter(store.service);
    writer.updateMessageContent({
      key: { remoteJid: "x@s", id: "A" },
      message: { conversation: "edited" },
    });
    expect(store.updateMessageContent).toHaveBeenCalledTimes(1);
    expect(store.upsertMessage).not.toHaveBeenCalled();
  });

  it("falls back to upsert when no existing row was updated", () => {
    const store = mockStore();
    store.updateMessageContent.mockReturnValue(0);
    const writer = buildWriter(store.service);
    writer.updateMessageContent({
      key: { remoteJid: "x@s", id: "A" },
      message: { conversation: "fresh" },
    });
    expect(store.upsertMessage).toHaveBeenCalledTimes(1);
  });
});

describe("StorageWriter.upsertChat", () => {
  it("requires an id and skips otherwise", () => {
    const store = mockStore();
    const writer = buildWriter(store.service);
    writer.upsertChat({});
    expect(store.upsertChat).not.toHaveBeenCalled();
  });

  it("converts seconds timestamp to milliseconds and flags groups", () => {
    const store = mockStore();
    const writer = buildWriter(store.service);
    writer.upsertChat({
      id: "g@g.us",
      subject: "Group",
      conversationTimestamp: 1700,
      unreadCount: 4,
    });
    const arg = store.upsertChat.mock.calls[0][0];
    expect(arg.id).toBe("g@g.us");
    expect(arg.name).toBe("Group");
    expect(arg.is_group).toBe(1);
    expect(arg.timestamp).toBe(1_700_000);
    expect(arg.unread_count).toBe(4);
  });

  it("supports Long-like timestamps via toNumber()", () => {
    const store = mockStore();
    const writer = buildWriter(store.service);
    writer.upsertChat({
      id: "x@s.whatsapp.net",
      conversationTimestamp: { toNumber: () => 1234 },
    });
    expect(store.upsertChat.mock.calls[0][0].timestamp).toBe(1_234_000);
  });
});

describe("StorageWriter.upsertContact", () => {
  it("is a no-op for null contact", () => {
    const store = mockStore();
    const writer = buildWriter(store.service);
    writer.upsertContact(null);
    expect(store.upsertContact).not.toHaveBeenCalled();
  });

  it("maps id-derived number when phoneNumber is missing", () => {
    const store = mockStore();
    const writer = buildWriter(store.service);
    writer.upsertContact({
      id: "447700900111@s.whatsapp.net",
      name: "Alice",
    });
    const arg = store.upsertContact.mock.calls[0][0];
    expect(arg.jid).toBe("447700900111@s.whatsapp.net");
    expect(arg.name).toBe("Alice");
    expect(arg.number).toBe("447700900111");
    expect(arg.is_group).toBe(0);
  });
});

describe("StorageWriter.persistGroupMetadata", () => {
  it("writes meta and participants in one call", () => {
    const store = mockStore();
    const writer = buildWriter(store.service);
    writer.persistGroupMetadata({
      id: "g@g.us",
      subject: "Team",
      owner: "owner@s.whatsapp.net",
      size: 3,
      creation: 100,
      participants: [
        { id: "a@s.whatsapp.net", admin: "admin" },
        { id: "b@s.whatsapp.net", admin: null },
      ],
    });
    expect(store.upsertGroupMeta).toHaveBeenCalledTimes(1);
    expect(store.upsertGroupMeta.mock.calls[0][0]).toMatchObject({
      jid: "g@g.us",
      subject: "Team",
      owner: "owner@s.whatsapp.net",
      size: 3,
    });
    expect(store.replaceGroupParticipants).toHaveBeenCalledWith(
      "g@g.us",
      expect.arrayContaining([
        expect.objectContaining({ participant_jid: "a@s.whatsapp.net" }),
        expect.objectContaining({ participant_jid: "b@s.whatsapp.net" }),
      ]),
    );
  });

  it("skips participants when the list is empty", () => {
    const store = mockStore();
    const writer = buildWriter(store.service);
    writer.persistGroupMetadata({ id: "g@g.us", subject: "Empty" });
    expect(store.upsertGroupMeta).toHaveBeenCalledTimes(1);
    expect(store.replaceGroupParticipants).not.toHaveBeenCalled();
  });

  it("ignores null metadata or metadata without id", () => {
    const store = mockStore();
    const writer = buildWriter(store.service);
    writer.persistGroupMetadata(null);
    writer.persistGroupMetadata({ subject: "no-id" });
    expect(store.upsertGroupMeta).not.toHaveBeenCalled();
  });
});
