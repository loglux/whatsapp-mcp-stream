import { describe, expect, it } from "vitest";
import { MessageIndexStore } from "../../src/services/message-index-store.js";

function fakeMsg(jid: string, id: string, extra: object = {}) {
  return {
    key: { remoteJid: jid, id },
    message: { conversation: `text ${id}` },
    ...extra,
  };
}

describe("MessageIndexStore.serializeMessageId / serializeKeyId", () => {
  it("uses remoteJid:id", () => {
    expect(MessageIndexStore.serializeMessageId(fakeMsg("a@s", "AAA"))).toBe(
      "a@s:AAA",
    );
    expect(
      MessageIndexStore.serializeKeyId({ remoteJid: "a@s", id: "AAA" }),
    ).toBe("a@s:AAA");
  });

  it("falls back to remoteJidAlt then 'unknown'", () => {
    expect(
      MessageIndexStore.serializeKeyId({
        remoteJidAlt: "alt@s",
        id: "BBB",
      }),
    ).toBe("alt@s:BBB");
    expect(MessageIndexStore.serializeKeyId({})).toBe("unknown:unknown");
  });
});

describe("MessageIndexStore add / getById / getByKeyId", () => {
  it("indexes by canonical id and by keyId", () => {
    const store = new MessageIndexStore();
    const msg = fakeMsg("a@s", "AAA");
    store.add(msg);
    expect(store.getById("a@s:AAA")).toBe(msg);
    expect(store.getByKeyId("AAA")).toBe(msg);
    expect(store.hasId("a@s:AAA")).toBe(true);
  });

  it("indexes by chat and respects per-chat cap (FIFO)", () => {
    const store = new MessageIndexStore({ maxPerChat: 2 });
    const a = fakeMsg("a@s", "A");
    const b = fakeMsg("a@s", "B");
    const c = fakeMsg("a@s", "C");
    store.add(a);
    store.add(b);
    store.add(c);
    expect(store.listByChat("a@s").map((m) => m.key.id)).toEqual(["B", "C"]);
  });

  it("evicts oldest from the global index when maxIndexSize is reached", () => {
    const store = new MessageIndexStore({ maxIndexSize: 2 });
    store.add(fakeMsg("a@s", "A"));
    store.add(fakeMsg("b@s", "B"));
    store.add(fakeMsg("c@s", "C"));
    expect(store.messageCount).toBe(2);
    expect(store.getById("a@s:A")).toBeUndefined();
    expect(store.getById("b@s:B")).toBeDefined();
    expect(store.getById("c@s:C")).toBeDefined();
  });

  it("messageCount and chatCount reflect the current state", () => {
    const store = new MessageIndexStore();
    store.add(fakeMsg("a@s", "A"));
    store.add(fakeMsg("a@s", "B"));
    store.add(fakeMsg("b@s", "C"));
    expect(store.messageCount).toBe(3);
    expect(store.chatCount).toBe(2);
    expect(store.chatJids().sort()).toEqual(["a@s", "b@s"]);
  });

  it("messages() iterates over the global index", () => {
    const store = new MessageIndexStore();
    store.add(fakeMsg("a@s", "A"));
    store.add(fakeMsg("b@s", "B"));
    const ids = Array.from(store.messages()).map((m) => m.key.id);
    expect(ids.sort()).toEqual(["A", "B"]);
  });
});

describe("MessageIndexStore updateById", () => {
  it("returns the merged value and persists it", () => {
    const store = new MessageIndexStore();
    store.add(fakeMsg("a@s", "A"));
    const merged = store.updateById("a@s:A", (existing) => ({
      ...existing,
      reactions: [{ emoji: "👍" }],
    }));
    expect(merged.reactions).toEqual([{ emoji: "👍" }]);
    expect(store.getById("a@s:A").reactions).toEqual([{ emoji: "👍" }]);
  });

  it("returns undefined when the id is not indexed and does not insert it", () => {
    const store = new MessageIndexStore();
    const result = store.updateById("missing", () => ({ x: 1 }));
    expect(result).toBeUndefined();
    expect(store.hasId("missing")).toBe(false);
  });
});

describe("MessageIndexStore deleteByKey / deleteByChat", () => {
  it("deleteByKey removes the entry from all three indices", () => {
    const store = new MessageIndexStore();
    store.add(fakeMsg("a@s", "A"));
    store.add(fakeMsg("a@s", "B"));
    store.deleteByKey({ remoteJid: "a@s", id: "A" });
    expect(store.getById("a@s:A")).toBeUndefined();
    expect(store.getByKeyId("A")).toBeUndefined();
    expect(store.listByChat("a@s").map((m) => m.key.id)).toEqual(["B"]);
  });

  it("deleteByKey is a no-op when the key has missing fields", () => {
    const store = new MessageIndexStore();
    store.add(fakeMsg("a@s", "A"));
    store.deleteByKey({ remoteJid: "a@s" });
    store.deleteByKey({ id: "A" });
    store.deleteByKey(null);
    expect(store.hasId("a@s:A")).toBe(true);
  });

  it("deleteByChat clears the per-chat list", () => {
    const store = new MessageIndexStore();
    store.add(fakeMsg("a@s", "A"));
    store.add(fakeMsg("b@s", "B"));
    store.deleteByChat("a@s");
    expect(store.listByChat("a@s")).toEqual([]);
    expect(store.chatJids()).toEqual(["b@s"]);
  });
});
