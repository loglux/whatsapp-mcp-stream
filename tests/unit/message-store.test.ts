import { afterEach, beforeEach, describe, expect, it } from "vitest";
import fs from "fs";
import os from "os";
import path from "path";
import {
  MessageStore,
  type StoredIdempotencyRecord,
} from "../../src/storage/message-store.js";

let tmpDir: string;
let store: MessageStore;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "wa-mcp-test-"));
  store = new MessageStore(path.join(tmpDir, "store.sqlite"));
});

afterEach(() => {
  store?.close();
  if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe("MessageStore migrations", () => {
  it("creates schema with empty stats", () => {
    expect(store.stats()).toEqual({
      chats: 0,
      messages: 0,
      media: 0,
      contacts: 0,
    });
  });
});

describe("MessageStore chats", () => {
  it("upserts and lists chats", () => {
    store.upsertChat({
      id: "x@s.whatsapp.net",
      name: "Alice",
      is_group: 0,
      unread_count: 0,
      timestamp: 100,
    });
    const chats = store.listChats(10);
    expect(chats).toHaveLength(1);
    expect(chats[0].id).toBe("x@s.whatsapp.net");
    expect(chats[0].name).toBe("Alice");
  });

  it("preserves the higher timestamp on conflict", () => {
    store.upsertChat({
      id: "x@s.whatsapp.net",
      name: "Alice",
      is_group: 0,
      unread_count: 0,
      timestamp: 200,
    });
    store.upsertChat({
      id: "x@s.whatsapp.net",
      name: "Alice",
      is_group: 0,
      unread_count: 0,
      timestamp: 100,
    });
    expect(store.getChatById("x@s.whatsapp.net")?.timestamp).toBe(200);
  });
});

describe("MessageStore messages", () => {
  const sample = {
    id: "x@s.whatsapp.net:AAA",
    chat_jid: "x@s.whatsapp.net",
    from: "x@s.whatsapp.net",
    to: "me",
    timestamp: 1700000000,
    from_me: 0,
    body: "hello world",
    has_media: 0,
    type: "conversation",
  };

  it("upsert + getMessageById roundtrip preserves body and timestamp", () => {
    store.upsertMessage(sample);
    const got = store.getMessageById(sample.id);
    expect(got?.body).toBe("hello world");
    expect(got?.timestamp).toBe(1700000000);
    expect(got?.type).toBe("conversation");
  });

  it("INSERT OR IGNORE does not overwrite body on duplicate id", () => {
    store.upsertMessage(sample);
    store.upsertMessage({ ...sample, body: "changed" });
    expect(store.getMessageById(sample.id)?.body).toBe("hello world");
  });

  it("updateMessageContent returns the number of changed rows", () => {
    store.upsertMessage(sample);
    expect(store.updateMessageContent(sample.id, "edited", 0, "conversation")).toBe(1);
    expect(store.updateMessageContent("missing", "x", 0, "conversation")).toBe(0);
    expect(store.getMessageById(sample.id)?.body).toBe("edited");
  });

  it("searchMessages finds substring matches", () => {
    store.upsertMessage(sample);
    store.upsertMessage({ ...sample, id: "x:BBB", body: "no match here" });
    const hits = store.searchMessages("hello", 10);
    expect(hits.map((m) => m.id)).toEqual(["x@s.whatsapp.net:AAA"]);
  });

  it("listMessages orders by timestamp DESC and limits", () => {
    for (let i = 0; i < 3; i++) {
      store.upsertMessage({ ...sample, id: `x:${i}`, timestamp: i });
    }
    const result = store.listMessages("x@s.whatsapp.net", 2);
    expect(result.map((m) => m.id)).toEqual(["x:2", "x:1"]);
  });

  it("exposes from/to (aliased over sender/recipient columns)", () => {
    store.upsertMessage(sample);
    const row = store.getMessageById(sample.id);
    expect(row?.from).toBe("x@s.whatsapp.net");
    expect(row?.to).toBe("me");
  });
});

describe("MessageStore contacts and LID mapping", () => {
  it("upsertContact uses COALESCE to preserve fields when new value is null", () => {
    store.upsertContact({
      jid: "x@s.whatsapp.net",
      name: "Alice",
      pushname: null,
      number: "447700900111",
      is_group: 0,
      is_my_contact: 1,
      updated_at: 100,
    });
    store.upsertContact({
      jid: "x@s.whatsapp.net",
      name: null,
      pushname: "Ally",
      number: null,
      is_group: null,
      is_my_contact: null,
      updated_at: 200,
    });
    const got = store.getContactById("x@s.whatsapp.net");
    expect(got?.name).toBe("Alice");
    expect(got?.pushname).toBe("Ally");
    expect(got?.number).toBe("447700900111");
    expect(got?.updated_at).toBe(200);
  });

  it("resolves LID mappings by JID and by raw number", () => {
    store.upsertLidMapping({
      lid_jid: "abc@lid",
      pn_jid: "447700900111@s.whatsapp.net",
      pn_number: "447700900111",
      updated_at: 1,
    });
    expect(store.getLidForPn("447700900111@s.whatsapp.net")).toBe("abc@lid");
    expect(store.getLidForPn("447700900111")).toBe("abc@lid");
    expect(store.getPnForLid("abc@lid")).toEqual({
      pnJid: "447700900111@s.whatsapp.net",
      pnNumber: "447700900111",
    });
    expect(store.getLidForPn("unknown")).toBeNull();
  });
});

describe("MessageStore idempotency", () => {
  const record: StoredIdempotencyRecord = {
    key: "abc",
    operation: "send_message",
    scope_jid: "x@s.whatsapp.net",
    request_fingerprint: "fp1",
    response_json: '{"ok":true}',
    message_id: "x@s.whatsapp.net:M1",
    created_at: 1000,
    expires_at: 2000,
  };

  it("roundtrips through upsert + get", () => {
    store.upsertIdempotencyRecord(record);
    const got = store.getIdempotencyRecord("abc");
    expect(got).toEqual(record);
  });

  it("deleteExpiredIdempotencyRecords returns the number deleted", () => {
    store.upsertIdempotencyRecord(record);
    store.upsertIdempotencyRecord({
      ...record,
      key: "def",
      expires_at: 5000,
    });
    expect(store.deleteExpiredIdempotencyRecords(3000)).toBe(1);
    expect(store.getIdempotencyRecord("abc")).toBeNull();
    expect(store.getIdempotencyRecord("def")).not.toBeNull();
  });
});
