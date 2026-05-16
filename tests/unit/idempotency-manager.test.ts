import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { IdempotencyManager } from "../../src/services/idempotency-manager.js";
import type {
  StoreService,
  StoredIdempotencyRecord,
} from "../../src/providers/store/store-service.js";

class MockStore {
  records = new Map<string, StoredIdempotencyRecord>();

  getIdempotencyRecord(key: string): StoredIdempotencyRecord | null {
    return this.records.get(key) ?? null;
  }

  upsertIdempotencyRecord(record: StoredIdempotencyRecord): void {
    this.records.set(record.key, { ...record });
  }
}

function mockedStore(): { store: MockStore; service: StoreService } {
  const store = new MockStore();
  return { store, service: store as unknown as StoreService };
}

describe("IdempotencyManager fingerprint helpers", () => {
  const mgr = new IdempotencyManager(null);

  it("buildSendDedupKey joins jid and message with a newline", () => {
    expect(mgr.buildSendDedupKey("a@s", "hello")).toBe("a@s\nhello");
  });

  it("buildRequestFingerprint is deterministic for identical inputs", () => {
    const a = mgr.buildRequestFingerprint("a@s", "hello");
    const b = mgr.buildRequestFingerprint("a@s", "hello");
    expect(a).toBe(b);
    expect(a).toHaveLength(64);
  });

  it("buildRequestFingerprint differs for different inputs", () => {
    const a = mgr.buildRequestFingerprint("a@s", "hello");
    const b = mgr.buildRequestFingerprint("a@s", "hello world");
    expect(a).not.toBe(b);
  });
});

describe("IdempotencyManager send dedup window", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("returns null when no prior send is remembered", () => {
    const mgr = new IdempotencyManager(null);
    expect(mgr.getRecentSendResult("x@s", "hi")).toBeNull();
  });

  it("returns a marked-deduplicated result when within the window", () => {
    const mgr = new IdempotencyManager(null, { sendDedupWindowMs: 45000 });
    mgr.rememberSendResult("x@s", "hi", {
      key: { remoteJid: "x@s", id: "AAA" },
      messageTimestamp: 1,
    });
    const dup = mgr.getRecentSendResult("x@s", "hi");
    expect(dup.__deduplicated).toBe(true);
    expect(dup.__originalMessageId).toBe("x@s:AAA");
    expect(dup.messageTimestamp).toBe(1);
  });

  it("returns null and clears the entry once the window expires", () => {
    const mgr = new IdempotencyManager(null, { sendDedupWindowMs: 1000 });
    mgr.rememberSendResult("x@s", "hi", { key: { remoteJid: "x@s", id: "A" } });
    vi.advanceTimersByTime(2000);
    expect(mgr.getRecentSendResult("x@s", "hi")).toBeNull();
    // After expiry, a fresh remember/get cycle works again.
    mgr.rememberSendResult("x@s", "hi", { key: { remoteJid: "x@s", id: "B" } });
    expect(mgr.getRecentSendResult("x@s", "hi")?.__originalMessageId).toBe(
      "x@s:B",
    );
  });

  it("is a no-op when sendDedupWindowMs is 0", () => {
    const mgr = new IdempotencyManager(null, { sendDedupWindowMs: 0 });
    mgr.rememberSendResult("x@s", "hi", { key: { remoteJid: "x@s", id: "A" } });
    expect(mgr.getRecentSendResult("x@s", "hi")).toBeNull();
  });
});

describe("IdempotencyManager executeIdempotent", () => {
  it("runs the action verbatim when no idempotencyKey is provided", async () => {
    const { service } = mockedStore();
    const mgr = new IdempotencyManager(service);
    const result = await mgr.executeIdempotent(
      "send_message",
      "fp",
      async () => ({
        ok: true,
      }),
    );
    expect(result).toEqual({ ok: true });
  });

  it("persists the first call and replays a marked-deduplicated result on the second", async () => {
    const { service } = mockedStore();
    const mgr = new IdempotencyManager(service);
    const action = vi.fn(async () => ({
      key: { remoteJid: "x@s", id: "AAA" },
      ok: true,
    }));
    const first = await mgr.executeIdempotent("send_message", "fp", action, {
      idempotencyKey: "k1",
      scopeJid: "x@s",
    });
    const second = await mgr.executeIdempotent("send_message", "fp", action, {
      idempotencyKey: "k1",
      scopeJid: "x@s",
    });
    expect(action).toHaveBeenCalledTimes(1);
    expect(first).toMatchObject({ ok: true });
    expect(first.__deduplicated).not.toBe(true);
    expect(second.__deduplicated).toBe(true);
    expect(second.__originalMessageId).toBe("x@s:AAA");
  });

  it("throws when the same key is reused for a different operation", async () => {
    const { service } = mockedStore();
    const mgr = new IdempotencyManager(service);
    await mgr.executeIdempotent(
      "send_message",
      "fp",
      async () => ({ ok: true }),
      {
        idempotencyKey: "k1",
      },
    );
    await expect(
      mgr.executeIdempotent("logout", "fp", async () => ({ ok: true }), {
        idempotencyKey: "k1",
      }),
    ).rejects.toThrow(/already used for send_message/);
  });

  it("throws when the same key is reused with a different request fingerprint", async () => {
    const { service } = mockedStore();
    const mgr = new IdempotencyManager(service);
    await mgr.executeIdempotent(
      "send_message",
      "fp-a",
      async () => ({ ok: true }),
      { idempotencyKey: "k1" },
    );
    await expect(
      mgr.executeIdempotent(
        "send_message",
        "fp-b",
        async () => ({ ok: true }),
        { idempotencyKey: "k1" },
      ),
    ).rejects.toThrow(/different send_message parameters/);
  });

  it("ignores expired stored records and re-runs the action", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));
    try {
      const { service } = mockedStore();
      const mgr = new IdempotencyManager(service, {
        idempotencyTtlMs: 60_000,
      });
      const action = vi.fn(async () => ({ ok: true }));
      await mgr.executeIdempotent("send_message", "fp", action, {
        idempotencyKey: "k1",
      });
      vi.advanceTimersByTime(120_000);
      await mgr.executeIdempotent("send_message", "fp", action, {
        idempotencyKey: "k1",
      });
      expect(action).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe("IdempotencyManager executeSendWithDedup", () => {
  it("runs the operation when nothing is in-flight", async () => {
    const mgr = new IdempotencyManager(null);
    const op = vi.fn(async () => ({ key: { remoteJid: "x@s", id: "A" } }));
    const result = await mgr.executeSendWithDedup("k", "x@s", "hi", op);
    expect(result.key.id).toBe("A");
    expect(op).toHaveBeenCalledTimes(1);
  });

  it("joins an in-flight request and returns a marked-deduplicated result", async () => {
    const mgr = new IdempotencyManager(null);
    let release!: (v: any) => void;
    const op = vi.fn(
      () =>
        new Promise((resolve) => {
          release = resolve;
        }),
    );
    const firstPromise = mgr.executeSendWithDedup("k", "x@s", "hi", op);
    const secondPromise = mgr.executeSendWithDedup("k", "x@s", "hi", vi.fn());
    release({ key: { remoteJid: "x@s", id: "A" } });
    const [first, second] = await Promise.all([firstPromise, secondPromise]);
    expect(op).toHaveBeenCalledTimes(1);
    expect(first.__deduplicated).not.toBe(true);
    expect(second.__deduplicated).toBe(true);
    expect(second.__originalMessageId).toBe("x@s:A");
  });

  it("clears the in-flight slot on success so a later call can proceed", async () => {
    const mgr = new IdempotencyManager(null);
    const op = vi.fn(async () => ({ key: { remoteJid: "x@s", id: "A" } }));
    await mgr.executeSendWithDedup("k", "x@s", "hi", op);
    await mgr.executeSendWithDedup("k", "x@s", "hi", op);
    expect(op).toHaveBeenCalledTimes(2);
  });

  it("clears the in-flight slot on error so a later call can retry", async () => {
    const mgr = new IdempotencyManager(null);
    const failing = vi.fn(async () => {
      throw new Error("boom");
    });
    await expect(
      mgr.executeSendWithDedup("k", "x@s", "hi", failing),
    ).rejects.toThrow("boom");
    const succeeding = vi.fn(async () => ({
      key: { remoteJid: "x@s", id: "A" },
    }));
    const result = await mgr.executeSendWithDedup("k", "x@s", "hi", succeeding);
    expect(result.key.id).toBe("A");
  });
});
