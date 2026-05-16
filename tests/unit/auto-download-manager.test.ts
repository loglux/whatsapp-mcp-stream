import { describe, it, expect, vi } from "vitest";
import { AutoDownloadManager } from "../../src/services/auto-download-manager.js";
import type { StoreService } from "../../src/providers/store/store-service.js";

const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

function mediaMsg(opts: {
  id?: string;
  jid?: string;
  type?: "image" | "video" | "audio" | "document" | "sticker";
  size?: number;
}) {
  const type = opts.type ?? "image";
  return {
    key: {
      remoteJid: opts.jid ?? "x@s.whatsapp.net",
      id: opts.id ?? "AAA",
    },
    message: {
      [`${type}Message`]: { fileLength: opts.size },
    },
  };
}

function fakeStore(hasMediaIds: string[] = []): {
  service: StoreService;
  calls: string[];
} {
  const calls: string[] = [];
  const service = {
    getMediaByMessageId(id: string) {
      calls.push(id);
      return hasMediaIds.includes(id) ? ({} as unknown as object) : null;
    },
  } as unknown as StoreService;
  return { service, calls };
}

describe("AutoDownloadManager.extractMediaPayload", () => {
  it("returns the matching payload for each media subtype", () => {
    for (const type of [
      "image",
      "video",
      "audio",
      "document",
      "sticker",
    ] as const) {
      const r = AutoDownloadManager.extractMediaPayload({
        [`${type}Message`]: { foo: 1 },
      });
      expect(r).toEqual({ type, content: { foo: 1 } });
    }
  });

  it("returns null for plain text or null input", () => {
    expect(
      AutoDownloadManager.extractMediaPayload({ conversation: "hi" }),
    ).toBeNull();
    expect(AutoDownloadManager.extractMediaPayload(null)).toBeNull();
  });
});

describe("AutoDownloadManager.maybeProcess", () => {
  it("is a no-op when disabled", () => {
    const downloadMedia = vi.fn(async () => undefined);
    const mgr = new AutoDownloadManager(
      { storeService: null, downloadMedia },
      { enabled: false },
    );
    mgr.maybeProcess(mediaMsg({}));
    expect(downloadMedia).not.toHaveBeenCalled();
  });

  it("skips status broadcasts", () => {
    const downloadMedia = vi.fn(async () => undefined);
    const mgr = new AutoDownloadManager(
      { storeService: null, downloadMedia },
      { enabled: true },
    );
    mgr.maybeProcess(mediaMsg({ jid: "status@broadcast" }));
    expect(downloadMedia).not.toHaveBeenCalled();
  });

  it("skips messages with no media payload", () => {
    const downloadMedia = vi.fn(async () => undefined);
    const mgr = new AutoDownloadManager(
      { storeService: null, downloadMedia },
      { enabled: true },
    );
    mgr.maybeProcess({
      key: { remoteJid: "x@s.whatsapp.net", id: "AAA" },
      message: { conversation: "hi" },
    });
    expect(downloadMedia).not.toHaveBeenCalled();
  });

  it("skips messages whose media is already downloaded", () => {
    const { service } = fakeStore(["x@s.whatsapp.net:AAA"]);
    const downloadMedia = vi.fn(async () => undefined);
    const mgr = new AutoDownloadManager(
      { storeService: service, downloadMedia },
      { enabled: true },
    );
    mgr.maybeProcess(mediaMsg({}));
    expect(downloadMedia).not.toHaveBeenCalled();
  });

  it("skips media over the configured size limit", () => {
    const downloadMedia = vi.fn(async () => undefined);
    const mgr = new AutoDownloadManager(
      { storeService: null, downloadMedia },
      { enabled: true, maxBytes: 100 },
    );
    mgr.maybeProcess(mediaMsg({ size: 999 }));
    expect(downloadMedia).not.toHaveBeenCalled();
  });

  it("enqueues a download for eligible messages", async () => {
    const downloadMedia = vi.fn(async () => undefined);
    const mgr = new AutoDownloadManager(
      { storeService: null, downloadMedia },
      { enabled: true, concurrency: 1 },
    );
    mgr.maybeProcess(mediaMsg({ size: 50 }));
    await tick();
    await tick();
    expect(downloadMedia).toHaveBeenCalledWith("x@s.whatsapp.net:AAA");
  });

  it("swallows download errors without breaking the queue", async () => {
    const downloadMedia = vi.fn(async () => {
      throw new Error("network");
    });
    const mgr = new AutoDownloadManager(
      { storeService: null, downloadMedia },
      { enabled: true, concurrency: 1 },
    );
    mgr.maybeProcess(mediaMsg({ id: "A" }));
    mgr.maybeProcess(mediaMsg({ id: "B" }));
    await tick();
    await tick();
    await tick();
    expect(downloadMedia).toHaveBeenCalledTimes(2);
  });
});

describe("AutoDownloadManager.getStats", () => {
  it("reports enabled flag and queue depths", async () => {
    const downloadMedia = vi.fn(
      () => new Promise<void>(() => {}), // never resolves
    );
    const mgr = new AutoDownloadManager(
      { storeService: null, downloadMedia },
      { enabled: true, concurrency: 1 },
    );
    mgr.maybeProcess(mediaMsg({ id: "A" }));
    mgr.maybeProcess(mediaMsg({ id: "B" }));
    await tick();
    const stats = mgr.getStats();
    expect(stats.enabled).toBe(true);
    expect(stats.inFlight).toBe(1);
    expect(stats.queued).toBe(1);
  });
});
