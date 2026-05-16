import { log } from "../utils/logger.js";
import { ConcurrencyQueue } from "../utils/concurrency-queue.js";
import { MessageIndexStore } from "./message-index-store.js";
import type { StoreService } from "../providers/store/store-service.js";

export interface AutoDownloadDeps {
  storeService: StoreService | null;
  downloadMedia: (messageId: string) => Promise<unknown>;
}

export interface AutoDownloadOptions {
  enabled?: boolean;
  maxBytes?: number | null;
  concurrency?: number;
  maxQueued?: number;
}

export type MediaPayloadType =
  | "image"
  | "video"
  | "audio"
  | "document"
  | "sticker";

export class AutoDownloadManager {
  private readonly queue: ConcurrencyQueue;
  private readonly enabled: boolean;
  private readonly maxBytes: number | null;

  constructor(
    private readonly deps: AutoDownloadDeps,
    options: AutoDownloadOptions = {},
  ) {
    this.enabled = options.enabled ?? false;
    this.maxBytes =
      options.maxBytes && options.maxBytes > 0 ? options.maxBytes : null;
    this.queue = new ConcurrencyQueue({
      concurrency: Math.max(1, options.concurrency ?? 3),
      maxQueued: Math.max(0, options.maxQueued ?? 200),
      onDrop: (queueSize) =>
        log.warn(
          { queueSize },
          "Auto-download backlog overflow; dropped oldest pending job",
        ),
    });
  }

  static extractMediaPayload(
    message: any,
  ): { type: MediaPayloadType; content: any } | null {
    if (!message) return null;
    if (message.imageMessage)
      return { type: "image", content: message.imageMessage };
    if (message.videoMessage)
      return { type: "video", content: message.videoMessage };
    if (message.audioMessage)
      return { type: "audio", content: message.audioMessage };
    if (message.documentMessage)
      return { type: "document", content: message.documentMessage };
    if (message.stickerMessage)
      return { type: "sticker", content: message.stickerMessage };
    return null;
  }

  maybeProcess(msg: any): void {
    if (!this.enabled) return;
    if (!msg?.message) return;
    const jid = msg?.key?.remoteJid;
    if (jid === "status@broadcast") return;

    const payload = AutoDownloadManager.extractMediaPayload(msg.message);
    if (!payload) return;

    const messageId = MessageIndexStore.serializeMessageId(msg);
    if (this.deps.storeService?.getMediaByMessageId(messageId)) return;

    const rawSize =
      payload.content?.fileLength ??
      payload.content?.fileSize ??
      payload.content?.fileLength?.toNumber?.() ??
      payload.content?.fileSize?.toNumber?.();
    const size = rawSize ? Number(rawSize) : null;
    if (this.maxBytes && size && size > this.maxBytes) {
      log.info(
        { messageId, size, maxBytes: this.maxBytes },
        "Skipping auto-download: media too large",
      );
      return;
    }

    this.queue.enqueue(async () => {
      try {
        await this.deps.downloadMedia(messageId);
      } catch (error) {
        log.warn({ err: error, messageId }, "Auto-download media failed");
      }
    });
  }

  getStats(): { inFlight: number; queued: number; enabled: boolean } {
    return {
      inFlight: this.queue.inFlight,
      queued: this.queue.queued,
      enabled: this.enabled,
    };
  }
}
