import { createRequire } from "module";
import { webcrypto } from "crypto";
import fs from "fs";
import path from "path";
import pino from "pino";
import { log } from "../../utils/logger.js";

const require = createRequire(import.meta.url);
const baileys = require("@whiskeysockets/baileys");

const {
  fetchLatestBaileysVersion,
  useMultiFileAuthState,
  makeInMemoryStore,
} = baileys;

export class BaileysClient {
  private sock: any | null = null;
  private store: any | null = null;
  private storePath: string | null = null;
  private storeFlushTimer: NodeJS.Timeout | null = null;
  private sessionDir: string;

  constructor(sessionDir: string) {
    this.sessionDir = sessionDir;
    if (!(globalThis as any).crypto) {
      (globalThis as any).crypto = webcrypto;
    }
  }

  async initialize(getMessage?: (key: any) => Promise<any> | any): Promise<void> {
    fs.mkdirSync(this.sessionDir, { recursive: true });
    const { state, saveCreds } = await useMultiFileAuthState(this.sessionDir);

    const { version } = await fetchLatestBaileysVersion();
    const logLevel = process.env.BAILEYS_LOG_LEVEL || "silent";
    const logger = (pino as unknown as (opts: any) => any)({ level: logLevel });

    if (typeof makeInMemoryStore === "function") {
      this.store = makeInMemoryStore({ logger });
      this.storePath =
        process.env.STORE_PATH || path.join(this.sessionDir, "store.json");
      try {
        if (this.storePath && fs.existsSync(this.storePath) && this.store?.readFromFile) {
          this.store.readFromFile(this.storePath);
          log.info({ storePath: this.storePath }, "Loaded WhatsApp store from disk");
        }
      } catch (error) {
        log.warn({ err: error }, "Failed to load WhatsApp store from disk");
      }
      if (this.store?.writeToFile) {
        this.storeFlushTimer = setInterval(() => {
          try {
            if (this.storePath) {
              this.store.writeToFile(this.storePath);
            }
          } catch (error) {
            log.warn({ err: error }, "Failed to persist WhatsApp store");
          }
        }, 10000);
      }
    } else {
      this.store = null;
    }

    const makeWASocket = baileys.default;
    this.sock = makeWASocket({
      version,
      auth: state,
      printQRInTerminal: false,
      logger,
      syncFullHistory: true,
      browser: ["MCP", "Desktop", "1.0.0"],
      getMessage: async (key: any) => (getMessage ? await getMessage(key) : undefined),
    });

    if (this.store?.bind) {
      this.store.bind(this.sock.ev);
    }

    this.sock.ev.on("creds.update", saveCreds);
  }

  getSocket(): any {
    if (!this.sock) {
      throw new Error("WhatsApp socket not initialized");
    }
    return this.sock;
  }

  getSocketOptional(): any | null {
    return this.sock;
  }

  getStore(): any | null {
    return this.store;
  }

  getStoreOptional(): any | null {
    return this.store;
  }

  async destroy(): Promise<void> {
    if (!this.sock) return;
    try {
      this.sock.end(new Error("Client destroyed"));
    } catch (_error) {
      // ignore
    }
    if (this.storeFlushTimer) {
      clearInterval(this.storeFlushTimer);
      this.storeFlushTimer = null;
    }
    this.sock = null;
  }
}
