import { webcrypto } from "crypto";
import fs from "fs";
import pino from "pino";

import makeWASocket, { fetchLatestBaileysVersion, useMultiFileAuthState } from "baileys";

export class BaileysClient {
  private sock: any | null = null;
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

    this.sock = makeWASocket({
      version,
      auth: state,
      printQRInTerminal: false,
      logger,
      syncFullHistory: true,
      browser: ["MCP", "Desktop", "1.0.0"],
      getMessage: async (key: any) => (getMessage ? await getMessage(key) : undefined),
    });

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

  async destroy(): Promise<void> {
    if (!this.sock) return;
    try {
      this.sock.end(new Error("Client destroyed"));
    } catch (_error) {
      // ignore
    }
    this.sock = null;
  }
}
