import { webcrypto } from "crypto";
import fs from "fs";
import pino from "pino";

import makeWASocket, {
  Browsers,
  fetchLatestBaileysVersion,
  useMultiFileAuthState,
} from "baileys";

export interface BaileysLogEvent {
  level: "trace" | "debug" | "info" | "warn" | "error" | "fatal";
  message: string;
  payload?: unknown;
}

export class BaileysClient {
  private sock: any | null = null;
  private sessionDir: string;
  private readonly onLogEvent?: (event: BaileysLogEvent) => void;

  constructor(
    sessionDir: string,
    onLogEvent?: (event: BaileysLogEvent) => void,
  ) {
    this.sessionDir = sessionDir;
    this.onLogEvent = onLogEvent;
    if (!(globalThis as any).crypto) {
      (globalThis as any).crypto = webcrypto;
    }
  }

  async initialize(getMessage?: (key: any) => Promise<any> | any): Promise<void> {
    fs.mkdirSync(this.sessionDir, { recursive: true });
    const { state, saveCreds } = await useMultiFileAuthState(this.sessionDir);

    const { version } = await fetchLatestBaileysVersion();
    const logLevel = process.env.BAILEYS_LOG_LEVEL || "silent";
    const baseLogger = (pino as unknown as (opts: any) => any)({
      level: logLevel,
    });
    const logger = this.wrapLogger(baseLogger);

    this.sock = makeWASocket({
      version,
      auth: state,
      printQRInTerminal: false,
      logger,
      syncFullHistory: true,
      browser: Browsers.macOS("Desktop"),
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

  private wrapLogger(baseLogger: any): any {
    const levels = ["trace", "debug", "info", "warn", "error", "fatal"] as const;
    const wrapped = Object.create(baseLogger);

    for (const level of levels) {
      const original = baseLogger[level]?.bind(baseLogger);
      if (!original) continue;
      wrapped[level] = (...args: any[]) => {
        try {
          const event = this.extractLogEvent(level, args);
          if (event && this.onLogEvent) {
            this.onLogEvent(event);
          }
        } catch (_error) {
          // Ignore logger hook failures and preserve Baileys logging.
        }
        return original(...args);
      };
    }

    wrapped.child = (...args: any[]) => this.wrapLogger(baseLogger.child(...args));
    return wrapped;
  }

  private extractLogEvent(
    level: BaileysLogEvent["level"],
    args: any[],
  ): BaileysLogEvent | null {
    const message = args.find((arg) => typeof arg === "string");
    if (!message) return null;
    const payload = args.find(
      (arg) => arg && typeof arg === "object" && !Array.isArray(arg),
    );
    return {
      level,
      message,
      payload,
    };
  }
}
