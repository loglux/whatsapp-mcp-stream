import { pino } from "pino";
import fs from "fs";
import path from "path";

// Determine if we're in a browser environment
const isBrowser =
  typeof globalThis !== "undefined" &&
  typeof (globalThis as any).window !== "undefined" &&
  typeof (globalThis as any).window.localStorage !== "undefined";

// Create log directory if it doesn't exist
const logDir = path.join(process.cwd(), "logs");
if (!isBrowser && !fs.existsSync(logDir)) {
  try {
    fs.mkdirSync(logDir, { recursive: true });
  } catch (e) {
    // Silent fail if directory creation fails
  }
}

// Define log file path
const logFilePath = path.join(logDir, "mcp-whatsapp.log");

// Node uses pino.destination (sonic-boom, async, buffered) so logging does
// not block the event loop on every write. Browser falls back to localStorage.
const fileDestination = !isBrowser
  ? pino.destination({ dest: logFilePath, sync: false, mkdir: true })
  : null;

// Match prior "silent fail" behaviour: never let a write error reach the app.
fileDestination?.on?.("error", () => {});

const browserDestination = {
  write: (msg: string) => {
    try {
      const storage = (globalThis as any).window.localStorage;
      const key = "mcp_whatsapp_log";
      const existingLog = storage.getItem(key) || "";
      // Keep only last 100KB to prevent localStorage overflow
      const maxSize = 100 * 1024;
      const newLog =
        existingLog.length > maxSize
          ? existingLog.substring(existingLog.length - maxSize / 2) + msg
          : existingLog + msg;
      storage.setItem(key, newLog);
    } catch (e) {
      // Silent fail if localStorage is not available
    }
    return true;
  },
};

// Configure pino logger
export const log = pino(
  {
    level: process.env.LOG_LEVEL || "info",
  },
  fileDestination ?? browserDestination,
);

// Flush pending async writes before exiting; safe to call multiple times.
export function flushLogs(): void {
  try {
    fileDestination?.flushSync?.();
  } catch {
    // Silent fail — best effort during shutdown.
  }
}

// Add a method to retrieve logs (useful for debugging)
export const getLogs = (): string => {
  if (isBrowser) {
    return (
      (globalThis as any).window.localStorage.getItem("mcp_whatsapp_log") || ""
    );
  } else {
    try {
      return fs.existsSync(logFilePath)
        ? fs.readFileSync(logFilePath, "utf8")
        : "";
    } catch (e) {
      return "";
    }
  }
};

// Example usage:
// log.info('This is an info message');
// log.warn('This is a warning');
// log.error(new Error('Something went wrong'), 'Error details');
// log.debug({ data: { key: 'value' } }, 'Debugging data');
// log.verbose('Verbose message', JSON.stringify({ complex: { nested: true } }));
