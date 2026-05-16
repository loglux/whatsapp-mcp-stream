import { log } from "../utils/logger.js";

export interface RecoveryDeps {
  withLifecycleLock<T>(fn: () => Promise<T>): Promise<T>;
  destroyInternal(): Promise<void>;
  initializeWithinLifecycleLock(): Promise<void>;
  forceResync(): Promise<void>;
  restart(): Promise<void>;
  isReady(): boolean;
}

export interface RecoveryOptions {
  syncRecoveryCooldownMs?: number;
  syncRecoveryWindowMs?: number;
  syncSoftRecoveryLimit?: number;
  disconnectRecoveryDelayMs?: number;
  disconnectRecoveryRestartCodes?: Iterable<number>;
  resyncReconnectEnabled?: boolean;
  resyncReconnectDelayMs?: number;
  readinessGraceMs?: number;
}

export interface RecoveryStats {
  reconnecting: boolean;
  reconnectAttempts: number;
  syncRecoveryInProgress: boolean;
  syncRecoveryAttempts: number;
  disconnectRecoveryInProgress: boolean;
  lastDisconnectReason: string | null;
  lastDisconnectAt: number | null;
  lastRecoveryReason: string | null;
  lastRecoveryAt: number | null;
  sessionReplaced: boolean;
}

const DEFAULT_SYNC_COOLDOWN_MS = 300_000;
const DEFAULT_SYNC_WINDOW_MS = 900_000;
const DEFAULT_SYNC_SOFT_LIMIT = 2;
const DEFAULT_DISCONNECT_DELAY_MS = 30_000;
const DEFAULT_DISCONNECT_RESTART_CODES = [428];
const DEFAULT_RESYNC_RECONNECT_DELAY_MS = 15_000;
const DEFAULT_READINESS_GRACE_MS = 180_000;

/**
 * Owns disconnect/sync-recovery state and schedules the recovery actions.
 *
 * **Deadlock invariant (load-bearing):** every recovery path that ends up
 * touching the WhatsApp lifecycle must run `destroyInternal` and
 * `initializeWithinLifecycleLock` *inside* `withLifecycleLock`. Never call the
 * deps' `initialize()` public entrypoint here (and avoid escalating to
 * `restart()` from within an already-locked section) — `initialize()`
 * re-acquires the same lifecycle lock and the recovery hangs forever. A real
 * production outage was caused by exactly that pattern; the fix was to route
 * recovery through `initializeWithinLifecycleLock`. The deps-callback wiring
 * is what keeps this invariant: don't replace it with direct
 * `WhatsAppService.*` calls.
 */
export class RecoveryManager {
  readonly syncRecoveryCooldownMs: number;
  readonly syncRecoveryWindowMs: number;
  readonly syncSoftRecoveryLimit: number;
  readonly disconnectRecoveryDelayMs: number;
  readonly disconnectRecoveryRestartCodes: Set<number>;
  readonly resyncReconnectEnabled: boolean;
  readonly resyncReconnectDelayMs: number;
  readonly readinessGraceMs: number;

  private syncRecoveryInProgress = false;
  private syncRecoveryAttempts = 0;
  private lastSyncFailureAt: number | null = null;
  private lastRecoveryAt: number | null = null;
  private lastRecoveryReason: string | null = null;

  private reconnecting = false;
  private reconnectAttempts = 0;

  private disconnectRecoveryTimer: NodeJS.Timeout | null = null;
  private disconnectRecoveryInProgress = false;
  private lastDisconnectReason: string | null = null;
  private lastDisconnectAt: number | null = null;
  private sessionReplaced = false;

  constructor(
    private readonly deps: RecoveryDeps,
    options: RecoveryOptions = {},
  ) {
    this.syncRecoveryCooldownMs = Math.max(
      0,
      options.syncRecoveryCooldownMs ?? DEFAULT_SYNC_COOLDOWN_MS,
    );
    this.syncRecoveryWindowMs = Math.max(
      0,
      options.syncRecoveryWindowMs ?? DEFAULT_SYNC_WINDOW_MS,
    );
    this.syncSoftRecoveryLimit = Math.max(
      1,
      options.syncSoftRecoveryLimit ?? DEFAULT_SYNC_SOFT_LIMIT,
    );
    this.disconnectRecoveryDelayMs = Math.max(
      0,
      options.disconnectRecoveryDelayMs ?? DEFAULT_DISCONNECT_DELAY_MS,
    );
    this.disconnectRecoveryRestartCodes = new Set(
      options.disconnectRecoveryRestartCodes ??
        DEFAULT_DISCONNECT_RESTART_CODES,
    );
    this.resyncReconnectEnabled = options.resyncReconnectEnabled ?? true;
    this.resyncReconnectDelayMs = Math.max(
      0,
      options.resyncReconnectDelayMs ?? DEFAULT_RESYNC_RECONNECT_DELAY_MS,
    );
    this.readinessGraceMs = Math.max(
      0,
      options.readinessGraceMs ?? DEFAULT_READINESS_GRACE_MS,
    );
  }

  scheduleSyncRecovery(reason: string): void {
    const now = Date.now();
    if (
      this.lastSyncFailureAt &&
      now - this.lastSyncFailureAt > this.syncRecoveryWindowMs
    ) {
      this.syncRecoveryAttempts = 0;
    }
    this.lastSyncFailureAt = now;
    this.syncRecoveryAttempts += 1;

    if (this.syncRecoveryInProgress) {
      log.warn(
        { reason, attempts: this.syncRecoveryAttempts },
        "App state recovery already in progress",
      );
      return;
    }

    if (
      this.lastRecoveryAt &&
      now - this.lastRecoveryAt < this.syncRecoveryCooldownMs
    ) {
      log.warn(
        {
          reason,
          attempts: this.syncRecoveryAttempts,
          cooldownMs: this.syncRecoveryCooldownMs,
        },
        "App state recovery skipped due to cooldown",
      );
      return;
    }

    this.syncRecoveryInProgress = true;
    this.lastRecoveryAt = now;
    this.lastRecoveryReason = reason;

    const strategy =
      this.syncRecoveryAttempts <= this.syncSoftRecoveryLimit
        ? "force-resync"
        : "restart";

    setTimeout(() => {
      void (async () => {
        try {
          if (strategy === "force-resync") {
            log.warn(
              { reason, attempts: this.syncRecoveryAttempts },
              "Detected corrupted WhatsApp app state; forcing resync",
            );
            await this.deps.forceResync();
          } else {
            log.warn(
              { reason, attempts: this.syncRecoveryAttempts },
              "Detected repeated app state corruption; restarting WhatsApp client",
            );
            await this.deps.restart();
          }
        } catch (error) {
          log.error(
            {
              err: error,
              reason,
              strategy,
              attempts: this.syncRecoveryAttempts,
            },
            "Automatic app state recovery failed",
          );
        } finally {
          this.syncRecoveryInProgress = false;
        }
      })();
    }, 0);
  }

  clearDisconnectRecoveryTimer(): void {
    if (!this.disconnectRecoveryTimer) return;
    clearTimeout(this.disconnectRecoveryTimer);
    this.disconnectRecoveryTimer = null;
  }

  scheduleDisconnectRecovery(
    statusCode: number | undefined,
    reasonText: string,
  ): void {
    this.clearDisconnectRecoveryTimer();

    const normalizedReason = reasonText.toLowerCase();
    const shouldRestart =
      (typeof statusCode === "number" &&
        this.disconnectRecoveryRestartCodes.has(statusCode)) ||
      normalizedReason.includes("connection terminated");
    const strategy = shouldRestart ? "restart" : "reconnect";

    log.warn(
      {
        statusCode,
        reason: reasonText,
        strategy,
        delayMs: this.disconnectRecoveryDelayMs,
      },
      "Scheduled disconnect recovery watchdog",
    );

    this.disconnectRecoveryTimer = setTimeout(() => {
      this.disconnectRecoveryTimer = null;
      if (this.deps.isReady()) {
        log.info(
          { statusCode, reason: reasonText, strategy },
          "Skipping disconnect recovery because WhatsApp is already ready",
        );
        return;
      }
      if (this.disconnectRecoveryInProgress) {
        log.warn(
          { statusCode, reason: reasonText, strategy },
          "Disconnect recovery watchdog skipped because recovery is already in progress",
        );
        return;
      }

      this.disconnectRecoveryInProgress = true;
      void (async () => {
        try {
          log.warn(
            { statusCode, reason: reasonText, strategy },
            "Running disconnect recovery watchdog",
          );
          if (strategy === "restart") {
            await this.deps.restart();
          } else {
            await this.reconnect();
          }
        } catch (error) {
          log.error(
            { err: error, statusCode, reason: reasonText, strategy },
            "Disconnect recovery watchdog failed",
          );
        } finally {
          this.disconnectRecoveryInProgress = false;
        }
      })();
    }, this.disconnectRecoveryDelayMs);
  }

  async reconnect(): Promise<void> {
    if (this.reconnecting) {
      log.info(
        "Reconnect requested while another reconnect is already running",
      );
      return;
    }
    this.reconnecting = true;
    try {
      const attempt = Math.min(this.reconnectAttempts, 6);
      const delayMs = Math.min(30000, 1000 * Math.pow(2, attempt));
      this.reconnectAttempts += 1;
      log.warn(
        { attempt: attempt + 1, delayMs },
        "Starting WhatsApp reconnect",
      );
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      // Critical: stay inside withLifecycleLock and use the *within-lock*
      // initializer. Calling deps.initialize() here would re-acquire the
      // same lock and deadlock. See class docstring.
      await this.deps.withLifecycleLock(async () => {
        log.info("Reconnect: destroying current WhatsApp client");
        await this.deps.destroyInternal();
        log.info("Reconnect: initializing WhatsApp client");
        await this.deps.initializeWithinLifecycleLock();
      });
      log.info("Reconnect flow completed");
    } catch (error) {
      log.error({ err: error }, "Reconnect failed");
    } finally {
      this.reconnecting = false;
    }
  }

  ensureReconnectAfterResync(): void {
    if (!this.resyncReconnectEnabled) return;
    if (!this.resyncReconnectDelayMs || this.resyncReconnectDelayMs <= 0) {
      return;
    }
    setTimeout(() => {
      if (this.deps.isReady()) return;
      log.warn(
        { delayMs: this.resyncReconnectDelayMs },
        "Force resync did not reach ready state; reconnecting",
      );
      void this.reconnect();
    }, this.resyncReconnectDelayMs);
  }

  markConnectionOpen(): void {
    this.sessionReplaced = false;
    this.lastDisconnectReason = null;
    this.lastDisconnectAt = null;
    this.reconnectAttempts = 0;
    this.clearDisconnectRecoveryTimer();
    this.disconnectRecoveryInProgress = false;
  }

  markDisconnect(reason: string | null): void {
    this.lastDisconnectReason = reason;
    this.lastDisconnectAt = Date.now();
  }

  markSessionReplaced(): void {
    this.sessionReplaced = true;
  }

  isReconnecting(): boolean {
    return this.reconnecting;
  }

  isSyncRecoveryInProgress(): boolean {
    return this.syncRecoveryInProgress;
  }

  isDisconnectRecoveryInProgress(): boolean {
    return this.disconnectRecoveryInProgress;
  }

  getStats(): RecoveryStats {
    return {
      reconnecting: this.reconnecting,
      reconnectAttempts: this.reconnectAttempts,
      syncRecoveryInProgress: this.syncRecoveryInProgress,
      syncRecoveryAttempts: this.syncRecoveryAttempts,
      disconnectRecoveryInProgress: this.disconnectRecoveryInProgress,
      lastDisconnectReason: this.lastDisconnectReason,
      lastDisconnectAt: this.lastDisconnectAt,
      lastRecoveryReason: this.lastRecoveryReason,
      lastRecoveryAt: this.lastRecoveryAt,
      sessionReplaced: this.sessionReplaced,
    };
  }
}
