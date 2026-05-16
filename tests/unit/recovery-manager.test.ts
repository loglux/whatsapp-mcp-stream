import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
  type MockInstance,
} from "vitest";
import {
  RecoveryManager,
  type RecoveryDeps,
} from "../../src/services/recovery-manager.js";

function buildDeps(overrides: Partial<RecoveryDeps> = {}): {
  deps: RecoveryDeps;
  withLifecycleLock: MockInstance;
  destroyInternal: MockInstance;
  initializeWithinLifecycleLock: MockInstance;
  forceResync: MockInstance;
  restart: MockInstance;
  isReady: MockInstance;
} {
  const withLifecycleLock = vi.fn(async (fn: () => Promise<unknown>) => fn());
  const destroyInternal = vi.fn(async () => {});
  const initializeWithinLifecycleLock = vi.fn(async () => {});
  const forceResync = vi.fn(async () => {});
  const restart = vi.fn(async () => {});
  const isReady = vi.fn(() => false);
  const deps: RecoveryDeps = {
    withLifecycleLock,
    destroyInternal,
    initializeWithinLifecycleLock,
    forceResync,
    restart,
    isReady,
    ...overrides,
  };
  return {
    deps,
    withLifecycleLock,
    destroyInternal,
    initializeWithinLifecycleLock,
    forceResync,
    restart,
    isReady,
  };
}

describe("RecoveryManager scheduleSyncRecovery", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("triggers forceResync on the first sync failure", async () => {
    const { deps, forceResync } = buildDeps();
    const mgr = new RecoveryManager(deps);
    mgr.scheduleSyncRecovery("decode mutation");
    expect(mgr.isSyncRecoveryInProgress()).toBe(true);
    await vi.runOnlyPendingTimersAsync();
    expect(forceResync).toHaveBeenCalledTimes(1);
    expect(mgr.isSyncRecoveryInProgress()).toBe(false);
  });

  it("escalates to restart after the soft-recovery limit is exceeded", async () => {
    const { deps, forceResync, restart } = buildDeps();
    const mgr = new RecoveryManager(deps, {
      syncSoftRecoveryLimit: 2,
      syncRecoveryCooldownMs: 0,
    });
    mgr.scheduleSyncRecovery("first");
    await vi.runOnlyPendingTimersAsync();
    mgr.scheduleSyncRecovery("second");
    await vi.runOnlyPendingTimersAsync();
    mgr.scheduleSyncRecovery("third");
    await vi.runOnlyPendingTimersAsync();
    expect(forceResync).toHaveBeenCalledTimes(2);
    expect(restart).toHaveBeenCalledTimes(1);
  });

  it("skips when another sync recovery is in progress", async () => {
    // Block the in-flight recovery so it never resolves.
    const forceResync = vi.fn(() => new Promise(() => {}));
    const { deps } = buildDeps({ forceResync });
    const mgr = new RecoveryManager(deps);
    mgr.scheduleSyncRecovery("first");
    await vi.runOnlyPendingTimersAsync();
    expect(mgr.isSyncRecoveryInProgress()).toBe(true);
    mgr.scheduleSyncRecovery("second");
    await vi.runOnlyPendingTimersAsync();
    expect(forceResync).toHaveBeenCalledTimes(1);
  });

  it("skips when within the cooldown window after a recent recovery", async () => {
    const { deps, forceResync } = buildDeps();
    const mgr = new RecoveryManager(deps, {
      syncRecoveryCooldownMs: 300_000,
    });
    mgr.scheduleSyncRecovery("first");
    await vi.runOnlyPendingTimersAsync();
    expect(forceResync).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(60_000); // still within cooldown
    mgr.scheduleSyncRecovery("second");
    await vi.runOnlyPendingTimersAsync();
    expect(forceResync).toHaveBeenCalledTimes(1);
  });

  it("resets the attempt counter once the recovery window elapses", async () => {
    const { deps, forceResync, restart } = buildDeps();
    const mgr = new RecoveryManager(deps, {
      syncSoftRecoveryLimit: 1,
      syncRecoveryCooldownMs: 0,
      syncRecoveryWindowMs: 1000,
    });
    mgr.scheduleSyncRecovery("a");
    await vi.runOnlyPendingTimersAsync();
    mgr.scheduleSyncRecovery("b");
    await vi.runOnlyPendingTimersAsync();
    expect(forceResync).toHaveBeenCalledTimes(1);
    expect(restart).toHaveBeenCalledTimes(1);
    // Wait past the window so attempts reset back to 1 → force-resync again.
    vi.advanceTimersByTime(2000);
    mgr.scheduleSyncRecovery("c");
    await vi.runOnlyPendingTimersAsync();
    expect(forceResync).toHaveBeenCalledTimes(2);
  });
});

describe("RecoveryManager scheduleDisconnectRecovery", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("escalates to restart for configured restart status codes", async () => {
    const { deps, restart } = buildDeps();
    const mgr = new RecoveryManager(deps, {
      disconnectRecoveryDelayMs: 1000,
      disconnectRecoveryRestartCodes: [428],
    });
    mgr.scheduleDisconnectRecovery(428, "Connection Terminated");
    await vi.advanceTimersByTimeAsync(1000);
    expect(restart).toHaveBeenCalledTimes(1);
  });

  it("uses reconnect for non-restart codes", async () => {
    const { deps, restart, withLifecycleLock } = buildDeps();
    const mgr = new RecoveryManager(deps, {
      disconnectRecoveryDelayMs: 1000,
      disconnectRecoveryRestartCodes: [428],
    });
    mgr.scheduleDisconnectRecovery(500, "Server Error");
    await vi.advanceTimersByTimeAsync(1000);
    await vi.runAllTimersAsync(); // reconnect's internal setTimeout backoff
    expect(restart).not.toHaveBeenCalled();
    expect(withLifecycleLock).toHaveBeenCalledTimes(1);
  });

  it("skips the watchdog when the socket is already ready", async () => {
    const { deps, restart } = buildDeps({ isReady: vi.fn(() => true) });
    const mgr = new RecoveryManager(deps, {
      disconnectRecoveryDelayMs: 1000,
      disconnectRecoveryRestartCodes: [428],
    });
    mgr.scheduleDisconnectRecovery(428, "Connection Terminated");
    await vi.advanceTimersByTimeAsync(1000);
    expect(restart).not.toHaveBeenCalled();
  });

  it("supersedes a pending timer when scheduled twice", async () => {
    const { deps, restart } = buildDeps();
    const mgr = new RecoveryManager(deps, {
      disconnectRecoveryDelayMs: 1000,
      disconnectRecoveryRestartCodes: [428],
    });
    mgr.scheduleDisconnectRecovery(428, "first");
    vi.advanceTimersByTime(500);
    mgr.scheduleDisconnectRecovery(428, "second"); // first timer cleared
    await vi.advanceTimersByTimeAsync(1000);
    // Only the second watchdog fires (1 invocation total)
    expect(restart).toHaveBeenCalledTimes(1);
  });

  it("clearDisconnectRecoveryTimer cancels the pending watchdog", async () => {
    const { deps, restart } = buildDeps();
    const mgr = new RecoveryManager(deps, {
      disconnectRecoveryDelayMs: 1000,
      disconnectRecoveryRestartCodes: [428],
    });
    mgr.scheduleDisconnectRecovery(428, "x");
    mgr.clearDisconnectRecoveryTimer();
    await vi.advanceTimersByTimeAsync(1000);
    expect(restart).not.toHaveBeenCalled();
  });
});

describe("RecoveryManager reconnect", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("acquires the lock, destroys and re-initialises", async () => {
    const {
      deps,
      withLifecycleLock,
      destroyInternal,
      initializeWithinLifecycleLock,
    } = buildDeps();
    const mgr = new RecoveryManager(deps);
    const promise = mgr.reconnect();
    await vi.runAllTimersAsync(); // exhaust the backoff delay
    await promise;
    expect(withLifecycleLock).toHaveBeenCalledTimes(1);
    expect(destroyInternal).toHaveBeenCalledTimes(1);
    expect(initializeWithinLifecycleLock).toHaveBeenCalledTimes(1);
  });

  it("does not run twice concurrently", async () => {
    const { deps, withLifecycleLock } = buildDeps();
    const mgr = new RecoveryManager(deps);
    const a = mgr.reconnect();
    const b = mgr.reconnect();
    await vi.runAllTimersAsync();
    await Promise.all([a, b]);
    expect(withLifecycleLock).toHaveBeenCalledTimes(1);
  });

  it("backs off exponentially based on reconnectAttempts", async () => {
    const { deps } = buildDeps();
    const mgr = new RecoveryManager(deps);
    // First reconnect: 1000ms backoff (attempt 0 → 1000 * 2^0)
    const p1 = mgr.reconnect();
    await vi.advanceTimersByTimeAsync(999);
    expect(mgr.getStats().reconnecting).toBe(true);
    await vi.advanceTimersByTimeAsync(1);
    await p1;
    expect(mgr.getStats().reconnecting).toBe(false);
    expect(mgr.getStats().reconnectAttempts).toBe(1);
    // Second reconnect: 2000ms backoff
    const p2 = mgr.reconnect();
    await vi.advanceTimersByTimeAsync(1999);
    expect(mgr.getStats().reconnecting).toBe(true);
    await vi.advanceTimersByTimeAsync(1);
    await p2;
  });
});

describe("RecoveryManager ensureReconnectAfterResync", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("is a no-op when disabled", () => {
    const { deps, withLifecycleLock } = buildDeps();
    const mgr = new RecoveryManager(deps, { resyncReconnectEnabled: false });
    mgr.ensureReconnectAfterResync();
    vi.advanceTimersByTime(60_000);
    expect(withLifecycleLock).not.toHaveBeenCalled();
  });

  it("is a no-op when the delay is zero", () => {
    const { deps, withLifecycleLock } = buildDeps();
    const mgr = new RecoveryManager(deps, { resyncReconnectDelayMs: 0 });
    mgr.ensureReconnectAfterResync();
    vi.advanceTimersByTime(60_000);
    expect(withLifecycleLock).not.toHaveBeenCalled();
  });

  it("skips reconnect when the socket is already ready at fire time", async () => {
    const { deps, withLifecycleLock } = buildDeps({
      isReady: vi.fn(() => true),
    });
    const mgr = new RecoveryManager(deps, { resyncReconnectDelayMs: 1000 });
    mgr.ensureReconnectAfterResync();
    await vi.advanceTimersByTimeAsync(1000);
    expect(withLifecycleLock).not.toHaveBeenCalled();
  });

  it("triggers reconnect when still not ready at fire time", async () => {
    const { deps, withLifecycleLock } = buildDeps();
    const mgr = new RecoveryManager(deps, { resyncReconnectDelayMs: 1000 });
    mgr.ensureReconnectAfterResync();
    await vi.advanceTimersByTimeAsync(1000);
    // The reconnect itself adds its own backoff timer; exhaust it.
    await vi.runAllTimersAsync();
    expect(withLifecycleLock).toHaveBeenCalledTimes(1);
  });
});

describe("RecoveryManager state mutations", () => {
  it("markConnectionOpen resets disconnect/reconnect state", () => {
    const { deps } = buildDeps();
    const mgr = new RecoveryManager(deps);
    mgr.markDisconnect("kicked");
    mgr.markSessionReplaced();
    mgr.markConnectionOpen();
    const stats = mgr.getStats();
    expect(stats.sessionReplaced).toBe(false);
    expect(stats.lastDisconnectReason).toBeNull();
    expect(stats.lastDisconnectAt).toBeNull();
    expect(stats.reconnectAttempts).toBe(0);
    expect(stats.disconnectRecoveryInProgress).toBe(false);
  });

  it("markDisconnect records reason and timestamp", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));
    try {
      const { deps } = buildDeps();
      const mgr = new RecoveryManager(deps);
      mgr.markDisconnect("428");
      const stats = mgr.getStats();
      expect(stats.lastDisconnectReason).toBe("428");
      expect(stats.lastDisconnectAt).toBe(Date.now());
    } finally {
      vi.useRealTimers();
    }
  });

  it("markSessionReplaced flips the flag", () => {
    const { deps } = buildDeps();
    const mgr = new RecoveryManager(deps);
    expect(mgr.getStats().sessionReplaced).toBe(false);
    mgr.markSessionReplaced();
    expect(mgr.getStats().sessionReplaced).toBe(true);
  });
});
