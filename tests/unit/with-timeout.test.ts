import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { withTimeout } from "../../src/utils/with-timeout.js";

describe("withTimeout", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("returns the resolved value when the promise wins the race", async () => {
    const inner = Promise.resolve("ok");
    await expect(withTimeout(inner, 1000, "task")).resolves.toBe("ok");
  });

  it("rejects with the inner error when the promise rejects first", async () => {
    const inner = Promise.reject(new Error("boom"));
    await expect(withTimeout(inner, 1000, "task")).rejects.toThrow("boom");
  });

  it("throws a timeout error if the promise never settles before the deadline", async () => {
    const inner = new Promise<string>(() => {});
    const racing = withTimeout(inner, 1000, "task");
    // Attach the rejection handler before advancing timers so the timeout
    // rejection is observed and does not surface as an unhandled rejection.
    const assertion = expect(racing).rejects.toThrow(
      /task timed out after 1000ms/,
    );
    await vi.advanceTimersByTimeAsync(1000);
    await assertion;
  });

  it("falls back to a plain await when timeoutMs <= 0", async () => {
    const inner = Promise.resolve("ok");
    await expect(withTimeout(inner, 0, "task")).resolves.toBe("ok");
    await expect(withTimeout(inner, -1, "task")).resolves.toBe("ok");
  });
});
