import { describe, it, expect, vi } from "vitest";
import { ConcurrencyQueue } from "../../src/utils/concurrency-queue.js";

const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

describe("ConcurrencyQueue", () => {
  it("runs a single job to completion", async () => {
    const q = new ConcurrencyQueue({ concurrency: 1, maxQueued: 10 });
    const job = vi.fn(async () => "done");
    q.enqueue(job);
    await tick();
    await tick();
    expect(job).toHaveBeenCalledTimes(1);
  });

  it("serialises jobs when concurrency is 1", async () => {
    const q = new ConcurrencyQueue({ concurrency: 1, maxQueued: 10 });
    const order: number[] = [];
    let releaseA!: () => void;
    let releaseB!: () => void;
    q.enqueue(
      () =>
        new Promise<void>((resolve) => {
          order.push(1);
          releaseA = () => {
            order.push(11);
            resolve();
          };
        }),
    );
    q.enqueue(
      () =>
        new Promise<void>((resolve) => {
          order.push(2);
          releaseB = () => {
            order.push(22);
            resolve();
          };
        }),
    );
    await tick();
    // Job 2 must not have started yet.
    expect(order).toEqual([1]);
    expect(q.inFlight).toBe(1);
    expect(q.queued).toBe(1);
    releaseA();
    await tick();
    await tick();
    expect(order).toEqual([1, 11, 2]);
    releaseB();
    await tick();
    expect(order).toEqual([1, 11, 2, 22]);
    expect(q.inFlight).toBe(0);
    expect(q.queued).toBe(0);
  });

  it("runs in parallel up to the concurrency limit", async () => {
    const q = new ConcurrencyQueue({ concurrency: 3, maxQueued: 10 });
    const releases: Array<() => void> = [];
    const started: number[] = [];
    for (let i = 0; i < 5; i++) {
      q.enqueue(
        () =>
          new Promise<void>((resolve) => {
            started.push(i);
            releases.push(resolve);
          }),
      );
    }
    await tick();
    expect(started).toEqual([0, 1, 2]);
    expect(q.inFlight).toBe(3);
    expect(q.queued).toBe(2);
    releases[0]();
    await tick();
    expect(started).toEqual([0, 1, 2, 3]);
    releases[1]();
    await tick();
    expect(started).toEqual([0, 1, 2, 3, 4]);
  });

  it("drops oldest pending jobs when the backlog exceeds maxQueued", async () => {
    const onDrop = vi.fn();
    const q = new ConcurrencyQueue({
      concurrency: 1,
      maxQueued: 2,
      onDrop,
    });
    let releaseFirst!: () => void;
    const ran: string[] = [];
    q.enqueue(
      () =>
        new Promise<void>((resolve) => {
          ran.push("first");
          releaseFirst = resolve;
        }),
    );
    await tick(); // let the first job's Promise constructor run so releaseFirst is bound.
    // Fill the backlog beyond maxQueued.
    q.enqueue(async () => {
      ran.push("a");
    });
    q.enqueue(async () => {
      ran.push("b");
    });
    q.enqueue(async () => {
      ran.push("c");
    });
    expect(q.queued).toBe(2);
    expect(onDrop).toHaveBeenCalled();
    releaseFirst();
    await tick();
    await tick();
    await tick();
    // "a" was the oldest pending and should have been dropped.
    expect(ran).toEqual(["first", "b", "c"]);
  });

  it("keeps draining after a job throws", async () => {
    const q = new ConcurrencyQueue({ concurrency: 1, maxQueued: 10 });
    const ran: string[] = [];
    q.enqueue(async () => {
      ran.push("a");
      throw new Error("boom");
    });
    q.enqueue(async () => {
      ran.push("b");
    });
    await tick();
    await tick();
    await tick();
    expect(ran).toEqual(["a", "b"]);
    expect(q.inFlight).toBe(0);
  });
});
