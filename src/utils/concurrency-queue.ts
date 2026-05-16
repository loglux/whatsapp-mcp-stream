export interface ConcurrencyQueueOptions {
  /** Maximum number of jobs running in parallel. */
  concurrency: number;
  /** Maximum number of jobs waiting in the queue. Excess is dropped FIFO. */
  maxQueued: number;
  /** Called with the dropped job when the queue overflows. */
  onDrop?: (queueSize: number) => void;
}

/**
 * In-process FIFO queue with bounded concurrency and a bounded backlog.
 * Once `maxQueued` waiters accumulate, the oldest pending job is dropped to
 * keep recent submissions running. Errors thrown by jobs are caught so a
 * single failure does not stall the worker pool.
 */
export class ConcurrencyQueue {
  private readonly concurrency: number;
  private readonly maxQueued: number;
  private readonly onDrop?: (queueSize: number) => void;
  private readonly pending: Array<() => Promise<unknown>> = [];
  private active = 0;

  constructor(options: ConcurrencyQueueOptions) {
    this.concurrency = Math.max(1, options.concurrency);
    this.maxQueued = Math.max(0, options.maxQueued);
    this.onDrop = options.onDrop;
  }

  enqueue(job: () => Promise<unknown>): void {
    this.pending.push(job);
    while (this.pending.length > this.maxQueued) {
      this.pending.shift();
      this.onDrop?.(this.pending.length);
    }
    this.tryStartNext();
  }

  private tryStartNext(): void {
    while (this.active < this.concurrency && this.pending.length > 0) {
      const job = this.pending.shift()!;
      this.active += 1;
      Promise.resolve()
        .then(() => job())
        .catch(() => {
          // Errors are job-specific; swallow so the worker pool keeps draining.
        })
        .finally(() => {
          this.active -= 1;
          this.tryStartNext();
        });
    }
  }

  get queued(): number {
    return this.pending.length;
  }

  get inFlight(): number {
    return this.active;
  }
}
