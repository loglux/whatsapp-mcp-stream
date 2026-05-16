/**
 * Race `promise` against a deadline. Returns the resolved value if it settles
 * first, propagates the rejection if it rejects first, or throws a timeout
 * error if neither happens within `timeoutMs`. The original promise keeps
 * running after a timeout — this helper does not cancel work.
 *
 * Pass `timeoutMs <= 0` to await without a timeout (escape hatch).
 */
export async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  description: string,
): Promise<T> {
  if (timeoutMs <= 0) return await promise;
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`${description} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}
