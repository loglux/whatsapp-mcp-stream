/**
 * Insert (or update) `key`, evicting the least-recently-written entries until
 * `map.size <= maxSize`. Updates re-insert the key, which moves it to the end
 * of the Map's insertion order; eviction always removes from the front. Reads
 * via `map.get` do *not* refresh the position — this is FIFO on writes, not
 * true LRU on access.
 */
export function setBoundedMapEntry<K, V>(
  map: Map<K, V>,
  key: K,
  value: V,
  maxSize: number,
): void {
  if (map.has(key)) {
    map.delete(key);
  }
  map.set(key, value);
  while (map.size > maxSize) {
    const oldestKey = map.keys().next().value;
    if (oldestKey === undefined) break;
    map.delete(oldestKey);
  }
}
