'use strict';

/**
 * Keyspace hashing for sharded deployments. Every process that joins a sharded mesh
 * must hash each key the same way, so that cache/atomic/locks for a given key always
 * land on the single broker that owns it (preserving correctness). This is the one
 * shared implementation used by both the library (src/sharded-client.js) and the
 * benchmark harness (bench/shard.js re-exports it) — they can never drift apart.
 */

/** FNV-1a 32-bit hash of a string — fast, dependency-free, good enough for sharding. */
function fnv1a(str) {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

/** Pick the shard index for a key given the number of shards. */
function shardIndex(key, n) {
  if (n <= 1) return 0;
  return fnv1a(String(key)) % n;
}

module.exports = { fnv1a, shardIndex };
