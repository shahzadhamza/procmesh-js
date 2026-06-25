'use strict';

const { LRUCache } = require('lru-cache');

/** Structural equality via canonical JSON; undefined is treated as "absent" (null). */
function eq(a, b) {
  return JSON.stringify(a === undefined ? null : a) === JSON.stringify(b === undefined ? null : b);
}

/** Rough byte size of a value, for optional maxSize-based eviction. */
function approxSize(value) {
  try {
    return Buffer.byteLength(JSON.stringify(value) || '') + 1;
  } catch {
    return 1;
  }
}

/**
 * The authoritative key-value store, held in the broker process only.
 * Wraps lru-cache for size/TTL-based eviction. All mutations run on the broker's
 * single event loop, so atomic ops (incr/decr/cas) need no internal locking.
 */
class Store {
  constructor({ max = 10000, ttl = 0, maxSize = 0 } = {}) {
    const opts = {};
    // lru-cache requires at least one bound. We always set `max` unless a byte
    // budget is given. Per-item TTLs work as long as the ttl feature is enabled,
    // so we enable it with `ttl: 0` (no default expiry) when no global ttl is set.
    if (maxSize > 0) {
      opts.maxSize = maxSize;
      opts.sizeCalculation = approxSize;
    } else {
      opts.max = max;
    }
    opts.ttl = ttl > 0 ? ttl : 0;
    opts.ttlAutopurge = ttl > 0; // proactively purge expired entries when a default ttl exists
    opts.allowStale = false;
    this.cache = new LRUCache(opts);
  }

  get(key) {
    return this.cache.get(key);
  }

  set(key, value, ttl) {
    const opts = ttl && ttl > 0 ? { ttl } : undefined;
    this.cache.set(key, value, opts);
    return true;
  }

  del(key) {
    return this.cache.delete(key);
  }

  has(key) {
    return this.cache.has(key);
  }

  keys() {
    return [...this.cache.keys()];
  }

  clear() {
    this.cache.clear();
    return true;
  }

  /**
   * Bulk get. Returns { values, found } so callers can distinguish a missing key
   * from a stored `null`/`undefined` even across a JSON boundary (which would
   * otherwise collapse array holes to null).
   */
  mget(keys) {
    const values = [];
    const found = [];
    for (const k of keys) {
      const hit = this.cache.has(k);
      found.push(hit);
      values.push(hit ? this.cache.get(k) : null);
    }
    return { values, found };
  }

  mset(entries) {
    for (const [k, v] of entries) this.cache.set(k, v);
    return true;
  }

  incr(key, by = 1) {
    const cur = this.cache.get(key);
    const base = cur === undefined ? 0 : cur;
    if (typeof base !== 'number') {
      const err = new Error(`value at "${key}" is not a number`);
      err.code = 'ENOTNUMBER';
      throw err;
    }
    const next = base + by;
    // Read-modify-write must not reset an existing per-item TTL: a counter created with an
    // expiry keeps counting down rather than becoming immortal on the next incr.
    this.cache.set(key, next, { noUpdateTTL: true });
    return next;
  }

  /** Compare-and-set. Sets to `next` only if current value equals `prev`. */
  cas(key, prev, next) {
    const cur = this.cache.get(key);
    if (!eq(cur, prev)) return false;
    if (next === undefined) this.cache.delete(key);
    else this.cache.set(key, next, { noUpdateTTL: true }); // in-place update preserves any TTL
    return true;
  }

  get size() {
    return this.cache.size;
  }

  /**
   * Remaining lifetime of `key` in ms, or 0 for "no expiry" (and for a missing/expired key).
   * Used to mirror the live TTL into the persistence log after an atomic op.
   */
  remainingTTL(key) {
    const r = this.cache.getRemainingTTL(key);
    return r === Infinity ? 0 : Math.max(0, r);
  }

  /**
   * Snapshot every live entry as `{ k, v, e }` where `e` is an ABSOLUTE expiry timestamp
   * (ms epoch), or 0 for no expiry. Absolute (not remaining) so a reload after delay restores
   * the correct lifetime. Expired entries are skipped.
   */
  dump() {
    const now = Date.now();
    const entries = [];
    for (const key of this.cache.keys()) {
      const remaining = this.cache.getRemainingTTL(key);
      if (remaining <= 0) continue; // expired (0) — drop it
      const value = this.cache.peek(key); // no recency churn during a dump
      entries.push({ k: key, v: value, e: remaining === Infinity ? 0 : now + remaining });
    }
    return entries;
  }

  /** Restore entries produced by `dump()` (or individual persisted set records). */
  load(entries) {
    const now = Date.now();
    for (const { k, v, e } of entries) {
      if (e && e <= now) continue; // already expired
      const ttl = e ? e - now : undefined;
      this.cache.set(k, v, ttl ? { ttl } : undefined);
    }
  }
}

module.exports = Store;
