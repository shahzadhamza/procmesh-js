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
 * Map-backed stand-in for LRUCache used when the user configures NO bounds
 * (no max/maxSize/ttl): unbounded storage with per-item TTL support and lazy
 * expiry (an expired entry is purged when touched or iterated — no timers).
 * Implements only the LRUCache surface Store uses.
 */
class UnboundedTtlMap {
  constructor() {
    this.map = new Map(); // key -> { v, exp }  exp: 0 = no expiry, else absolute ms epoch
  }

  /** Live entry or undefined; purges an expired entry on touch. */
  _live(key) {
    const e = this.map.get(key);
    if (!e) return undefined;
    if (e.exp !== 0 && e.exp <= Date.now()) {
      this.map.delete(key);
      return undefined;
    }
    return e;
  }

  get(key) {
    const e = this._live(key);
    return e ? e.v : undefined;
  }

  peek(key) {
    return this.get(key); // no recency to preserve in a Map
  }

  has(key) {
    return this._live(key) !== undefined;
  }

  set(key, value, opts) {
    const { ttl, noUpdateTTL } = opts || {};
    const cur = this._live(key); // purges if expired, so expired counts as a new insert
    // Mirrors lru-cache: noUpdateTTL preserves a LIVE entry's clock; a new insert takes
    // the provided ttl (or none). A plain set on a live entry clears any per-item TTL.
    const exp = cur && noUpdateTTL ? cur.exp : ttl && ttl > 0 ? Date.now() + ttl : 0;
    this.map.set(key, { v: value, exp });
    return this;
  }

  delete(key) {
    if (this._live(key) === undefined) return false; // missing, or expired (already purged)
    return this.map.delete(key);
  }

  *keys() {
    for (const key of [...this.map.keys()]) {
      if (this._live(key)) yield key;
    }
  }

  clear() {
    this.map.clear();
  }

  // O(1); may count expired-but-unpurged entries, same as lru-cache without ttlAutopurge.
  get size() {
    return this.map.size;
  }

  getRemainingTTL(key) {
    const e = this.map.get(key);
    if (!e) return 0;
    if (e.exp === 0) return Infinity;
    const r = e.exp - Date.now();
    return r > 0 ? r : 0;
  }
}

/**
 * The authoritative key-value store, held in the broker process only.
 * All mutations run on the broker's single event loop, so atomic ops
 * (incr/decr/cas) need no internal locking.
 *
 * Two backends: with any bound configured (max entries, maxSize byte budget,
 * or a global ttl) it wraps lru-cache for size/TTL-based eviction; with no
 * bounds at all it uses an unbounded Map (lru-cache refuses to construct
 * without a bound). Per-item TTLs work on both.
 */
class Store {
  constructor({ max, ttl = 0, maxSize = 0 } = {}) {
    const bounded = (max != null && max > 0) || maxSize > 0 || ttl > 0;
    if (!bounded) {
      this.cache = new UnboundedTtlMap();
      return;
    }
    const opts = {};
    if (maxSize > 0) {
      opts.maxSize = maxSize;
      opts.sizeCalculation = approxSize;
    } else if (max > 0) {
      opts.max = max;
    }
    opts.ttl = ttl > 0 ? ttl : 0;
    // ttl-only config: autopurge makes the ttl a real bound (and avoids lru-cache's
    // LRU_CACHE_UNBOUNDED warning when neither max nor maxSize is set).
    opts.ttlAutopurge = ttl > 0;
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
