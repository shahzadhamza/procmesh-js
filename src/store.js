'use strict';

const { LRUCache } = require('lru-cache');

/**
 * CAS equality: structural, with a top-level undefined treated as "absent" (null). This replaces a
 * JSON.stringify comparison, which was sensitive to object key order (a false CAS failure) and
 * rendered every Map/Set as `{}` (a false CAS SUCCESS under the msgpack codec).
 */
function eq(a, b) {
  return deepEqual(a === undefined ? null : a, b === undefined ? null : b);
}

function deepEqual(a, b) {
  if (a === b || (a !== a && b !== b)) return true; // identical, or both NaN
  if (a === null || b === null || typeof a !== 'object' || typeof b !== 'object') return false;
  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) if (!deepEqual(a[i], b[i])) return false;
    return true;
  }
  if (a instanceof Date || b instanceof Date) {
    return a instanceof Date && b instanceof Date && a.getTime() === b.getTime();
  }
  if (ArrayBuffer.isView(a) || ArrayBuffer.isView(b)) {
    if (!ArrayBuffer.isView(a) || !ArrayBuffer.isView(b)) return false;
    const ba = Buffer.from(a.buffer, a.byteOffset, a.byteLength);
    const bb = Buffer.from(b.buffer, b.byteOffset, b.byteLength);
    return a.constructor === b.constructor && ba.equals(bb);
  }
  if (a instanceof Map || b instanceof Map) {
    if (!(a instanceof Map) || !(b instanceof Map) || a.size !== b.size) return false;
    for (const [k, v] of a) if (!b.has(k) || !deepEqual(v, b.get(k))) return false;
    return true;
  }
  if (a instanceof Set || b instanceof Set) {
    if (!(a instanceof Set) || !(b instanceof Set) || a.size !== b.size) return false;
    for (const x of a) {
      if (b.has(x)) continue;
      let found = false;
      for (const y of b) {
        if (deepEqual(x, y)) {
          found = true;
          break;
        }
      }
      if (!found) return false;
    }
    return true;
  }
  // Plain objects: key order doesn't matter; an undefined-valued key counts as absent (as in JSON).
  const ka = Object.keys(a).filter((k) => a[k] !== undefined);
  const kb = Object.keys(b).filter((k) => b[k] !== undefined);
  if (ka.length !== kb.length) return false;
  for (const k of ka) {
    if (!Object.prototype.hasOwnProperty.call(b, k) || !deepEqual(a[k], b[k])) return false;
  }
  return true;
}

/** Rough byte size of a value, for optional maxSize-based eviction. */
function approxSize(value) {
  try {
    return Buffer.byteLength(JSON.stringify(value) || '') + 1;
  } catch {
    return 1;
  }
}

/** Background expiry sweep: examine up to SWEEP_BATCH entries every SWEEP_INTERVAL ms. */
const SWEEP_INTERVAL = 250;
const SWEEP_BATCH = 2000;

/**
 * Map-backed stand-in for LRUCache used when the user configures NO bounds
 * (no max/maxSize/ttl): unbounded storage with per-item TTL support. Expired
 * entries are purged when touched, and — so a TTL'd key that is never read
 * again can't leak — by an incremental background sweep that runs only while
 * at least one entry carries a TTL (unref'd; bounded work per tick).
 * Implements only the LRUCache surface Store uses.
 */
class UnboundedTtlMap {
  constructor({ sweepInterval = SWEEP_INTERVAL, sweepBatch = SWEEP_BATCH } = {}) {
    this.map = new Map(); // key -> { v, exp }  exp: 0 = no expiry, else absolute ms epoch
    this.ttlCount = 0; // entries with exp !== 0 (drives the sweep timer)
    this.sweepInterval = sweepInterval;
    this.sweepBatch = sweepBatch;
    this._sweepTimer = null;
    this._sweepIter = null;
  }

  /** Remove `key`'s raw entry (live or not), keeping ttlCount in step. */
  _remove(key) {
    const e = this.map.get(key);
    if (!e) return false;
    if (e.exp !== 0) this._ttlDelta(-1);
    return this.map.delete(key);
  }

  _ttlDelta(d) {
    this.ttlCount += d;
    if (this.ttlCount > 0 && !this._sweepTimer && this.sweepInterval > 0) {
      this._sweepTimer = setInterval(() => this.sweep(), this.sweepInterval);
      if (this._sweepTimer.unref) this._sweepTimer.unref();
    } else if (this.ttlCount <= 0 && this._sweepTimer) {
      this._stopSweep();
    }
  }

  _stopSweep() {
    clearInterval(this._sweepTimer);
    this._sweepTimer = null;
    this._sweepIter = null;
  }

  /**
   * Purge expired entries, resuming where the last call stopped (Map iterators are live and
   * deletion-safe). Examines at most `sweepBatch` entries; returns the number purged.
   */
  sweep(batch = this.sweepBatch) {
    const now = Date.now();
    let purged = 0;
    if (!this._sweepIter) this._sweepIter = this.map.entries();
    const iter = this._sweepIter; // purging the last TTL entry stops the sweep and drops the field
    for (let i = 0; i < batch; i++) {
      const next = iter.next();
      if (next.done) {
        if (this._sweepIter === iter) this._sweepIter = null;
        break;
      }
      const [key, e] = next.value;
      if (e.exp !== 0 && e.exp <= now) {
        this._remove(key);
        purged++;
        if (this.ttlCount === 0) break; // nothing left that can expire
      }
    }
    return purged;
  }

  /** Live entry or undefined; purges an expired entry on touch. */
  _live(key) {
    const e = this.map.get(key);
    if (!e) return undefined;
    if (e.exp !== 0 && e.exp <= Date.now()) {
      this._remove(key);
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
    const delta = (exp !== 0 ? 1 : 0) - (cur && cur.exp !== 0 ? 1 : 0);
    this.map.set(key, { v: value, exp });
    if (delta) this._ttlDelta(delta);
    return this;
  }

  delete(key) {
    if (this._live(key) === undefined) return false; // missing, or expired (already purged)
    return this._remove(key);
  }

  *keys() {
    for (const key of [...this.map.keys()]) {
      if (this._live(key)) yield key;
    }
  }

  clear() {
    this.map.clear();
    this.ttlCount = 0;
    if (this._sweepTimer) this._stopSweep();
  }

  /** Stop the background sweep (the data stays readable). */
  close() {
    if (this._sweepTimer) this._stopSweep();
  }

  // O(1); may briefly count expired entries the sweep hasn't reached yet.
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
    this.maxEntrySize = 0;
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
    // lru-cache silently refuses a value larger than maxSize; remember the bound so set() can
    // reject it loudly instead of acking a write that was never stored.
    this.maxEntrySize = maxSize > 0 ? maxSize : 0;
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

  /** Release background timers (the unbounded backend's expiry sweep). */
  close() {
    if (typeof this.cache.close === 'function') this.cache.close();
  }

  /** Throw ETOOLARGE (before any mutation) for a value that exceeds the maxSize budget. */
  _checkSize(value) {
    if (this.maxEntrySize > 0 && approxSize(value) > this.maxEntrySize) {
      const err = new Error(`value exceeds cache maxSize (${this.maxEntrySize} bytes)`);
      err.code = 'ETOOLARGE';
      throw err;
    }
  }

  set(key, value, ttl) {
    this._checkSize(value);
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
    for (const [, v] of entries) this._checkSize(v); // all-or-nothing
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
    if (next !== undefined) this._checkSize(next);
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
