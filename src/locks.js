'use strict';

/**
 * Cross-process mutex manager, held in the broker. Each lock has at most one
 * owner; contenders queue FIFO. A lock auto-releases after its TTL (so a crashed
 * owner can't deadlock the system) and all of a connection's locks are released
 * when it disconnects.
 *
 * Connections are identified by an opaque `connId`. acquire() resolves with a
 * boolean: true if the lock was granted, false if it timed out / was unavailable.
 */
class LockManager {
  /**
   * @param {object} [opts]
   * @param {function} [opts.mintToken]  returns the next monotonic fencing token; the broker
   *   injects one backed by a (persisted) global counter. Defaults to a private counter so the
   *   manager is usable standalone in tests.
   */
  constructor({ mintToken } = {}) {
    this.locks = new Map(); // key -> { owner, ttl, timer, token, waiters: [{ connId, ttl, resolve, timer }] }
    this.fenceHigh = new Map(); // lockKey -> highest fencing token ever issued (never deleted)
    let n = 0;
    this.mintToken = mintToken || (() => (n += 1));
  }

  acquire(key, connId, { ttl = 30000, wait = 0 } = {}) {
    return new Promise((resolve) => {
      let lock = this.locks.get(key);
      if (!lock) {
        lock = { owner: null, ttl, timer: null, waiters: [] };
        this.locks.set(key, lock);
      }
      if (lock.owner === null) {
        this._grant(key, lock, connId, ttl);
        return resolve({ acquired: true, token: lock.token });
      }
      if (wait <= 0) {
        this._gcIfEmpty(key, lock);
        return resolve({ acquired: false, token: null });
      }
      const waiter = { connId, ttl, resolve, timer: null };
      waiter.timer = setTimeout(() => {
        lock.waiters = lock.waiters.filter((w) => w !== waiter);
        this._gcIfEmpty(key, lock);
        resolve({ acquired: false, token: null });
      }, wait);
      if (waiter.timer.unref) waiter.timer.unref();
      lock.waiters.push(waiter);
    });
  }

  release(key, connId) {
    const lock = this.locks.get(key);
    if (!lock || lock.owner !== connId) return false;
    this._release(key);
    return true;
  }

  /** Release everything held or awaited by a (disconnected) connection. */
  releaseAll(connId) {
    for (const [key, lock] of this.locks) {
      lock.waiters = lock.waiters.filter((w) => {
        if (w.connId === connId) {
          clearTimeout(w.timer);
          return false;
        }
        return true;
      });
      if (lock.owner === connId) this._release(key);
      else this._gcIfEmpty(key, lock);
    }
  }

  _grant(key, lock, connId, ttl) {
    lock.owner = connId;
    lock.ttl = ttl;
    // Each grant gets a fresh, strictly-larger token (its epoch). Raising the fence bar
    // here means the instant a lock is (re-)granted — including a TTL-expiry hand-off to a
    // waiter — any prior holder's token is already below the bar and will be fenced off.
    lock.token = this.mintToken();
    this.bumpFence(key, lock.token);
    clearTimeout(lock.timer);
    lock.timer = setTimeout(() => this._release(key), ttl);
    if (lock.timer.unref) lock.timer.unref();
  }

  _release(key) {
    const lock = this.locks.get(key);
    if (!lock) return;
    clearTimeout(lock.timer);
    lock.owner = null;
    lock.timer = null;
    const next = lock.waiters.shift();
    if (next) {
      clearTimeout(next.timer);
      this._grant(key, lock, next.connId, next.ttl);
      next.resolve({ acquired: true, token: lock.token });
    } else {
      this._gcIfEmpty(key, lock);
    }
  }

  _gcIfEmpty(key, lock) {
    if (lock.owner === null && lock.waiters.length === 0) {
      clearTimeout(lock.timer);
      this.locks.delete(key);
    }
  }

  /** Raise the per-key fence bar. Never lowered, never deleted (a freed key may be re-locked). */
  bumpFence(key, token) {
    const cur = this.fenceHigh.get(key) || 0;
    if (token > cur) this.fenceHigh.set(key, token);
  }

  /** Highest fencing token ever issued for a key (0 if never locked). */
  getFenceHigh(key) {
    return this.fenceHigh.get(key) || 0;
  }

  /** Lightweight observability snapshot: held lock count and total queued waiters. */
  stats() {
    let waiters = 0;
    for (const lock of this.locks.values()) waiters += lock.waiters.length;
    return { locks: this.locks.size, waiters };
  }
}

module.exports = LockManager;
