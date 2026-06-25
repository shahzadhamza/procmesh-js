'use strict';

const fs = require('fs');
const path = require('path');
const os = require('os');
const { encodeFrame, FrameDecoder } = require('./protocol');

/**
 * Crash-survival persistence for the broker: a periodic snapshot + an append-only log (AOF)
 * of mutation EFFECTS (absolute set / delete / clear), so replay is idempotent and order-
 * independent. Connection-scoped state (locks, subscriptions, RPC regs, in-flight calls) is
 * intentionally NOT persisted — see the production-hardening plan. Node built-ins only.
 *
 * Durability modes (fsync policy):
 *   'no'       — best-effort async writes; OS decides when to flush.
 *   'everysec' — async writes + ~1s periodic fdatasync (default); ≤1s power-loss window.
 *   'always'   — synchronous write + fsync before returning; durable, blocks the loop.
 *
 * The fencing-token counter is kept monotonic across restarts by reserving token blocks: each
 * time issuance crosses a block boundary we log a 'token' record for the new ceiling, so the
 * restored seed is always ≥ any token ever issued.
 */

const SNAPSHOT_VERSION = 1;
const TOKEN_BLOCK = 1024; // reserve fencing tokens in blocks; ~1 AOF record per 1024 grants
const DEFAULT_AOF_REWRITE_OPS = 100000; // compact (snapshot + truncate) after this many appends

/** No-op persistence used when the feature is disabled (zero-config default). */
class NullPersistence {
  constructor() {
    this.enabled = false;
    this.loadedToken = 0;
  }

  // eslint-disable-next-line class-methods-use-this
  async load() {}

  // eslint-disable-next-line class-methods-use-this
  logMutation() {}

  // eslint-disable-next-line class-methods-use-this
  noteToken() {}

  // eslint-disable-next-line class-methods-use-this
  start() {}

  // eslint-disable-next-line class-methods-use-this
  async flushAndClose() {}
}

class Persistence {
  constructor({ dir, mode = 'everysec', codec, snapshotInterval = 0, aofRewriteOps = DEFAULT_AOF_REWRITE_OPS } = {}) {
    this.enabled = true;
    this.dir = dir;
    this.mode = mode; // 'no' | 'everysec' | 'always'
    this.codec = codec;
    this.snapshotInterval = snapshotInterval;
    this.aofRewriteOps = aofRewriteOps;

    this.snapshotPath = path.join(dir, 'snapshot.bin');
    this.aofPath = path.join(dir, 'aof.bin');
    this.lockPath = path.join(dir, 'broker.lock');

    this.fd = null; // AOF file descriptor (append)
    this.queue = []; // pending frames (async modes)
    this.writing = false;
    this.dirty = false; // unsynced bytes present
    this.opsSinceSnapshot = 0;

    this.loadedToken = 0; // highest reserved token recovered on load
    this.tokenReserved = 0; // current reserved ceiling
    this._store = null; // set in load(), used for compaction snapshots

    this._fsyncTimer = null;
    this._snapshotTimer = null;
    this._closed = false;
  }

  // --------------------------------------------------------------------- recovery

  async load(store) {
    this._store = store;
    fs.mkdirSync(this.dir, { recursive: true });
    this._acquireLock();

    // 1. Snapshot (compaction base).
    if (fs.existsSync(this.snapshotPath)) {
      try {
        const snap = this.codec.decode(fs.readFileSync(this.snapshotPath));
        if (snap && snap.version === SNAPSHOT_VERSION) {
          store.load(snap.entries || []);
          this.loadedToken = Math.max(this.loadedToken, snap.fenceToken || 0);
        }
      } catch {
        // Corrupt/foreign snapshot — recover from the AOF alone rather than refuse to start.
      }
    }

    // 2. AOF tail. FrameDecoder yields only complete frames, so a torn final record (kill -9
    //    mid-write) is silently dropped — the log self-truncates at the last good frame.
    if (fs.existsSync(this.aofPath)) {
      const decoder = new FrameDecoder(this.codec);
      const buf = fs.readFileSync(this.aofPath);
      try {
        decoder.push(buf, (rec) => this._apply(store, rec));
      } catch {
        // Decode error mid-stream — stop at the corruption; valid prefix is already applied.
      }
    }

    // 3. Compact: fold what we just recovered into a fresh snapshot, then start a clean AOF.
    this.tokenReserved = this.loadedToken;
    this._writeSnapshot(store);
    this.fd = fs.openSync(this.aofPath, 'a');
  }

  _apply(store, rec) {
    switch (rec.op) {
      case 'set':
        store.load([{ k: rec.k, v: rec.v, e: rec.e || 0 }]);
        break;
      case 'del':
        store.del(rec.k);
        break;
      case 'clear':
        store.clear();
        break;
      case 'token':
        this.loadedToken = Math.max(this.loadedToken, rec.n || 0);
        break;
      default:
        break;
    }
  }

  // --------------------------------------------------------------------- logging

  /** Record a mutation effect. `rec` is { op:'set',k,v,e } | { op:'del',k } | { op:'clear' }. */
  logMutation(rec) {
    this._append(rec);
    this.opsSinceSnapshot += 1;
    if (this.opsSinceSnapshot >= this.aofRewriteOps && this._store) this._compact();
  }

  /** Called by the broker on every fencing-token mint; reserves a block when crossed. */
  noteToken(n) {
    if (n > this.tokenReserved) {
      this.tokenReserved = Math.ceil((n + 1) / TOKEN_BLOCK) * TOKEN_BLOCK;
      this._append({ op: 'token', n: this.tokenReserved });
    }
  }

  _append(rec) {
    if (this._closed || this.fd == null) {
      // Before the AOF fd is open (during load), records are already in the snapshot/store.
      return;
    }
    const frame = encodeFrame(this.codec, rec);
    if (this.mode === 'always') {
      try {
        fs.writeSync(this.fd, frame);
        fs.fsyncSync(this.fd);
      } catch (err) {
        this._onWriteError(err);
      }
      return;
    }
    this.queue.push(frame);
    this._drain();
  }

  _drain() {
    if (this.writing || this.queue.length === 0 || this.fd == null) return;
    const batch = this.queue.length === 1 ? this.queue[0] : Buffer.concat(this.queue);
    this.queue = [];
    this.writing = true;
    fs.write(this.fd, batch, (err) => {
      this.writing = false;
      if (err) {
        this._onWriteError(err);
        return;
      }
      this.dirty = true;
      if (this.queue.length) this._drain();
    });
  }

  _onWriteError(err) {
    // Disk full / read-only / etc.: keep serving from memory, surface the failure, stop trying.
    this.writeError = err;
    if (this.onError) this.onError(err);
  }

  // ------------------------------------------------------------------- snapshots

  _writeSnapshot(store) {
    const payload = this.codec.encode({
      version: SNAPSHOT_VERSION,
      createdAt: Date.now(),
      fenceToken: this.tokenReserved,
      entries: store.dump(),
    });
    const tmp = `${this.snapshotPath}.tmp`;
    const fd = fs.openSync(tmp, 'w');
    try {
      fs.writeSync(fd, payload);
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    fs.renameSync(tmp, this.snapshotPath); // atomic replace
  }

  /** Compaction: snapshot current state, then truncate the AOF (snapshot now subsumes it). */
  _compact() {
    if (!this._store) return;
    this._writeSnapshot(this._store);
    this.opsSinceSnapshot = 0;
    // Truncate and reopen the AOF so future appends start from empty.
    if (this.fd != null) fs.closeSync(this.fd);
    this.fd = fs.openSync(this.aofPath, 'w'); // 'w' truncates
    fs.closeSync(this.fd);
    this.fd = fs.openSync(this.aofPath, 'a');
  }

  // ------------------------------------------------------------------- lifecycle

  start() {
    if (this.mode === 'everysec') {
      this._fsyncTimer = setInterval(() => {
        if (this.dirty && this.fd != null) {
          fs.fdatasync(this.fd, () => {});
          this.dirty = false;
        }
      }, 1000);
      if (this._fsyncTimer.unref) this._fsyncTimer.unref();
    }
    if (this.snapshotInterval > 0) {
      this._snapshotTimer = setInterval(() => {
        if (this._store) this._compact();
      }, this.snapshotInterval);
      if (this._snapshotTimer.unref) this._snapshotTimer.unref();
    }
  }

  async flushAndClose() {
    this._closed = true;
    if (this._fsyncTimer) clearInterval(this._fsyncTimer);
    if (this._snapshotTimer) clearInterval(this._snapshotTimer);
    // Final compaction makes a planned shutdown lossless.
    try {
      if (this._store) this._writeSnapshot(this._store);
    } catch (err) {
      this._onWriteError(err);
    }
    if (this.fd != null) {
      try {
        fs.fsyncSync(this.fd);
        fs.closeSync(this.fd);
      } catch {
        /* ignore */
      }
      this.fd = null;
    }
    this._releaseLock();
  }

  // ------------------------------------------------------------------- dir lock

  _acquireLock() {
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        const fd = fs.openSync(this.lockPath, 'wx'); // fail if exists
        fs.writeSync(fd, String(process.pid));
        fs.closeSync(fd);
        return;
      } catch (err) {
        if (err.code !== 'EEXIST') throw err;
        const stale = this._lockIsStale();
        if (!stale) {
          const e = new Error(`persist dir ${this.dir} is locked by a live broker`);
          e.code = 'EPERSISTLOCKED';
          throw e;
        }
        try {
          fs.unlinkSync(this.lockPath);
        } catch {
          /* race: another cleared it; retry */
        }
      }
    }
  }

  _lockIsStale() {
    try {
      const pid = parseInt(fs.readFileSync(this.lockPath, 'utf8').trim(), 10);
      if (!pid) return true;
      process.kill(pid, 0); // throws ESRCH if the pid is gone
      return false; // pid alive → not stale
    } catch (err) {
      return err.code === 'ESRCH' || err.code === 'ENOENT'; // gone → stale
    }
  }

  _releaseLock() {
    try {
      fs.unlinkSync(this.lockPath);
    } catch {
      /* ignore */
    }
  }
}

/**
 * Build a Persistence (or a no-op) from broker options.
 * Disabled unless `opts` is set or PROCMESH_PERSIST_DIR is present (zero-config stays in-memory).
 */
function createPersistence(opts, name, codec) {
  const envDir = process.env.PROCMESH_PERSIST_DIR;
  if (!opts && !envDir) return new NullPersistence();
  const cfg = opts === true ? {} : opts || {};
  if (cfg.mode === 'off') return new NullPersistence();
  const dir = cfg.dir || envDir || path.join(os.tmpdir(), `procmesh-${name || 'default'}`);
  return new Persistence({ ...cfg, dir, codec });
}

module.exports = { Persistence, NullPersistence, createPersistence };
