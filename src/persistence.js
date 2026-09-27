'use strict';

const fs = require('fs');
const path = require('path');
const os = require('os');
const { encodeFrame, FrameDecoder } = require('./protocol');
const { tryPidLock, releasePidLock, assertPrivateDir } = require('./transport');

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
const OFFSET_BLOCK = 1024; // reserve pub/sub offsets in blocks; keeps offsets monotonic across restart
const DEFAULT_AOF_REWRITE_OPS = 100000; // compact (snapshot + truncate) after this many appends

/** No-op persistence used when the feature is disabled (zero-config default). */
class NullPersistence {
  constructor() {
    this.enabled = false;
    this.loadedToken = 0;
    this.loadedOffset = 0;
  }

  // eslint-disable-next-line class-methods-use-this
  async load() {}

  // eslint-disable-next-line class-methods-use-this
  logMutation() {}

  // eslint-disable-next-line class-methods-use-this
  logPublishSync() {}

  // eslint-disable-next-line class-methods-use-this
  noteToken() {}

  // eslint-disable-next-line class-methods-use-this
  noteOffset() {}

  // eslint-disable-next-line class-methods-use-this
  start() {}

  // eslint-disable-next-line class-methods-use-this
  async flushAndClose() {}
}

class Persistence {
  constructor({
    dir,
    mode = 'everysec',
    codec,
    snapshotInterval = 0,
    aofRewriteOps = DEFAULT_AOF_REWRITE_OPS,
    privateDir = false,
  } = {}) {
    this.enabled = true;
    this.dir = dir;
    // The DEFAULT dir sits in the shared temp dir, where another user could pre-create it (or a
    // symlink) and have us write snapshots wherever they like: verify ownership + permissions.
    this.privateDir = privateDir;
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
    this.loadedOffset = 0; // highest reserved pub/sub offset recovered on load
    this.offsetReserved = 0; // current reserved offset ceiling
    this._store = null; // set in load(), used for compaction snapshots

    // Pub/sub persistence hooks, wired by the broker. Pub records are NOT store state, so on
    // recovery they're routed to the broker's retention ring instead of the cache store, and every
    // snapshot carries the still-retained ones so replay survives an AOF rewrite.
    this.onPubRecord = null; // (rec) => void — called for each recovered { op:'pub', ... }
    this.onCompactPubReplay = null; // () => rec[] — retained records to carry in each snapshot

    this._fsyncTimer = null;
    this._snapshotTimer = null;
    this._closed = false;
    this._compactScheduled = false; // a compaction is queued for the next turn of the loop
    this._compactAfterWrite = false; // ...and is waiting for the in-flight async write to land
    this._idleWaiters = []; // resolved when the in-flight async write completes
  }

  // --------------------------------------------------------------------- recovery

  async load(store) {
    this._store = store;
    if (this.privateDir) assertPrivateDir(this.dir, { create: true });
    else fs.mkdirSync(this.dir, { recursive: true });
    this._acquireLock();

    // 1. Snapshot (compaction base).
    if (fs.existsSync(this.snapshotPath)) {
      let snap;
      try {
        snap = this.codec.decode(fs.readFileSync(this.snapshotPath));
      } catch (err) {
        snap = undefined;
        // Corrupt/foreign snapshot — recover from the AOF alone rather than refuse to start, but
        // keep the bad file for inspection instead of silently overwriting it below.
        this._quarantine(this.snapshotPath, err);
      }
      if (snap && snap.version === SNAPSHOT_VERSION) {
        store.load(snap.entries || []);
        this.loadedToken = Math.max(this.loadedToken, snap.fenceToken || 0);
        this.loadedOffset = Math.max(this.loadedOffset, snap.offset || 0);
        // Retained pub/sub messages live in the snapshot too, so compaction can't lose them.
        for (const rec of snap.pubs || []) this._apply(store, rec);
      } else if (snap !== undefined) {
        this._quarantine(this.snapshotPath, new Error(`unsupported snapshot version ${snap && snap.version}`));
      }
    }

    // 2. AOF tail. FrameDecoder yields only complete frames, so a torn final record (kill -9
    //    mid-write) is silently dropped — the log self-truncates at the last good frame.
    if (fs.existsSync(this.aofPath)) {
      const decoder = new FrameDecoder(this.codec);
      const buf = fs.readFileSync(this.aofPath);
      try {
        decoder.push(buf, (rec) => this._apply(store, rec));
      } catch (err) {
        // Decode error mid-stream — stop at the corruption; the valid prefix is already applied.
        // The AOF is truncated below, so preserve a copy of what couldn't be read.
        this._quarantine(this.aofPath, err, { copy: true });
      }
    }

    // 3. Compact: fold what we just recovered into a fresh snapshot, then start a clean AOF. The
    //    snapshot now subsumes the old log (store + retained pubs + reservations), so truncating it
    //    is safe — and required, or recovered pub records would be replayed twice on the next boot.
    this.tokenReserved = this.loadedToken;
    this.offsetReserved = this.loadedOffset;
    this._writeSnapshot(store);
    this.fd = fs.openSync(this.aofPath, 'w');
  }

  /** Move (or copy) an unreadable file aside as `<file>.corrupt-<ts>` and report it. */
  _quarantine(file, cause, { copy = false } = {}) {
    const dest = `${file}.corrupt-${Date.now()}`;
    try {
      if (copy) fs.copyFileSync(file, dest);
      else fs.renameSync(file, dest);
    } catch {
      /* best effort */
    }
    const err = new Error(
      `unreadable persistence file ${path.basename(file)} (kept as ${path.basename(dest)}): ${cause && cause.message}`
    );
    err.code = 'EPERSISTCORRUPT';
    this._onWriteError(err);
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
      case 'offset':
        this.loadedOffset = Math.max(this.loadedOffset, rec.n || 0);
        break;
      case 'pub':
        // A pub record may be durable (fsync'd before its ack) while the offset reservation that
        // covered it was not — so its own offset must also raise the recovered seed, or new
        // publishes would reuse it and consumers would skip them as already seen.
        this.loadedOffset = Math.max(this.loadedOffset, rec.offset || 0);
        // Not store state — hand to the broker's retention ring (bounded there).
        if (this.onPubRecord) this.onPubRecord(rec);
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
    this._maybeCompact();
  }

  /**
   * Durably append a record and fsync BEFORE returning, regardless of the configured fsync mode.
   * Used for `acks:'all'` publishes when pub/sub persistence is on, so a broker crash right after
   * the producer's ack can't lose the message. The AOF is opened in append mode, so each write is
   * atomic even if async cache writes are also draining. Jumping ahead of queued records is safe:
   * a pub record is independent of cache records, and replay dedupes pubs by offset.
   */
  logPublishSync(rec) {
    if (this._closed || this.fd == null) return;
    const frame = encodeFrame(this.codec, rec);
    try {
      fs.writeSync(this.fd, frame);
      fs.fsyncSync(this.fd);
      this.dirty = false;
    } catch (err) {
      this._onWriteError(err);
      return;
    }
    this.opsSinceSnapshot += 1;
    this._maybeCompact();
  }

  /**
   * Schedule a compaction once the op budget is spent. Deferred to the next loop turn, so the full
   * snapshot never runs inside a request (whose reply is already on its way), and a snapshot error
   * can't turn an applied mutation into an error reply the client would retry.
   */
  _maybeCompact() {
    if (this._compactScheduled || this.opsSinceSnapshot < this.aofRewriteOps || !this._store) return;
    this._compactScheduled = true;
    setImmediate(() => {
      this._compactScheduled = false;
      this._runCompact();
    });
  }

  /** Compact now, unless an async write is in flight — then right after it lands. */
  _runCompact() {
    if (this._closed || !this._store) return;
    if (this.writing) {
      this._compactAfterWrite = true;
      return;
    }
    try {
      this._compact();
    } catch (err) {
      this._onWriteError(err);
    }
  }

  /**
   * Called by the broker on every fencing-token mint; reserves a block when crossed. The
   * reservation is written + fsync'd BEFORE the token is handed out (in every mode): if it were
   * lost in a crash, the restarted broker would re-issue tokens already held. ~1 sync per block.
   */
  noteToken(n) {
    if (n > this.tokenReserved) {
      this.tokenReserved = Math.ceil((n + 1) / TOKEN_BLOCK) * TOKEN_BLOCK;
      this._appendSync({ op: 'token', n: this.tokenReserved });
    }
  }

  /** Called by the broker on every pub/sub offset mint; reserves a block (durably) when crossed. */
  noteOffset(n) {
    if (n > this.offsetReserved) {
      this.offsetReserved = Math.ceil((n + 1) / OFFSET_BLOCK) * OFFSET_BLOCK;
      this._appendSync({ op: 'offset', n: this.offsetReserved });
    }
  }

  /** Write + fsync one record now, bypassing the async queue (O_APPEND keeps each write atomic). */
  _appendSync(rec) {
    if (this._closed || this.fd == null) return;
    try {
      fs.writeSync(this.fd, encodeFrame(this.codec, rec));
      fs.fsyncSync(this.fd);
    } catch (err) {
      this._onWriteError(err);
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
    fs.write(this.fd, batch, (err, written) => {
      this.writing = false;
      if (err) {
        this._onWriteError(err);
      } else {
        this.dirty = true;
        // A short write would tear a frame and corrupt every record after it: requeue the rest.
        if (written < batch.length) this.queue.unshift(batch.subarray(written));
      }
      if (this._compactAfterWrite) {
        this._compactAfterWrite = false;
        this._runCompact();
      }
      for (const resolve of this._idleWaiters.splice(0)) resolve();
      if (!err && this.queue.length) this._drain();
    });
  }

  /** Resolve once no async write is in flight. */
  _whenIdle() {
    if (!this.writing) return Promise.resolve();
    return new Promise((resolve) => this._idleWaiters.push(resolve));
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
      offset: this.offsetReserved,
      entries: store.dump(),
      // Retained pub records aren't store state; carrying them in the (atomically renamed)
      // snapshot is what lets compaction truncate the AOF without a window where durably-acked
      // messages exist only in memory.
      pubs: this.onCompactPubReplay ? this.onCompactPubReplay() : [],
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
    // The rename itself must reach disk, or a power loss can resurrect the old snapshot while the
    // AOF it covered has already been truncated. (Directories can't be fsync'd on Windows.)
    if (process.platform !== 'win32') {
      try {
        const dfd = fs.openSync(this.dir, 'r');
        try {
          fs.fsyncSync(dfd);
        } finally {
          fs.closeSync(dfd);
        }
      } catch {
        /* best effort */
      }
    }
  }

  /**
   * Compaction: snapshot current state, then truncate the AOF (snapshot now subsumes it). Callers
   * guarantee no async write is in flight (see _runCompact), so closing the fd can't strand a
   * pending write or let it land on a reused descriptor. If the snapshot fails, the AOF is left
   * untouched; if reopening fails, the error propagates with `fd` cleared rather than stale.
   */
  _compact() {
    if (!this._store) return;
    this._writeSnapshot(this._store);
    this.opsSinceSnapshot = 0;
    // Truncate and reopen so future appends start from empty. (Reopen rather than ftruncate:
    // Windows append-mode handles lack the write access ftruncate needs.)
    if (this.fd != null) fs.closeSync(this.fd);
    this.fd = null;
    fs.closeSync(fs.openSync(this.aofPath, 'w')); // 'w' truncates
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
        this._runCompact(); // waits out any in-flight write
      }, this.snapshotInterval);
      if (this._snapshotTimer.unref) this._snapshotTimer.unref();
    }
  }

  async flushAndClose() {
    if (this._closed) return;
    this._closed = true;
    if (this._fsyncTimer) clearInterval(this._fsyncTimer);
    if (this._snapshotTimer) clearInterval(this._snapshotTimer);
    // Let an in-flight append land before touching the fd (closing under it strands the write).
    await this._whenIdle();
    // Final compaction makes a planned shutdown lossless: the snapshot captures everything
    // (store + retained pubs + reservations), so the AOF — which may lack records still queued in
    // memory — is truncated. Replaying a log missing its newest records over a newer snapshot
    // would roll those keys back.
    let snapshotted = false;
    try {
      if (this._store) {
        this._writeSnapshot(this._store);
        snapshotted = true;
      }
    } catch (err) {
      this._onWriteError(err);
    }
    if (this.fd != null) {
      try {
        if (snapshotted) {
          fs.closeSync(this.fd);
          fs.closeSync(fs.openSync(this.aofPath, 'w'));
        } else {
          // No fresh snapshot: keep the log and flush what's queued so nothing is dropped.
          if (this.queue.length) fs.writeSync(this.fd, Buffer.concat(this.queue));
          fs.fsyncSync(this.fd);
          fs.closeSync(this.fd);
        }
      } catch {
        /* ignore */
      }
      this.queue = [];
      this.fd = null;
    }
    this._releaseLock();
  }

  // ------------------------------------------------------------------- dir lock

  _acquireLock() {
    if (!tryPidLock(this.lockPath)) {
      const e = new Error(`persist dir ${this.dir} is locked by a live broker`);
      e.code = 'EPERSISTLOCKED';
      throw e;
    }
  }

  _releaseLock() {
    releasePidLock(this.lockPath);
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
  return new Persistence({ ...cfg, dir, codec, privateDir: !cfg.dir && !envDir });
}

module.exports = { Persistence, NullPersistence, createPersistence };
