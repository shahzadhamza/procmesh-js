'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');

/**
 * Per-user directory holding the default POSIX sockets: `<tmpdir>/procmesh-<uid>`. Sockets used to
 * sit directly in the shared, world-writable temp dir, where another local user could squat the
 * path with their own listener (and collect HELLO tokens). A private per-user directory, checked
 * by both broker and client (assertPrivateDir), closes that. Null on Windows (named pipes).
 */
function defaultSocketDir() {
  if (process.platform === 'win32') return null;
  const uid = typeof process.getuid === 'function' ? process.getuid() : 'user';
  return path.join(os.tmpdir(), `procmesh-${uid}`);
}

/**
 * Resolve the local socket address for a given broker name.
 *
 * On Windows we must use a named pipe (`\\.\pipe\<name>`); on POSIX systems we
 * use a Unix domain socket file in a private per-user dir under the OS temp dir
 * (see defaultSocketDir). Node's `net` module accepts both forms transparently as
 * the `path` argument to listen()/connect().
 *
 * Precedence: explicit env override (PROCMESH_SOCKET) > derived from name.
 */
function resolveAddress(name = 'default') {
  if (process.env.PROCMESH_SOCKET) return process.env.PROCMESH_SOCKET;
  const id = `procmesh-${name}`;
  if (process.platform === 'win32') {
    return `\\\\.\\pipe\\${id}`;
  }
  return path.join(defaultSocketDir(), `${id}.sock`);
}

/** True when `address` is a socket in the default per-user dir (so the privacy checks apply). */
function isDefaultSocket(address) {
  const dir = defaultSocketDir();
  return dir != null && typeof address === 'string' && path.dirname(address) === dir;
}

/**
 * Ensure `dir` is safe to hold our sockets or state: a real directory (not a symlink), owned by
 * this user, not writable by group/others. With `create`, a missing dir is made (mode 0700);
 * without it, a missing dir passes (nothing to trust yet). Throws EUNSAFEDIR otherwise. A no-op
 * beyond creation on platforms without POSIX ownership (Windows).
 */
function assertPrivateDir(dir, { create = false } = {}) {
  if (create) fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  if (typeof process.getuid !== 'function') return;
  let st;
  try {
    st = fs.lstatSync(dir);
  } catch (err) {
    if (err.code === 'ENOENT' && !create) return;
    throw err;
  }
  const uid = process.getuid();
  let why = null;
  if (st.isSymbolicLink()) why = 'is a symlink';
  else if (!st.isDirectory()) why = 'is not a directory';
  else if (st.uid !== uid) why = `is owned by uid ${st.uid}, not ${uid}`;
  else if (st.mode & 0o022) why = `is group/world-writable (mode ${(st.mode & 0o777).toString(8)})`;
  if (why) {
    const err = new Error(`refusing to use ${dir}: it ${why}`);
    err.code = 'EUNSAFEDIR';
    throw err;
  }
}

/** True if the address is a Windows named pipe (no filesystem entry to clean up). */
function isPipe(address) {
  return (
    typeof address === 'string' &&
    (address.startsWith('\\\\.\\pipe\\') || address.startsWith('\\\\?\\pipe\\'))
  );
}

/**
 * Try to take an exclusive pid lockfile at `file` (O_EXCL create). A lockfile left behind by a
 * process that no longer exists is reclaimed. Returns true if acquired, false if a live process
 * holds it; other filesystem errors are thrown.
 */
function tryPidLock(file) {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const fd = fs.openSync(file, 'wx'); // fail if exists
      try {
        fs.writeSync(fd, String(process.pid));
      } finally {
        fs.closeSync(fd);
      }
      return true;
    } catch (err) {
      if (err.code !== 'EEXIST') throw err;
      if (!pidLockIsStale(file)) return false; // (a reused pid reads as live: delete `file` by hand)
      try {
        fs.unlinkSync(file);
      } catch {
        /* race: another process cleared it; retry */
      }
    }
  }
  return false;
}

function pidLockIsStale(file) {
  try {
    const pid = parseInt(fs.readFileSync(file, 'utf8').trim(), 10);
    if (!pid) return true;
    process.kill(pid, 0); // throws ESRCH if the pid is gone
    return false; // pid alive → not stale
  } catch (err) {
    return err.code === 'ESRCH' || err.code === 'ENOENT'; // gone → stale
  }
}

function releasePidLock(file) {
  try {
    fs.unlinkSync(file);
  } catch {
    /* ignore */
  }
}

module.exports = {
  resolveAddress,
  defaultSocketDir,
  isDefaultSocket,
  assertPrivateDir,
  isPipe,
  tryPidLock,
  releasePidLock,
};
