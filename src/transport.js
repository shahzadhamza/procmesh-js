'use strict';

const os = require('os');
const path = require('path');

/**
 * Resolve the local socket address for a given broker name.
 *
 * On Windows we must use a named pipe (`\\.\pipe\<name>`); on POSIX systems we
 * use a Unix domain socket file under the OS temp dir. Node's `net` module
 * accepts both forms transparently as the `path` argument to listen()/connect().
 *
 * Precedence: explicit env override (PROCMESH_SOCKET) > derived from name.
 */
function resolveAddress(name = 'default') {
  if (process.env.PROCMESH_SOCKET) return process.env.PROCMESH_SOCKET;
  const id = `procmesh-${name}`;
  if (process.platform === 'win32') {
    return `\\\\.\\pipe\\${id}`;
  }
  return path.join(os.tmpdir(), `${id}.sock`);
}

/** True if the address is a Windows named pipe (no filesystem entry to clean up). */
function isPipe(address) {
  return (
    typeof address === 'string' &&
    (address.startsWith('\\\\.\\pipe\\') || address.startsWith('\\\\?\\pipe\\'))
  );
}

module.exports = { resolveAddress, isPipe };
