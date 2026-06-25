'use strict';

class ProcMeshError extends Error {
  constructor(message, code) {
    super(message);
    this.name = this.constructor.name;
    this.code = code;
  }
}

class Disconnected extends ProcMeshError {
  constructor(message = 'not connected to broker') {
    super(message, 'EDISCONNECTED');
  }
}

class CallTimeout extends ProcMeshError {
  constructor(message = 'request timed out') {
    super(message, 'ETIMEOUT');
  }
}

class LockTimeout extends ProcMeshError {
  constructor(message = 'could not acquire lock') {
    super(message, 'ELOCKTIMEOUT');
  }
}

/** A fenced write was rejected: the caller's lock was superseded (stale fencing token). */
class Fenced extends ProcMeshError {
  constructor(message = 'fenced: stale lock token') {
    super(message, 'EFENCED');
  }
}

/** An error raised on the broker or a remote RPC handler, relayed to the caller. */
class RemoteError extends ProcMeshError {
  constructor(message, code) {
    super(message, code || 'EREMOTE');
  }
}

module.exports = { ProcMeshError, Disconnected, CallTimeout, LockTimeout, Fenced, RemoteError };
