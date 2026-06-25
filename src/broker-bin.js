#!/usr/bin/env node
'use strict';

// Standalone broker entrypoint. Spawned (detached) by clients on auto-start, and
// invoked by the CLI `serve` command. Configuration arrives as JSON via the
// PROCMESH_BROKER_OPTS environment variable.

const Broker = require('./broker');

let opts = {};
try {
  opts = JSON.parse(process.env.PROCMESH_BROKER_OPTS || '{}');
} catch {
  opts = {};
}

const broker = new Broker(opts);

broker
  .start()
  .then(() => {
    // Signal readiness to a parent that used an IPC channel (not the detached case).
    if (typeof process.send === 'function') process.send('ready');
    if (process.env.PROCMESH_VERBOSE) {
      // eslint-disable-next-line no-console
      console.log(`procmesh broker listening on ${broker.address}`);
    }
  })
  .catch((err) => {
    // Lost the race to bind — another broker already owns the socket. That's fine.
    if (err && err.code === 'EADDRINUSE') process.exit(0);
    // eslint-disable-next-line no-console
    console.error('procmesh broker failed to start:', err && err.message);
    process.exit(1);
  });

function shutdown() {
  broker.close().then(() => process.exit(0));
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
