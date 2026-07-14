#!/usr/bin/env node
'use strict';

const os = require('os');
const path = require('path');
const { Broker, Client } = require('./index');

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--socket' || a === '-s') out.socket = argv[++i];
    else if (a === '--name' || a === '-n') out.name = argv[++i];
    else if (a === '--token' || a === '-t') out.token = argv[++i];
    else if (a === '--json') out.json = true;
    else if (a === '--persist-dir') out.persistDir = argv[++i];
    else if (a === '--no-persist') out.noPersist = true;
    else if (a === '--dedup-max') out.dedupMax = Number(argv[++i]);
    else if (a === '--dedup-ttl') out.dedupTtl = Number(argv[++i]);
    else if (a === '--no-dedup') out.noDedup = true;
    else if (a === '--pubsub-persist') out.pubsubPersist = true;
    else if (a === '--pubsub-retention') out.pubsubRetention = Number(argv[++i]);
  }
  return out;
}

/** True when `dir` resolves to somewhere inside the OS temp directory. */
function isUnderTmp(dir) {
  if (!dir) return false;
  const tmp = path.resolve(os.tmpdir());
  const rel = path.relative(tmp, path.resolve(dir));
  return rel === '' || (!rel.startsWith('..') && !path.isAbsolute(rel));
}

function usage() {
  // eslint-disable-next-line no-console
  console.log(`procmesh — shared in-memory cache & IPC for Node processes

Options: [--name <name>] [--socket <addr>] [--token <secret>]

Usage:
  procmesh serve     run a foreground broker
  procmesh status    check if a broker is up
  procmesh stats     print broker metrics ([--json])
  procmesh stop      ask the broker to shut down

serve pub/sub options:
  --dedup-max <n>        idempotency dedup cache size (default 100000)
  --dedup-ttl <ms>       idle TTL before a producer's dedup state ages out (default 600000)
  --no-dedup             disable idempotency dedup entirely
  --pubsub-persist       persist published messages (durable acks + replay-on-subscribe)
  --pubsub-retention <n> retained messages per channel for replay (default 1000)
`);
}

async function main() {
  const [cmd, ...rest] = process.argv.slice(2);
  const args = parseArgs(rest);

  switch (cmd) {
    case 'serve': {
      // Persistence: on by default for a long-lived `serve` broker (it's the production entry
      // point); disable with --no-persist. Dir from --persist-dir or PROCMESH_PERSIST_DIR.
      const persist = args.noPersist ? { mode: 'off' } : { dir: args.persistDir };
      const dedup = args.noDedup
        ? { enabled: false }
        : { max: args.dedupMax == null ? undefined : args.dedupMax, ttl: args.dedupTtl == null ? undefined : args.dedupTtl };
      // Use == null (not ||) so a legitimate 0 survives: --pubsub-retention 0 means "persist but
      // retain nothing / disable replay", which `|| undefined` would silently turn into the default.
      const pubsub = { persist: !!args.pubsubPersist, retention: args.pubsubRetention == null ? undefined : args.pubsubRetention };
      const broker = new Broker({
        name: args.name,
        address: args.socket,
        token: args.token,
        idleTimeout: 0,
        persist,
        dedup,
        pubsub,
      });
      await broker.start();
      // eslint-disable-next-line no-console
      console.log(`procmesh broker listening on ${broker.address}`);
      // Persistence is on by default for `serve`. Always say WHERE state is written, and warn loudly
      // when it lands in the OS temp dir (which can be cleared on reboot — not safe for production).
      if (broker.persist && broker.persist.enabled) {
        const dir = broker.persist.dir;
        // eslint-disable-next-line no-console
        console.log(`procmesh persistence ON → ${dir} (mode: ${broker.persist.mode})`);
        if (isUnderTmp(dir)) {
          // eslint-disable-next-line no-console
          console.warn(
            `WARNING: persisting under the OS temp dir (${os.tmpdir()}), which may be cleared on ` +
              'reboot. Pass --persist-dir <path> (or PROCMESH_PERSIST_DIR) for durable storage.'
          );
        }
      } else {
        // eslint-disable-next-line no-console
        console.log('procmesh persistence OFF (in-memory only)');
      }
      const stop = () => broker.close().then(() => process.exit(0));
      process.on('SIGINT', stop);
      process.on('SIGTERM', stop);
      break;
    }
    case 'status': {
      const client = new Client({ name: args.name, address: args.socket, token: args.token, autoSpawn: false, reconnect: false });
      try {
        await client.connect();
        await client.ping();
        const keys = await client.keys();
        // eslint-disable-next-line no-console
        console.log(`broker UP at ${client.address} — ${keys.length} key(s) cached`);
        await client.close();
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log(`broker DOWN at ${client.address} (${err.code || err.message})`);
        process.exit(1);
      }
      break;
    }
    case 'stats': {
      const client = new Client({ name: args.name, address: args.socket, token: args.token, autoSpawn: false, reconnect: false });
      try {
        await client.connect();
        const s = await client.stats();
        await client.close();
        if (args.json) {
          // eslint-disable-next-line no-console
          console.log(JSON.stringify(s, null, 2));
        } else {
          // eslint-disable-next-line no-console
          console.log(
            `broker UP at ${client.address}\n` +
              `  uptime ${Math.round(s.uptimeMs / 1000)}s | conns ${s.connections} | cache ${s.cacheSize} keys\n` +
              `  locks ${s.locks} (waiters ${s.lockWaiters}) | pending calls ${s.pendingCalls} | subs ${s.subscriptions}\n` +
              `  dropped ${s.dropped} | duplicates ${s.duplicates || 0} | reaped ${s.reaped} | cpu ${(s.cpuCoreFraction * 100).toFixed(1)}% of a core\n` +
              `  rss ${Math.round(s.memory.rss / 1048576)}MB | heap ${Math.round(s.memory.heapUsed / 1048576)}MB`
          );
        }
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log(`broker DOWN at ${client.address} (${err.code || err.message})`);
        process.exit(1);
      }
      break;
    }
    case 'stop': {
      const client = new Client({ name: args.name, address: args.socket, token: args.token, autoSpawn: false, reconnect: false });
      try {
        await client.connect();
        await client.shutdownBroker();
        await client.close();
        // eslint-disable-next-line no-console
        console.log('broker shutting down');
      } catch (err) {
        // eslint-disable-next-line no-console
        console.log(`could not reach broker (${err.code || err.message})`);
        process.exit(1);
      }
      break;
    }
    default:
      usage();
      if (cmd) process.exit(1);
  }
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err);
  process.exit(1);
});
