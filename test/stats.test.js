'use strict';

const net = require('node:net');
const { test, before, after } = require('node:test');
const assert = require('node:assert');
const { startBroker, client, delay, once } = require('./helpers');
const { encodeFrame } = require('../src/protocol');
const { jsonCodec } = require('../src/codec');

let broker;

before(async () => {
  broker = await startBroker();
});

after(async () => {
  await broker.close();
});

test('stats() reports a coherent operational snapshot', async () => {
  const a = await client(broker);
  const b = await client(broker);
  try {
    await a.set('k1', 1);
    await a.set('k2', 2);
    await a.incr('c', 3);
    await b.subscribe('topic', () => {});
    const rel = await a.lock('L', { wait: 0 });

    const s = await a.stats();
    assert.ok(s.uptimeMs >= 0);
    assert.strictEqual(s.connections, 2);
    assert.ok(s.cacheSize >= 3, `cacheSize ${s.cacheSize}`);
    assert.ok(s.ops.set >= 2, 'counts set ops');
    assert.strictEqual(s.locks, 1);
    assert.strictEqual(s.subscriptions, 1);
    assert.ok(s.memory && typeof s.memory.rss === 'number');
    assert.ok(typeof s.cpuCoreFraction === 'number');

    await rel();
  } finally {
    await a.close();
    await b.close();
  }
});

test('unknown message types never create ops counter keys (memory-DoS guard)', async () => {
  // A peer can open a socket and send arbitrary `t` strings. If the broker keyed its ops counter
  // by `t` before validating the type, this would grow an object without bound → OOM.
  const sock = net.connect(broker.address);
  await once(sock, 'connect');
  for (let i = 0; i < 200; i++) {
    sock.write(encodeFrame(jsonCodec, { t: `junk-${i}`, id: i }));
  }
  await delay(50);
  const snap = broker.snapshot();
  const junk = Object.keys(snap.ops).filter((k) => k.startsWith('junk-'));
  assert.strictEqual(junk.length, 0, `unknown types must not be counted, got: ${junk.slice(0, 5)}`);
  sock.destroy();
});

test("broker emits 'connect' and 'disconnect' with conn ids", async () => {
  const seen = { connect: 0, disconnect: 0 };
  const onConnect = () => (seen.connect += 1);
  const onDisconnect = () => (seen.disconnect += 1);
  broker.on('connect', onConnect);
  broker.on('disconnect', onDisconnect);
  try {
    const c = await client(broker);
    await c.ping();
    await c.close();
    // allow the close to propagate to the broker
    await new Promise((r) => setTimeout(r, 30));
    assert.ok(seen.connect >= 1, 'connect fired');
    assert.ok(seen.disconnect >= 1, 'disconnect fired');
  } finally {
    broker.removeListener('connect', onConnect);
    broker.removeListener('disconnect', onDisconnect);
  }
});
