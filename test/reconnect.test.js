'use strict';

const net = require('node:net');
const { test } = require('node:test');
const assert = require('node:assert');
const { createClient, createBroker } = require('../src');
const { resolveAddress } = require('../src/transport');
const { Peer, TYPES, PROTOCOL_VERSION } = require('../src/protocol');
const { jsonCodec } = require('../src/codec');
const { uniqueName, delay, once } = require('./helpers');

test('client reconnects and replays subscriptions after a broker restart', async () => {
  const name = uniqueName();
  let broker = createBroker({ name, idleTimeout: 0 });
  await broker.start();
  const address = broker.address;

  // reconnect-enabled client (autoSpawn off so we control the broker lifecycle)
  const sub = await createClient({ address, autoSpawn: false, reconnect: true });

  const received = [];
  await sub.subscribe('events', (p) => received.push(p));

  // Kill the broker; expect a disconnect.
  const disconnected = once(sub, 'disconnect');
  await broker.close();
  await disconnected;

  // Bring a fresh broker back up on the same address; expect reconnect.
  const reconnected = once(sub, 'reconnect', 8000);
  broker = createBroker({ name, address, idleTimeout: 0 });
  await broker.start();
  await reconnected;

  // The subscription must have been replayed: a new publisher reaches the sub.
  const pub = await createClient({ address, autoSpawn: false, reconnect: false });
  await pub.publish('events', 'after-restart');
  await delay(50);

  assert.deepStrictEqual(received, ['after-restart']);

  await pub.close();
  await sub.close();
  await broker.close();
});

test('client keepalive tears down an unresponsive broker on a live socket (ping timeout)', async () => {
  // A wedged broker that completes the handshake but never answers PINGs, while the socket stays
  // open — exactly the half-open case the broker's own heartbeat can't help a client detect.
  const peers = [];
  const server = net.createServer((socket) => {
    const peer = new Peer(socket, jsonCodec);
    peers.push(peer);
    peer.on('message', (msg) => {
      if (msg.t === TYPES.HELLO) {
        peer.send({ t: TYPES.WELCOME, id: msg.id, version: PROTOCOL_VERSION, broker: 'wedged' });
      }
      // Intentionally ignore PING — no PONG ever comes back.
    });
    peer.on('error', () => {});
  });
  const address = resolveAddress(uniqueName());
  await new Promise((r) => server.listen(address, r));

  const c = await createClient({ address, autoSpawn: false, reconnect: false, pingInterval: 120 });
  try {
    // No socket FIN is sent — the only way the client notices is its own unanswered keepalive ping.
    await once(c, 'disconnect', 4000);
    assert.ok(!c.connected, 'client detected the dead broker via keepalive and disconnected');
  } finally {
    await c.close();
    for (const p of peers) p.destroy();
    await new Promise((r) => server.close(r));
  }
});

test('autoSpawn starts a broker when none is running', async () => {
  const name = uniqueName();
  // No broker started — the client must spawn one (real detached child process).
  const c = await createClient({ name, autoSpawn: true, brokerIdleTimeout: 1000 });
  try {
    await c.set('spawned', true);
    assert.strictEqual(await c.get('spawned'), true);
  } finally {
    await c.shutdownBroker().catch(() => {});
    await c.close();
  }
});

test('a failed handshake leaves the client disconnected, not half-open', async () => {
  const address = resolveAddress(uniqueName());
  const sockets = [];
  const mute = net.createServer((s) => {
    sockets.push(s); // accepts, never answers HELLO
    s.resume(); // flowing, so the client's FIN surfaces as end → close
  });
  await new Promise((r) => mute.listen(address, r));
  try {
    await assert.rejects(
      () => createClient({ address, autoSpawn: false, reconnect: false, callTimeout: 150 }),
      (e) => e.code === 'ETIMEOUT'
    );
    await new Promise((r) => setTimeout(r, 50));
    assert.ok(sockets.length > 0 && sockets.every((s) => s.destroyed), 'client dropped the half-open socket');
  } finally {
    for (const s of sockets) s.destroy();
    await new Promise((r) => mute.close(r));
  }
});

test('the default socket dir is refused if another user could write into it', { skip: typeof process.getuid !== 'function' }, () => {
  const fs = require('node:fs');
  const os = require('node:os');
  const path = require('node:path');
  const { assertPrivateDir } = require('../src/transport');
  const base = fs.mkdtempSync(path.join(os.tmpdir(), 'pm-dir-'));
  const open = path.join(base, 'open');
  fs.mkdirSync(open);
  fs.chmodSync(open, 0o777);
  assert.throws(() => assertPrivateDir(open), (e) => e.code === 'EUNSAFEDIR');
  const link = path.join(base, 'link');
  fs.symlinkSync(base, link);
  assert.throws(() => assertPrivateDir(link), (e) => e.code === 'EUNSAFEDIR');
  const fresh = path.join(base, 'fresh');
  assertPrivateDir(fresh, { create: true });
  assert.strictEqual(fs.statSync(fresh).mode & 0o777, 0o700);
  assert.doesNotThrow(() => assertPrivateDir(path.join(base, 'missing')), 'missing + no create is fine');
  fs.rmSync(base, { recursive: true, force: true });
});
