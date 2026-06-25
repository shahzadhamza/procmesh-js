'use strict';

const net = require('net');
const { test } = require('node:test');
const assert = require('node:assert');
const { startBroker, once } = require('./helpers');

test('an unresponsive connection is reaped by the heartbeat', async () => {
  const broker = await startBroker({ heartbeatInterval: 60 });
  try {
    const reaped = once(broker, 'reap', 4000);
    // Raw socket that connects and then never sends anything (won't answer PINGs).
    const sock = net.connect(broker.address);
    await once(sock, 'connect');

    const [connId] = await reaped;
    assert.ok(connId >= 1, 'broker should reap the idle connection');
    sock.destroy();
  } finally {
    await broker.close();
  }
});

test('a responsive client is NOT reaped (PONG keeps it alive)', async () => {
  const { client } = require('./helpers');
  const broker = await startBroker({ heartbeatInterval: 60 });
  try {
    const c = await client(broker);
    let reapedId = null;
    broker.on('reap', (id) => { reapedId = id; });
    // Live long enough for several heartbeat cycles; client auto-replies PONG.
    await new Promise((r) => setTimeout(r, 300));
    assert.strictEqual(reapedId, null, 'responsive client must not be reaped');
    assert.strictEqual(await c.get('anything'), undefined); // still usable
    await c.close();
  } finally {
    await broker.close();
  }
});
