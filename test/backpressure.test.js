'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { startBroker, client, delay } = require('./helpers');

test('a slow/stalled subscriber gets messages dropped, not the broker OOM-ing', async () => {
  // Tiny HWM so a paused consumer trips backpressure quickly.
  const broker = await startBroker({ sendHighWaterMark: 1024, sendHardLimit: 64 * 1024 * 1024 });
  const slow = await client(broker);
  const fast = await client(broker);
  try {
    let fastReceived = 0;
    await fast.subscribe('fire', () => { fastReceived += 1; });
    await slow.subscribe('fire', () => {}); // handler never matters — we stall the socket

    // Stop the slow subscriber from reading: the broker's write buffer to it fills.
    slow.peer.socket.pause();

    const payload = 'x'.repeat(4096); // 4 KiB each

    // Publish a burst from a third (fast) connection.
    const pub = await client(broker);
    for (let i = 0; i < 500; i++) {
      // eslint-disable-next-line no-await-in-loop
      await pub.publish('fire', payload);
    }
    await delay(50);
    await pub.close();

    assert.ok(broker.dropped > 0, `expected some dropped frames, got ${broker.dropped}`);
    assert.ok(fastReceived > 0, 'fast subscriber should keep receiving while slow one is throttled');

    slow.peer.socket.resume();
  } finally {
    await slow.close();
    await fast.close();
    await broker.close();
  }
});
