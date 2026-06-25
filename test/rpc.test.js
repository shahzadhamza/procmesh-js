'use strict';

const { test, before, after } = require('node:test');
const assert = require('node:assert');
const { startBroker, client, delay } = require('./helpers');

let broker;
let caller;
let worker;

before(async () => {
  broker = await startBroker();
  caller = await client(broker);
  worker = await client(broker);
});

after(async () => {
  await caller.close();
  await worker.close();
  await broker.close();
});

test('call invokes a handler registered by another process and returns its result', async () => {
  await worker.register('sum', (a, b) => a + b);
  assert.strictEqual(await caller.call('sum', [2, 3]), 5);
});

test('async handlers are awaited', async () => {
  await worker.register('slow', async (x) => {
    await delay(20);
    return x * 10;
  });
  assert.strictEqual(await caller.call('slow', [4]), 40);
});

test('handler errors are relayed to the caller', async () => {
  await worker.register('boom', () => {
    throw new Error('kaboom');
  });
  await assert.rejects(() => caller.call('boom', []), /kaboom/);
});

test('calling an unregistered proc rejects with ENOHANDLER', async () => {
  await assert.rejects(() => caller.call('does-not-exist', []), (err) => {
    assert.strictEqual(err.code, 'ENOHANDLER');
    return true;
  });
});

test('in-flight call rejects if the handler owner disconnects', async () => {
  const flaky = await client(broker);
  await flaky.register('hang', () => new Promise(() => {})); // never resolves
  const pending = caller.call('hang', [], { timeout: 2000 });
  await delay(30);
  await flaky.close();
  await assert.rejects(() => pending, (err) => {
    assert.strictEqual(err.code, 'EHANDLERGONE');
    return true;
  });
});

test('worker pool: N processes registering the same name share the load', async () => {
  const workers = await Promise.all([client(broker), client(broker), client(broker)]);
  try {
    await Promise.all(
      workers.map((w, i) =>
        w.register('work', async () => {
          await delay(30); // hold inflight so least-busy spreads concurrent calls
          return i; // worker index handling this call
        })
      )
    );

    // Fire many calls concurrently so multiple are in-flight at once.
    const results = await Promise.all(Array.from({ length: 12 }, () => caller.call('work')));
    assert.strictEqual(results.length, 12);

    const distinct = new Set(results);
    assert.ok(distinct.size >= 2, `expected load spread across workers, got ${[...distinct]}`);
  } finally {
    await Promise.all(workers.map((w) => w.close()));
  }
});

test('a hung-but-connected worker is reaped broker-side after the call times out', async () => {
  // Dedicated broker with a tiny grace so the backstop fires quickly in the test.
  const b = await startBroker({ callTimeoutGrace: 100 });
  const caller2 = await client(b);
  const w = await client(b);
  try {
    let n = 0;
    await w.register('maybe-hangs', () => {
      n += 1;
      if (n === 1) return new Promise(() => {}); // first call never resolves (worker stays connected)
      return 'ok'; // later calls succeed
    });

    // First call hangs; the caller times out client-side.
    await assert.rejects(() => caller2.call('maybe-hangs', [], { timeout: 150 }), (err) => {
      assert.strictEqual(err.code, 'ETIMEOUT');
      return true;
    });

    // The broker's backstop (timeout + grace) must clear the leaked pending entry and decrement the
    // worker's inflight count — otherwise the worker would look perpetually "busy".
    await delay(250);
    assert.strictEqual((await caller2.stats()).pendingCalls, 0, 'broker freed the hung pending call');

    // Because inflight was decremented, the same worker still receives a fresh call.
    assert.strictEqual(await caller2.call('maybe-hangs', []), 'ok', 'worker still serves after reap');
  } finally {
    await caller2.close();
    await w.close();
    await b.close();
  }
});

test('worker pool survives one worker disconnecting', async () => {
  const w1 = await client(broker);
  const w2 = await client(broker);
  try {
    await w1.register('poolfn', () => 'one');
    await w2.register('poolfn', () => 'two');
    await w1.close(); // drop one worker
    await delay(30);
    // Remaining worker still serves calls.
    const out = await Promise.all([caller.call('poolfn'), caller.call('poolfn')]);
    assert.deepStrictEqual(out, ['two', 'two']);
  } finally {
    await w2.close();
  }
});
