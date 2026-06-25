'use strict';

const { test, before, after } = require('node:test');
const assert = require('node:assert');
const { createClient, ShardedClient } = require('../src');
const { shardIndex } = require('../src/hashring');
const { startBroker, client, delay, once } = require('./helpers');

const N = 3;
let brokers;

before(async () => {
  brokers = [];
  for (let i = 0; i < N; i++) brokers.push(await startBroker());
});

after(async () => {
  await Promise.all(brokers.map((b) => b.close()));
});

/** Build a sharded mesh over the pre-started brokers (no autospawn). */
function shardSpecs() {
  return brokers.map((b) => ({ address: b.address }));
}
async function makeMesh(opts = {}) {
  return createClient({ shards: shardSpecs(), autoSpawn: false, reconnect: false, ...opts });
}

/** Smallest `kN`-style key that hashes to `target` shard. */
function keyForShard(target) {
  for (let i = 0; ; i++) {
    const key = `k${i}`;
    if (shardIndex(key, N) === target) return key;
  }
}

// 1. placement -------------------------------------------------------------
test('keys land on exactly their hashed shard and nowhere else', async () => {
  const mesh = await makeMesh();
  try {
    const keys = ['alpha', 'beta', 'gamma', 'delta', 'epsilon'];
    for (const k of keys) await mesh.set(k, `v:${k}`);

    let total = 0;
    for (let s = 0; s < N; s++) {
      const raw = await client(brokers[s]);
      try {
        const here = await raw.keys();
        total += here.length;
        for (const k of keys) {
          const expected = shardIndex(k, N);
          assert.strictEqual(here.includes(k), s === expected, `key ${k} on shard ${s}?`);
        }
      } finally {
        await raw.close();
      }
    }
    assert.strictEqual(total, keys.length, 'each key stored on exactly one shard');
  } finally {
    await mesh.close();
  }
});

// 2. cache/atomic equivalence ----------------------------------------------
test('cache and atomic ops behave like a single broker', async () => {
  const mesh = await makeMesh();
  try {
    await mesh.set('a', 1);
    assert.strictEqual(await mesh.get('a'), 1);
    assert.strictEqual(await mesh.has('a'), true);
    assert.strictEqual(await mesh.del('a'), true);
    assert.strictEqual(await mesh.get('a'), undefined);

    assert.strictEqual(await mesh.incr('counter', 5), 5);
    assert.strictEqual(await mesh.decr('counter', 2), 3);
    assert.strictEqual(await mesh.cas('cfg', undefined, 'v1'), true);
    assert.strictEqual(await mesh.cas('cfg', 'wrong', 'v2'), false);
    assert.strictEqual(await mesh.get('cfg'), 'v1');
  } finally {
    await mesh.close();
  }
});

// 3. mget order ------------------------------------------------------------
test('mget returns values aligned to input order across shards, undefined for misses', async () => {
  const mesh = await makeMesh();
  try {
    const keys = ['m0', 'm1', 'm2', 'm3', 'm4', 'm5', 'm6'];
    assert.ok(new Set(keys.map((k) => shardIndex(k, N))).size > 1, 'keys must span >1 shard');
    for (let i = 0; i < keys.length; i += 2) await mesh.set(keys[i], i); // even keys only

    const out = await mesh.mget(keys);
    assert.strictEqual(out.length, keys.length);
    keys.forEach((k, i) => {
      assert.strictEqual(out[i], i % 2 === 0 ? i : undefined, `slot ${i} (${k})`);
    });
  } finally {
    await mesh.close();
  }
});

// 4. mset ------------------------------------------------------------------
test('mset routes entries to the correct shards (object and array forms)', async () => {
  const mesh = await makeMesh();
  try {
    await mesh.mset({ s1: 'one', s2: 'two' });
    await mesh.mset([
      ['s3', 'three'],
      ['s4', 'four'],
    ]);
    assert.deepStrictEqual(await mesh.mget(['s1', 's2', 's3', 's4']), ['one', 'two', 'three', 'four']);

    for (const k of ['s1', 's2', 's3', 's4']) {
      const raw = await client(brokers[shardIndex(k, N)]);
      try {
        assert.strictEqual(await raw.has(k), true, `${k} on its shard`);
      } finally {
        await raw.close();
      }
    }
  } finally {
    await mesh.close();
  }
});

// 5. keys()/clear() --------------------------------------------------------
test('keys() unions all shards and clear() empties every shard', async () => {
  const mesh = await makeMesh();
  try {
    await mesh.clear(); // brokers are shared across tests — start from empty
    const keys = ['c1', 'c2', 'c3', 'c4', 'c5'];
    for (const k of keys) await mesh.set(k, 1);
    assert.deepStrictEqual((await mesh.keys()).sort(), [...keys].sort());

    await mesh.clear();
    assert.deepStrictEqual(await mesh.keys(), []);
    for (const b of brokers) {
      const raw = await client(b);
      try {
        assert.strictEqual((await raw.keys()).length, 0);
      } finally {
        await raw.close();
      }
    }
  } finally {
    await mesh.close();
  }
});

// 6. locks / fencing -------------------------------------------------------
test('locks contend on one broker and fenced writes reject stale tokens', async () => {
  const a = await makeMesh();
  const b = await makeMesh();
  try {
    const rel = await a.lock('job:1', { wait: 0 });
    assert.ok(rel, 'first acquire succeeds');
    assert.strictEqual(await b.lock('job:1', { wait: 0 }), null, 'second contends');

    // A write fenced by a stale (already-released-and-superseded) token is rejected.
    await rel();
    const rel2 = await b.lock('job:1', { wait: 0 }); // bumps the fence past rel.token
    assert.ok(rel2);
    await assert.rejects(
      () => a.fencedSet('job:1', rel.token, 'job:1', 'late'),
      (err) => err.code === 'EFENCED'
    );
    await rel2();
  } finally {
    await a.close();
    await b.close();
  }
});

// 7. lock data-key colocation gotcha --------------------------------------
test('withLock ctx.set writes to the lock shard, not the data-key shard', async () => {
  // Find a data key that hashes to a DIFFERENT shard than the lock key.
  const lockKey = 'L';
  let dataKey = null;
  for (let i = 0; i < 1000; i++) {
    const cand = `d${i}`;
    if (shardIndex(cand, N) !== shardIndex(lockKey, N)) {
      dataKey = cand;
      break;
    }
  }
  assert.ok(dataKey, 'found a data key on a different shard');

  const mesh = await makeMesh();
  try {
    await mesh.withLock(lockKey, async (ctx) => {
      await ctx.set(dataKey, 'guarded');
    });
    // It landed on the LOCK's shard...
    const lockBroker = await client(brokers[shardIndex(lockKey, N)]);
    const dataBroker = await client(brokers[shardIndex(dataKey, N)]);
    try {
      assert.strictEqual(await lockBroker.get(dataKey), 'guarded', 'on lock shard');
      assert.strictEqual(await dataBroker.get(dataKey), undefined, 'NOT on data-key shard');
    } finally {
      await lockBroker.close();
      await dataBroker.close();
    }
    // ...so mesh.get(dataKey) (which routes by data key) does NOT see it — the documented gotcha.
    assert.strictEqual(await mesh.get(dataKey), undefined);
  } finally {
    await mesh.close();
  }
});

// 8. RPC by name -----------------------------------------------------------
test('RPC routes by proc name; distinct names spread across shards', async () => {
  // Two names that hash to different shards.
  const names = [];
  for (let i = 0; names.length < 2 && i < 1000; i++) {
    const cand = `proc${i}`;
    if (!names.length || shardIndex(cand, N) !== shardIndex(names[0], N)) names.push(cand);
  }
  assert.strictEqual(new Set(names.map((n) => shardIndex(n, N))).size, 2);

  const worker = await makeMesh();
  const caller = await makeMesh();
  try {
    await worker.register(names[0], (x) => x + 1);
    await worker.register(names[1], (x) => x * 2);
    assert.strictEqual(await caller.call(names[0], [10]), 11);
    assert.strictEqual(await caller.call(names[1], [10]), 20);

    for (const name of names) {
      const raw = await client(brokers[shardIndex(name, N)]);
      try {
        const s = await raw.stats();
        assert.ok(s.procs.some((p) => p.name === name), `${name} registered on its shard`);
      } finally {
        await raw.close();
      }
    }
  } finally {
    await worker.close();
    await caller.close();
  }
});

// 9. RPC least-busy within one name ---------------------------------------
test('two workers on one proc name both serve calls (least-busy dispatch)', async () => {
  const w1 = await makeMesh();
  const w2 = await makeMesh();
  const caller = await makeMesh();
  try {
    const seen = { a: 0, b: 0 };
    await w1.register('work', async () => {
      seen.a += 1;
      await delay(5);
      return 'a';
    });
    await w2.register('work', async () => {
      seen.b += 1;
      await delay(5);
      return 'b';
    });
    const calls = [];
    for (let i = 0; i < 20; i++) calls.push(caller.call('work', []));
    await Promise.all(calls);
    assert.ok(seen.a > 0 && seen.b > 0, `both workers used: ${JSON.stringify(seen)}`);
  } finally {
    await w1.close();
    await w2.close();
    await caller.close();
  }
});

// 10. pub/sub exact --------------------------------------------------------
test('exact pub/sub delivers once with a correct delivered count', async () => {
  const pub = await makeMesh();
  const sub = await makeMesh();
  try {
    const got = [];
    await sub.subscribe('news', (p) => got.push(p));
    const delivered = await pub.publish('news', { headline: 'hi' });
    assert.strictEqual(delivered, 1);
    await delay(40);
    assert.deepStrictEqual(got, [{ headline: 'hi' }]);
  } finally {
    await pub.close();
    await sub.close();
  }
});

// 11. wildcard across shards (no-fan-out regression) ----------------------
test('wildcard subscription catches channels that hash to different shards, once each', async () => {
  // Two channels under one prefix that land on different shards.
  const chans = ['orders.a', 'orders.b', 'orders.c', 'orders.d', 'orders.e', 'orders.f'];
  const a = chans.find((c) => true);
  const b = chans.find((c) => shardIndex(c, N) !== shardIndex(a, N));
  assert.ok(b, 'found two orders.* channels on different shards');

  const pub = await makeMesh();
  const sub = await makeMesh();
  try {
    const got = [];
    const off = await sub.subscribe('orders.*', (payload, channel) => got.push(channel));
    assert.strictEqual(await pub.publish(a, 1), 1, `${a} delivered`);
    assert.strictEqual(await pub.publish(b, 1), 1, `${b} delivered`);
    await delay(50);
    assert.deepStrictEqual(got.sort(), [a, b].sort());
    await off();
  } finally {
    await pub.close();
    await sub.close();
  }
});

// 12. pub/sub dedup --------------------------------------------------------
test('a handler subscribed exactly and by pattern fires only once', async () => {
  const pub = await makeMesh();
  const sub = await makeMesh();
  try {
    let count = 0;
    const handler = () => (count += 1);
    const off1 = await sub.subscribe('metrics.cpu', handler);
    const off2 = await sub.subscribe('metrics.*', handler);
    await pub.publish('metrics.cpu', 1);
    await delay(50);
    assert.strictEqual(count, 1);
    await off1();
    await off2();
  } finally {
    await pub.close();
    await sub.close();
  }
});

// 13. unsubscribe mirrors --------------------------------------------------
test('wildcard unsubscribe tears down on all shards', async () => {
  const pub = await makeMesh();
  const sub = await makeMesh();
  try {
    const got = [];
    const off = await sub.subscribe('evt.*', (p, c) => got.push(c));
    await off();
    // Publish to channels on different shards — none should be delivered now.
    for (const c of ['evt.a', 'evt.b', 'evt.c', 'evt.d']) await pub.publish(c, 1);
    await delay(50);
    assert.deepStrictEqual(got, []);
  } finally {
    await pub.close();
    await sub.close();
  }
});

// 14. stats aggregation ----------------------------------------------------
test('stats() aggregates across shards with a per-shard array', async () => {
  const mesh = await makeMesh();
  try {
    await mesh.clear(); // brokers are shared across tests — start from empty
    for (const k of ['x1', 'x2', 'x3', 'x4']) await mesh.set(k, 1);
    const s = await mesh.stats();
    assert.strictEqual(s.shardCount, N);
    assert.strictEqual(s.shards.length, N);
    assert.strictEqual(s.cacheSize, 4);
    assert.ok(s.connections >= N, 'at least one conn per shard');
    assert.ok(typeof s.cpuCoreFraction === 'number');
    assert.ok(s.memory && typeof s.memory.rss === 'number');
  } finally {
    await mesh.close();
  }
});

// 15. events ---------------------------------------------------------------
test("mesh emits 'connect' when all shards are up and 'shard-disconnect' on shard loss", async () => {
  const extra = [];
  for (let i = 0; i < N; i++) extra.push(await startBroker());
  const mesh = new ShardedClient({
    shards: extra.map((b) => ({ address: b.address })),
    autoSpawn: false,
    reconnect: false,
  });
  try {
    const connected = once(mesh, 'connect');
    await mesh.connect();
    await connected; // fired during connect()

    // Pick a key on shard 0, then kill a DIFFERENT shard so ops on shard 0 still work.
    const liveKey = keyForShardAmong(0);
    await mesh.set(liveKey, 'ok');

    const victim = (shardIndex(liveKey, N) + 1) % N;
    const disconnected = once(mesh, 'shard-disconnect');
    await extra[victim].close();
    const [idx] = await disconnected;
    assert.strictEqual(idx, victim);

    assert.strictEqual(await mesh.get(liveKey), 'ok', 'live shard still serves');
  } finally {
    await mesh.close();
    await Promise.all(extra.map((b) => b.close().catch(() => {})));
  }
});

function keyForShardAmong(target) {
  for (let i = 0; ; i++) {
    const key = `live${i}`;
    if (shardIndex(key, N) === target) return key;
  }
}

// 16. close idempotency ----------------------------------------------------
test('close() is idempotent', async () => {
  const mesh = await makeMesh();
  await mesh.close();
  await mesh.close(); // must not throw
});

// 17. dispatch -------------------------------------------------------------
test('createClient returns a plain Client for shards<=1 and a ShardedClient for shards>1', async () => {
  const { Client } = require('../src');
  const single = await createClient({ address: brokers[0].address, autoSpawn: false, reconnect: false, shards: 1 });
  try {
    assert.ok(single instanceof Client, 'shards:1 → plain Client');
  } finally {
    await single.close();
  }

  const sharded = await makeMesh();
  try {
    assert.ok(sharded instanceof ShardedClient, 'shards:[...] → ShardedClient');
    // Same method surface used by callers.
    for (const m of ['get', 'set', 'del', 'incr', 'cas', 'subscribe', 'publish', 'register', 'call', 'lock', 'withLock']) {
      assert.strictEqual(typeof sharded[m], 'function', `ShardedClient has ${m}()`);
    }
  } finally {
    await sharded.close();
  }
});
