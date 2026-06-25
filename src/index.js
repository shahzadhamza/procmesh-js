'use strict';

const Client = require('./client');
const ShardedClient = require('./sharded-client');
const Broker = require('./broker');
const errors = require('./errors');
const { resolveAddress } = require('./transport');

/** True when opts asks for more than one shard (a count > 1 or a multi-element array). */
function isSharded(opts) {
  const s = opts.shards;
  return (typeof s === 'number' && s > 1) || (Array.isArray(s) && s.length > 1);
}

/**
 * Connect to (and, by default, auto-spawn) the shared broker.
 * Returns a connected Client — or, when `opts.shards` requests more than one shard, a
 * ShardedClient that spreads work across N broker processes behind the identical API.
 *
 * @param {object} [opts]
 * @param {string} [opts.name='default']   logical mesh name (maps to a socket)
 * @param {string} [opts.address]          explicit socket path / pipe name
 * @param {string|object} [opts.codec]     'json' (default) | 'msgpack' | custom
 * @param {boolean} [opts.autoSpawn=true]  spawn a broker if none is running
 * @param {boolean} [opts.reconnect=true]  auto-reconnect on connection loss
 * @param {object} [opts.cache]            broker cache config { max, ttl, maxSize }
 * @param {number|Array<string|object>} [opts.shards]  shard across N brokers: a count N
 *        (auto-spawns brokers named `${name}#0..#N-1`) or an array of names/{name,address} specs
 */
async function createClient(opts = {}) {
  const client = isSharded(opts) ? new ShardedClient(opts) : new Client(opts);
  await client.connect();
  return client;
}

/** Create (but do not start) a Broker. Call `.start()` to begin listening. */
function createBroker(opts = {}) {
  return new Broker(opts);
}

module.exports = {
  createClient,
  createBroker,
  Client,
  ShardedClient,
  Broker,
  resolveAddress,
  errors,
};
