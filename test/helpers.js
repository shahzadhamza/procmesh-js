'use strict';

const { createBroker, createClient } = require('../src');

let counter = 0;
function uniqueName() {
  counter += 1;
  return `test-${process.pid}-${counter}-${Math.random().toString(36).slice(2, 8)}`;
}

async function startBroker(opts = {}) {
  const broker = createBroker({ name: uniqueName(), idleTimeout: 0, ...opts });
  await broker.start();
  return broker;
}

function client(broker, opts = {}) {
  return createClient({ address: broker.address, autoSpawn: false, reconnect: false, ...opts });
}

const delay = (ms) => new Promise((r) => setTimeout(r, ms));

/** Resolve when `emitter` fires `event`, or reject after `ms`. */
function once(emitter, event, ms = 5000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      emitter.removeListener(event, onEvent);
      reject(new Error(`timed out waiting for "${event}"`));
    }, ms);
    function onEvent(...args) {
      clearTimeout(timer);
      resolve(args);
    }
    emitter.once(event, onEvent);
  });
}

module.exports = { uniqueName, startBroker, client, delay, once };
