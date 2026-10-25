import assert from 'node:assert/strict';
import { getEventListeners } from 'node:events';
import test from 'node:test';
import { setup, intercept } from './helpers.js';

test('native pre-abort cannot be hidden by an own aborted property', async t => {
  const { port, input, events } = setup();
  const network = intercept(t);
  const controller = new AbortController();
  controller.abort();
  Object.defineProperty(controller.signal, 'aborted', { value: false });
  input.signal = controller.signal;

  await assert.rejects(port.execute(input), { code: 'DISPATCH_BINDING_INVALID' });
  assert.equal(network.dns, 0);
  assert.equal(network.requests, 0);
  assert.equal(network.writes, 0);
  assert.deepEqual(events, []);
});

for (const property of ['aborted', 'addEventListener', 'removeEventListener']) {
  test(`caller-owned ${property} getter is not executed by the public port`, async t => {
    const { port, input } = setup();
    const network = intercept(t);
    const controller = new AbortController();
    let reads = 0;
    Object.defineProperty(controller.signal, property, {
      get() {
        reads++;
        throw new Error('SYNTHETIC_SIGNAL_ACCESSOR');
      },
    });
    input.signal = controller.signal;

    await assert.rejects(port.execute(input), { code: 'DISPATCH_BINDING_INVALID' });
    assert.equal(network.dns, 0);
    assert.equal(network.writes, 0);
    assert.equal(reads, 0);
  });
}

test('shadowed listener methods are rejected before dispatch', async t => {
  const { port, input } = setup();
  const network = intercept(t);
  const controller = new AbortController();
  let methodCalls = 0;
  Object.defineProperties(controller.signal, {
    addEventListener: { value() { methodCalls++; } },
    removeEventListener: { value() { methodCalls++; } },
  });
  input.signal = controller.signal;
  await assert.rejects(port.execute(input), { code: 'DISPATCH_BINDING_INVALID' });
  assert.equal(network.dns, 0);
  assert.equal(network.writes, 0);
  assert.equal(methodCalls, 0);
});

test('native post-dispatch cancellation survives shadows installed after registration', async t => {
  const { port, input } = setup();
  const controller = new AbortController();
  input.signal = controller.signal;
  let reads = 0;
  const network = intercept(t, { afterWrite() {
    Object.defineProperties(controller.signal, {
      aborted: { value: false },
      removeEventListener: { get() { reads++; throw new Error('SYNTHETIC_SIGNAL_ACCESSOR'); } },
    });
    controller.abort();
  } });

  const result = await port.execute(input);

  assert.equal(result.status, 'cancelled');
  assert.equal(result.usage.completeness, 'unknown');
  assert.equal(network.writes, 1);
  assert.equal(reads, 0);
});

for (const boundary of ['authorization', 'final-fence', 'body']) {
  test(`earlier stopImmediatePropagation cannot hide cancellation at ${boundary}`, async t => {
    const { port, input } = setup();
    const controller = new AbortController();
    controller.signal.addEventListener('abort', event => event.stopImmediatePropagation());
    input.signal = controller.signal;
    if (boundary === 'authorization') {
      input.revalidateDispatch = () => { controller.abort(); return new Promise(() => {}); };
    } else if (boundary === 'final-fence') {
      input.assertDispatchCurrent = () => { controller.abort(); return 'intent-fixture'; };
    }
    const network = intercept(t, boundary === 'body' ? { afterWrite() { controller.abort(); } } : {});

    const result = await port.execute(input);

    assert.equal(result.status, 'cancelled');
    assert.equal(result.usage.completeness, 'unknown');
    assert.equal(network.writes, boundary === 'body' ? 1 : 0);
  });
}

test('composite signals do not inspect shadowed state on their underlying sources', async t => {
  const { port, input } = setup();
  const source = new AbortController();
  input.signal = AbortSignal.any([source.signal]);
  let reads = 0;
  Object.defineProperty(source.signal, 'aborted', {
    get() { reads++; throw new Error('SYNTHETIC_SOURCE_ACCESSOR'); },
  });
  const network = intercept(t, { afterWrite() { source.abort(); } });

  const result = await port.execute(input);

  assert.equal(result.status, 'cancelled');
  assert.equal(result.usage.completeness, 'unknown');
  assert.equal(network.writes, 1);
  assert.equal(reads, 0);
});

for (const deny of [false, true]) {
  test(`abort listeners are removed after ${deny ? 'denied' : 'completed'} dispatch despite late method shadows`, async t => {
    const { port, input } = setup();
    const controller = new AbortController();
    input.signal = controller.signal;
    let reads = 0;
    const baseline = getEventListeners(input.signal, 'abort').length;
    input.revalidateDispatch = () => {
      Object.defineProperty(input.signal, 'removeEventListener', {
        get() { reads++; throw new Error('SYNTHETIC_REMOVE_ACCESSOR'); },
      });
      return deny ? 'denied' : 'intent-fixture';
    };
    const network = intercept(t);

    if (deny) await assert.rejects(port.execute(input), { code: 'DISPATCH_AUTHORIZATION_REJECTED' });
    else assert.equal((await port.execute(input)).status, 'complete');

    assert.equal(network.writes, deny ? 0 : 1);
    assert.equal(getEventListeners(input.signal, 'abort').length, baseline);
    assert.equal(reads, 0);
  });
}
