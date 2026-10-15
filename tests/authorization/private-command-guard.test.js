import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createPrivateCommandGuard, normalizePrivateCommandInput } from '../../src/internal/private-command-guard.js';

const POLICY = Object.freeze({
  maxBodyBytes: 512,
  maxTextBytes: 64,
  maxSourceRefs: 2,
  maxSourceRefBytes: 32,
  maxIdentityBytes: 48,
  minVariantCount: 1,
  maxVariantCount: 3,
  allowedMediaTypes: Object.freeze(['application/json']),
  allowedOperations: Object.freeze(['read', 'lookup', 'submit', 'status', 'resume', 'cancel', 'retrieve', 'reserve', 'dispatch', 'mutate'])
});
const ACTIONS = Object.freeze([
  Object.freeze({ action: 'project.read', operation: 'read', kind: 'project', id: 'project-A' }),
  Object.freeze({ action: 'artifact.lookup', operation: 'lookup', kind: 'artifact', id: 'artifact-A' }),
  Object.freeze({ action: 'artifact.read', operation: 'read', kind: 'artifact', id: 'artifact-A' }),
  Object.freeze({ action: 'work.submit', operation: 'submit', kind: 'project', id: 'project-A' }),
  Object.freeze({ action: 'job.status', operation: 'status', kind: 'job', id: 'job-A' }),
  Object.freeze({ action: 'job.resume', operation: 'resume', kind: 'job', id: 'job-A' }),
  Object.freeze({ action: 'job.cancel', operation: 'cancel', kind: 'job', id: 'job-A' }),
  Object.freeze({ action: 'source.retrieve', operation: 'retrieve', kind: 'source', id: 'source-A' }),
  Object.freeze({ action: 'billing.reserve', operation: 'reserve', kind: 'billing', id: 'billing-A' }),
  Object.freeze({ action: 'billing.dispatch', operation: 'dispatch', kind: 'billing_attempt', id: 'attempt-A' }),
  Object.freeze({ action: 'provider.dispatch', operation: 'dispatch', kind: 'work', id: 'work-A' }),
  Object.freeze({ action: 'state.mutate', operation: 'mutate', kind: 'work', id: 'work-A' })
]);
const EFFECTS = Object.freeze([
  'readProject',
  'lookupArtifact',
  'readArtifact',
  'enqueueWork',
  'retrieveSource',
  'reserveBilling',
  'dispatchBilling',
  'dispatchProvider',
  'mutateState'
]);

function command(actionName, overrides = {}) {
  const descriptor = ACTIONS.find(item => item.action === actionName);
  assert.ok(descriptor, 'test action must be explicitly listed');
  return {
    version: 'agent-harness.private-command.v1',
    action: descriptor.action,
    resource: { kind: descriptor.kind, id: descriptor.id },
    mediaType: 'application/json',
    operation: descriptor.operation,
    body: {},
    ...overrides
  };
}

function allowedDecision(request, overrides = {}) {
  return {
    outcome: 'allow',
    action: request.action,
    resource: { kind: request.resource.kind, id: request.resource.id },
    scope: { tenantId: 'tenant-A', projectId: 'project-A' },
    expiresAtMs: 2_000,
    revoked: false,
    membershipRevision: 'membership-1',
    currentMembershipRevision: 'membership-1',
    grantRevision: 'grant-1',
    currentGrantRevision: 'grant-1',
    ...overrides
  };
}

function localPolicy(overrides = {}) {
  return {
    maxBodyBytes: 256,
    maxTextBytes: 4,
    maxSourceRefs: 2,
    maxSourceRefBytes: 8,
    maxIdentityBytes: 32,
    minVariantCount: 1,
    maxVariantCount: 2,
    allowedMediaTypes: ['application/json'],
    allowedOperations: ['read', 'lookup', 'submit', 'status', 'resume', 'cancel', 'retrieve', 'reserve', 'dispatch', 'mutate'],
    ...overrides
  };
}

function fakeHarness({ resolve, now = () => 1_000, onFactory, callbacks, policy = POLICY } = {}) {
  const counters = Object.fromEntries(EFFECTS.map(name => [name, 0]));
  const resolverCalls = [];
  let factoryCalls = 0;
  const effectCallbacks = Object.create(null);
  for (const name of EFFECTS) {
    effectCallbacks[name] = callbacks?.[name] ?? (async () => {
      counters[name] += 1;
      throw new Error(`private fake failure ${name}`);
    });
  }
  const guard = createPrivateCommandGuard({
    policy,
    now,
    resolveCurrentDecision(request) {
      resolverCalls.push(request);
      return resolve ? resolve(request, resolverCalls.length) : allowedDecision(request);
    },
    createDownstream(context) {
      factoryCalls += 1;
      onFactory?.(context);
      return effectCallbacks[context.effect];
    }
  });
  return {
    guard,
    counters,
    resolverCalls,
    get factoryCalls() { return factoryCalls; }
  };
}

function assertNoBusinessEffects(harness) {
  assert.deepEqual(harness.counters, Object.fromEntries(EFFECTS.map(name => [name, 0])));
}

function assertFixedFailure(value, code) {
  assert.deepEqual(value, { ok: false, code });
  assert.deepEqual(Object.keys(value).sort(), ['code', 'ok']);
  assert.ok(Object.isFrozen(value));
}

test('private normalization can accept a valid bounded request without granting authority', async () => {
  const input = command('work.submit', { body: { text: 'synthetic', sourceRefs: ['ref-1'], variantCount: 2 } });
  const normalized = normalizePrivateCommandInput(input, POLICY);
  assert.equal(normalized.ok, true);
  assert.equal(normalized.command.action, 'work.submit');
  assert.equal(normalized.command.payload.text, 'synthetic');

  const harness = fakeHarness({ resolve: () => Object.freeze({ outcome: 'deny' }) });
  assertFixedFailure(await harness.guard.execute(input), 'AUTH_DENIED');
  assert.equal(harness.resolverCalls.length, 1);
  assert.equal(harness.factoryCalls, 0);
  assertNoBusinessEffects(harness);
});

test('foreign and missing artifact denials have the same bounded projection before lookup', async () => {
  const harness = fakeHarness({ resolve: () => Object.freeze({ outcome: 'deny', internalReason: 'ignored' }) });
  const foreign = command('artifact.read', {
    resource: { kind: 'artifact', id: 'artifact-foreign' },
    body: { scope: { tenantId: 'tenant-A', projectId: 'project-A' } }
  });
  const missing = command('artifact.read', {
    resource: { kind: 'artifact', id: 'artifact-missing' },
    body: { scope: { tenantId: 'tenant-A', projectId: 'project-A' } }
  });
  const first = await harness.guard.execute(foreign);
  const second = await harness.guard.execute(missing);
  assert.deepEqual(first, second);
  assertFixedFailure(first, 'AUTH_DENIED');
  assert.equal(harness.factoryCalls, 0);
  assertNoBusinessEffects(harness);
  assert.deepEqual(harness.resolverCalls.map(item => item.action), ['artifact.read', 'artifact.read']);
  assert.deepEqual(harness.resolverCalls.map(item => item.resource.id), ['artifact-foreign', 'artifact-missing']);
  assert.ok(harness.resolverCalls.every(item => Object.keys(item).sort().join(',') === 'action,resource'));
});

test('all nine business-effect callbacks remain untouched when authorization denies each action', async () => {
  const harness = fakeHarness({ resolve: () => Object.freeze({ outcome: 'deny' }) });
  for (const item of ACTIONS) {
    const body = item.action === 'work.submit' ? { text: 'ok' } : {};
    assertFixedFailure(await harness.guard.execute(command(item.action, { body })), 'AUTH_DENIED');
  }
  assert.equal(harness.resolverCalls.length, ACTIONS.length);
  assert.equal(harness.factoryCalls, 0);
  assertNoBusinessEffects(harness);
});

test('forged body scope and body-provided role, capabilities, or query never grant access', async () => {
  const harness = fakeHarness();
  const forgedScope = command('artifact.read', {
    body: { scope: { tenantId: 'tenant-B', projectId: 'project-B' } }
  });
  assertFixedFailure(await harness.guard.execute(forgedScope), 'AUTH_DENIED');

  const forgedRole = command('artifact.read', { body: { role: 'owner' } });
  assertFixedFailure(await harness.guard.execute(forgedRole), 'INPUT_INVALID');
  const forgedCapabilities = command('artifact.read', { body: { capabilities: ['agent.use'] } });
  assertFixedFailure(await harness.guard.execute(forgedCapabilities), 'INPUT_INVALID');
  const forgedQuery = { ...command('artifact.read'), query: { tenantId: 'tenant-A' } };
  assertFixedFailure(await harness.guard.execute(forgedQuery), 'INPUT_INVALID');

  assert.equal(harness.resolverCalls.length, 1);
  assert.equal(harness.factoryCalls, 0);
  assertNoBusinessEffects(harness);
});

test('the decision must bind the exact action, resource kind, and artifact identifier', async () => {
  const mismatchAction = fakeHarness({ resolve: request => allowedDecision(request, { action: 'project.read' }) });
  assertFixedFailure(await mismatchAction.guard.execute(command('artifact.read')), 'AUTH_DENIED');
  assert.equal(mismatchAction.factoryCalls, 0);
  assertNoBusinessEffects(mismatchAction);

  const mismatchResource = fakeHarness({ resolve: request => allowedDecision(request, {
    resource: { kind: 'artifact', id: 'artifact-other' }
  }) });
  assertFixedFailure(await mismatchResource.guard.execute(command('artifact.read')), 'AUTH_DENIED');
  assert.equal(mismatchResource.factoryCalls, 0);
  assertNoBusinessEffects(mismatchResource);

  const wrongKind = fakeHarness();
  assertFixedFailure(await wrongKind.guard.execute(command('artifact.read', {
    resource: { kind: 'project', id: 'artifact-A' }
  })), 'INPUT_INVALID');
  assert.equal(wrongKind.resolverCalls.length, 0);
  assert.equal(wrongKind.factoryCalls, 0);
});

test('expiry, revocation, stale membership, and stale grant fail closed before factories', async () => {
  const cases = [
    ['job.status', { expiresAtMs: 999 }],
    ['job.resume', { revoked: true }],
    ['job.cancel', { membershipRevision: 'membership-old', currentMembershipRevision: 'membership-new' }],
    ['job.status', { grantRevision: 'grant-old', currentGrantRevision: 'grant-new' }]
  ];
  for (const [action, changes] of cases) {
    const harness = fakeHarness({ resolve: request => allowedDecision(request, changes) });
    assertFixedFailure(await harness.guard.execute(command(action)), 'AUTH_DENIED');
    assert.equal(harness.resolverCalls.length, 1);
    assert.equal(harness.factoryCalls, 0);
    assertNoBusinessEffects(harness);
  }
});

test('status, resume, and cancel resolve current authority on every call after state changes', async () => {
  let state = 'active';
  const harness = fakeHarness({
    resolve(request) {
      const decision = allowedDecision(request);
      if (state === 'revoked') return { ...decision, revoked: true };
      if (state === 'stale-membership') {
        return { ...decision, membershipRevision: 'membership-old', currentMembershipRevision: 'membership-new' };
      }
      if (state === 'stale-grant') return { ...decision, grantRevision: 'grant-old', currentGrantRevision: 'grant-new' };
      return decision;
    },
    callbacks: {
      readProject: async () => undefined,
      enqueueWork: async () => undefined,
      mutateState: async () => undefined
    }
  });
  assert.deepEqual(await harness.guard.execute(command('job.status')), { ok: true, code: 'EXECUTED' });
  state = 'revoked';
  assertFixedFailure(await harness.guard.execute(command('job.status')), 'AUTH_DENIED');
  state = 'stale-membership';
  assertFixedFailure(await harness.guard.execute(command('job.resume')), 'AUTH_DENIED');
  state = 'stale-grant';
  assertFixedFailure(await harness.guard.execute(command('job.cancel')), 'AUTH_DENIED');
  assert.deepEqual(harness.resolverCalls.map(item => item.action), [
    'job.status', 'job.status', 'job.resume', 'job.cancel'
  ]);
  assert.equal(harness.factoryCalls, 1);
  assert.equal(harness.counters.readProject, 0);
  assert.equal(harness.counters.enqueueWork, 0);
  assert.equal(harness.counters.mutateState, 0);
});

test('internal normalized policy accepts exact UTF-8 and count bounds but rejects each over-bound value', async () => {
  const policy = localPolicy();
  const exact = command('work.submit', {
    body: { text: 'éé', sourceRefs: ['ref-1', 'ref-2'], variantCount: 2 }
  });
  const normalized = normalizePrivateCommandInput(exact, policy);
  assert.equal(normalized.ok, true);
  assert.equal(Buffer.byteLength(normalized.command.payload.text, 'utf8'), 4);
  assert.equal(normalized.command.payload.sourceRefs.length, 2);
  assert.equal(normalized.command.payload.variantCount, 2);

  assertFixedFailure(normalizePrivateCommandInput(command('work.submit', {
    body: { text: 'ééa' }
  }), policy), 'INPUT_INVALID');
  assertFixedFailure(normalizePrivateCommandInput(command('work.submit', {
    body: { sourceRefs: ['one', 'two', 'three'] }
  }), policy), 'INPUT_INVALID');
  assertFixedFailure(normalizePrivateCommandInput(command('work.submit', {
    body: { sourceRefs: ['123456789'] }
  }), policy), 'INPUT_INVALID');
  assertFixedFailure(normalizePrivateCommandInput(command('work.submit', {
    body: { variantCount: 3 }
  }), policy), 'INPUT_INVALID');

  assertFixedFailure(normalizePrivateCommandInput(command('work.submit'), localPolicy({ maxBodyBytes: 4097 })), 'INPUT_INVALID');
  assertFixedFailure(normalizePrivateCommandInput(command('work.submit'), localPolicy({ allowedOperations: 'submit' })), 'INPUT_INVALID');
  assertFixedFailure(normalizePrivateCommandInput(command('work.submit'), localPolicy({ allowedMediaTypes: ['application/json', 'application/json'] })), 'INPUT_INVALID');
  assertFixedFailure(normalizePrivateCommandInput(command('work.submit'), localPolicy({ allowedMediaTypes: ['application/json\n'] })), 'INPUT_INVALID');

  const exactBodyPolicy = localPolicy({ maxBodyBytes: 12 });
  assert.equal(normalizePrivateCommandInput(command('work.submit', { body: { text: 'x' } }), exactBodyPolicy).ok, true);
  assertFixedFailure(normalizePrivateCommandInput(command('work.submit', {
    body: { text: 'x', sourceRefs: ['r'] }
  }), exactBodyPolicy), 'INPUT_INVALID');

  const boundedGuard = fakeHarness({ policy });
  for (const body of [
    { text: 'ééa' },
    { sourceRefs: ['one', 'two', 'three'] },
    { sourceRefs: ['123456789'] },
    { variantCount: 3 }
  ]) {
    assertFixedFailure(await boundedGuard.guard.execute(command('work.submit', { body })), 'INPUT_INVALID');
  }
  assert.equal(boundedGuard.resolverCalls.length, 0);
  assert.equal(boundedGuard.factoryCalls, 0);
  assertNoBusinessEffects(boundedGuard);
});

test('unsupported media, operation, version, URL scheme, shape, and hostile input controls fail before resolution', async () => {
  let getterCalls = 0;
  const base = command('work.submit', { body: { text: 'ok' } });
  const accessor = { ...base };
  Object.defineProperty(accessor, 'action', {
    enumerable: true,
    get() { getterCalls += 1; return 'work.submit'; }
  });
  let proxyCalls = 0;
  const proxy = new Proxy(base, { get() { proxyCalls += 1; throw new Error('raw proxy error'); } });
  const revokedObject = Proxy.revocable({}, {});
  revokedObject.revoke();
  const revokedArray = Proxy.revocable([], {});
  revokedArray.revoke();
  const cases = [
    command('work.submit', { mediaType: 'application/pdf', body: { text: 'ok' } }),
    command('work.submit', { operation: 'admin.grant', body: { text: 'ok' } }),
    command('work.submit', { version: 'agent-harness.private-command.v9', body: { text: 'ok' } }),
    command('artifact.read', { resource: { kind: 'artifact', id: 'artifact-A\n' } }),
    command('work.submit', { body: [] }),
    command('work.submit', { body: { text: 7 } }),
    command('work.submit', { body: { sourceRefs: ['file:///private/item'] } }),
    command('work.submit', { body: { sourceRefs: ['reference-1\n'] } }),
    revokedObject.proxy,
    command('work.submit', { body: { sourceRefs: revokedArray.proxy } }),
    accessor,
    proxy
  ];
  const harness = fakeHarness();
  for (const input of cases) assertFixedFailure(await harness.guard.execute(input), 'INPUT_INVALID');
  assert.equal(getterCalls, 0);
  assert.equal(proxyCalls, 0);
  assert.equal(harness.resolverCalls.length, 0);
  assert.equal(harness.factoryCalls, 0);
  assertNoBusinessEffects(harness);
});

test('clock failures and unusable downstream factories return fixed failures before effects', async () => {
  for (const now of [
    () => { throw new Error('secret clock state'); },
    () => Number.NaN,
    () => Infinity
  ]) {
    const harness = fakeHarness({ now });
    assertFixedFailure(await harness.guard.execute(command('artifact.read')), 'AUTHORITY_UNAVAILABLE');
    assert.equal(harness.resolverCalls.length, 1);
    assert.equal(harness.factoryCalls, 0);
    assertNoBusinessEffects(harness);
  }

  for (const factoryMode of ['throw', 'missing']) {
    let resolverCalls = 0;
    let factoryCalls = 0;
    const guard = createPrivateCommandGuard({
      policy: POLICY,
      now: () => 1_000,
      resolveCurrentDecision(request) {
        resolverCalls += 1;
        return allowedDecision(request);
      },
      createDownstream() {
        factoryCalls += 1;
        if (factoryMode === 'throw') throw new Error('secret setup failure');
        return undefined;
      }
    });
    assertFixedFailure(await guard.execute(command('artifact.read')), 'DOWNSTREAM_UNAVAILABLE');
    assert.equal(resolverCalls, 1);
    assert.equal(factoryCalls, 1);
  }
});

test('resolver throws and malformed or hostile decisions fail closed without diagnostic leakage', async () => {
  const cases = [
    () => { throw new Error('secret resolver stack and principal'); },
    () => new Proxy({}, { get() { throw new Error('secret getter'); } }),
    () => {
      const result = {};
      Object.defineProperty(result, 'outcome', {
        enumerable: true,
        get() { throw new Error('secret decision getter'); }
      });
      return result;
    },
    request => ({ ...allowedDecision(request), resource: new Proxy({}, { ownKeys() { throw new Error('secret resource'); } }) }),
    request => ({ outcome: 'allow', action: request.action })
  ];
  for (const resolve of cases) {
    const harness = fakeHarness({ resolve });
    const result = await harness.guard.execute(command('artifact.read'));
    assert.ok(result.code === 'AUTHORITY_UNAVAILABLE' || result.code === 'AUTH_DENIED');
    assert.deepEqual(Object.keys(result).sort(), ['code', 'ok']);
    assert.equal(JSON.stringify(result).includes('secret'), false);
    assert.equal(harness.factoryCalls, 0);
    assertNoBusinessEffects(harness);
  }
});

test('a permitted fake forwards only verified minimal scope once after resolution', async () => {
  const sequence = [];
  let effectInput;
  let factoryContext;
  const harness = fakeHarness({
    resolve(request) {
      sequence.push('resolve');
      return allowedDecision(request);
    },
    onFactory(context) {
      sequence.push('factory');
      factoryContext = context;
    },
    callbacks: {
      readArtifact: async input => {
        sequence.push('effect');
        effectInput = input;
      }
    }
  });
  const input = command('artifact.read', {
    body: {
      scope: { tenantId: 'tenant-A', projectId: 'project-A' },
      text: 'safe'
    }
  });
  const result = await harness.guard.execute(input);
  assert.deepEqual(result, { ok: true, code: 'EXECUTED' });
  assert.deepEqual(sequence, ['resolve', 'factory', 'effect']);
  assert.equal(harness.resolverCalls.length, 1);
  assert.deepEqual(Object.keys(harness.resolverCalls[0]).sort(), ['action', 'resource']);
  assert.equal(harness.factoryCalls, 1);
  assert.deepEqual(factoryContext.scope, { tenantId: 'tenant-A', projectId: 'project-A' });
  assert.deepEqual(Object.keys(factoryContext.scope).sort(), ['projectId', 'tenantId']);
  assert.equal(factoryContext.resource.id, 'artifact-A');
  assert.equal(effectInput.resource.id, 'artifact-A');
  assert.deepEqual(effectInput.scope, { tenantId: 'tenant-A', projectId: 'project-A' });
  assert.deepEqual(Object.keys(effectInput.payload).sort(), ['text']);
  assert.equal('assertedScope' in effectInput, false);
});

test('downstream callback failure is fixed and does not echo its message', async () => {
  const harness = fakeHarness({ callbacks: {
    readArtifact: async () => { throw new Error('private artifact body should not escape'); }
  } });
  const result = await harness.guard.execute(command('artifact.read'));
  assertFixedFailure(result, 'DOWNSTREAM_FAILED');
  assert.equal(JSON.stringify(result).includes('private artifact'), false);
});
