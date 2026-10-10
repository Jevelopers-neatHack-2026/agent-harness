import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  NEATLOGS_SDK_MAPPING_LIMITS,
  mapNeatlogsSourceRecord,
  sourceRecordFromNeatlogsAttributes,
  validateNeatlogsSourceRecords
} from '../../../src/observability/private/neatlogs-sdk-mapper.js';
import { createNeatlogsSdkMask } from '../../../src/observability/private/neatlogs-sdk-mask.js';
import { createNeatlogsSdkCapture } from '../../../src/observability/private/neatlogs-sdk-capture.js';
import { SYNTHETIC_CANARIES } from './helpers.js';

function token(character) {
  return `tok1_${character.repeat(64)}`;
}

function record(sequence = 1, overrides = {}) {
  return Object.assign(Object.create(null), {
    schema: 'agent-harness.telemetry.v1',
    sequence,
    eventId: token('a'),
    workflowId: token('b'),
    revision: 2,
    eventType: 'state_transition',
    fromState: 'running',
    toState: 'succeeded',
    fence: 4,
    occurredAtMs: 1234,
    ...overrides
  });
}

test('the mapper round-trips all allowlisted source fields, including nullable states', () => {
  const input = record(3, { fromState: null, toState: 'running', occurredAtMs: 987654 });
  const attributes = mapNeatlogsSourceRecord(input);
  const reconstructed = sourceRecordFromNeatlogsAttributes(attributes);
  assert.deepEqual({ ...reconstructed }, { ...input });
  assert.equal(attributes['agent_harness.telemetry.from_state_is_null'], true);
  assert.equal(Object.hasOwn(attributes, 'agent_harness.telemetry.from_state'), false);
  assert.equal(attributes['agent_harness.telemetry.source_occurred_at_ms'], 987654);
});

test('the private SDK boundary rejects malformed, oversized, accessor, proxy, cyclic, and out-of-order inputs before SDK import', async () => {
  const capture = createNeatlogsSdkCapture();
  const accessor = record();
  let accessorCalled = false;
  Object.defineProperty(accessor, 'eventId', {
    enumerable: true,
    configurable: true,
    get() {
      accessorCalled = true;
      return token('c');
    }
  });
  const cyclic = record();
  cyclic.fromState = cyclic;
  let proxyTrapCalled = false;
  const proxied = new Proxy(record(), {
    ownKeys() {
      proxyTrapCalled = true;
      return [];
    },
    get() {
      proxyTrapCalled = true;
      throw new Error('must not execute');
    }
  });
  const invalidToken = record(2, { eventId: 'raw-event-id' });
  const oversizeRecord = record(3, { eventType: 'x'.repeat(4096) });
  const outOfOrder = [record(4), record(2)];
  const tooMany = Array.from({ length: 33 }, (_, index) => record(index + 1));
  const atLimit = Array.from({ length: 32 }, (_, index) => record(index + 1, {
    eventId: `tok1_${index.toString(16).padStart(64, '0')}`
  }));
  assert.equal(validateNeatlogsSourceRecords(atLimit).length, 32);
  assert.deepEqual(NEATLOGS_SDK_MAPPING_LIMITS, {
    sourceRecords: 32,
    sourceRecordBytes: 1024,
    sourceBatchBytes: 64 * 1024,
    rootSpans: 32,
    completionMarkerSpans: 32,
    totalSpans: 64
  });
  const invalidCases = [
    [accessor],
    [proxied],
    [cyclic],
    [invalidToken],
    [oversizeRecord],
    outOfOrder,
    tooMany,
    [Object.assign(record(), { extra: { nested: { value: 1 } } })]
  ];
  for (const input of invalidCases) {
    const result = await capture.prepare(input);
    assert.equal(result.status, 'failed');
    assert.equal(typeof result.reasonCode, 'string');
  }
  const proxyArray = new Proxy([record()], {
    ownKeys() {
      proxyTrapCalled = true;
      return [];
    }
  });
  assert.throws(() => validateNeatlogsSourceRecords(proxyArray));
  assert.equal(accessorCalled, false);
  assert.equal(proxyTrapCalled, false);
  assert.equal(capture.diagnostics().sdkImported, false);
  assert.equal(capture.diagnostics().clientCreated, false);
  await capture.close();
});

test('the total mask rebuilds a narrow envelope and strips ambient, input, output, error, resource, and session fields', () => {
  const source = record(5, { fromState: null, toState: 'running' });
  const sourceAttributes = mapNeatlogsSourceRecord(source);
  const unsafe = {
    trace_id: '1'.repeat(32),
    span_id: '2'.repeat(16),
    parent_span_id: null,
    name: 'agent_harness.workflow_event',
    kind: 'workflow',
    start_time: 1_800_000_000_000_000_000,
    end_time: 1_800_000_000_000_000_100,
    duration_ns: 100,
    attributes: {
      ...sourceAttributes,
      'input.value': SYNTHETIC_CANARIES[3],
      'output.value': SYNTHETIC_CANARIES[3],
      'exception.message': SYNTHETIC_CANARIES[4],
      'neatlogs.verification.marker': SYNTHETIC_CANARIES[5],
      'neatlogs.session.id': SYNTHETIC_CANARIES[0],
      'customer.name': SYNTHETIC_CANARIES[3]
    },
    resource: {
      attributes: {
        'service.name': 'agent-harness-neatlogs-local-capture',
        'service.version': '1.1.28',
        'neatlogs.workflow_name': 'agent-harness-neatlogs-local-capture',
        'neatlogs.verification.marker': SYNTHETIC_CANARIES[5],
        'host.name': SYNTHETIC_CANARIES[0]
      }
    },
    status: { code: 'UNSET', description: SYNTHETIC_CANARIES[4] },
    links: [],
    events: []
  };
  const state = createNeatlogsSdkMask();
  const result = state.mask(unsafe);
  assert.ok(result);
  const serialized = JSON.stringify(result);
  for (const canary of SYNTHETIC_CANARIES) assert.equal(serialized.includes(canary), false);
  assert.deepEqual({ ...sourceRecordFromNeatlogsAttributes(result.attributes) }, { ...source });
  assert.deepEqual({ ...result.resource.attributes }, {
    'service.name': 'agent-harness-neatlogs-local-capture',
    'service.version': '1.1.28',
    'neatlogs.workflow_name': 'agent-harness-neatlogs-local-capture'
  });
  assert.deepEqual({ ...result.status }, { code: 'UNSET', description: null });
  assert.equal(result.attributes['neatlogs.verification.marker'], undefined);
  assert.equal(Object.isFrozen(result.events), true);
  assert.equal(Object.isFrozen(result.links), true);
  const frozenMaskOutput = JSON.stringify(result);
  for (const collection of [result.events, result.links]) {
    assert.throws(() => collection.push({ name: 'SYNTHETIC_MASK_MUTATION' }));
    assert.equal(Reflect.set(collection, '0', { name: 'SYNTHETIC_MASK_INDEX' }), false);
    assert.throws(() => collection.splice(0, 0, { name: 'SYNTHETIC_MASK_SPLICE' }));
    assert.equal(JSON.stringify(result), frozenMaskOutput);
  }

  const unknownEnvelope = { ...unsafe, unexpected: 'must-drop' };
  const unsafeResourceValue = {
    ...unsafe,
    resource: {
      attributes: {
        ...unsafe.resource.attributes,
        'service.version': SYNTHETIC_CANARIES[2]
      }
    }
  };
  const oversizedResource = {
    ...unsafe,
    resource: {
      attributes: Object.fromEntries(Array.from({ length: 33 }, (_, index) => [
        `unapproved.${index}`,
        SYNTHETIC_CANARIES[0]
      ]))
    }
  };
  const unsafeKind = { ...unsafe, kind: SYNTHETIC_CANARIES[2] };
  const accessor = {};
  Object.defineProperty(accessor, 'trace_id', { enumerable: true, get() { throw new Error('unsafe'); } });
  assert.equal(state.mask(undefined), null);
  assert.equal(state.mask(unknownEnvelope), null);
  assert.equal(state.mask(unsafeResourceValue), null);
  assert.equal(state.mask(oversizedResource), null);
  assert.equal(state.mask(unsafeKind), null);
  assert.equal(state.mask(accessor), null);
  assert.equal(state.diagnostics().malformedDrops, 6);
});
