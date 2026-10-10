import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import * as runtime from '@jevelopers/agent-harness-runtime';
import { createLocalAttemptObservation } from '../../../src/adapters/jev/attempt-recording.js';
import { validateJevResponseBytes } from '../../../src/adapters/jev/response-validation.js';

function fixture(name) {
  return JSON.parse(readFileSync(new URL(`../../fixtures/jev/${name}`, import.meta.url), 'utf8'));
}

function bytes(value) {
  return Buffer.from(JSON.stringify(value), 'utf8');
}

function identities(attempt) {
  return {
    logicalOperationRef: 'logical-fixture-01',
    billableAttemptRef: 'opaque-billable-ref-' + attempt,
    transportAttemptRef: 'transport-fixture-' + attempt,
    requestedModel: 'jev-1.13.0',
    dispatchCertainty: 'response_received'
  };
}

test('privacy_identity: pure observation helper keeps usage immutable and identities separate', () => {
  const request = fixture('positive-request.json');
  const good = fixture('positive-response.json');
  const valid = validateJevResponseBytes(bytes(good), request);
  const bad = structuredClone(good);
  bad.answers.routing.choice = 'invalid-option';
  const invalid = validateJevResponseBytes(bytes(bad), request);

  const first = createLocalAttemptObservation({
    ...identities('one'), responseValidation: valid, providerRequestId: 'req-synthetic-one'
  });
  const second = createLocalAttemptObservation({
    ...identities('two'), responseValidation: invalid, providerRequestId: 'req-synthetic-two'
  });
  const unresolved = createLocalAttemptObservation({
    logicalOperationRef: 'logical-timeout-fixture',
    billableAttemptRef: 'opaque-unknown-ref',
    transportAttemptRef: 'transport-timeout-fixture',
    requestedModel: 'jev-1.13.0',
    dispatchCertainty: 'possible'
  });

  assert.notEqual(first.observationId, second.observationId);
  assert.equal(first.logicalOperationRef, second.logicalOperationRef);
  assert.notEqual(first.billableAttemptRef, second.billableAttemptRef);
  assert.notEqual(first.transportAttemptRef, second.transportAttemptRef);
  assert.equal(first.usage.kind, 'known');
  assert.equal(second.decisionConformance, 'invalid');
  assert.deepEqual(second.usage, { kind: 'known', inputTokens: 83, outputTokens: 19 });
  assert.equal(unresolved.dispatchCertainty, 'possible');
  assert.deepEqual(unresolved.usage, { kind: 'unknown', reason: 'missing_or_invalid' });
  assert.equal(unresolved.persistence, 'memory_only');
  assert.equal(Object.hasOwn(first, 'authorized'), false);
  assert.equal(Object.hasOwn(first, 'settled'), false);
  assert.ok(Object.isFrozen(first));
  assert.ok(Object.isFrozen(first.usage));
  assert.throws(() => { first.usage.inputTokens = 0; }, TypeError);

  const unsafeId = createLocalAttemptObservation({
    ...identities('three'), responseValidation: valid, providerRequestId: 'req\nsecret'
  });
  assert.equal(unsafeId.providerRequestId, null);
});

test('durable_evidence: in-memory helper remains separate from durable storage and finance authority', () => {
  const manifest = JSON.parse(readFileSync(new URL('../../fixtures/jev/fixture-manifest.json', import.meta.url), 'utf8'));
  const observation = createLocalAttemptObservation({
    logicalOperationRef: 'logical-crash-fixture',
    billableAttemptRef: 'opaque-crash-fixture',
    transportAttemptRef: 'transport-crash-fixture',
    requestedModel: 'jev-1.13.0',
    dispatchCertainty: 'possible'
  });

  assert.equal(manifest.dependencyMode, 'synthetic_offline');
  assert.equal(manifest.externalProviderCalls, 0);
  assert.equal(observation.persistence, 'memory_only');
  assert.equal(Object.hasOwn(observation, 'outboxEventId'), false);
  assert.equal(Object.hasOwn(observation, 'financeStatus'), false);
  assert.equal(Object.hasOwn(observation, 'authorityGrant'), false);
});

test('privacy_identity: identical synthetic payloads with distinct workflow attempts do not deduplicate', () => {
  const request = fixture('positive-request.json');
  const response = fixture('positive-response.json');
  const validation = validateJevResponseBytes(bytes(response), request);
  const left = createLocalAttemptObservation({
    logicalOperationRef: 'workflow-left-logical',
    billableAttemptRef: 'workflow-left-billable',
    transportAttemptRef: 'workflow-left-transport',
    requestedModel: request.model,
    dispatchCertainty: 'response_received',
    responseValidation: validation
  });
  const right = createLocalAttemptObservation({
    logicalOperationRef: 'workflow-right-logical',
    billableAttemptRef: 'workflow-right-billable',
    transportAttemptRef: 'workflow-right-transport',
    requestedModel: request.model,
    dispatchCertainty: 'response_received',
    responseValidation: validation
  });

  assert.notEqual(left.observationId, right.observationId);
  assert.notEqual(left.logicalOperationRef, right.logicalOperationRef);
  assert.notEqual(left.billableAttemptRef, right.billableAttemptRef);
  assert.notEqual(left.transportAttemptRef, right.transportAttemptRef);
});

test('gates: package exports contain no provider executor and live mode remains refused', () => {
  assert.equal(Object.hasOwn(runtime, 'sendJevOnce'), false);
  assert.equal(Object.hasOwn(runtime, 'createJevDecisionPort'), false);
  assert.equal(Object.hasOwn(runtime, 'DecisionPort'), false);
  assert.equal(runtime.INTERNAL_SCHEMA_VERSION, 5);
  assert.throws(() => runtime.assertExecutionModeAvailable({ mode: 'live' }), error => {
    assert.equal(error.code, 'LIVE_EXECUTION_UNAVAILABLE');
    assert.match(error.message, /canonical ports and external authorities/u);
    return true;
  });
});
