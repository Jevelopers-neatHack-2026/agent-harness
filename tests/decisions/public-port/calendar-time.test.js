import assert from 'node:assert/strict';
import test from 'node:test';
import { createJevDecisionPort } from '@jevelopers/agent-harness-runtime/decisions';
import { contracts, eligibility, setup, intercept, API_KEY, NOW } from './helpers.js';

const NORMALIZED_INVALID_TIMES = [
  '2027-02-29T23:59:59Z',
  '2028-02-30T23:59:59.1Z',
  '2027-04-31T23:59:59.12Z',
  '2100-02-29T23:59:59.123Z',
  '2027-01-01T24:00:00Z',
];

test('calendar-invalid eligibility and invalid clock fields fail closed before route selection', () => {
  const invalidTimes = [...NORMALIZED_INVALID_TIMES,
    '2027-00-01T00:00:00Z', '2027-13-01T00:00:00Z',
    '2027-01-00T00:00:00Z', '2027-01-32T00:00:00Z',
    '2027-01-01T25:00:00Z', '2027-01-01T23:60:00Z', '2027-01-01T23:59:60Z',
    '2027-01-01T00:00:00.1234Z', '2027-01-01T00:00:00+00:00'];
  const outcomes = invalidTimes.map(expiresAt => {
    try {
      createJevDecisionPort({ apiKey: API_KEY, environment: 'test',
        eligibility: { ...eligibility(), expiresAt }, contracts, clock: () => NOW });
      return [expiresAt, 'accepted'];
    } catch (error) { return [expiresAt, error.code]; }
  });
  assert.deepEqual(outcomes, invalidTimes.map(value => [value, 'ELIGIBILITY_REQUIRED']));
});

test('calendar-invalid deadlines reject before transport despite an owner-bound canonical target', async t => {
  const network = intercept(t);
  const outcomes = [];
  const ownerCalls = [];
  for (const deadline of NORMALIZED_INVALID_TIMES) {
    const { port, input, events } = setup();
    input.request.deadline_at = input.deadline_at = deadline;
    // The real pinned consumer accepts these strings and hashes their exact bytes.
    // The harness must enforce its own UTC timestamp boundary before dispatch.
    contracts.validateDecisionRequest(input.request, { trustedScope: input.scope });
    input.target_binding_sha256 = contracts.decisionExecutionTargetHash(input.request);
    try { outcomes.push([deadline, (await port.execute(input)).status]); }
    catch (error) { outcomes.push([deadline, error.code]); }
    ownerCalls.push(...events);
  }
  assert.deepEqual(outcomes, NORMALIZED_INVALID_TIMES.map(value => [value, 'INVALID_REQUEST']));
  assert.deepEqual(ownerCalls, []);
  assert.equal(network.dns, 0);
  assert.equal(network.writes, 0);
});

test('valid calendar boundaries and UTC precisions remain eligible until the exact expiry', () => {
  for (const expiresAt of [
    '0000-01-01T00:00:00Z', '2000-02-29T23:59:59Z',
    '2027-01-01T00:00:00.1Z', '2028-02-29T23:59:59.12Z',
    '2400-02-29T23:59:59.123Z', '9999-12-31T23:59:59.999Z',
  ]) {
    const expiry = Date.parse(expiresAt);
    let current = expiry - 1;
    const port = createJevDecisionPort({ apiKey: API_KEY, environment: 'test',
      eligibility: { ...eligibility(), expiresAt }, contracts, clock: () => current });
    const input = { scope: { tenant_id: 'tenant-fixture', project_id: 'project-fixture' },
      actor_id: 'actor-fixture', decision: { execution_mode: 'independent_batch', state: {}, questions: {} } };
    assert.equal(port.resolveRoute(input).provider.model, 'jev-1.13.0', expiresAt);
    current = expiry;
    assert.throws(() => port.resolveRoute(input), error => error.code === 'ELIGIBILITY_EXPIRED', expiresAt);
  }
});

test('the exact eligibility expiry refuses execution before owner callbacks or transport', async t => {
  const network = intercept(t);
  let current = NOW;
  const { port, input, events } = setup({ clock: () => current,
    eligibility: { ...eligibility(), expiresAt: '2026-10-12T15:00:01.000Z' } });
  current = NOW + 1_000;
  await assert.rejects(port.execute(input), error => error.code === 'ELIGIBILITY_EXPIRED');
  assert.deepEqual(events, []);
  assert.equal(network.dns, 0);
  assert.equal(network.writes, 0);
});

test('valid execution deadlines retain zero through three fractional digits', async t => {
  const network = intercept(t);
  for (const fraction of ['', '.1', '.12', '.123']) {
    const { port, input, events } = setup();
    input.request.deadline_at = input.deadline_at = `2026-10-12T15:00:05${fraction}Z`;
    input.target_binding_sha256 = contracts.decisionExecutionTargetHash(input.request);
    const result = await port.execute(input);
    assert.equal(result.status, 'complete', fraction);
    assert.equal(result.usage.completeness, 'complete', fraction);
    assert.deepEqual(events, ['revalidate', 'record', 'assert']);
  }
  assert.equal(network.writes, 4);
});

test('the exact execution deadline remains a timeout with unknown usage and no dispatch', async t => {
  const network = intercept(t);
  const { port, input, events } = setup();
  input.request.deadline_at = input.deadline_at = '2026-10-12T15:00:00.000Z';
  input.target_binding_sha256 = contracts.decisionExecutionTargetHash(input.request);
  const result = await port.execute(input);
  assert.equal(result.status, 'timeout');
  assert.equal(result.usage.completeness, 'unknown');
  assert.deepEqual(events, []);
  assert.equal(network.dns, 0);
  assert.equal(network.writes, 0);
});
