import assert from 'node:assert/strict';
import test from 'node:test';
import { createJevDecisionPort } from '@jevelopers/agent-harness-runtime/decisions';
import { contracts, eligibility, setup, intercept, wireResponse, API_KEY, NOW } from './helpers.js';

test('public canonical port traverses actual JEV wire transport and owner validators', async t => {
  const { port, input, events } = setup();
  const network = intercept(t);
  const result = await port.execute(input);
  assert.equal(result.status, 'complete');
  assert.equal(network.writes, 1);
  assert.equal(network.options.headers.authorization, 'Bearer ' + API_KEY);
  assert.equal(network.options.path, '/v1/systemone');
  assert.equal(network.wire.model, 'jev-1.13.0');
  assert.deepEqual(Object.keys(network.wire.questions.layout).sort(), ['criteria', 'instructions', 'type']);
  assert.equal(network.wire.questions.layout.question_schema_version, undefined);
  assert.deepEqual(events, ['revalidate', 'record', 'assert']);
  assert.deepEqual(result.usage, { completeness: 'complete', input_tokens: 10, output_tokens: 4 });
  assert.equal(Object.hasOwn(result.answers.matches.answer, 'confidence'), false);
  assert.match(result.provider_request_ref, /^sha256:[a-f0-9]{64}$/);
  assert.equal(JSON.stringify(result).includes('provider-request-fixture'), false);
  assert.equal(JSON.stringify(result).includes(API_KEY), false);
  assert.equal(Object.isFrozen(result.answers.layout.answer), true);
  contracts.validateDecisionResult(input.request, result, { trustedScope: input.scope });
});

test('actual owner admission helper binds an unreserved request to its later canonical reservation', async t => {
  const { port, input } = setup(); const network = intercept(t);
  const unreserved = structuredClone(input.request);
  delete unreserved.budget_ref.reservation_ref;
  assert.throws(() => contracts.validateDecisionRequest(unreserved, { trustedScope: input.scope }));
  const admitted = contracts.decisionExecutionTargetHash(unreserved, { trustedScope: input.scope });
  assert.equal(network.writes, 0);
  assert.equal(admitted, contracts.decisionExecutionTargetHash(input.request));
  input.target_binding_sha256 = admitted;
  const result = await port.execute(input);
  assert.equal(result.status, 'complete'); assert.equal(network.writes, 1);
});

test('owner target rejects post-admission mutations in every material execution field', async t => {
  const network = intercept(t);
  for (const mutate of [
    input => { input.request.state.text = 'A different synthetic state.'; },
    input => { input.request.questions.layout.instructions = 'A different bounded question.'; },
    input => { input.request.questions.layout.policy.minimum_confidence = 0.8; },
    input => { input.request.source_refs = ['changed-source-fixture']; },
    input => { input.request.event_id = 'changed-event-fixture'; },
    input => { input.request.request_id = 'changed-request-fixture'; },
    input => { input.request.deadline_at = input.deadline_at = '2026-10-12T15:00:04.000Z'; },
    input => { input.request.budget_ref.policy_version = input.attempt.budget.policy_version = 'budget-policy/2'; },
  ]) {
    const { port, input } = setup(); mutate(input);
    await assert.rejects(port.execute(input), error => error.code === 'DISPATCH_BINDING_INVALID');
  }
  assert.equal(network.dns, 0); assert.equal(network.writes, 0);
});

test('reservation exclusion from admission hash does not bypass the final actual reservation binding', async t => {
  const { port, input } = setup(); const network = intercept(t);
  const admitted = input.target_binding_sha256;
  input.request.budget_ref.reservation_ref = 'different-reservation-fixture';
  assert.equal(contracts.decisionExecutionTargetHash(input.request), admitted);
  await assert.rejects(port.execute(input), error => error.code === 'DISPATCH_BINDING_INVALID');
  assert.equal(network.dns, 0); assert.equal(network.writes, 0);
});

test('unsupported chained decisions reject at route selection before owner reservation or transport', async t => {
  const { port, input } = setup(); const network = intercept(t);
  assert.throws(() => port.resolveRoute({ scope: input.scope, actor_id: input.actor_id,
    decision: { execution_mode: 'sequential', state: input.request.state, questions: input.request.questions } }),
  error => error.code === 'INVALID_REQUEST');
  assert.equal(network.dns, 0); assert.equal(network.writes, 0);
});

test('missing, placeholder, subscription-only and stale eligibility configurations reject safely', () => {
  const base = { apiKey: API_KEY, environment: 'test', eligibility: eligibility(), contracts, clock: () => NOW };
  for (const apiKey of [undefined, '', 'changeme', 'your-api-key', '<secret>']) {
    assert.throws(() => createJevDecisionPort({ ...base, apiKey }), error => error.code === 'CONFIG_MISSING');
  }
  for (const record of [undefined, { ...eligibility(), eligible: false }, { ...eligibility(), environment: 'production' },
    { ...eligibility(), model: 'jev-latest' }, { ...eligibility(), accountRef: '' }]) {
    assert.throws(() => createJevDecisionPort({ ...base, eligibility: record }), error => error.code === 'ELIGIBILITY_REQUIRED');
  }
  assert.throws(() => createJevDecisionPort({ ...base, eligibility: { ...eligibility(), expiresAt: '2026-10-11T00:00:00Z' } }),
    error => error.code === 'ELIGIBILITY_EXPIRED');
  assert.throws(() => createJevDecisionPort({ ...base, GPT_IMAGE_ACCESS_MODE: 'codex_subscription' }), error => error.code === 'CONFIG_MISSING');
});

test('canonical errors, foreign identities and multiple/dependent batches never reach DNS', async t => {
  const network = intercept(t);
  const mutations = [
    input => { input.request.schema_version = '2.0'; },
    input => { input.request.model_policy.model_id = 'jev-latest'; },
    input => { input.request.tenant_id = 'foreign-tenant'; },
    input => { input.attempt.project_id = 'foreign-project'; },
    input => { input.reservation_ref = 'wrong-reservation'; },
    input => { input.request.billing_mode = 'simulated'; },
    input => { input.request.environment = 'production'; },
    input => { input.request.execution_mode = 'sequential'; input.request.questions.quality.depends_on = ['layout']; },
    input => { input.request.attempt_limit = 2; },
    input => { input.target_binding_sha256 = 'sha256:' + 'f'.repeat(64); },
  ];
  for (const mutate of mutations) {
    const { port, input } = setup(); mutate(input);
    await assert.rejects(port.execute(input), error => ['INVALID_REQUEST', 'DISPATCH_BINDING_INVALID'].includes(error.code));
  }
  assert.equal(network.dns, 0);
  assert.equal(network.writes, 0);
});

test('authorization is refreshed after TLS and denied or stale permits never write', async t => {
  const { port, input, events } = setup();
  let tlsFinished = false;
  const network = intercept(t, { beforeTls() { tlsFinished = true; } });
  input.revalidateDispatch = async () => { assert.equal(tlsFinished, true); events.push('denied'); return 'wrong-intent'; };
  await assert.rejects(port.execute(input), error => error.code === 'DISPATCH_AUTHORIZATION_REJECTED');
  assert.equal(network.requests, 1);
  assert.equal(network.writes, 0);
  assert.deepEqual(events, ['denied']);
});

test('Finance transport acknowledgment is required before the write', async t => {
  const { port, input } = setup();
  const network = intercept(t);
  input.recordTransportAttempt = async () => { throw new Error('SYNTHETIC_PRIVATE_REJECTED_DATA'); };
  await assert.rejects(port.execute(input), error => error.code === 'DISPATCH_AUTHORIZATION_REJECTED' &&
    !String(error).includes('SYNTHETIC_PRIVATE_REJECTED_DATA'));
  assert.equal(network.writes, 0);
});

test('authority invalidated across an awaited permit cannot cross the final synchronous fence', async t => {
  const { port, input } = setup(); const network = intercept(t);
  let current = true; let finalChecks = 0;
  input.recordTransportAttempt = async () => { queueMicrotask(() => { current = false; }); return 'intent-fixture'; };
  input.assertDispatchCurrent = () => { finalChecks++; assert.equal(current, true); return 'intent-fixture'; };
  await assert.rejects(port.execute(input), error => error.code === 'DISPATCH_AUTHORIZATION_REJECTED');
  assert.equal(finalChecks, 1); assert.equal(network.writes, 0);
});

test('cancellation at the synchronous owner fence prevents an HTTP write', async t => {
  const { port, input } = setup(); const network = intercept(t);
  const controller = new AbortController(); input.signal = controller.signal;
  input.assertDispatchCurrent = () => { controller.abort(); return 'intent-fixture'; };
  const result = await port.execute(input);
  assert.equal(result.status, 'cancelled'); assert.equal(result.usage.completeness, 'unknown');
  assert.equal(network.writes, 0);
});

test('deadline expiry at the synchronous owner fence prevents an HTTP write', async t => {
  let current = NOW; const { port, input } = setup({ clock: () => current }); const network = intercept(t);
  input.assertDispatchCurrent = () => { current = NOW + 5_001; return 'intent-fixture'; };
  const result = await port.execute(input);
  assert.equal(result.status, 'timeout'); assert.equal(result.usage.completeness, 'unknown');
  assert.equal(network.writes, 0);
});

test('strict canonical response rejection retains actual known counters', async t => {
  const { port, input } = setup();
  const body = wireResponse();
  body.answers.layout.probabilities.light = 0.955; // Valid private-wire tolerance, invalid canonical distribution.
  const network = intercept(t, { body });
  const result = await port.execute(input);
  assert.equal(result.status, 'unavailable');
  assert.equal(result.error.code, 'malformed_response');
  assert.equal(result.usage.completeness, 'complete');
  assert.equal(result.usage.input_tokens, 10);
  assert.equal(result.answers.layout.status, 'unavailable');
  assert.equal(network.writes, 1);
});

test('unverified response model retains reported counters as incomplete rather than settling a guessed model', async t => {
  const { port, input } = setup(); const body = wireResponse(); body.model = 'jev-1.99.0';
  const network = intercept(t, { body }); const result = await port.execute(input);
  assert.equal(result.status, 'unavailable'); assert.equal(result.error.code, 'unsupported_model');
  assert.equal(result.usage.completeness, 'incomplete'); assert.equal(result.usage.input_tokens, 10);
  assert.equal(result.usage.reason, 'provider_model_unverified'); assert.equal(network.writes, 1);
});

test('uncertain Noul and low confidence follow canonical review policy', async t => {
  const { port, input } = setup();
  const body = wireResponse(); body.answers.matches.noul = 0.5;
  intercept(t, { body });
  const result = await port.execute(input);
  assert.equal(result.status, 'review');
  assert.equal(result.answers.matches.answer.type, 'noul');
  assert.equal(result.answers.matches.answer.noul, 0.5);
  assert.equal(Object.hasOwn(result.answers.matches.answer, 'confidence'), false);
});

test('reported Choice confidence below the question policy requires review', async t => {
  const { port, input } = setup();
  const body = wireResponse(); body.answers.layout.probabilities = { light: 0.6, dark: 0.4 };
  body.answers.layout.confidence = 0.2;
  intercept(t, { body });
  const result = await port.execute(input);
  assert.equal(result.status, 'review');
  assert.equal(result.answers.layout.answer.confidence, 0.2);
});

test('lost response preserves unknown usage and never retries', async t => {
  const { port, input } = setup();
  const network = intercept(t, { afterWrite(request) { setImmediate(() => request.emit('error', new Error('private-provider-error'))); } });
  const result = await port.execute(input);
  assert.equal(result.status, 'unavailable');
  assert.equal(result.usage.completeness, 'unknown');
  assert.equal(result.error.retryable, false);
  assert.equal(network.writes, 1);
  assert.equal(JSON.stringify(result).includes('private-provider-error'), false);
});

test('cancellation after a write preserves unknown usage', async t => {
  const { port, input } = setup();
  const controller = new AbortController(); input.signal = controller.signal;
  const network = intercept(t, { afterWrite() { controller.abort(); } });
  const result = await port.execute(input);
  assert.equal(result.status, 'cancelled');
  assert.equal(result.usage.completeness, 'unknown');
  assert.equal(network.writes, 1);
});

test('elapsed deadlines return truthful unavailable results without network', async t => {
  const network = intercept(t);
  const { port, input } = setup();
  input.request.deadline_at = input.deadline_at = '2026-10-12T14:59:59.000Z';
  input.target_binding_sha256 = contracts.decisionExecutionTargetHash(input.request);
  const result = await port.execute(input);
  assert.equal(result.status, 'timeout'); assert.equal(result.usage.completeness, 'unknown');
  assert.equal(network.dns, 0);
});

test('already cancelled input never reaches DNS', async t => {
  const network = intercept(t);
  const { port, input } = setup(); const controller = new AbortController(); controller.abort();
  input.signal = controller.signal;
  const result = await port.execute(input);
  assert.equal(result.status, 'cancelled'); assert.equal(result.usage.completeness, 'unknown');
  assert.equal(network.dns, 0);
});

test('a cancelled final owner hook cannot send when its late result arrives', async t => {
  const network = intercept(t);
  const { port, input } = setup();
  const controller = new AbortController(); input.signal = controller.signal;
  let latePermit;
  input.revalidateDispatch = () => new Promise(resolve => { latePermit = resolve; controller.abort(); });
  const result = await port.execute(input);
  assert.equal(result.status, 'cancelled'); assert.equal(network.writes, 0);
  latePermit('intent-fixture');
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(network.writes, 0);
});

test('an elapsed owner callback is bounded by the actual deadline and never sends', async t => {
  const network = intercept(t);
  const { port, input } = setup();
  input.request.deadline_at = input.deadline_at = '2026-10-12T15:00:00.050Z';
  input.target_binding_sha256 = contracts.decisionExecutionTargetHash(input.request);
  input.revalidateDispatch = () => new Promise(() => {});
  const result = await port.execute(input);
  assert.equal(result.status, 'timeout'); assert.equal(network.writes, 0);
});

test('HTTP authorization failure and missing usage cannot masquerade as a successful decision', async t => {
  const network = intercept(t, { status: 401, body: 'private-provider-content' });
  const { port, input } = setup();
  const result = await port.execute(input);
  assert.equal(result.status, 'unavailable'); assert.equal(result.usage.completeness, 'unknown');
  assert.equal(result.error.retryable, false); assert.equal(network.writes, 1);
  assert.equal(JSON.stringify(result).includes('private-provider-content'), false);
});

test('missing provider usage produces canonical unavailable status with an explicit unknown record', async t => {
  const body = wireResponse(); delete body.usage;
  const network = intercept(t, { body }); const { port, input } = setup();
  const result = await port.execute(input);
  assert.equal(result.status, 'unavailable'); assert.equal(result.usage.completeness, 'unknown');
  assert.equal(result.answers.layout.status, 'unavailable'); assert.equal(network.writes, 1);
});

test('hostile accessor requests fail without running user code or logging private values', async t => {
  const { port, input } = setup();
  const network = intercept(t); let getters = 0;
  Object.defineProperty(input.request.state, 'text', { enumerable: true, get() { getters++; throw new Error(API_KEY); } });
  await assert.rejects(port.execute(input), error => error.code === 'INVALID_REQUEST' && !String(error).includes(API_KEY));
  assert.equal(getters, 0); assert.equal(network.dns, 0);
});
