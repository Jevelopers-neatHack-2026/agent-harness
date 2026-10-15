import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import https from 'node:https';
import { Resolver } from 'node:dns/promises';
import { validateDecisionRequest, validateDecisionResult, decisionExecutionTargetHash } from 'creative-engine/contracts/decision-port/v1';
import { validateMeteredAttempt } from 'creative-engine/contracts/metering';
import { createJevDecisionPort } from '@jevelopers/agent-harness-runtime/decisions';
import '../jev-adapter.contract/network-guard.js';

export const NOW = Date.parse('2026-10-12T15:00:00.000Z');
export const API_KEY = 'synthetic-intercepted-key';
export const contracts = { validateDecisionRequest, validateDecisionResult, validateMeteredAttempt,
  decisionExecutionTargetHash };

export function eligibility() {
  return { provider: 'typesafe', model: 'jev-1.13.0', environment: 'test', accountRef: 'account-fixture',
    projectRef: 'provider-project-fixture', evidenceRef: 'admin-evidence-fixture',
    expiresAt: '2026-10-13T15:00:00.000Z', eligible: true };
}

export function setup(overrides = {}) {
  const port = createJevDecisionPort({ apiKey: API_KEY, environment: 'test', eligibility: eligibility(),
    contracts, clock: () => NOW, ...overrides });
  const scope = { tenant_id: 'tenant-fixture', project_id: 'project-fixture', job_id: 'job-fixture' };
  const policy = { minimum_confidence: 0.7, low_confidence_action: 'review' };
  const base = { question_schema_version: '1.0', policy_version: 'question-policy/1', instructions: 'Use synthetic bounded state.' };
  const request = { contract: 'DecisionPortRequest', schema_version: '1.0', event_id: 'event-fixture',
    tenant_id: scope.tenant_id, project_id: scope.project_id, job_id: scope.job_id,
    correlation_id: 'correlation-fixture', producer: 'intercepted-test', occurred_at: '2026-10-12T15:00:00.000Z',
    ingested_at: '2026-10-12T15:00:00.000Z', environment: 'test', billing_mode: 'live', source_refs: ['source-fixture'],
    request_id: 'request-fixture', logical_operation_id: 'operation-fixture', billable_attempt_id: 'attempt-fixture',
    attempt_number: 1, attempt_limit: 1, budget_ref: { budget_scope_ref: 'budget-fixture',
      reservation_ref: 'reservation-fixture', policy_version: 'budget-policy/1' },
    deadline_at: '2026-10-12T15:00:05.000Z', cancellation_ref: 'job-fixture',
    model_policy: { model_id: 'jev-1.13.0', policy_version: 'decision-policy/1' }, execution_mode: 'independent_batch',
    state: { text: 'Synthetic evidence only.' }, questions: {
      layout: { ...base, type: 'choice', criteria: { light: 'Light', dark: 'Dark' }, eligible_candidate_ids: ['light', 'dark'], policy },
      quality: { ...base, type: 'score', criteria: ['Low', 'High'], policy },
      matches: { ...base, type: 'noul', criteria: { true: 'Matches', false: 'Does not match' },
        policy: { uncertain_probability_band: { lower: 0.4, upper: 0.6 } } },
    } };
  const route = port.resolveRoute({ scope, actor_id: 'actor-fixture', decision: {
    execution_mode: request.execution_mode, state: request.state, questions: request.questions } });
  const attempt = { schema_version: 'creative.metering.proposal/v1', tenant_id: scope.tenant_id,
    project_id: scope.project_id, budget_scope_id: 'budget-fixture', logical_operation_id: request.logical_operation_id,
    billable_attempt_id: request.billable_attempt_id, retry_mapping_ref: route.retry_mapping_ref,
    environment: 'test', billing_mode: 'live', operation_kind: 'jev_decision',
    budget: { scope_id: 'budget-fixture', policy_version: 'budget-policy/1' }, provider: route.provider,
    correlation_id: request.correlation_id };
  const events = [];
  const input = { scope, actor_id: 'actor-fixture', request, attempt, reservation_ref: 'reservation-fixture',
    transport_attempt_id: 'transport-fixture', dispatch_intent_ref: 'intent-fixture',
    target_binding_sha256: contracts.decisionExecutionTargetHash(request),
    deadline_at: request.deadline_at, signal: new AbortController().signal,
    async recordTransportAttempt(id) { events.push('record'); assert.equal(id, 'transport-fixture'); return 'intent-fixture'; },
    assertDispatchCurrent() { events.push('assert'); return 'intent-fixture'; },
    async revalidateDispatch(binding) { events.push('revalidate');
      assert.equal(binding.target_binding_sha256, input.target_binding_sha256);
      assert.equal(binding.signal, input.signal); return 'intent-fixture'; },
  };
  return { port, scope, request, attempt, input, events };
}

export function wireResponse() {
  return { model: 'jev-1.13.0', answers: {
    layout: { type: 'choice', choice: 'light', probabilities: { light: 0.95, dark: 0.05 }, confidence: 0.9 },
    quality: { type: 'score', score: 0.75, legend: { '0': 'Low', '1': 'High' }, probabilities: { '0': 0.25, '1': 0.75 }, confidence: 0.9 },
    matches: { type: 'noul', noul: 0.9 },
  }, usage: { input_tokens: 10, output_tokens: 4 } };
}

export function intercept(t, { body = wireResponse(), status = 200, afterWrite, beforeTls } = {}) {
  const observed = { dns: 0, requests: 0, writes: 0, wire: null, options: null };
  t.mock.method(Resolver.prototype, 'resolve4', async host => { assert.equal(host, 'api.typesafe.ai'); observed.dns++; return ['93.184.216.34']; });
  t.mock.method(Resolver.prototype, 'resolve6', async host => { assert.equal(host, 'api.typesafe.ai'); return []; });
  t.mock.method(https, 'request', (options, callback) => {
    observed.requests++; observed.options = options;
    const request = new EventEmitter();
    request.destroy = () => {};
    request.end = bytes => {
      observed.writes++; observed.wire = JSON.parse(bytes.toString());
      if (afterWrite) { afterWrite(request); return; }
      const response = new EventEmitter();
      const bytesOut = Buffer.from(typeof body === 'string' ? body : JSON.stringify(body));
      response.statusCode = status;
      response.headers = { 'content-length': String(bytesOut.length), 'x-typesafe-request-id': 'provider-request-fixture' };
      response.rawHeaders = ['content-length', String(bytesOut.length), 'x-typesafe-request-id', 'provider-request-fixture'];
      setImmediate(() => { callback(response); response.emit('data', bytesOut); response.emit('end'); });
    };
    const socket = new EventEmitter();
    socket.encrypted = true; socket.authorized = true; socket.secureConnecting = true;
    socket.destroy = () => {};
    setImmediate(() => { request.emit('socket', socket); beforeTls?.(); socket.secureConnecting = false; socket.emit('secureConnect'); });
    return request;
  });
  return observed;
}
