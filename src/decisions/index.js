import { createHash, randomUUID } from 'node:crypto';
import { sendJevOnce, JevTransportError } from '../adapters/jev/http-transport.js';
import { JEV_MODEL_ID } from '../adapters/jev/request-validation.js';
import { isPlainDataRecord, isProxy, snapshotJsonValue } from '../adapters/jev/safe-json.js';

const ENDPOINT = 'https://api.typesafe.ai/v1/systemone';
const ENVIRONMENTS = ['development', 'test', 'production'];
const MAX_DEADLINE_MS = 8_000;
const MESSAGES = Object.freeze({
  CONFIG_MISSING: 'The TypeSafe server credential and explicit host configuration are required.',
  CONFIG_INVALID: 'The TypeSafe host configuration is invalid.',
  ELIGIBILITY_REQUIRED: 'Current administrator evidence of TypeSafe account and model eligibility is required.',
  ELIGIBILITY_EXPIRED: 'The TypeSafe account and model eligibility evidence has expired.',
  INVALID_REQUEST: 'The canonical decision request is invalid or unsupported by the single-batch executor.',
  DISPATCH_BINDING_INVALID: 'The decision does not match the authorized scope, attempt, reservation, or dispatch intent.',
  DISPATCH_AUTHORIZATION_REJECTED: 'The existing execution owner did not authorize this decision dispatch.',
});

export class JevDecisionError extends Error {
  constructor(code) {
    super(MESSAGES[code] ?? MESSAGES.INVALID_REQUEST);
    this.name = 'JevDecisionError';
    this.code = code;
  }
}

function fail(code) { throw new JevDecisionError(code); }

function fields(value, required, optional = [], code = 'CONFIG_INVALID') {
  if (!isPlainDataRecord(value)) fail(code);
  const allowed = new Set([...required, ...optional]);
  const keys = Reflect.ownKeys(value);
  if (keys.length > allowed.size || keys.some(key => typeof key !== 'string' || !allowed.has(key))) fail(code);
  const result = Object.create(null);
  for (const key of keys) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) fail(code);
    result[key] = descriptor.value;
  }
  if (required.some(key => !Object.hasOwn(result, key))) fail(code);
  return result;
}

function freeze(value) {
  if (value && typeof value === 'object') {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
}

function data(value, code = 'INVALID_REQUEST') {
  try {
    // The wire snapshot rejects proxies, accessors, cycles and excessive data.
    // Canonical validators use normal Array methods, so restore array prototypes
    // in a private copy rather than handing them the wire snapshot's null arrays.
    return freeze(JSON.parse(JSON.stringify(snapshotJsonValue(value, { allowPrimitive: false }))));
  } catch { fail(code); }
}

function ref(value) {
  return typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$/u.test(value);
}

function time(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/u.test(value)) return NaN;
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) return NaN;
  // Date.parse can roll impossible dates and 24:00 into the next calendar day.
  // Compare exact UTC fields while preserving the supported fractional precision.
  const normalized = value.replace(/(?:\.(\d{1,3}))?Z$/u,
    (_suffix, fraction = '') => `.${fraction.padEnd(3, '0')}Z`);
  return new Date(parsed).toISOString() === normalized ? parsed : NaN;
}

function signal(value) {
  if (value === undefined) return true;
  if (value === null || typeof value !== 'object' || isProxy(value)) return false;
  try {
    if (Object.getPrototypeOf(value) !== AbortSignal.prototype) return false;
    Object.getOwnPropertyDescriptor(AbortSignal.prototype, 'aborted').get.call(value);
    return true;
  } catch { return false; }
}

function eligibilityRecord(value, environment) {
  const record = fields(value, ['provider', 'model', 'environment', 'accountRef', 'projectRef', 'evidenceRef', 'expiresAt', 'eligible'], [], 'ELIGIBILITY_REQUIRED');
  if (record.provider !== 'typesafe' || record.model !== JEV_MODEL_ID || record.environment !== environment ||
      record.eligible !== true || !ref(record.accountRef) || !ref(record.projectRef) || !ref(record.evidenceRef) ||
      !Number.isFinite(time(record.expiresAt))) fail('ELIGIBILITY_REQUIRED');
  return Object.freeze(record);
}

function resultEnvelope(request, clock) {
  const result = Object.create(null);
  for (const key of ['tenant_id', 'project_id', 'correlation_id', 'environment', 'billing_mode', 'source_refs',
    'request_id', 'logical_operation_id', 'billable_attempt_id', 'workflow_id', 'run_id', 'order_id', 'job_id',
    'design_version_id', 'artifact_id', 'transport_attempt_id']) {
    if (Object.hasOwn(request, key)) result[key] = request[key];
  }
  const timestamp = new Date(clock()).toISOString();
  return Object.assign(result, { contract: 'DecisionPortResult', schema_version: '1.0', event_id: randomUUID(),
    producer: 'agent-harness.jev', occurred_at: timestamp, ingested_at: timestamp,
    model_id: JEV_MODEL_ID, model_policy_version: request.model_policy.policy_version });
}

function usage(validation) {
  if (validation?.usage?.kind !== 'known') return { completeness: 'unknown', reason: 'provider_usage_unavailable' };
  const counts = { input_tokens: validation.usage.inputTokens, output_tokens: validation.usage.outputTokens };
  return validation.resolvedModel === JEV_MODEL_ID
    ? { completeness: 'complete', ...counts }
    : { completeness: 'incomplete', ...counts, reason: 'provider_model_unverified' };
}

function unavailable(request, clock, status, code, recordedUsage) {
  const answers = Object.fromEntries(Object.entries(request.questions).map(([id, question]) => [id, {
    status: 'unavailable', question_schema_version: question.question_schema_version,
    policy_version: question.policy_version, reason: 'decision_response_unavailable',
  }]));
  return { ...resultEnvelope(request, clock), status, answers, usage: recordedUsage,
    error: { code, retryable: false, fallback: 'review', user_message: 'The decision requires review or reconciliation.' } };
}

function decisionResult(request, response, clock) {
  const recordedUsage = usage(response?.validation);
  if (response.kind !== 'response' || response.validation.kind !== 'valid') {
    const errorCode = response.kind === 'response'
      ? response.validation.resolvedModel && response.validation.resolvedModel !== JEV_MODEL_ID ? 'unsupported_model' : 'malformed_response'
      : 'provider_unavailable';
    return unavailable(request, clock, 'unavailable', errorCode, recordedUsage);
  }
  let review = false;
  const answers = Object.fromEntries(Object.entries(request.questions).map(([id, question]) => {
    const answer = response.validation.answers[id];
    if (question.type === 'noul') {
      const band = question.policy.uncertain_probability_band;
      if (answer.noul >= band.lower && answer.noul <= band.upper) review = true;
    } else if (answer.confidence < question.policy.minimum_confidence) review = true;
    return [id, { status: 'answered', question_schema_version: question.question_schema_version,
      policy_version: question.policy_version, answer }];
  }));
  return { ...resultEnvelope(request, clock), status: review ? 'review' : 'complete', answers, usage: recordedUsage };
}

/** One canonical batch under the consumer's existing C03 execution authority. */
export function createJevDecisionPort(options) {
  const config = fields(options, ['apiKey', 'environment', 'eligibility', 'contracts'], ['clock'], 'CONFIG_MISSING');
  if (typeof config.apiKey !== 'string' || config.apiKey.length < 8 || config.apiKey.length > 512 ||
      /[\u0000-\u0020\u007f]/u.test(config.apiKey) || /^(?:your[_-]|placeholder|change.?me|replace.?me|sentinel|undefined|null|<)/iu.test(config.apiKey)) fail('CONFIG_MISSING');
  if (!ENVIRONMENTS.includes(config.environment)) fail('CONFIG_INVALID');
  const contracts = fields(config.contracts, ['validateDecisionRequest', 'validateDecisionResult', 'validateMeteredAttempt', 'decisionExecutionTargetHash']);
  if (Object.values(contracts).some(callback => typeof callback !== 'function')) fail('CONFIG_INVALID');
  const clock = config.clock ?? Date.now;
  if (typeof clock !== 'function') fail('CONFIG_INVALID');
  const eligibility = eligibilityRecord(config.eligibility, config.environment);
  const checkEligibility = () => {
    const current = clock();
    if (!Number.isFinite(current)) fail('CONFIG_INVALID');
    if (time(eligibility.expiresAt) <= current) fail('ELIGIBILITY_EXPIRED');
    return current;
  };
  checkEligibility();
  const route = freeze({ retry_mapping_ref: 'agent-harness.jev.single-send/v1', provider: {
    name: 'typesafe', model: JEV_MODEL_ID, endpoint: ENDPOINT,
    sdk: { name: 'agent-harness-http', version: '0.1.0' },
  } });

  return Object.freeze({
    resolveRoute(input) {
      const binding = fields(input, ['scope', 'actor_id', 'decision'], [], 'DISPATCH_BINDING_INVALID');
      const scope = data(binding.scope, 'DISPATCH_BINDING_INVALID');
      if (!ref(scope.tenant_id) || !ref(scope.project_id) || !ref(binding.actor_id)) fail('DISPATCH_BINDING_INVALID');
      const decision = fields(data(binding.decision), ['execution_mode', 'state', 'questions'], [], 'INVALID_REQUEST');
      if (decision.execution_mode !== 'independent_batch') fail('INVALID_REQUEST');
      checkEligibility();
      return route;
    },
    async execute(input) {
      const binding = fields(input, ['scope', 'actor_id', 'request', 'attempt', 'reservation_ref',
        'transport_attempt_id', 'dispatch_intent_ref', 'target_binding_sha256', 'recordTransportAttempt',
        'revalidateDispatch', 'assertDispatchCurrent', 'deadline_at'], ['signal'], 'DISPATCH_BINDING_INVALID');
      if (!ref(binding.actor_id) || !ref(binding.reservation_ref) || !ref(binding.transport_attempt_id) ||
          !ref(binding.dispatch_intent_ref) || typeof binding.recordTransportAttempt !== 'function' ||
          typeof binding.revalidateDispatch !== 'function' || typeof binding.assertDispatchCurrent !== 'function' ||
          !signal(binding.signal)) fail('DISPATCH_BINDING_INVALID');
      const scope = data(binding.scope, 'DISPATCH_BINDING_INVALID');
      const request = data(binding.request);
      // The consumer allocates optional object identities. The uncached owner
      // callbacks below establish their authorization; this scope only validates
      // the consumer's bound canonical envelope, and never grants a privilege.
      const trustedScope = { ...scope, ...(request.job_id ? { job_id: request.job_id } : {}) };
      let attempt;
      try {
        contracts.validateDecisionRequest(request, { trustedScope });
        attempt = data(contracts.validateMeteredAttempt(data(binding.attempt, 'DISPATCH_BINDING_INVALID'), {
          tenant_id: scope.tenant_id, project_id: scope.project_id, budget_scope_id: request.budget_ref.budget_scope_ref,
        }), 'DISPATCH_BINDING_INVALID');
      } catch { fail('INVALID_REQUEST'); }
      if (request.execution_mode !== 'independent_batch' || request.attempt_number !== 1 || request.attempt_limit !== 1) fail('INVALID_REQUEST');
      if (request.environment !== config.environment || request.billing_mode !== 'live' ||
          request.deadline_at !== binding.deadline_at || request.budget_ref.reservation_ref !== binding.reservation_ref ||
          request.logical_operation_id !== attempt.logical_operation_id || request.billable_attempt_id !== attempt.billable_attempt_id ||
          request.environment !== attempt.environment || request.billing_mode !== attempt.billing_mode ||
          request.budget_ref.policy_version !== attempt.budget.policy_version || attempt.operation_kind !== 'jev_decision' ||
          attempt.retry_mapping_ref !== route.retry_mapping_ref ||
          (attempt.correlation_id !== undefined && attempt.correlation_id !== request.correlation_id) ||
          attempt.provider.name !== route.provider.name || attempt.provider.model !== route.provider.model ||
          attempt.provider.endpoint !== route.provider.endpoint || attempt.provider.sdk.name !== route.provider.sdk.name ||
          attempt.provider.sdk.version !== route.provider.sdk.version ||
          (request.transport_attempt_id !== undefined && request.transport_attempt_id !== binding.transport_attempt_id)) fail('DISPATCH_BINDING_INVALID');
      let targetBinding;
      try { targetBinding = contracts.decisionExecutionTargetHash(request); } catch { fail('INVALID_REQUEST'); }
      if (typeof targetBinding !== 'string' || !/^sha256:[a-f0-9]{64}$/u.test(targetBinding)) fail('INVALID_REQUEST');
      if (targetBinding !== binding.target_binding_sha256) fail('DISPATCH_BINDING_INVALID');
      const deadline = time(binding.deadline_at);
      const remaining = deadline - checkEligibility();
      const ownerOptions = { trustedScope };
      if (!Number.isFinite(remaining)) fail('INVALID_REQUEST');
      if (remaining <= 0 || binding.signal?.aborted) {
        return freeze(contracts.validateDecisionResult(request, unavailable(request, clock,
          binding.signal?.aborted ? 'cancelled' : 'timeout', binding.signal?.aborted ? 'cancelled' : 'deadline_exceeded', usage()), ownerOptions));
      }
      const controller = new AbortController();
      const onAbort = () => controller.abort();
      binding.signal?.addEventListener('abort', onAbort, { once: true });
      const timer = setTimeout(onAbort, Math.min(remaining, MAX_DEADLINE_MS));
      let response;
      let checked;
      try {
        response = await sendJevOnce({ request: { model: JEV_MODEL_ID, state: request.state,
          questions: Object.fromEntries(Object.entries(request.questions).map(([id, question]) => [id, {
            type: question.type, instructions: question.instructions, criteria: question.criteria,
          }])) }, apiKey: config.apiKey, signal: controller.signal,
          async beforeWrite() {
            if (controller.signal.aborted || checkEligibility() >= deadline) fail('DISPATCH_AUTHORIZATION_REJECTED');
            const permit = await binding.revalidateDispatch(Object.freeze({ reservation_ref: binding.reservation_ref,
              billable_attempt_id: request.billable_attempt_id, transport_attempt_id: binding.transport_attempt_id,
              dispatch_intent_ref: binding.dispatch_intent_ref, target_binding_sha256: targetBinding, signal: binding.signal }));
            if (permit !== binding.dispatch_intent_ref || controller.signal.aborted || checkEligibility() >= deadline) fail('DISPATCH_AUTHORIZATION_REJECTED');
            const recorded = await binding.recordTransportAttempt(binding.transport_attempt_id);
            if (recorded !== binding.dispatch_intent_ref || controller.signal.aborted || checkEligibility() >= deadline) fail('DISPATCH_AUTHORIZATION_REJECTED');
            return () => {
              if (controller.signal.aborted || checkEligibility() >= deadline ||
                  binding.assertDispatchCurrent() !== binding.dispatch_intent_ref) fail('DISPATCH_AUTHORIZATION_REJECTED');
              if (controller.signal.aborted || checkEligibility() >= deadline) fail('DISPATCH_AUTHORIZATION_REJECTED');
            };
          } });
        checked = decisionResult(request, response, clock);
        if (response.providerRequestId) checked.provider_request_ref = 'sha256:' +
          createHash('sha256').update(response.providerRequestId).digest('hex');
      } catch (error) {
        if (!(error instanceof JevTransportError)) throw new JevDecisionError('DISPATCH_AUTHORIZATION_REJECTED');
        const cancelled = binding.signal?.aborted === true;
        const timeout = !cancelled && (controller.signal.aborted || clock() >= deadline || error.code.startsWith('DEADLINE'));
        if (error.code === 'DISPATCH_AUTHORIZATION_REJECTED' && !cancelled && !timeout) throw new JevDecisionError('DISPATCH_AUTHORIZATION_REJECTED');
        checked = unavailable(request, clock, cancelled ? 'cancelled' : timeout ? 'timeout' : 'unavailable',
          cancelled ? 'cancelled' : timeout ? 'timeout' : 'provider_unavailable', usage());
      } finally {
        clearTimeout(timer);
        binding.signal?.removeEventListener('abort', onAbort);
      }
      try { return freeze(contracts.validateDecisionResult(request, checked, ownerOptions)); }
      catch {
        // The canonical contract is stricter than the private wire decoder.
        // Preserve counters even when its distribution or shape is rejected.
        return freeze(contracts.validateDecisionResult(request,
          { ...unavailable(request, clock, 'unavailable', 'malformed_response', usage(response?.validation)),
            ...(checked.provider_request_ref ? { provider_request_ref: checked.provider_request_ref } : {}) }, ownerOptions));
      }
    },
  });
}
