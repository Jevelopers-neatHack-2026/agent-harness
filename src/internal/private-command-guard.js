import { types as utilTypes } from 'node:util';

const VERSION = 'agent-harness.private-command.v1';
const INVALID_INPUT = Object.freeze({ ok: false, code: 'INPUT_INVALID' });
const DENIED = Object.freeze({ ok: false, code: 'AUTH_DENIED' });
const AUTHORITY_UNAVAILABLE = Object.freeze({ ok: false, code: 'AUTHORITY_UNAVAILABLE' });
const DOWNSTREAM_UNAVAILABLE = Object.freeze({ ok: false, code: 'DOWNSTREAM_UNAVAILABLE' });
const DOWNSTREAM_FAILED = Object.freeze({ ok: false, code: 'DOWNSTREAM_FAILED' });
const EXECUTED = Object.freeze({ ok: true, code: 'EXECUTED' });
const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]*$/u;
const MEDIA_TYPE = /^[a-z0-9!#$&^_.+-]+\/[a-z0-9!#$&^_.+-]+$/u;
const OPERATION = /^[a-z][a-z0-9._-]{0,31}$/u;
const POLICY_KEYS = Object.freeze([
  'maxBodyBytes',
  'maxTextBytes',
  'maxSourceRefs',
  'maxSourceRefBytes',
  'maxIdentityBytes',
  'minVariantCount',
  'maxVariantCount',
  'allowedMediaTypes',
  'allowedOperations'
]);
const REQUEST_KEYS = Object.freeze(['version', 'action', 'resource', 'mediaType', 'operation', 'body']);
const RESOURCE_KEYS = Object.freeze(['kind', 'id']);
const BODY_KEYS = Object.freeze(['scope', 'text', 'sourceRefs', 'variantCount']);
const SCOPE_KEYS = Object.freeze(['tenantId', 'projectId']);
const ALLOW_DECISION_KEYS = Object.freeze([
  'outcome',
  'action',
  'resource',
  'scope',
  'expiresAtMs',
  'revoked',
  'membershipRevision',
  'currentMembershipRevision',
  'grantRevision',
  'currentGrantRevision'
]);
const REVISION_KEYS = Object.freeze([
  'membershipRevision',
  'currentMembershipRevision',
  'grantRevision',
  'currentGrantRevision'
]);

const COMMANDS = Object.freeze(Object.assign(Object.create(null), {
  'project.read': Object.freeze({ resourceKind: 'project', operation: 'read', effect: 'readProject' }),
  'artifact.lookup': Object.freeze({ resourceKind: 'artifact', operation: 'lookup', effect: 'lookupArtifact' }),
  'artifact.read': Object.freeze({ resourceKind: 'artifact', operation: 'read', effect: 'readArtifact' }),
  'work.submit': Object.freeze({ resourceKind: 'project', operation: 'submit', effect: 'enqueueWork' }),
  'job.status': Object.freeze({ resourceKind: 'job', operation: 'status', effect: 'readProject' }),
  'job.resume': Object.freeze({ resourceKind: 'job', operation: 'resume', effect: 'enqueueWork' }),
  'job.cancel': Object.freeze({ resourceKind: 'job', operation: 'cancel', effect: 'mutateState' }),
  'source.retrieve': Object.freeze({ resourceKind: 'source', operation: 'retrieve', effect: 'retrieveSource' }),
  'billing.reserve': Object.freeze({ resourceKind: 'billing', operation: 'reserve', effect: 'reserveBilling' }),
  'billing.dispatch': Object.freeze({ resourceKind: 'billing_attempt', operation: 'dispatch', effect: 'dispatchBilling' }),
  'provider.dispatch': Object.freeze({ resourceKind: 'work', operation: 'dispatch', effect: 'dispatchProvider' }),
  'state.mutate': Object.freeze({ resourceKind: 'work', operation: 'mutate', effect: 'mutateState' })
}));

function isPlainObject(value) {
  try {
    if (value === null || typeof value !== 'object' || utilTypes.isProxy(value) || Array.isArray(value)) return false;
    const prototype = Object.getPrototypeOf(value);
    return prototype === Object.prototype || prototype === null;
  } catch {
    return false;
  }
}

function copyOwnData(value, allowedKeys, requiredKeys = []) {
  if (!isPlainObject(value)) return null;
  let keys;
  try {
    keys = Reflect.ownKeys(value);
  } catch {
    return null;
  }
  if (keys.length > allowedKeys.length) return null;
  const result = Object.create(null);
  for (const key of keys) {
    if (typeof key !== 'string' || !allowedKeys.includes(key)) return null;
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      return null;
    }
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) return null;
    result[key] = descriptor.value;
  }
  if (requiredKeys.some(key => !Object.hasOwn(result, key))) return null;
  return result;
}

function readOwnData(value, key) {
  if (!isPlainObject(value)) return { ok: false };
  try {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) return { ok: false };
    return { ok: true, value: descriptor.value };
  } catch {
    return { ok: false };
  }
}

function safeArray(value, maximumLength) {
  try {
    if (utilTypes.isProxy(value) || !Array.isArray(value) || Object.getPrototypeOf(value) !== Array.prototype) return null;
    const lengthDescriptor = Object.getOwnPropertyDescriptor(value, 'length');
    const length = lengthDescriptor?.value;
    if (!Number.isSafeInteger(length) || length < 0 || length > maximumLength) return null;
    const keys = Reflect.ownKeys(value);
    if (keys.length !== length + 1 || !keys.includes('length')) return null;
    const result = [];
    for (let index = 0; index < length; index += 1) {
      const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
      if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) return null;
      result.push(descriptor.value);
    }
    return result;
  } catch {
    return null;
  }
}

function isIntegerWithin(value, minimum, maximum) {
  return Number.isSafeInteger(value) && value >= minimum && value <= maximum;
}

function matchesEntire(pattern, value) {
  const match = pattern.exec(value);
  return match !== null && match[0] === value;
}

function validIdentifier(value, maxBytes) {
  return typeof value === 'string'
    && value.length <= maxBytes
    && Buffer.byteLength(value, 'utf8') <= maxBytes
    && matchesEntire(SAFE_ID, value);
}

function normalizePolicy(policy) {
  const copied = copyOwnData(policy, POLICY_KEYS, POLICY_KEYS);
  if (!copied) return null;
  const numericCaps = [
    ['maxBodyBytes', 1, 4096],
    ['maxTextBytes', 0, 1024],
    ['maxSourceRefs', 0, 8],
    ['maxSourceRefBytes', 1, 128],
    ['maxIdentityBytes', 1, 128],
    ['minVariantCount', 1, 10],
    ['maxVariantCount', 1, 10]
  ];
  for (const [key, minimum, maximum] of numericCaps) {
    if (!isIntegerWithin(copied[key], minimum, maximum)) return null;
  }
  if (copied.minVariantCount > copied.maxVariantCount) return null;
  const mediaTypes = safeArray(copied.allowedMediaTypes, 3);
  const operations = safeArray(copied.allowedOperations, 12);
  if (!mediaTypes || mediaTypes.length === 0 || !operations || operations.length === 0) return null;
  if (mediaTypes.some(value => typeof value !== 'string' || value.length > 64 || !matchesEntire(MEDIA_TYPE, value))) return null;
  if (operations.some(value => typeof value !== 'string' || !matchesEntire(OPERATION, value))) return null;
  if (new Set(mediaTypes).size !== mediaTypes.length || new Set(operations).size !== operations.length) return null;
  return Object.freeze({
    ...copied,
    allowedMediaTypes: Object.freeze([...mediaTypes]),
    allowedOperations: Object.freeze([...operations])
  });
}

function normalizeInput(input, policy) {
  const request = copyOwnData(input, REQUEST_KEYS, REQUEST_KEYS);
  if (!request || request.version !== VERSION) return null;
  if (typeof request.action !== 'string' || !Object.hasOwn(COMMANDS, request.action)) return null;
  const command = COMMANDS[request.action];
  if (!policy.allowedOperations.includes(request.operation)
      || request.operation !== command.operation) return null;
  if (!policy.allowedMediaTypes.includes(request.mediaType)) return null;

  const resource = copyOwnData(request.resource, RESOURCE_KEYS, RESOURCE_KEYS);
  if (!resource || resource.kind !== command.resourceKind
      || !validIdentifier(resource.id, policy.maxIdentityBytes)) return null;

  const body = copyOwnData(request.body, BODY_KEYS);
  if (!body) return null;
  let assertedScope = null;
  if (Object.hasOwn(body, 'scope')) {
    const scope = copyOwnData(body.scope, SCOPE_KEYS, SCOPE_KEYS);
    if (!scope
        || !validIdentifier(scope.tenantId, policy.maxIdentityBytes)
        || !validIdentifier(scope.projectId, policy.maxIdentityBytes)) return null;
    assertedScope = Object.freeze({ tenantId: scope.tenantId, projectId: scope.projectId });
  }

  const payload = Object.create(null);
  if (Object.hasOwn(body, 'text')) {
    if (typeof body.text !== 'string'
        || body.text.length > policy.maxTextBytes
        || Buffer.byteLength(body.text, 'utf8') > policy.maxTextBytes) return null;
    payload.text = body.text;
  }
  if (Object.hasOwn(body, 'sourceRefs')) {
    const refs = safeArray(body.sourceRefs, policy.maxSourceRefs);
    if (!refs || refs.some(value => !validIdentifier(value, policy.maxSourceRefBytes))) return null;
    payload.sourceRefs = Object.freeze([...refs]);
  }
  if (Object.hasOwn(body, 'variantCount')) {
    if (!isIntegerWithin(body.variantCount, policy.minVariantCount, policy.maxVariantCount)) return null;
    payload.variantCount = body.variantCount;
  }

  const boundedBody = Object.create(null);
  if (assertedScope !== null) boundedBody.scope = assertedScope;
  if (Object.hasOwn(payload, 'text')) boundedBody.text = payload.text;
  if (Object.hasOwn(payload, 'sourceRefs')) boundedBody.sourceRefs = payload.sourceRefs;
  if (Object.hasOwn(payload, 'variantCount')) boundedBody.variantCount = payload.variantCount;
  if (Buffer.byteLength(JSON.stringify(boundedBody), 'utf8') > policy.maxBodyBytes) return null;

  return Object.freeze({
    action: request.action,
    operation: request.operation,
    effect: command.effect,
    resource: Object.freeze({ kind: resource.kind, id: resource.id }),
    assertedScope,
    payload: Object.freeze(payload)
  });
}

export function normalizePrivateCommandInput(input, policyInput) {
  const policy = normalizePolicy(policyInput);
  if (!policy) return INVALID_INPUT;
  const command = normalizeInput(input, policy);
  return command ? Object.freeze({ ok: true, command }) : INVALID_INPUT;
}

function validDecision(rawDecision, command, nowMs, policy) {
  const copied = copyOwnData(rawDecision, ALLOW_DECISION_KEYS, ALLOW_DECISION_KEYS);
  if (!copied || copied.outcome !== 'allow') return null;
  if (copied.action !== command.action) return null;
  const resource = copyOwnData(copied.resource, RESOURCE_KEYS, RESOURCE_KEYS);
  const scope = copyOwnData(copied.scope, SCOPE_KEYS, SCOPE_KEYS);
  if (!resource || !scope) return null;
  if (resource.kind !== command.resource.kind || resource.id !== command.resource.id) return null;
  if (!validIdentifier(scope.tenantId, policy.maxIdentityBytes)
      || !validIdentifier(scope.projectId, policy.maxIdentityBytes)) return null;
  if (!isIntegerWithin(copied.expiresAtMs, 0, Number.MAX_SAFE_INTEGER)) return null;
  if (typeof copied.revoked !== 'boolean') return null;
  if (REVISION_KEYS.some(key => !validIdentifier(copied[key], policy.maxIdentityBytes))) return null;
  const decision = Object.freeze({
    action: copied.action,
    resource: Object.freeze({ kind: resource.kind, id: resource.id }),
    scope: Object.freeze({ tenantId: scope.tenantId, projectId: scope.projectId }),
    expiresAtMs: copied.expiresAtMs,
    revoked: copied.revoked,
    membershipRevision: copied.membershipRevision,
    currentMembershipRevision: copied.currentMembershipRevision,
    grantRevision: copied.grantRevision,
    currentGrantRevision: copied.currentGrantRevision
  });
  if (decision.revoked
      || decision.expiresAtMs <= nowMs
      || decision.membershipRevision !== decision.currentMembershipRevision
      || decision.grantRevision !== decision.currentGrantRevision) return null;
  if (command.assertedScope !== null
      && (command.assertedScope.tenantId !== decision.scope.tenantId
        || command.assertedScope.projectId !== decision.scope.projectId)) return null;
  return decision;
}

export function createPrivateCommandGuard(options) {
  const setup = copyOwnData(options,
    ['resolveCurrentDecision', 'createDownstream', 'now', 'policy'],
    ['resolveCurrentDecision', 'createDownstream', 'now', 'policy']);
  const policy = setup && normalizePolicy(setup.policy);
  if (!setup || !policy
      || typeof setup.resolveCurrentDecision !== 'function'
      || typeof setup.createDownstream !== 'function'
      || typeof setup.now !== 'function') {
    throw new TypeError('The private command guard setup is invalid.');
  }
  const resolveCurrentDecision = setup.resolveCurrentDecision;
  const createDownstream = setup.createDownstream;
  const now = setup.now;

  return Object.freeze({
    async execute(input) {
      const command = normalizeInput(input, policy);
      if (!command) return INVALID_INPUT;

      let request;
      let rawDecision;
      try {
        request = Object.freeze({
          action: command.action,
          resource: command.resource
        });
        rawDecision = await resolveCurrentDecision(request);
      } catch {
        return AUTHORITY_UNAVAILABLE;
      }

      const outcome = readOwnData(rawDecision, 'outcome');
      if (!outcome.ok) return AUTHORITY_UNAVAILABLE;
      if (outcome.value === 'deny') return DENIED;
      if (outcome.value !== 'allow') return AUTHORITY_UNAVAILABLE;

      let currentTime;
      try {
        currentTime = now();
      } catch {
        return AUTHORITY_UNAVAILABLE;
      }
      if (!isIntegerWithin(currentTime, 0, Number.MAX_SAFE_INTEGER)) return AUTHORITY_UNAVAILABLE;
      const decision = validDecision(rawDecision, command, currentTime, policy);
      if (!decision) return DENIED;

      const factoryContext = Object.freeze({
        action: decision.action,
        operation: command.operation,
        effect: command.effect,
        resource: decision.resource,
        scope: decision.scope
      });
      let effect;
      try {
        effect = createDownstream(factoryContext);
      } catch {
        return DOWNSTREAM_UNAVAILABLE;
      }
      if (typeof effect !== 'function') return DOWNSTREAM_UNAVAILABLE;

      const effectInput = Object.freeze({
        action: decision.action,
        operation: command.operation,
        resource: decision.resource,
        scope: decision.scope,
        payload: command.payload
      });
      try {
        await effect(effectInput);
      } catch {
        return DOWNSTREAM_FAILED;
      }
      return EXECUTED;
    }
  });
}
