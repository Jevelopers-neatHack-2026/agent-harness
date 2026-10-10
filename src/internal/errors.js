import { randomUUID } from 'node:crypto';

const SAFE_CODES = new Set([
  'CONFIG_MISSING',
  'CONFIG_INVALID',
  'UNSUPPORTED_RUNTIME',
  'SQLITE_UNSUPPORTED',
  'LIVE_EXECUTION_UNAVAILABLE',
  'PORT_UNAVAILABLE',
  'PORT_INVALID_RESULT',
  'STORE_OPEN_FAILED',
  'STORE_WRITE_FAILED',
  'STORE_BUSY',
  'STORE_CLOSED',
  'DURABILITY_REQUIRES_FILE',
  'UNSUPPORTED_SCHEMA_VERSION',
  'UNVERSIONED_DATA',
  'INVALID_INPUT',
  'WORKFLOW_NOT_FOUND',
  'WORKFLOW_ALREADY_EXISTS',
  'ILLEGAL_TRANSITION',
  'CONCURRENT_MODIFICATION',
  'LEASE_HELD',
  'LEASE_REQUIRED',
  'STALE_FENCE',
  'LEASE_EXPIRED',
  'TERMINAL_STATE',
  'OUTCOME_UNRESOLVED',
  'ATTEMPT_NOT_FOUND',
  'ATTEMPT_ALREADY_EXISTS',
  'ATTEMPT_STATE_INVALID',
  'LOGICAL_OPERATION_COMPLETED',
  'OBSERVATION_IDENTITY_CONFLICT',
  'ARTIFACT_IMMUTABLE',
  'ARTIFACT_NOT_FOUND',
  'IDEMPOTENCY_CONFLICT',
  'CHECKPOINT_CORRUPT',
  'TELEMETRY_INVALID_INPUT',
  'TELEMETRY_REDACTION_FAILED',
  'TELEMETRY_STORE_UNAVAILABLE',
  'TELEMETRY_SINK_UNAVAILABLE',
  'TELEMETRY_SINK_TIMEOUT',
  'TELEMETRY_CURSOR_GAP',
  'TELEMETRY_CURSOR_CONFLICT'
]);

export class RuntimeError extends Error {
  constructor({ module, operation, code, retryable, safe_message, diagnostic_ref }) {
    if (!SAFE_CODES.has(code)) {
      throw new TypeError('RuntimeError requires a registered safe code.');
    }

    super(safe_message);
    this.name = 'RuntimeError';
    this.module = module;
    this.operation = operation;
    this.code = code;
    this.retryable = Boolean(retryable);
    this.safe_message = safe_message;
    this.diagnostic_ref = diagnostic_ref || ('diag-' + randomUUID());
  }

  toJSON() {
    return {
      module: this.module,
      operation: this.operation,
      code: this.code,
      retryable: this.retryable,
      safe_message: this.safe_message,
      diagnostic_ref: this.diagnostic_ref
    };
  }
}

export function runtimeError(details) {
  return new RuntimeError(details);
}

export function toSafeErrorRecord(error) {
  if (error instanceof RuntimeError) return error.toJSON();
  return new RuntimeError({
    module: 'runtime',
    operation: 'boundary',
    code: 'PORT_UNAVAILABLE',
    retryable: false,
    safe_message: 'An internal operation failed.',
    diagnostic_ref: 'diag-' + randomUUID()
  }).toJSON();
}

export async function invokeInjected(operation, handler, validateResult) {
  if (typeof handler !== 'function' || typeof validateResult !== 'function') {
    throw runtimeError({
      module: 'runtime',
      operation,
      code: 'INVALID_INPUT',
      retryable: false,
      safe_message: 'The injected operation is not configured correctly.'
    });
  }

  let result;
  try {
    result = await handler();
  } catch {
    throw runtimeError({
      module: 'runtime',
      operation,
      code: 'PORT_UNAVAILABLE',
      retryable: true,
      safe_message: 'An injected dependency is unavailable.'
    });
  }

  let valid = false;
  try {
    valid = validateResult(result) === true;
  } catch {
    valid = false;
  }
  if (!valid) {
    throw runtimeError({
      module: 'runtime',
      operation,
      code: 'PORT_INVALID_RESULT',
      retryable: false,
      safe_message: 'An injected dependency returned an invalid result.'
    });
  }
  return result;
}
