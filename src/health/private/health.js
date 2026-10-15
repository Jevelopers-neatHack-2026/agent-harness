import { types as utilTypes } from 'node:util';
import { DurableWorkflowStore } from '../../internal/durable-store.js';
import { OPERATIONS_STORAGE_CAPABILITY } from '../../internal/private-ops-store-capability.js';
import { operationsStoragePolicy, readOperationsConfig } from '../../worker/private/config.js';

const LIFECYCLE = new Set(['starting', 'accepting', 'saturated', 'stopping', 'stopped', 'degraded']);

function ownInput(value) {
  if (value === null || typeof value !== 'object' || utilTypes.isProxy(value) || Array.isArray(value)) return null;
  let prototype;
  let keys;
  try {
    prototype = Object.getPrototypeOf(value);
    keys = Reflect.ownKeys(value);
  } catch {
    return null;
  }
  const allowed = new Set([
    'store', 'config', 'lifecycle', 'heartbeatAgeMs',
    'telemetrySinkState', 'telemetrySinkPendingRecords', 'telemetrySinkFailed'
  ]);
  if ((prototype !== Object.prototype && prototype !== null) || keys.length > allowed.size) return null;
  const result = Object.create(null);
  for (const key of keys) {
    if (typeof key !== 'string' || !allowed.has(key)) return null;
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      return null;
    }
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) return null;
    result[key] = descriptor.value;
  }
  return result;
}

/** Fixed, secret-free health snapshot over bounded store counters and lifecycle facts. */
export function readOperationsHealth(input) {
  const fields = ownInput(input);
  if (!fields || utilTypes.isProxy(fields.store)
      || !(fields.store instanceof DurableWorkflowStore)
      || !LIFECYCLE.has(fields.lifecycle)
      || !Number.isSafeInteger(fields.heartbeatAgeMs) || fields.heartbeatAgeMs < 0) {
    throw new TypeError('The operations health input is invalid.');
  }
  const telemetrySinkState = fields.telemetrySinkState ?? 'not_configured';
  const telemetrySinkPendingRecords = fields.telemetrySinkPendingRecords ?? 0;
  const telemetrySinkFailed = fields.telemetrySinkFailed ?? false;
  if (!new Set(['not_configured', 'idle', 'flushing', 'sink-pending', 'unavailable', 'closed']).has(telemetrySinkState)
      || !Number.isSafeInteger(telemetrySinkPendingRecords) || telemetrySinkPendingRecords < 0
      || typeof telemetrySinkFailed !== 'boolean') {
    throw new TypeError('The operations telemetry health input is invalid.');
  }
  const config = readOperationsConfig(fields.config);
  if (fields.lifecycle === 'stopped') {
    return Object.freeze({
      schema: 'agent-harness.operations-health.v1',
      state: 'stopped', live: false, ready: false, localReady: false,
      externalReady: false, externalDispatchEnabled: false,
      degradedReasons: Object.freeze([]),
      process: Object.freeze({ childCount: 0, workerCount: config.workerCount, waiterCount: 0, rssBytes: 0 }),
      authority: Object.freeze({ status: 'authority_unavailable', externalDispatchEnabled: false }),
      provider: Object.freeze({ status: 'provider_unavailable', externalDispatchEnabled: false }),
      retention: Object.freeze({ status: 'retention_not_configured', destructivePruningEnabled: false })
    });
  }
  const accepting = fields.lifecycle === 'accepting' || fields.lifecycle === 'saturated';
  const live = fields.lifecycle !== 'starting' && fields.lifecycle !== 'stopped';
  const snapshot = fields.store._operationsHealth(OPERATIONS_STORAGE_CAPABILITY, {
    policy: operationsStoragePolicy(config),
    accepting,
    live,
    heartbeatAgeMs: fields.heartbeatAgeMs
  });
  const telemetryReasons = [];
  if (telemetrySinkState === 'unavailable') telemetryReasons.push('TELEMETRY_SINK_UNAVAILABLE');
  if (telemetrySinkState === 'sink-pending') telemetryReasons.push('TELEMETRY_SINK_PENDING');
  if (telemetrySinkState === 'closed') telemetryReasons.push('TELEMETRY_SINK_CLOSED');
  if (telemetrySinkFailed) telemetryReasons.push('TELEMETRY_SINK_ERROR');
  const stale = fields.heartbeatAgeMs > Math.max(250, config.pollIntervalMs * 5);
  const degradedReasons = stale
    ? Object.freeze([...snapshot.degradedReasons, ...telemetryReasons, 'DIAGNOSTICS_STALE'])
    : Object.freeze([...snapshot.degradedReasons, ...telemetryReasons]);
  const state = fields.lifecycle === 'starting'
    ? fields.lifecycle : stale ? 'degraded'
      : fields.lifecycle === 'saturated' && snapshot.state === 'accepting'
        ? 'saturated' : snapshot.state;
  return Object.freeze({
    ...snapshot,
    state,
    degradedReasons,
    telemetrySink: Object.freeze({
      status: telemetrySinkState,
      pendingRecords: Math.min(telemetrySinkPendingRecords, 1000),
      pendingCapped: telemetrySinkPendingRecords > 1000,
      failed: telemetrySinkFailed
    }),
    live,
    ready: false,
    localReady: accepting && !stale && degradedReasons.length === 0,
    externalReady: false,
    externalDispatchEnabled: false
  });
}

export function readStoreCompatibility(databasePath) {
  return DurableWorkflowStore.inspectCompatibility({ databasePath, busyTimeoutMs: 20 });
}

export function requestOwnerRetention(store, request) {
  if (utilTypes.isProxy(store) || !(store instanceof DurableWorkflowStore)) {
    throw new TypeError('A durable workflow store is required.');
  }
  return store._operationsRetentionRequest(OPERATIONS_STORAGE_CAPABILITY, request);
}
