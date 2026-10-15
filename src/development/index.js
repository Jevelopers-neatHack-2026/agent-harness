import { JevDecisionError } from '../decisions/index.js';
import { isPlainDataRecord, snapshotJsonValue } from '../adapters/jev/safe-json.js';

function manifest(value) {
  if (!isPlainDataRecord(value)) throw new JevDecisionError('CONFIG_INVALID');
  const required = ['application', 'agent_harness', 'creative_engine'];
  const allowed = new Set([...required, 'event_intelligence', 'finance']);
  // Hidden pins must not disappear from validation, comparison or provenance.
  const keys = Reflect.ownKeys(value);
  if (keys.length > allowed.size || required.some(key => !Object.hasOwn(value, key)) ||
      keys.some(key => typeof key !== 'string' || !allowed.has(key))) {
    throw new JevDecisionError('CONFIG_INVALID');
  }
  const record = Object.create(null);
  for (const key of keys.sort()) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value') ||
        typeof descriptor.value !== 'string' || !/^[a-f0-9]{40}$/u.test(descriptor.value)) {
      throw new JevDecisionError('CONFIG_INVALID');
    }
    record[key] = descriptor.value;
  }
  return Object.freeze(record);
}

/** Development invokes the same authenticated application host, never a parallel executor. */
export async function invokeAgentCreativeDevelopment({ host, request, source_manifest }) {
  if (!host || !['development', 'test'].includes(host.environment) || typeof host.invoke !== 'function') {
    throw new JevDecisionError('CONFIG_INVALID');
  }
  const pinned = manifest(source_manifest);
  const loaded = manifest(host.source_manifest);
  if (JSON.stringify(pinned) !== JSON.stringify(loaded)) throw new JevDecisionError('CONFIG_INVALID');
  let input;
  try { input = JSON.parse(JSON.stringify(snapshotJsonValue(request, { allowPrimitive: false }))); }
  catch { throw new JevDecisionError('INVALID_REQUEST'); }
  if (input.environment !== host.environment) throw new JevDecisionError('DISPATCH_BINDING_INVALID');
  return Object.freeze({ source_manifest: pinned, evidence_mode: 'development_only', response: await host.invoke(input) });
}
