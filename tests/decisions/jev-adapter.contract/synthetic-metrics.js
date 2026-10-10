import { appendFileSync } from 'node:fs';

const METRIC_NAMES = new Set([
  'controlled_dns_resolve4',
  'controlled_dns_resolve6',
  'controlled_https_request',
  'controlled_https_write',
  'blocked_external_network_attempt',
  'crash_before_dispatch',
  'crash_after_possible_write',
  'crash_after_response_before_observation',
  'crash_after_observation_commit'
]);

export function recordSyntheticMetric(name) {
  if (!METRIC_NAMES.has(name)) throw new TypeError('Unknown synthetic network metric.');
  const file = process.env.JEV_OFFLINE_METRICS_FILE;
  if (file === undefined) return;
  if (typeof file !== 'string' || file.length > 4096 || !file.startsWith('/')) {
    throw new TypeError('The synthetic metrics destination is invalid.');
  }
  appendFileSync(file, name + '\n', { encoding: 'utf8', flag: 'a' });
}
