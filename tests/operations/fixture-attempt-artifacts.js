import { encodeFixtureRetryDiagnostic } from './fixture-busy-retry.js';

const MAX_TAP_BYTES = 2 * 1024 * 1024;
const MAX_FRAME_BYTES = 9_216;
const EXPECTED = Object.freeze([
  'OPS05.C12:expired:0', 'OPS05.C12:expired:1',
  'OPS05.C12:terminal:0', 'OPS05.C12:terminal:1',
  'OPS05.C08:0', 'OPS05.C08:1', 'OPS05.C08:2'
]);

function exactKeys(value, keys) {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).sort().join(',') === [...keys].sort().join(',');
}

// Retain only canonical, closed fixture records from the original TAP. The
// remaining TAP and any malformed/private content are never copied into the
// uploaded diagnostic JSON. This does not change the OPS case oracle.
export function parseFixtureAttemptArtifacts(text) {
  const records = [];
  const seen = new Set();
  let reason = 'complete';
  let invalidFrames = 0;
  const finish = () => {
    const missing = EXPECTED.filter(id => !seen.has(id));
    const unavailable = records.filter(record => record.diagnostic === null).map(record => record.id);
    const valid = invalidFrames === 0 && !['invalid_input', 'oversize', 'truncated'].includes(reason);
    const complete = valid && missing.length === 0 && unavailable.length === 0;
    return { schema: 'agent-harness.ops-fixture-attempt-artifacts.v1', valid, complete,
      reason: reason === 'complete' && !complete ? records.length === 0 ? 'missing' : 'incomplete' : reason,
      expectedCount: EXPECTED.length, observedCount: records.length, invalidFrames,
      missing, unavailable, records };
  };
  if (typeof text !== 'string') { reason = 'invalid_input'; return finish(); }
  if (Buffer.byteLength(text, 'utf8') > MAX_TAP_BYTES) { reason = 'oversize'; return finish(); }
  if (!text.endsWith('\n')) { reason = 'truncated'; return finish(); }
  let frameCount = 0;
  for (const line of text.split(/\r?\n/u)) {
    if (!/^\s*#\s*OPS05_C(?:08|12)_RETRY/u.test(line)) continue;
    const match = line.match(/^[\t ]*# (OPS05_C(?:08|12)_RETRY_V1) (.*)$/u);
    frameCount += 1;
    if (frameCount > EXPECTED.length || Buffer.byteLength(line, 'utf8') > MAX_FRAME_BYTES) {
      invalidFrames += 1; reason = 'oversize'; break;
    }
    if (!match) { invalidFrames += 1; reason = 'invalid_frame'; continue; }
    try {
      const input = JSON.parse(match[2]);
      const recovery = match[1] === 'OPS05_C12_RETRY_V1';
      if (!exactKeys(input, recovery ? ['race', 'slot', 'diagnostic'] : ['slot', 'diagnostic'])
          || !Number.isSafeInteger(input.slot) || input.slot < 0 || input.slot > (recovery ? 1 : 2)
          || (recovery && !['expired', 'terminal'].includes(input.race))) throw new Error('shape');
      const id = recovery ? `OPS05.C12:${input.race}:${input.slot}` : `OPS05.C08:${input.slot}`;
      if (seen.has(id)) { invalidFrames += 1; reason = 'duplicate'; continue; }
      let diagnostic = null;
      if (input.diagnostic !== null) {
        const encoded = encodeFixtureRetryDiagnostic(input.diagnostic);
        if (encoded === null) throw new Error('diagnostic');
        diagnostic = JSON.parse(encoded.slice(encoded.indexOf(' ') + 1));
        if (diagnostic.kind !== (recovery ? 'claim' : 'admission')) throw new Error('kind');
      }
      const canonical = recovery ? { race: input.race, slot: input.slot, diagnostic }
        : { slot: input.slot, diagnostic };
      if (JSON.stringify(canonical) !== match[2]) throw new Error('canonical');
      seen.add(id);
      records.push({ id, ...canonical });
    } catch { invalidFrames += 1; reason = 'invalid_frame'; }
  }
  return finish();
}
