import { readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { createNeatlogsSdkCapture } from '../../../../src/observability/private/neatlogs-sdk-capture.js';

const phase = process.argv[2];
const recordsPath = resolve(process.argv[3]);
const readyPath = resolve(process.argv[4]);
const outputPath = resolve(process.argv[5]);

if (phase === 'before') {
  writeFileSync(readyPath, 'ready');
  setInterval(() => {}, 60_000);
} else if (phase === 'after') {
  const records = JSON.parse(readFileSync(recordsPath, 'utf8'));
  const capture = createNeatlogsSdkCapture();
  const result = await capture.prepare(records);
  await capture.close();
  if (result.status !== 'captured_local') process.exit(2);
  writeFileSync(outputPath, JSON.stringify(result.envelope));
  writeFileSync(readyPath, 'ready');
  setInterval(() => {}, 60_000);
} else {
  process.exit(2);
}
