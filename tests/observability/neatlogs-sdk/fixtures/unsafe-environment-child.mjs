import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { createNeatlogsSdkCapture } from '../../../../src/observability/private/neatlogs-sdk-capture.js';

process.env.NEATLOGS_LOG_SPANS = '1';
const records = JSON.parse(readFileSync(resolve(process.argv[2]), 'utf8'));
const capture = createNeatlogsSdkCapture();
const result = await capture.prepare(records);
const diagnostics = capture.diagnostics();
await capture.close();
process.stdout.write(`${JSON.stringify({
  status: result.status,
  reasonCode: result.reasonCode,
  sdkImported: diagnostics.sdkImported,
  clientCreated: diagnostics.clientCreated
})}\n`);
