import fs from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import net from 'node:net';
import tls from 'node:tls';

// Process-exit and filesystem-failure controls only. No credentials are read
// from the environment and any unmocked socket/fetch attempt fails the child.
let realNetworkAttempts = 0;
const deny = () => { realNetworkAttempts += 1; throw new Error('real network forbidden'); };
globalThis.fetch = deny;
net.Socket.prototype.connect = deny;
tls.connect = deny;

const [directory, mode, failAtText, receiptPath] = process.argv.slice(2);
let batchWrites = 0;
let posts = 0;
const originalRename = fs.renameSync;
const originalFsync = fs.fsyncSync;
if (mode === 'fail-write' || mode === 'exit-before-post') {
  fs.renameSync = (source, destination) => {
    if (/\/batch-\d+-\d+\.json$/.test(String(destination)) && ++batchWrites === Number(failAtText)
        && mode === 'fail-write') {
      const error = new Error('synthetic durable rename failure');
      error.code = 'EIO';
      throw error;
    }
    return originalRename(source, destination);
  };
  if (mode === 'exit-before-post') fs.fsyncSync = fd => {
    originalFsync(fd);
    if (batchWrites === Number(failAtText) && fs.fstatSync(fd).isDirectory()) {
      const batch = JSON.parse(fs.readFileSync(listNeatlogsOutbox(directory)[0], 'utf8'));
      fs.writeFileSync(receiptPath, JSON.stringify({ batch, posts, batchWrites, realNetworkAttempts }), { mode: 0o600 });
      process.exit(71);
    }
  };
  syncBuiltinESMExports();
}
const { drainNeatlogsOutbox, listNeatlogsOutbox, runtimeNeatlogsTags } = await import(
  '../../../../src/observability/private/neatlogs-outbox.js');
const { loadNeatlogsConfig } = await import('../../../../src/observability/private/neatlogs-config.js');
const config = loadNeatlogsConfig({ env: {
  NEATLOGS_INGEST_KEY: 'synthetic-ingest-key-0123456789abcdef',
  NEATLOGS_READ_TOKEN: 'synthetic-readback-token-0123456789abcdef',
  NEATLOGS_PROJECT_ID: '00000000-0000-4000-8000-000000000001'
} });
const tags = runtimeNeatlogsTags({ revision: 'b'.repeat(40), harnessMode: 'mock' });
try {
  await drainNeatlogsOutbox({ directory, config, tags, recover: true,
    sendOptions: { maxAttempts: 1 }, fetch: async (url, init) => {
      if (!String(url).endsWith('/v1/traces')) throw new Error('unexpected fake route');
      posts += 1;
      const batch = JSON.parse(fs.readFileSync(listNeatlogsOutbox(directory)[0], 'utf8'));
      fs.writeFileSync(receiptPath, JSON.stringify({ bodyBase64: Buffer.from(init.body).toString('base64'),
        batch, realNetworkAttempts }), { mode: 0o600 });
      if (mode === 'crash') process.exit(71);
      return new Response(null, { status: 200 });
    } });
  process.stdout.write(JSON.stringify({ code: null, posts, batchWrites, realNetworkAttempts }));
} catch (error) {
  process.stdout.write(JSON.stringify({ code: error.code ?? 'UNEXPECTED', posts, batchWrites, realNetworkAttempts }));
}
