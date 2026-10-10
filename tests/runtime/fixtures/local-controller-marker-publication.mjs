import { existsSync, renameSync, writeFileSync } from 'node:fs';

const [markerPath, mode, partialReadyPath, releasePath] = process.argv.slice(2);
const temporaryPath = markerPath + '.' + process.pid + '.tmp';

if (mode === 'fragmented') {
  writeFileSync(temporaryPath, '');
  writeFileSync(partialReadyPath, 'temporary marker started');
  while (!existsSync(releasePath)) await new Promise(resolve => setTimeout(resolve, 5));
  writeFileSync(temporaryPath, '{"stage":"fragmented","complete":true}');
  renameSync(temporaryPath, markerPath);
} else if (mode === 'malformed') {
  writeFileSync(temporaryPath, '{');
  renameSync(temporaryPath, markerPath);
} else if (mode === 'valid') {
  writeFileSync(temporaryPath, '{"stage":"valid","complete":true}');
  renameSync(temporaryPath, markerPath);
} else if (mode !== 'silent') {
  process.exitCode = 2;
  process.exit();
}

await new Promise(() => setInterval(() => {}, 60_000));
