import { randomUUID } from 'node:crypto';
import { open, rename, rm } from 'node:fs/promises';

const DEFAULT_FILESYSTEM = Object.freeze({ open, rename, rm });

export async function writeJsonAtomically(path, value, { afterPartialWrite, filesystem = DEFAULT_FILESYSTEM } = {}) {
  const bytes = Buffer.from(`${JSON.stringify(value, null, 2)}\n`, 'utf8');
  const temporaryPath = `${path}.${process.pid}.${randomUUID()}.tmp`;
  let handle;
  let acquired = false;
  let closeAttempted = false;
  try {
    handle = await filesystem.open(temporaryPath, 'wx', 0o600);
    acquired = true;
    if (typeof afterPartialWrite === 'function') {
      const partialLength = Math.max(1, Math.floor(bytes.length / 2));
      await handle.writeFile(bytes.subarray(0, partialLength));
      await afterPartialWrite({ temporaryPath, partialBytes: partialLength, totalBytes: bytes.length });
      await handle.writeFile(bytes.subarray(partialLength));
    } else {
      await handle.writeFile(bytes);
    }
    closeAttempted = true;
    await handle.close();
    handle = null;
    await filesystem.rename(temporaryPath, path);
  } catch (error) {
    if (acquired && handle && !closeAttempted) {
      closeAttempted = true;
      try { await handle.close(); } catch {}
    }
    if (acquired) {
      try { await filesystem.rm(temporaryPath, { force: true }); } catch {}
    }
    throw error;
  }
}
