import { readFileSync, statSync } from 'node:fs';
import { inspect } from 'node:util';

// Recognized names. NEATLOGS_INGEST_KEY is the project ingest key (OTLP
// `x-api-key`; it can only write), NEATLOGS_READ_TOKEN is a read-only
// service-account Public API token, NEATLOGS_PROJECT_ID selects the bound
// project, and NEATLOGS_ENDPOINT / NEATLOGS_HOST are the regional ingest and
// app origins. NEATLOGS_API_KEY and NEATLOGS_TOKEN (the names in the Neatlogs
// docs) are still read as fallbacks while hosts rename their settings; the
// new name wins when both are set.
export const NEATLOGS_CONFIG_NAMES = Object.freeze([
  'NEATLOGS_INGEST_KEY',
  'NEATLOGS_READ_TOKEN',
  'NEATLOGS_PROJECT_ID',
  'NEATLOGS_ENDPOINT',
  'NEATLOGS_HOST',
  'NEATLOGS_API_KEY',
  'NEATLOGS_TOKEN'
]);
export const NEATLOGS_LEGACY_NAMES = Object.freeze({
  NEATLOGS_INGEST_KEY: 'NEATLOGS_API_KEY',
  NEATLOGS_READ_TOKEN: 'NEATLOGS_TOKEN'
});
const NAME_SET = new Set(NEATLOGS_CONFIG_NAMES);
const REGIONS = Object.freeze({
  us: Object.freeze({ ingest: 'https://ingest.neatlogs.com', app: 'https://app.neatlogs.com' }),
  eu: Object.freeze({ ingest: 'https://eu.ingest.neatlogs.com', app: 'https://eu.app.neatlogs.com' })
});
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const CREDENTIAL = /^[\x21-\x7e]{16,512}$/;
const MAX_ENV_FILE_BYTES = 16 * 1024;
const MAX_ENV_FILE_LINES = 256;

export class NeatlogsConfigError extends Error {
  constructor(code, name = null) {
    super(`Neatlogs configuration is invalid (${code}${name ? `: ${name}` : ''}).`);
    this.name = 'NeatlogsConfigError';
    this.code = code;
    this.setting = name;
  }
}

function unquote(raw) {
  const value = raw.trim();
  if (value.length >= 2) {
    const first = value[0];
    if ((first === '"' || first === "'") && value.at(-1) === first) return value.slice(1, -1);
  }
  if (value.includes(' #')) return value.slice(0, value.indexOf(' #')).trim();
  return value;
}

// A deliberately small dotenv reader: KEY=VALUE lines, optional `export`,
// whole-line comments, single or double quotes, no interpolation. Only the
// seven recognized Neatlogs names are returned; every other line is ignored.
export function readNeatlogsEnvFile(path) {
  let stats;
  try {
    stats = statSync(path);
  } catch {
    throw new NeatlogsConfigError('ENV_FILE_UNREADABLE');
  }
  if (!stats.isFile() || stats.size > MAX_ENV_FILE_BYTES) {
    throw new NeatlogsConfigError('ENV_FILE_UNREADABLE');
  }
  const lines = readFileSync(path, 'utf8').split(/\r?\n/);
  if (lines.length > MAX_ENV_FILE_LINES) throw new NeatlogsConfigError('ENV_FILE_UNREADABLE');
  const values = Object.create(null);
  for (const line of lines) {
    const trimmed = line.trim();
    if (trimmed === '' || trimmed.startsWith('#')) continue;
    const match = /^(?:export\s+)?([A-Z][A-Z0-9_]*)\s*=(.*)$/.exec(trimmed);
    if (!match || !NAME_SET.has(match[1])) continue;
    if (Object.hasOwn(values, match[1])) throw new NeatlogsConfigError('ENV_FILE_DUPLICATE', match[1]);
    values[match[1]] = unquote(match[2]);
  }
  return values;
}

function pick(name, env, file) {
  const fromEnv = typeof env?.[name] === 'string' ? env[name].trim() : '';
  if (fromEnv !== '') return fromEnv;
  const fromFile = typeof file?.[name] === 'string' ? file[name].trim() : '';
  return fromFile === '' ? null : fromFile;
}

/** The new name's value, or the legacy name's when only that is set. */
function pickRenamed(name, env, file) {
  return pick(name, env, file) ?? pick(NEATLOGS_LEGACY_NAMES[name], env, file);
}

function origin(name, value) {
  let url;
  try {
    url = new URL(value);
  } catch {
    throw new NeatlogsConfigError('INVALID_ORIGIN', name);
  }
  const path = url.pathname.replace(/\/+$/, '');
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash
      || (path !== '' && !(name === 'NEATLOGS_ENDPOINT' && path === '/v1/traces'))) {
    throw new NeatlogsConfigError('INVALID_ORIGIN', name);
  }
  return url.origin;
}

function resolveRegion(endpoint, host) {
  const ingest = endpoint === null ? null : origin('NEATLOGS_ENDPOINT', endpoint);
  const app = host === null ? null : origin('NEATLOGS_HOST', host);
  for (const [region, origins] of Object.entries(REGIONS)) {
    if ((ingest === null || ingest === origins.ingest) && (app === null || app === origins.app)) {
      if (ingest === null && app === null) return { region: 'us', ...REGIONS.us };
      return { region, ...origins };
    }
  }
  // Credentials are bound to their issuing deployment, so a mixed or unknown
  // ingest/app pair is refused rather than guessed.
  throw new NeatlogsConfigError('REGION_MISMATCH');
}

function credential(name, value) {
  if (value === null) return null;
  if (!CREDENTIAL.test(value)) throw new NeatlogsConfigError('INVALID_CREDENTIAL', name);
  return value;
}

/**
 * Builds Neatlogs configuration from an explicit environment object and an
 * optional explicitly named env file. Renamed credentials use canonical env,
 * canonical file, legacy env, then legacy file; blank values fall through.
 * Other settings use nonblank env before file. Nothing is read implicitly,
 * and credentials are only reachable through accessors; inspection and JSON
 * output are redacted.
 */
export function loadNeatlogsConfig({ env = {}, envFile = null } = {}) {
  const file = envFile === null ? null : readNeatlogsEnvFile(envFile);
  const apiKey = credential('NEATLOGS_INGEST_KEY', pickRenamed('NEATLOGS_INGEST_KEY', env, file));
  const token = credential('NEATLOGS_READ_TOKEN', pickRenamed('NEATLOGS_READ_TOKEN', env, file));
  const projectId = pick('NEATLOGS_PROJECT_ID', env, file);
  if (projectId !== null && !UUID.test(projectId)) {
    throw new NeatlogsConfigError('INVALID_PROJECT_ID', 'NEATLOGS_PROJECT_ID');
  }
  const { region, ingest, app } = resolveRegion(
    pick('NEATLOGS_ENDPOINT', env, file),
    pick('NEATLOGS_HOST', env, file)
  );
  const summary = Object.freeze({
    region,
    ingestOrigin: ingest,
    appOrigin: app,
    projectId,
    ingestKeyConfigured: apiKey !== null,
    readbackTokenConfigured: token !== null,
    ingestReady: apiKey !== null,
    readbackReady: token !== null && projectId !== null
  });
  const config = {
    summary,
    ingestKey() {
      if (apiKey === null) throw new NeatlogsConfigError('MISSING_SETTING', 'NEATLOGS_INGEST_KEY');
      return apiKey;
    },
    readbackToken() {
      if (token === null) throw new NeatlogsConfigError('MISSING_SETTING', 'NEATLOGS_READ_TOKEN');
      if (projectId === null) throw new NeatlogsConfigError('MISSING_SETTING', 'NEATLOGS_PROJECT_ID');
      return token;
    },
    toJSON() {
      return summary;
    },
    [inspect.custom]() {
      return `NeatlogsConfig ${inspect(summary)}`;
    }
  };
  return Object.freeze(config);
}

/**
 * Removes the recognized Neatlogs names from an environment object after the
 * caller has loaded them. The pinned SDK capture refuses to start while any
 * NEATLOGS_* name is present, and child processes should not inherit secrets.
 */
export function scrubNeatlogsEnvironment(env) {
  const removed = [];
  for (const name of NEATLOGS_CONFIG_NAMES) {
    if (Object.hasOwn(env, name)) {
      delete env[name];
      removed.push(name);
    }
  }
  return Object.freeze(removed);
}

export const NEATLOGS_REGIONS = REGIONS;
