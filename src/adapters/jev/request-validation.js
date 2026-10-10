import { JevWireError, isPlainDataRecord, snapshotJsonValue } from './safe-json.js';

export const JEV_MODEL_ID = 'jev-1.13.0';
export const JEV_MAX_REQUEST_BYTES = 256 * 1024;
export const JEV_MAX_QUESTIONS = 64;

function invalid() {
  throw new JevWireError('INVALID_REQUEST');
}

function exactKeys(value, required, optional = []) {
  if (!isPlainDataRecord(value)) invalid();
  const keys = Reflect.ownKeys(value);
  if (keys.some(key => typeof key !== 'string')) invalid();
  const allowed = new Set([...required, ...optional]);
  if (keys.some(key => !allowed.has(key))) invalid();
  if (required.some(key => !Object.hasOwn(value, key))) invalid();
}

function mapKeys(value, { minimum, maximum }) {
  if (!isPlainDataRecord(value)) invalid();
  const keys = Reflect.ownKeys(value);
  if (keys.length < minimum || keys.length > maximum) invalid();
  for (const key of keys) {
    if (typeof key !== 'string' || key.length === 0 || key.length > 128 || /[\u0000-\u001f\u007f]/u.test(key)) {
      invalid();
    }
  }
  return keys;
}

function structuredText(value, nullable = false) {
  if (nullable && value === null) return;
  if (typeof value === 'string') return;
  if (Array.isArray(value) || isPlainDataRecord(value)) return;
  invalid();
}

function validateQuestion(question) {
  if (!isPlainDataRecord(question) || !Object.hasOwn(question, 'type')) invalid();
  const type = question.type;
  if (type === 'noul') {
    exactKeys(question, ['type', 'instructions'], ['criteria']);
    structuredText(question.instructions);
    if (Object.hasOwn(question, 'criteria')) {
      const criteria = question.criteria;
      if (!isPlainDataRecord(criteria)) invalid();
      const keys = Reflect.ownKeys(criteria);
      if (keys.length > 2 || keys.some(key => key !== 'true' && key !== 'false')) invalid();
      for (const key of keys) structuredText(criteria[key]);
    }
    return;
  }
  if (type === 'choice') {
    exactKeys(question, ['type', 'instructions', 'criteria']);
    structuredText(question.instructions);
    const keys = mapKeys(question.criteria, { minimum: 1, maximum: 255 });
    for (const key of keys) structuredText(question.criteria[key], true);
    return;
  }
  if (type === 'score') {
    exactKeys(question, ['type', 'instructions', 'criteria']);
    structuredText(question.instructions);
    if (!Array.isArray(question.criteria) || question.criteria.length < 2 || question.criteria.length > 10) invalid();
    for (let index = 0; index < question.criteria.length; index += 1) structuredText(question.criteria[index]);
    return;
  }
  invalid();
}

export function validateJevRequest(input) {
  const request = snapshotJsonValue(input, { allowPrimitive: false });
  exactKeys(request, ['model', 'state', 'questions']);
  if (request.model !== JEV_MODEL_ID) invalid();
  if (typeof request.state !== 'string' && !Array.isArray(request.state) && !isPlainDataRecord(request.state)) invalid();
  const ids = mapKeys(request.questions, { minimum: 1, maximum: JEV_MAX_QUESTIONS });
  for (const id of ids) validateQuestion(request.questions[id]);
  return request;
}

export function serializeJevRequest(input) {
  const request = validateJevRequest(input);
  let json;
  try {
    json = JSON.stringify(request);
  } catch {
    invalid();
  }
  if (typeof json !== 'string' || Buffer.byteLength(json, 'utf8') > JEV_MAX_REQUEST_BYTES) invalid();
  return Object.freeze({ request, json, byteLength: Buffer.byteLength(json, 'utf8') });
}
