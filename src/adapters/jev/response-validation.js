import { JevWireError, isPlainDataRecord, parseStrictJsonBytes } from './safe-json.js';
import { JEV_MODEL_ID, validateJevRequest } from './request-validation.js';

export const JEV_PROBABILITY_SUM_TOLERANCE = 0.01;
export const JEV_WEIGHTED_SCORE_TOLERANCE = 0.02;

const UNKNOWN_USAGE = Object.freeze({ kind: 'unknown', reason: 'missing_or_invalid' });

function validRecord(value) {
  return isPlainDataRecord(value);
}

function exactKeys(value, required) {
  if (!validRecord(value)) return false;
  const keys = Reflect.ownKeys(value);
  if (keys.length !== required.length) return false;
  return required.every(key => Object.hasOwn(value, key))
    && keys.every(key => typeof key === 'string' && required.includes(key));
}

function knownUsage(value) {
  if (!exactKeys(value, ['input_tokens', 'output_tokens'])) return UNKNOWN_USAGE;
  if (!Number.isSafeInteger(value.input_tokens) || value.input_tokens < 0
      || !Number.isSafeInteger(value.output_tokens) || value.output_tokens < 0) {
    return UNKNOWN_USAGE;
  }
  return Object.freeze({
    kind: 'known',
    inputTokens: value.input_tokens,
    outputTokens: value.output_tokens
  });
}

function validUnit(value) {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 1;
}

function keysEqual(value, expected) {
  if (!validRecord(value)) return false;
  const keys = Reflect.ownKeys(value);
  if (keys.length !== expected.length) return false;
  const set = new Set(expected);
  return keys.every(key => typeof key === 'string' && set.has(key));
}

function validDistribution(value, expectedKeys) {
  if (!keysEqual(value, expectedKeys)) return null;
  let sum = 0;
  for (const key of expectedKeys) {
    if (!validUnit(value[key])) return null;
    sum += value[key];
  }
  if (!Number.isFinite(sum) || Math.abs(sum - 1) > JEV_PROBABILITY_SUM_TOLERANCE) return null;
  return sum;
}

function equalJson(left, right) {
  if (left === right) return true;
  if (left === null || right === null || typeof left !== 'object' || typeof right !== 'object') return false;
  const leftArray = Array.isArray(left);
  if (leftArray !== Array.isArray(right)) return false;
  if (leftArray) {
    if (left.length !== right.length) return false;
    for (let index = 0; index < left.length; index += 1) {
      if (!equalJson(left[index], right[index])) return false;
    }
    return true;
  }
  if (!validRecord(left) || !validRecord(right)) return false;
  const leftKeys = Reflect.ownKeys(left);
  const rightKeys = Reflect.ownKeys(right);
  if (leftKeys.length !== rightKeys.length) return false;
  return leftKeys.every(key => typeof key === 'string'
    && Object.hasOwn(right, key)
    && equalJson(left[key], right[key]));
}

function validAnswer(answer, question) {
  if (!validRecord(answer) || answer.type !== question.type) return false;
  if (question.type === 'noul') {
    return exactKeys(answer, ['type', 'noul']) && validUnit(answer.noul);
  }
  if (question.type === 'choice') {
    if (!exactKeys(answer, ['type', 'choice', 'probabilities', 'confidence'])) return false;
    const options = Reflect.ownKeys(question.criteria);
    if (typeof answer.choice !== 'string' || !Object.hasOwn(question.criteria, answer.choice)) return false;
    if (!validUnit(answer.confidence) || !validDistribution(answer.probabilities, options)) return false;
    const maximum = Math.max(...options.map(key => answer.probabilities[key]));
    return answer.probabilities[answer.choice] >= maximum - 1e-12;
  }
  if (question.type === 'score') {
    if (!exactKeys(answer, ['type', 'score', 'legend', 'probabilities', 'confidence'])) return false;
    const indices = [];
    for (let index = 0; index < question.criteria.length; index += 1) indices.push(String(index));
    if (!keysEqual(answer.legend, indices) || !keysEqual(answer.probabilities, indices)) return false;
    if (!validUnit(answer.confidence) || !validDistribution(answer.probabilities, indices)) return false;
    for (let index = 0; index < question.criteria.length; index += 1) {
      if (!equalJson(answer.legend[String(index)], question.criteria[index])) return false;
    }
    if (typeof answer.score !== 'number' || !Number.isFinite(answer.score)
        || answer.score < 0 || answer.score > question.criteria.length - 1) return false;
    let weighted = 0;
    for (let index = 0; index < indices.length; index += 1) {
      weighted += index * answer.probabilities[indices[index]];
    }
    return Math.abs(answer.score - weighted) <= JEV_WEIGHTED_SCORE_TOLERANCE;
  }
  return false;
}

function invalidResult(usage, resolvedModel) {
  return Object.freeze({
    kind: 'invalid',
    code: 'INVALID_RESPONSE',
    usage,
    requestedModel: JEV_MODEL_ID,
    resolvedModel
  });
}

function safeResolvedModel(value) {
  return typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/u.test(value) ? value : null;
}

export function validateJevResponseBytes(body, requestInput) {
  let request;
  try {
    request = validateJevRequest(requestInput);
  } catch {
    throw new JevWireError('INVALID_REQUEST');
  }

  let response;
  try {
    response = parseStrictJsonBytes(body);
  } catch {
    return invalidResult(UNKNOWN_USAGE, null);
  }

  const usage = validRecord(response) ? knownUsage(response.usage) : UNKNOWN_USAGE;
  const resolvedModel = validRecord(response) ? safeResolvedModel(response.model) : null;
  if (!validRecord(response)
      || !exactKeys(response, ['model', 'answers', 'usage'])
      || response.model !== request.model
      || !keysEqual(response.answers, Reflect.ownKeys(request.questions))) {
    return invalidResult(usage, resolvedModel);
  }

  for (const id of Reflect.ownKeys(request.questions)) {
    if (!validAnswer(response.answers[id], request.questions[id])) return invalidResult(usage, resolvedModel);
  }
  if (usage.kind !== 'known') return invalidResult(usage, resolvedModel);

  return Object.freeze({
    kind: 'valid',
    requestedModel: request.model,
    resolvedModel,
    usage,
    answers: response.answers
  });
}
