import { runtimeError } from '../../../src/internal/errors.js';

const mode = process.argv[2];
const sourceLocation = 'tests/operations/operations.test.js:4400:9';
const cases = {
  assertion: { name: 'AssertionError', code: 'ERR_ASSERTION', failureType: 'testCodeFailure', operator: 'strictEqual' },
  busy: { name: 'Error', code: 'SQLITE_BUSY', failureType: 'testCodeFailure', operator: 'throws' },
  'runtime-busy': { name: 'RuntimeError', code: 'STORE_BUSY', failureType: 'testCodeFailure', operator: 'throws' },
  'node-busy': { name: 'Error', code: 'ERR_SQLITE_ERROR', errcode: 5, failureType: 'testCodeFailure', operator: 'throws' },
  'node-locked': { name: 'Error', code: 'ERR_SQLITE_ERROR', errcode: 262, failureType: 'testCodeFailure', operator: 'throws' },
  'node-unsupported': { name: 'Error', code: 'ERR_SQLITE_ERROR', errcode: 1, failureType: 'testCodeFailure', operator: 'throws' },
  parse: { name: 'SyntaxError', code: 'ERR_TEST_FAILURE', failureType: 'testCodeFailure', operator: null }
};

if (mode === 'timeout') {
  process.stdout.write('TAP version 13\n# Subtest: OPS13.C01 controlled timeout\n');
  setInterval(() => {}, 60_000);
} else {
  const wrappedRuntimeBusy = mode === 'wrapped-runtime-busy';
  const failure = wrappedRuntimeBusy
    ? { name: 'AssertionError', code: 'ERR_ASSERTION', failureType: 'testCodeFailure', operator: 'strictEqual' }
    : cases[mode];
  if (!failure) process.exit(2);
  const actualRuntimeError = mode === 'runtime-busy' || wrappedRuntimeBusy
    ? runtimeError({
      module: 'state', operation: 'write', code: 'STORE_BUSY', retryable: true,
      safe_message: 'SYNTHETIC_PRIVATE_DIAGNOSTIC_CANARY_96d'
    }) : null;
  const errorLines = wrappedRuntimeBusy ? [
    '  error:',
    '    RuntimeError:',
    `      name: '${actualRuntimeError.name}'`,
    `      code: '${actualRuntimeError.code}',`,
    `      safe_message: '${actualRuntimeError.safe_message}'`
  ] : [];
  const topLevelRuntimeError = mode === 'runtime-busy' ? actualRuntimeError : null;
  const topLevelName = topLevelRuntimeError?.name ?? failure.name;
  const topLevelCode = topLevelRuntimeError?.code ?? failure.code;
  process.stdout.write([
    'TAP version 13',
    '1..1',
    'not ok 1 - OPS13.C01 controlled failure diagnostic',
    '  ---',
    `  failureType: '${failure.failureType}'`,
    `  location: '${sourceLocation}'`,
    `  name: '${topLevelName}'`,
    `  code: '${topLevelCode}'`,
    failure.errcode === undefined ? null : `  errcode: ${failure.errcode},`,
    failure.operator ? `  operator: '${failure.operator}'` : null,
    "  actual: 'SYNTHETIC_PRIVATE_DIAGNOSTIC_CANARY_96d'",
    "  expected: 'safe-control-value'",
    "  error: 'SYNTHETIC_PRIVATE_DIAGNOSTIC_CANARY_96d'",
    '  stack: |-',
    '    at controlled failure fixture',
    ...errorLines,
    '  ...',
    '# tests 1',
    '# pass 0',
    '# fail 1',
    '# skipped 0',
    ''
  ].filter(line => line !== null).join('\n'));
  process.exitCode = 1;
}
