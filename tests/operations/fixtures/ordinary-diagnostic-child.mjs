import assert from 'node:assert/strict';
import { test } from 'node:test';

test('a multiline string that resembles an error remains an assertion value', () => {
  assert.strictEqual('RuntimeError:\n  code: STORE_BUSY', 'ordinary expected string');
});

test('fields on separate sibling objects do not form one nested error', () => {
  assert.deepStrictEqual({
    first: { name: 'RuntimeError' },
    second: { code: 'STORE_BUSY' }
  }, {});
});

test('native error fields and SQLite code on separate siblings remain separate', () => {
  assert.deepStrictEqual({
    first: { name: 'Error', code: 'ERR_SQLITE_ERROR' },
    second: { errcode: 5 }
  }, {});
});
