import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {installScript} from '../src/install-script.mjs';

const program=installScript('https://example.test','https://example.test/agent');
for(const tag of ['', 'Bad Group', '-invalid', 'a'.repeat(31)]) {
  const result=spawnSync('/bin/sh',['-c',program],{encoding:'utf8',timeout:3000,
    env:{...process.env,JOIN_TOKEN:'test-only',SERVER_TAG:tag,POOL:''}});
  assert.equal(result.status,1);
  assert.match(result.stderr,/SERVER_TAG/);
  assert.doesNotMatch(result.stderr,/Docker|Install docker/,'tag validation must run before Docker or network operations');
}
console.log('4 required-tag installer preflight checks passed.');
