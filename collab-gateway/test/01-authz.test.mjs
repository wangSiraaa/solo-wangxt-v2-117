// T1 权限：坏 token、跨租户、非成员、viewer 只读；每次更新都重新鉴权。
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { buildServer } from '../src/server.mjs';
import { createPool } from '../src/repo.mjs';
import { config } from '../src/config.mjs';
import { openRaw, makeDoc, syncMsg, updateMsg, localEdit, TOKENS, listErrors } from './helpers.mjs';

let app, pool, baseUrl;
const DOC = `t1-${Date.now()}`;

before(async () => {
  pool = createPool(config.pg);
  app = await buildServer({ pool });
  await app.listen({ host: '127.0.0.1', port: 0 });
  baseUrl = `ws://127.0.0.1:${app.server.address().port}`;
  await makeDoc(pool, DOC);
});
after(async () => { await app.close(); });

test('bad token is rejected at connect (4401)', async () => {
  const c = openRaw(baseUrl, 'tok-nope', DOC);
  const { code } = await c.closed();
  assert.equal(code, 4401);
});

test('missing token is rejected at connect', async () => {
  const c = openRaw(baseUrl.replace('?token=', ''), '', DOC);
  const { code } = await c.closed();
  assert.equal(code, 4401);
});

test('non-member same tenant (carol on doc-plan) gets 4403', async () => {
  const c = openRaw(baseUrl, TOKENS.bob, 'doc-secret'); // Globex 的文档
  const { code } = await c.closed();
  assert.equal(code, 4403);
});

test('cross-tenant token cannot reach another tenant doc', async () => {
  const c = openRaw(baseUrl, TOKENS.dave, 'doc-note');
  const { code } = await c.closed();
  assert.equal(code, 4403);
});

test('viewer can sync but writes are denied per-update; editor writes succeed', async () => {
  const carol = openRaw(baseUrl, TOKENS.carol, DOC);
  await carol.opened();
  await carol.wait((m) => m.type === 'hello' && m.role === 'viewer');
  carol.send(syncMsg());
  await carol.wait((m) => m.type === 'sync');

  const [bytes] = localEdit((t) => t.insert(0, 'carol-write'), 3003);
  carol.send(updateMsg(bytes, 'viewer-write-nonce-001', 3003));
  const err = await carol.wait((m) => m.type === 'error' && m.code === 'not_allowed');
  assert.match(err.message, /read-only/);

  // 同一连接上 editor 的更新验证“每次更新重新鉴权”不影响正常角色
  const alice = openRaw(baseUrl, TOKENS.alice, DOC);
  await alice.opened();
  await alice.wait((m) => m.type === 'hello');
  const [ab] = localEdit((t) => t.insert(0, 'alice-ok'), 1001);
  const nonce = 'editor-ok-nonce-00000001';
  alice.send(updateMsg(ab, nonce, 1001));
  const ack = await alice.waitAck(nonce);
  assert.equal(ack.duplicated, false);
  assert.ok(Number.isInteger(ack.updateId));

  await carol.close();
  await alice.close();

  const errs = await listErrors(pool, DOC);
  assert.ok(errs.some((e) => e.kind === 'unauthorized' && e.nonce === 'viewer-write-nonce-001'));
});

test('client cannot claim another room inside the message body — docId comes from URL', async () => {
  // Alice 合法连入 DOC，却在消息里塞 docId/tenant 字段，服务端必须忽略，更新落在 URL 文档
  const c = openRaw(baseUrl, TOKENS.alice, DOC);
  await c.opened();
  await c.wait((m) => m.type === 'hello');
  const [bytes] = localEdit((t) => t.insert(0, 'spoof-room'), 1002);
  const nonce = 'spoof-room-nonce-00001';
  c.send({ ...updateMsg(bytes, nonce, 1002), docId: 'doc-secret', tenantId: 't-globex', room: 'doc-secret' });
  const ack = await c.waitAck(nonce);
  assert.ok(ack.updateId);

  const { rows } = await pool.query(
    'SELECT doc_id FROM doc_updates WHERE nonce=$1 AND doc_id=$2',
    [nonce, DOC],
  );
  assert.equal(rows.length, 1);
  assert.equal(rows[0].doc_id, DOC); // 落在 URL 指定文档，而非消息体声称的房间
  // 消息体声称的跨租户房间里不得出现这条更新
  const leak = await pool.query('SELECT 1 FROM doc_updates WHERE nonce=$1 AND doc_id=$2', [nonce, 'doc-secret']);
  assert.equal(leak.rows.length, 0);
  await c.close();
});
