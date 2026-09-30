// T3 未知/损坏消息：拒绝并落 doc_errors（可按 doc/nonce/id 定位），
// 不污染更新日志、不断开连接、不影响房间内其他客户端。
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { buildServer } from '../src/server.mjs';
import { createPool } from '../src/repo.mjs';
import { config } from '../src/config.mjs';
import { openRaw, makeDoc, syncMsg, updateMsg, localEdit, TOKENS, listErrors, countUpdates } from './helpers.mjs';

let app, pool, baseUrl;
const DOC = `t3-${Date.now()}`;

before(async () => {
  pool = createPool(config.pg);
  app = await buildServer({ pool });
  await app.listen({ host: '127.0.0.1', port: 0 });
  baseUrl = `ws://127.0.0.1:${app.server.address().port}`;
  await makeDoc(pool, DOC);
});
after(async () => { await app.close(); });

test('corrupt update bytes are rejected, recorded, and never persisted', async () => {
  const c = openRaw(baseUrl, TOKENS.alice, DOC);
  await c.opened(); await c.wait((m) => m.type === 'hello');

  // 1) 非合法 Yjs update 的随机字节
  const garbage = Buffer.from([0xde, 0xad, 0xbe, 0xef, 0x00, 0x11, 0x22]);
  const nonce = 'corrupt-nonce-0000001';
  c.send({ v: 1, type: 'update', nonce, update: garbage.toString('base64'), clientId: 7001 });
  const err1 = await c.wait((m) => m.type === 'error' && m.nonce === nonce);
  assert.equal(err1.code, 'corrupt_update');
  assert.ok(err1.errorId, 'errorId points to locatable row');

  // 2) 截断的合法 update
  const [good] = localEdit((t) => t.insert(0, 'will-be-truncated'), 7002);
  const truncated = good.subarray(0, Math.floor(good.length / 2));
  const nonce2 = 'corrupt-nonce-0000002';
  c.send({ v: 1, type: 'update', nonce: nonce2, update: Buffer.from(truncated).toString('base64'), clientId: 7002 });
  const err2 = await c.wait((m) => m.type === 'error' && m.nonce === nonce2);
  assert.equal(err2.code, 'corrupt_update');

  // 3) 非 base64 载荷
  const nonce3 = 'corrupt-nonce-0000003';
  c.send({ v: 1, type: 'update', nonce: nonce3, update: '!!!not-base64!!!', clientId: 7003 });
  const err3 = await c.wait((m) => m.type === 'error' && m.nonce === nonce3);
  assert.equal(err3.code, 'corrupt_update');

  const errs = await listErrors(pool, DOC);
  const corrupt = errs.filter((e) => e.kind === 'corrupt_update');
  assert.ok(corrupt.length >= 3);
  // payload_hex 可定位原始坏字节
  const { rows } = await pool.query(
    "SELECT payload_hex FROM doc_errors WHERE doc_id=$1 AND nonce='corrupt-nonce-0000001'",
    [DOC],
  );
  assert.ok(rows[0].payload_hex.startsWith('deadbeef'));

  assert.equal(await countUpdates(pool, DOC), 0, 'nothing corrupt entered the update log');

  // 连接仍然活着：紧接着发一条合法更新必须成功
  const [ok] = localEdit((t) => t.insert(0, 'after-errors'), 7004);
  const okNonce = 'after-errors-nonce-00001';
  c.send(updateMsg(ok, okNonce, 7004));
  const ack = await c.waitAck(okNonce);
  assert.equal(ack.duplicated, false);
  await c.close();
});

test('unknown message type and bad envelopes are isolated per message', async () => {
  const c = openRaw(baseUrl, TOKENS.bob, DOC);
  await c.opened(); await c.wait((m) => m.type === 'hello');

  c.send({ v: 1, type: 'teleport', nonce: 'unknown-nonce-0001', x: 1 });
  const u = await c.waitNext((m) => m.type === 'error' && m.code === 'unknown_type');
  assert.ok(u.errorId);

  c.send('{not-json');
  const bad = await c.waitNext((m) => m.type === 'error'
    && (m.code === 'bad_json' || m.code === 'bad_envelope'));
  assert.ok(['bad_json', 'bad_envelope'].includes(bad.code));

  c.send({ type: 'update' }); // 缺 v / nonce / update
  await c.waitNext((m) => m.type === 'error' && m.code === 'bad_envelope');

  c.send(syncMsg()); // 连接仍可用
  await c.waitNext((m) => m.type === 'sync');
  await c.close();

  const errs = await listErrors(pool, DOC);
  assert.ok(errs.some((e) => e.kind === 'unknown_type'));
  assert.ok(errs.some((e) => e.kind === 'bad_envelope'));
});
test('bad traffic does not disturb other clients in the room', async () => {
  const victim = openRaw(baseUrl, TOKENS.alice, DOC);
  const bad = openRaw(baseUrl, TOKENS.bob, DOC);
  await Promise.all([victim.opened(), bad.opened()]);
  await Promise.all([victim.wait((m) => m.type === 'hello'), bad.wait((m) => m.type === 'hello')]);

  // 坏客户端制造错误
  bad.send({ v: 1, type: 'update', nonce: 'noise-nonce-0000001', update: '////', clientId: 1 });
  await bad.wait((m) => m.type === 'error');

  // 好客户端写入，坏消息之后正常广播
  const [bytes] = localEdit((t) => t.insert(0, 'healthy'), 8001);
  victim.send(updateMsg(bytes, 'healthy-nonce-000001', 8001));
  await victim.waitAck('healthy-nonce-000001');

  // 坏客户端也仍能收到好客户端的更新
  const got = await bad.wait((m) => m.type === 'update' && m.originNonce === 'healthy-nonce-000001');
  assert.ok(got.update);
  await victim.close(); await bad.close();
});
