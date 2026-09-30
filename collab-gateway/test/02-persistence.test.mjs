// T2 持久化边界、幂等去重、乱序到达、状态向量补齐。
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as Y from 'yjs';
import { buildServer } from '../src/server.mjs';
import { createPool, loadDocState } from '../src/repo.mjs';
import { buildDoc, stateHash, docStateBytes, docText } from '../src/yjs-util.mjs';
import { config } from '../src/config.mjs';
import { openRaw, makeDoc, syncMsg, updateMsg, localEdit, TOKENS, countUpdates } from './helpers.mjs';

let app, pool, baseUrl;
const DOC = `t2-${Date.now()}`;

before(async () => {
  pool = createPool(config.pg);
  app = await buildServer({ pool });
  await app.listen({ host: '127.0.0.1', port: 0 });
  baseUrl = `ws://127.0.0.1:${app.server.address().port}`;
  await makeDoc(pool, DOC);
});
after(async () => { await app.close(); });

test('ack only after commit; row exists before ack is delivered', async () => {
  const c = openRaw(baseUrl, TOKENS.alice, DOC);
  await c.opened();
  await c.wait((m) => m.type === 'hello');
  c.send(syncMsg());
  await c.wait((m) => m.type === 'sync');

  const [bytes] = localEdit((t) => t.insert(0, 'first-persisted'), 2001);
  const nonce = 'boundary-nonce-000000001';
  c.send(updateMsg(bytes, nonce, 2001));
  const ack = await c.waitAck(nonce);

  // ack 里的 updateId 必须能在数据库里查到（即“先持久化后确认”）
  const { rows } = await pool.query('SELECT nonce, update FROM doc_updates WHERE id=$1', [ack.updateId]);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].nonce, nonce);
  assert.deepEqual(new Uint8Array(rows[0].update), bytes);
  await c.close();
});

test('duplicate update with same nonce is stored once and acked duplicated=true', async () => {
  const c = openRaw(baseUrl, TOKENS.bob, DOC);
  await c.opened();
  await c.wait((m) => m.type === 'hello');
  const [bytes] = localEdit((t) => t.insert(0, 'dedup'), 2002);
  const nonce = 'dedup-nonce-00000000001';

  c.send(updateMsg(bytes, nonce, 2002));
  const ack1 = await c.waitAck(nonce);
  assert.equal(ack1.duplicated, false);

  // 同字节 + 同 nonce 重发 3 次（模拟重连重放/重复投递）
  c.send(updateMsg(bytes, nonce, 2002));
  c.send(updateMsg(bytes, nonce, 2002));
  c.send(updateMsg(bytes, nonce, 9999)); // clientId 也改了，仍按 nonce 去重
  const acks = [];
  for (let i = 0; i < 3; i++) {
    acks.push(await c.wait((m) => m.type === 'ack' && m.nonce === nonce && m.duplicated === true));
  }
  assert.equal(acks.length, 3);

  const n = await countUpdates(pool, DOC);
  // 前面 boundary 测试插了 1 条，本条只应有 1 条
  const { rows } = await pool.query(
    'SELECT id, client_id FROM doc_updates WHERE nonce=$1 AND doc_id=$2',
    [nonce, DOC],
  );
  assert.equal(rows.length, 1, 'same nonce must produce exactly one row in this doc');
  assert.equal(Number(rows[0].client_id), 2002, 'first write wins; later metadata ignored');
  assert.equal(n, 2);
  await c.close();
});

test('different users sharing a nonce scope do not cross-dedupe (nonce is per doc)', async () => {
  const other = `t2b-${Date.now()}`;
  await makeDoc(pool, other);
  const c1 = openRaw(baseUrl, TOKENS.alice, DOC);
  const c2 = openRaw(baseUrl, TOKENS.alice, other);
  await Promise.all([c1.opened(), c2.opened()]);
  await Promise.all([c1.wait((m) => m.type === 'hello'), c2.wait((m) => m.type === 'hello')]);
  const [b1] = localEdit((t) => t.insert(0, 'A'), 2003);
  const [b2] = localEdit((t) => t.insert(0, 'B'), 2004);
  const shared = 'shared-nonce-0000000001';
  c1.send(updateMsg(b1, shared, 2003));
  c2.send(updateMsg(b2, shared, 2004));
  const a1 = await c1.waitAck(shared);
  const a2 = await c2.waitAck(shared);
  assert.equal(a1.duplicated, false);
  assert.equal(a2.duplicated, false);
  assert.notEqual(a1.updateId, a2.updateId);
  await c1.close(); await c2.close();
});

test('updates arriving out of order converge (CRDT + log replay idempotent)', async () => {
  const od = `t2o-${Date.now()}`;
  await makeDoc(pool, od);

  // 在同一离线 doc 上顺序做三个编辑，得到三条增量 update
  const doc = new Y.Doc();
  doc.clientID = 4001;
  const edits = [];
  doc.on('update', (u) => edits.push(u));
  doc.transact(() => doc.getText('content').insert(0, 'HELLO'), 'a');
  doc.transact(() => doc.getText('content').insert(5, ' WORLD'), 'b');
  doc.transact(() => doc.getText('content').delete(5, 1), 'c'); // 删掉空格（其 item 此刻还未到达）
  assert.equal(edits.length, 3);
  const expected = doc.getText('content').toString();
  assert.equal(expected, 'HELLOWORLD');

  const c = openRaw(baseUrl, TOKENS.alice, od);
  await c.opened();
  await c.wait((m) => m.type === 'hello');

  // 乱序发送：c, a, b
  c.send(updateMsg(edits[2], 'ooo-c-00000000001', 4001));
  c.send(updateMsg(edits[0], 'ooo-a-00000000001', 4001));
  c.send(updateMsg(edits[1], 'ooo-b-00000000001', 4001));
  await Promise.all([
    c.waitAck('ooo-c-00000000001'),
    c.waitAck('ooo-a-00000000001'),
    c.waitAck('ooo-b-00000000001'),
  ]);
  await c.close();

  // 独立从 PG 按 id 顺序重放（服务器收到的顺序是乱的，落库顺序按到达）
  const { snapshot, updates } = await loadDocState(pool, od);
  assert.equal(updates.length, 3);
  const rebuilt = buildDoc(snapshot?.bytes ?? null, updates);
  assert.equal(docText(rebuilt), 'HELLOWORLD', 'ordered replay of out-of-order arrivals converges');

  // 乱序到达的两条并发编辑（不同 client）也应收敛
  const od2 = `t2o2-${Date.now()}`;
  await makeDoc(pool, od2);
  const [x] = localEdit((t) => t.insert(0, 'X-START'), 5001);
  const [y] = localEdit((t) => t.insert(0, 'Y-START'), 5002);
  const c2 = openRaw(baseUrl, TOKENS.alice, od2);
  await c2.opened(); await c2.wait((m) => m.type === 'hello');
  c2.send(updateMsg(y, 'ooo-y-00000000001', 5002));
  c2.send(updateMsg(x, 'ooo-x-00000000001', 5001));
  await Promise.all([c2.waitAck('ooo-y-00000000001'), c2.waitAck('ooo-x-00000000001')]);
  await c2.close();
  const r2 = await loadDocState(pool, od2);
  const rb = buildDoc(r2.snapshot?.bytes ?? null, r2.updates);
  // 两个插入都必须存在（内容融合，互不覆盖）
  assert.match(docText(rb), /X-START/);
  assert.match(docText(rb), /Y-START/);
});

test('reconnect with state vector receives only the missing diff', async () => {
  const sv = `t2s-${Date.now()}`;
  await makeDoc(pool, sv);

  // 第一轮：alice 写入两条并拿到 ack
  let c = openRaw(baseUrl, TOKENS.alice, sv);
  await c.opened(); await c.wait((m) => m.type === 'hello');
  const doc = new Y.Doc(); doc.clientID = 6001;
  const edits = [];
  doc.on('update', (u) => edits.push(u));
  doc.transact(() => doc.getText('content').insert(0, 'offline-part-1'), 'e1');
  c.send(updateMsg(edits[0], 'sv-e1-000000000001', 6001));
  await c.waitAck('sv-e1-000000000001');
  // 应用服务端广播/或直接用 sync 拿全量，模拟客户端已拥有 e1 状态
  c.send(syncMsg(Y.encodeStateVector(doc)));
  const syncEmpty = await c.wait((m) => m.type === 'sync');
  assert.equal(syncEmpty.diff, 'AAA=', 'no diff when client already up to date');
  await c.close();

  // 断线期间：bob 写入一条
  const b = openRaw(baseUrl, TOKENS.bob, sv);
  await b.opened(); await b.wait((m) => m.type === 'hello');
  const [bobEdit] = localEdit((t) => t.insert(0, 'OFFLINE-NEW-'), 6002);
  b.send(updateMsg(bobEdit, 'sv-bob-00000000001', 6002));
  await b.waitAck('sv-bob-00000000001');
  await b.close();

  // alice 重连，带着旧状态向量（只包含自己的 e1）：
  c = openRaw(baseUrl, TOKENS.alice, sv);
  await c.opened(); await c.wait((m) => m.type === 'hello');
  c.send(syncMsg(Y.encodeStateVector(doc)));
  const diffMsg = await c.wait((m) => m.type === 'sync');
  const diffBytes = new Uint8Array(Buffer.from(diffMsg.diff, 'base64'));
  // 差异必须能应用，并使本地补齐到服务端状态
  Y.applyUpdate(doc, diffBytes, 'sync');

  // 与从 PG 完整恢复的权威状态比对字节哈希，而不是只比字符串
  const { snapshot, updates } = await loadDocState(pool, sv);
  const canonical = buildDoc(snapshot?.bytes ?? null, updates);
  assert.equal(
    stateHash(docStateBytes(doc)),
    stateHash(docStateBytes(canonical)),
    'state-vector diff brings reconnecting client to exact server state',
  );
  assert.match(docText(doc), /OFFLINE-NEW-/);
  await c.close();
});
