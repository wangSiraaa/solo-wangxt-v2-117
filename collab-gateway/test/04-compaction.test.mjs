// T4 压缩：快照 + 剩余日志恢复出完全相同的文档（字节级），
// 压缩接口在压缩前后做哈希校验；压缩与在线写入并发不丢更新。
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { buildServer } from '../src/server.mjs';
import { createPool, loadDocState, getDocStats } from '../src/repo.mjs';
import { buildDoc, stateHash, docStateBytes, docText } from '../src/yjs-util.mjs';
import { config } from '../src/config.mjs';
import { openRaw, makeDoc, updateMsg, localEdit, TOKENS, countUpdates } from './helpers.mjs';

let app, pool, baseUrl, httpPort;
const DOC = `t4-${Date.now()}`;

async function canonicalHash(docId) {
  const { snapshot, updates } = await loadDocState(pool, docId);
  const doc = buildDoc(snapshot?.bytes ?? null, updates);
  return { hash: stateHash(docStateBytes(doc)), text: docText(doc), snapshot, updates };
}

async function compact(docId, token = TOKENS.alice) {
  const r = await fetch(`http://127.0.0.1:${httpPort}/admin/docs/${docId}/compact?token=${token}`, { method: 'POST' });
  return { status: r.status, body: await r.json() };
}

before(async () => {
  pool = createPool(config.pg);
  app = await buildServer({ pool });
  await app.listen({ host: '127.0.0.1', port: 0 });
  httpPort = app.server.address().port;
  baseUrl = `ws://127.0.0.1:${httpPort}`;
  await makeDoc(pool, DOC);
});
after(async () => { await app.close(); });

test('snapshot + tail replays to identical bytes after log truncation', async () => {
  const c = openRaw(baseUrl, TOKENS.alice, DOC);
  await c.opened(); await c.wait((m) => m.type === 'hello');

  const nonces = [];
  for (let i = 0; i < 8; i++) {
    const [bytes] = localEdit((t) => t.insert(t.length, `seg${i}-`), 9000 + i);
    const nonce = `compact-pre-${i}-0000001`;
    c.send(updateMsg(bytes, nonce, 9000 + i));
    nonces.push(nonce);
  }
  for (const n of nonces) await c.waitAck(n);

  const before = await canonicalHash(DOC);
  assert.match(before.text, /seg7-/);

  const res = await compact(DOC);
  assert.equal(res.status, 200, JSON.stringify(res.body));
  assert.ok(res.body.compacted);
  assert.equal(res.body.hash, before.hash, 'compaction endpoint verifies content hash');

  const stats = await getDocStats(pool, DOC);
  assert.equal(stats.updates.n, 0, 'merged updates deleted from log');
  assert.ok(stats.snapshots >= 1);

  // 关键：日志已压缩删除，仅靠快照仍恢复出相同字节
  const after = await canonicalHash(DOC);
  assert.equal(after.hash, before.hash, 'document fully recoverable from snapshot alone');
  assert.equal(after.text, before.text);
  assert.equal(after.snapshot.id, res.body.snapshotId);
  assert.equal(after.updates.length, 0);

  await c.close();
});

test('edits after compaction layer on snapshot; second compaction still recovers', async () => {
  const c = openRaw(baseUrl, TOKENS.bob, DOC);
  await c.opened(); await c.wait((m) => m.type === 'hello');
  c.send({ v: 1, type: 'sync', sv: '', nonce: 'full-1' });
  await c.wait((m) => m.type === 'sync');

  const nonces = [];
  for (let i = 0; i < 4; i++) {
    const [bytes] = localEdit((t) => t.insert(t.length, `post${i}-`), 9100 + i);
    const nonce = `compact-post-${i}-000001`;
    c.send(updateMsg(bytes, nonce, 9100 + i));
    nonces.push(nonce);
  }
  for (const n of nonces) await c.waitAck(n);

  const before = await canonicalHash(DOC);
  const res = await compact(DOC);
  assert.equal(res.status, 200, JSON.stringify(res.body));
  assert.equal(res.body.hash, before.hash);
  const after = await canonicalHash(DOC);
  assert.equal(after.hash, before.hash);
  assert.match(after.text, /post3-/);
  assert.match(after.text, /seg0-/);
  assert.equal(await countUpdates(pool, DOC), 0);
  await c.close();
});

test('compaction concurrent with live edits loses nothing', async () => {
  const d = `t4c-${Date.now()}`;
  await makeDoc(pool, d);
  const c = openRaw(baseUrl, TOKENS.alice, d);
  await c.opened(); await c.wait((m) => m.type === 'hello');

  // 先写一批（确保快照非空）
  for (let i = 0; i < 5; i++) {
    const [bytes] = localEdit((t) => t.insert(t.length, `a${i}`), 9200 + i);
    c.send(updateMsg(bytes, `cc-a-${i}-0000000001`, 9200 + i));
  }
  await Promise.all(Array.from({ length: 5 }, (_, i) => c.waitAck(`cc-a-${i}-0000000001`)));

  const beforeLive = new Set();
  // 压缩的同时继续写入（制造“快照边界之后的尾巴”）
  const livePushes = [];
  for (let i = 0; i < 6; i++) {
    const [bytes] = localEdit((t) => t.insert(t.length, `b${i}`), 9300 + i);
    const nonce = `cc-b-${i}-0000000001`;
    beforeLive.add(nonce);
    livePushes.push(new Promise((resolve) => {
      setTimeout(() => { c.send(updateMsg(bytes, nonce, 9300 + i)); resolve(); }, i * 8);
    }));
  }
  const compactPromise = compact(d);
  await Promise.all(livePushes);
  const res = await compactPromise;
  // 并发压缩可能成功或基于当时状态合并；无论哪种，最终恢复必须包含所有 b 段
  await Promise.all([...beforeLive].map((n) => c.waitAck(n)));

  const { hash: h1, text } = await canonicalHash(d);
  for (let i = 0; i < 6; i++) assert.ok(text.includes(`b${i}`), `b${i} survived concurrent compaction`);

  // 再压一次（把尾巴也吃掉），恢复哈希必须不变
  const res2 = await compact(d);
  assert.equal(res2.status, 200, JSON.stringify(res2.body));
  const { hash: h2 } = await canonicalHash(d);
  assert.equal(h2, h1, 'final state identical after second compaction');
  await c.close();
});

test('non-owner cannot compact', async () => {
  const r = await compact(DOC, TOKENS.bob); // bob 是 editor，不是 owner
  assert.equal(r.status, 403);
});
