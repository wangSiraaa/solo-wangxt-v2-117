// T6 端到端（两个脚本客户端）：
//  - 并发插入/删除后字节级收敛，且与 PG 恢复一致；
//  - 一个客户端断线期间，另一个持续编辑；断线方重连按状态向量补齐并收敛；
//  - 不是“暂时显示相同字符串”：比较 Yjs 状态字节哈希。
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { buildServer } from '../src/server.mjs';
import { createPool } from '../src/repo.mjs';
import { stateHash } from '../src/yjs-util.mjs';
import { config } from '../src/config.mjs';
import { CollabClient } from '../scripts/collab-client.mjs';

let app, pool, port;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

before(async () => {
  pool = createPool(config.pg);
  app = await buildServer({ pool });
  await app.listen({ host: '127.0.0.1', port: 0 });
  port = app.server.address().port;
  await pool.query(
    "INSERT INTO documents (id, tenant_id, title) VALUES ($1,'t-acme','t6') ON CONFLICT DO NOTHING",
    [`t6-${Date.now()}`],
  );
});
after(async () => { await app.close(); });

function mk(name, token, docId, cid) {
  return new CollabClient({
    url: `ws://127.0.0.1:${port}`, token, docId, clientId: cid, name, log: () => {},
  });
}

test('concurrent inserts/deletes converge and match PostgreSQL rebuild', async () => {
  const docId = `t6a-${Date.now()}`;
  await pool.query("INSERT INTO documents (id,tenant_id,title) VALUES ($1,'t-acme','t6a') ON CONFLICT DO NOTHING", [docId]);
  await pool.query(
    "INSERT INTO document_members (doc_id,user_id,role) VALUES ($1,'u-alice','editor'),($1,'u-bob','editor') ON CONFLICT DO NOTHING",
    [docId],
  );

  const alice = mk('alice', 'tok-alice', docId, 6101);
  const bob = mk('bob', 'tok-bob', docId, 6202);
  await alice.connect(); await bob.connect();
  await Promise.all([alice.waitSynced(), bob.waitSynced()]);

  // 交错并发：插入 + 删除混合（删除针对各自视图中的位置，CRDT 负责消解）
  const edits = [
    () => alice.transact((t) => t.insert(0, 'AAAA'), 'a1').waitAck(),
    () => bob.transact((t) => t.insert(t.length, 'BBBB'), 'b1').waitAck(),
    () => alice.transact((t) => t.insert(2, 'aa'), 'a2').waitAck(),
    () => bob.transact((t) => t.delete(0, Math.min(2, t.length)), 'b-del').waitAck(),
    () => bob.transact((t) => t.insert(1, 'bb'), 'b2').waitAck(),
    () => alice.transact((t) => t.delete(t.length - 1, 1), 'a-del').waitAck(),
    () => alice.transact((t) => t.insert(t.length, 'ZZ'), 'a3').waitAck(),
    () => bob.transact((t) => t.insert(0, 'QQ'), 'b3').waitAck(),
  ];
  // 两两并发，制造时间窗重叠
  for (let i = 0; i < edits.length; i += 2) {
    await Promise.all([edits[i](), edits[i + 1]()]);
  }
  await sleep(200);

  const ha = stateHash(alice.stateBytes());
  const hb = stateHash(bob.stateBytes());
  assert.equal(ha, hb, 'live clients converge byte-for-byte');

  const res = await fetch(`http://127.0.0.1:${port}/admin/docs/${docId}/state?token=tok-alice`);
  const persisted = await res.json();
  assert.equal(persisted.hash, ha, 'persisted rebuild equals live state');
  // 删除确实发生过（长度不可能等于所有插入之和）
  assert.ok(persisted.length < 4 + 4 + 2 + 2 + 2 + 2);

  await alice.close(); await bob.close();
});

test('disconnected client misses edits; reconnect state-vector sync converges', async () => {
  const docId = `t6b-${Date.now()}`;
  await pool.query("INSERT INTO documents (id,tenant_id,title) VALUES ($1,'t-acme','t6b') ON CONFLICT DO NOTHING", [docId]);
  await pool.query(
    "INSERT INTO document_members (doc_id,user_id,role) VALUES ($1,'u-alice','editor'),($1,'u-bob','editor') ON CONFLICT DO NOTHING",
    [docId],
  );

  const alice = mk('alice', 'tok-alice', docId, 6301);
  const bob = mk('bob', 'tok-bob', docId, 6302);
  await alice.connect(); await bob.connect();
  await Promise.all([alice.waitSynced(), bob.waitSynced()]);

  const base = alice.transact((t) => t.insert(0, 'BASE-'), 'seed').waitAck();
  await base;
  await sleep(100);
  const hashAtBase = stateHash(bob.stateBytes());
  assert.equal(stateHash(alice.stateBytes()), hashAtBase);

  // 强制断开 alice 的底层 TCP（非优雅关闭）；客户端会走真实的 close→重连路径
  alice.forceDisconnect();

  // alice 断线期间 bob 连续编辑（含插入和删除）
  for (let i = 0; i < 5; i++) {
    await bob.transact((t) => t.insert(t.length, `offline${i}-`), `o${i}`).waitAck();
  }
  await bob.transact((t) => t.delete(0, 2), 'offline-del').waitAck();
  await sleep(50);

  // 等 alice 自动重连并补齐（重连后会发状态向量）
  const deadline = Date.now() + 8000;
  while (Date.now() < deadline) {
    if (stateHash(alice.stateBytes()) === stateHash(bob.stateBytes())) break;
    await sleep(50);
  }

  assert.equal(
    stateHash(alice.stateBytes()), stateHash(bob.stateBytes()),
    'reconnected client caught up via state-vector diff',
  );

  const res = await fetch(`http://127.0.0.1:${port}/admin/docs/${docId}/state?token=tok-alice`);
  const persisted = await res.json();
  assert.equal(persisted.hash, stateHash(alice.stateBytes()), 'both match persisted state');

  // 补齐后继续协作仍收敛
  const p1 = alice.transact((t) => t.insert(0, 'BACK-'), 'back').waitAck();
  const p2 = bob.transact((t) => t.insert(t.length, '-TAIL'), 'tail').waitAck();
  await Promise.all([p1, p2]);
  await sleep(150);
  assert.equal(stateHash(alice.stateBytes()), stateHash(bob.stateBytes()), 'converge after post-reconnect edits');
  assert.equal(alice.text, bob.text);

  await alice.close(); await bob.close();
});
