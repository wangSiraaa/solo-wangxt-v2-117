// T5 崩溃窗口：update 已落库但 ack/广播还没送达时进程硬退出，
// 客户端重连同一条 nonce：服务端返回 duplicated=true，不产生第二条日志，
// 文档状态收敛到与持久层一致（字节级），验证“重连收敛”而非暂时相同。
import { test, before } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import pg from 'pg';
import { config } from '../src/config.mjs';
import { CollabClient } from '../scripts/collab-client.mjs';
import { stateHash } from '../src/yjs-util.mjs';

const PORT = 8095;
const BASE = `ws://127.0.0.1:${PORT}`;
const DOC = `t5-${Date.now()}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitHealth(n = 80) {
  for (let i = 0; i < n; i++) {
    try {
      const r = await fetch(`http://127.0.0.1:${PORT}/health`);
      if (r.ok) return;
    } catch {}
    await sleep(150);
  }
  throw new Error('server never became healthy');
}

function startServer(env = {}) {
  return spawn(process.execPath, ['src/server.mjs'], {
    cwd: new URL('..', import.meta.url).pathname,
    env: { ...process.env, PORT: String(PORT), LOG_LEVEL: 'error', ...env },
    stdio: ['ignore', 'ignore', 'inherit'],
  });
}

// 会在“已提交未确认”窗口硬退出的客户端：建立连接后直接发一条原始 update，
// 不等待 ack；EXIT_AFTER_PERSIST=1 的服务端在提交后、ack 前 process.exit。
async function crashWindowWrite() {
  const { CollabClient } = await import('../scripts/collab-client.mjs');
  const alice = new CollabClient({
    url: BASE, token: 'tok-alice', docId: DOC, clientId: 9501, name: 'alice-crash',
    log: () => {},
  });
  await alice.connect();
  await alice.waitSynced();
  // 直接在 doc 上编辑：客户端已捕获 nonce/bytes 并尝试发送，
  // 服务端提交后立刻退出，ack 永远到不了。
  alice.doc.getText('content').insert(0, 'committed-before-crash');
  const nonce = [...alice.pending.keys()].at(-1);
  assert.ok(nonce, 'client has an unacked pending update');
  return { alice, nonce };
}

before(async () => {
  const c = new pg.Client(config.pg);
  await c.connect();
  await c.query(
    "INSERT INTO documents (id, tenant_id, title) VALUES ($1,'t-acme','crash doc') ON CONFLICT DO NOTHING",
    [DOC],
  );
  await c.query(
    `INSERT INTO document_members (doc_id,user_id,role) VALUES ($1,'u-alice','owner'),($1,'u-bob','editor')
     ON CONFLICT DO NOTHING`,
    [DOC],
  );
  await c.end();
});

test('hard exit in persist→ack window: replay dedupes and clients converge', async () => {
  // —— 第一阶段：崩溃进程。提交 1 条后在发 ack 前硬退出 ——
  const srv1 = startServer({ EXIT_AFTER_PERSIST: '1', EXIT_DELAY_MS: '2' });
  await waitHealth();

  const crashWriter = await crashWindowWrite();
  const exitCode = await new Promise((resolve) => srv1.on('exit', resolve));
  crashWriter.alice.dispose(); // 第一阶段客户端不再需要；阻止它自动重连常驻
  const nonce = crashWriter.nonce;
  assert.equal(exitCode, 0, 'server exits itself in the crash window');

  // 数据库里有且仅有这 1 条，且字节就是客户端提交的内容
  const probe = new pg.Client(config.pg);
  await probe.connect();
  const { rows } = await probe.query('SELECT nonce, created_by FROM doc_updates WHERE doc_id=$1', [DOC]);
  assert.equal(rows.length, 1, 'update was committed before exit');
  assert.equal(rows[0].nonce, nonce, 'committed row is the exact update the client never got an ack for');
  await probe.end();

  // —— 第二阶段：全新进程重启 ——
  const srv2 = startServer();
  await waitHealth();

  try {
    // 用 CollabClient 真实重连：它内部对同一条未确认更新以同 nonce 自动重放。
    // （重建同一个客户端实例来复现“同一编辑者带着 pending 重连”。）
    const alice = new CollabClient({
      url: BASE, token: 'tok-alice', docId: DOC, clientId: 9501, name: 'alice-restart',
      log: () => {},
    });

    // 把崩溃前那条未确认更新注入 pending，再连接：connect() 会自动重放它
    // 从库里取回原字节，保证与“丢了 ack 的同一消息”字节一致
    const pc = new pg.Client(config.pg);
    await pc.connect();
    const r = await pc.query('SELECT update FROM doc_updates WHERE nonce=$1', [nonce]);
    await pc.end();
    const origBytes = new Uint8Array(r.rows[0].update);
    alice.nonceBytes.set(nonce, origBytes);
    alice.pending.set(nonce, origBytes);
    // 在 connect() 自动重放之前先挂好 ack 等待
    const dupPromise = alice.waitAck(nonce, 8000);

    await alice.connect();
    await alice.waitSynced();
    const dupAck = await dupPromise;
    assert.equal(dupAck.duplicated, true, 'replayed commit is acknowledged as duplicate');

    // 没有产生第二条日志
    const pc2 = new pg.Client(config.pg);
    await pc2.connect();
    const n = (await pc2.query('SELECT count(*)::int n FROM doc_updates WHERE doc_id=$1', [DOC])).rows[0].n;
    await pc2.end();
    assert.equal(n, 1, 'replay did not duplicate the committed row');

    // —— 第三阶段：另一个客户端重连，双方与持久层字节级一致 ——
    const bob = new CollabClient({
      url: BASE, token: 'tok-bob', docId: DOC, clientId: 9502, name: 'bob-restart',
      log: () => {},
    });
    await bob.connect();
    await bob.waitSynced();

    const stateRes = await fetch(`http://127.0.0.1:${PORT}/admin/docs/${DOC}/state?token=tok-alice`);
    const persisted = await stateRes.json();
    assert.equal(persisted.basedOn.updatesApplied, 1);

    assert.equal(stateHash(alice.stateBytes()), persisted.hash, 'alice == persisted state');
    assert.equal(stateHash(bob.stateBytes()), persisted.hash, 'bob == persisted state');
    assert.equal(stateHash(alice.stateBytes()), stateHash(bob.stateBytes()), 'both clients converge');
    assert.equal(alice.text, 'committed-before-crash');
    assert.equal(bob.text, 'committed-before-crash');

    // 崩溃恢复后仍可继续正常协作与收敛
    const edit = bob.transact((t) => t.insert(t.length, ' +after-restart'), 'post-crash');
    await edit.waitAck();
    await sleep(150);
    assert.equal(alice.text, 'committed-before-crash +after-restart');
    assert.equal(bob.text, alice.text);

    await alice.close();
    await bob.close();
  } finally {
    srv2.kill('SIGTERM');
    await new Promise((r) => srv2.on('exit', r));
  }
});
