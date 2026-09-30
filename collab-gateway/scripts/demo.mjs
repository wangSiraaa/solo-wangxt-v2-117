#!/usr/bin/env node
// 并发编辑演示：两个脚本客户端（Alice/Bob）在同一文档上交错执行
// 插入与删除（谁也不是“最后一次保存覆盖”），观察 CRDT 收敛。
//
// 前置：npm run pg:start && npm run db:setup && 另一个终端 npm run server
// 或直接 npm run demo（本脚本会自动起一个临时网关）。
import { CollabClient } from './collab-client.mjs';
import { stateHash } from '../src/yjs-util.mjs';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(here, '..');
const GATEWAY = process.env.GATEWAY_URL || 'ws://127.0.0.1:8080';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitHealth(url0, n = 50) {
  for (let i = 0; i < n; i++) {
    try {
      const r = await fetch(url0 + '/health');
      if (r.ok) return;
    } catch {}
    await sleep(200);
  }
  throw new Error('server not healthy');
}

let spawnedServer = null;
async function ensureServer() {
  try { await waitHealth(GATEWAY.replace('ws:', 'http:'), 2); return; } catch {}
  console.log('[demo] starting temporary gateway...');
  spawnedServer = spawn(process.execPath, [path.join(root, 'src/server.mjs')], {
    cwd: root,
    env: { ...process.env, PORT: '8080', LOG_LEVEL: 'warn' },
    stdio: 'inherit',
  });
  await waitHealth(GATEWAY.replace('ws:', 'http:'));
}

const log = (m) => console.log(m);

const alice = new CollabClient({ url: GATEWAY, token: 'tok-alice', docId: 'doc-note', clientId: 1001, name: 'alice', log });
const bob   = new CollabClient({ url: GATEWAY, token: 'tok-bob',   docId: 'doc-note', clientId: 2002, name: 'bob',   log });

await ensureServer();

// 清空演示文档：用一个临时客户端把它改回基线做不到（CRDT 无法重置），
// 因此演示在固定种子文档 doc-note 上追加；为可重复运行，使用 demo 专用文档。
// （db-setup 里没有该文档，这里直接在 note 上进行；重复运行会累积，属正常 CRDT 行为。）

await alice.connect();
await bob.connect();
await Promise.all([alice.waitSynced(), bob.waitSynced()]);
console.log('\n--- baseline ---');
console.log('alice:', JSON.stringify(alice.text));
console.log('bob  :', JSON.stringify(bob.text));

// 并发交错：两人在不同位置同时插入；Bob 还删除一段；两人的事务时间窗重叠
const ops = [];
ops.push(alice.transact((t) => t.insert(0, 'AA1 '), 'alice@0').waitAck().catch((e) => `alice1 ${e.message}`));
await sleep(15);
ops.push(bob.transact((t) => t.insert(t.length, 'BB1 '), 'bob@end').waitAck().catch((e) => `bob1 ${e.message}`));
ops.push(alice.transact((t) => t.insert(3, 'AA2 '), 'alice@3').waitAck().catch((e) => `alice2 ${e.message}`));
await sleep(15);
// Bob 在自己看到的位置删除；CRDT 会自动修正索引冲突
ops.push(bob.transact((t) => t.delete(0, Math.min(4, t.length)), 'bob-del@0').waitAck().catch((e) => `bob2 ${e.message}`));
ops.push(alice.transact((t) => t.insert(t.length, 'AA3 '), 'alice@end').waitAck().catch((e) => `alice3 ${e.message}`));
await Promise.all(ops);

// 等传播
await sleep(300);

console.log('\n--- after concurrent edits ---');
console.log('alice:', JSON.stringify(alice.text));
console.log('bob  :', JSON.stringify(bob.text));
const ha = stateHash(alice.stateBytes());
const hb = stateHash(bob.stateBytes());
console.log(`state hash alice=${ha} bob=${hb} => ${ha === hb ? 'CONVERGED ✓' : 'DIVERGED ✗'}`);

// 从服务端持久层独立恢复一份，验证不是“两端暂时显示相同”
const res = await fetch(`http://127.0.0.1:${new URL(GATEWAY).port}/admin/docs/doc-note/state?token=tok-alice`);
const persisted = await res.json();
console.log('\n--- rebuild from PostgreSQL (snapshot + update log) ---');
console.log('persisted hash:', persisted.hash, persisted.hash === ha ? '== live ✓' : '!= live ✗');
console.log('rebuilt text  :', JSON.stringify(persisted.text));
console.log('basedOn       :', JSON.stringify(persisted.basedOn));

await alice.close();
await bob.close();
if (spawnedServer) spawnedServer.kill('SIGTERM');
process.exit(ha === hb && persisted.hash === ha ? 0 : 1);
