#!/usr/bin/env node
// 本地免安装 PostgreSQL 控制：init | start | stop | status
// 仅用于开发/测试环境（生产使用外部 PG 实例，由 DATABASE_URL 指向）。
import { spawn, spawnSync } from 'node:child_process';
import { mkdirSync, existsSync, writeFileSync, openSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const PKG = path.join(root, 'node_modules/@embedded-postgres/linux-arm64/native');
const BIN = path.join(PKG, 'bin');
const LIB = path.join(PKG, 'lib');
const PGDATA = process.env.PGDATA || path.join(root, 'data/pgdata');
const PORT = Number(process.env.PGPORT || 55432);
const SOCKET_DIR = process.env.PGSOCKET || `/tmp/collab-pg-sock-${process.getuid?.() ?? 1000}`;

function env() {
  return {
    ...process.env,
    LD_LIBRARY_PATH: LIB + (process.env.LD_LIBRARY_PATH ? ':' + process.env.LD_LIBRARY_PATH : ''),
  };
}

function run(bin, args) {
  return spawnSync(path.join(BIN, bin), args, { env: env(), encoding: 'utf8' });
}

function init() {
  if (existsSync(path.join(PGDATA, 'PG_VERSION'))) {
    console.log(`[pg] data dir already initialized: ${PGDATA}`);
    return;
  }
  mkdirSync(PGDATA, { recursive: true });
  console.log(`[pg] initdb -> ${PGDATA}`);
  const r = run('initdb', ['-D', PGDATA, '--auth=trust', '--username=postgres', '-E', 'UTF8']);
  if (r.status !== 0) {
    console.error(r.stdout);
    console.error(r.stderr);
    process.exit(r.status ?? 1);
  }
  // unix socket 放在数据目录下，避免 /var/run 无权限
  mkdirSync(SOCKET_DIR, { recursive: true });
  const conf = `
# --- local dev overrides ---
port = ${PORT}
unix_socket_directories = '${SOCKET_DIR}'
listen_addresses = 'localhost'
max_connections = 50
shared_buffers = 16MB
fsync = on
synchronous_commit = on
`;
  writeFileSync(path.join(PGDATA, 'postgresql.conf'), conf, { flag: 'a' });
  console.log('[pg] initdb done');
}

function sleep(ms) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

async function start() {
  if (!existsSync(path.join(PGDATA, 'PG_VERSION'))) init();
  const logFile = path.join(PGDATA, 'server.log');
  const out = openSync(logFile, 'a');
  const child = spawn(path.join(BIN, 'postgres'), ['-D', PGDATA], {
    env: env(),
    stdio: ['ignore', out, out],
    detached: true,
  });
  child.unref();
  // 等就绪（直接用 TCP 连接探测）
  const net = await import('node:net');
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    const ok = await new Promise((resolve) => {
      const s = net.connect({ host: 'localhost', port: PORT }, () => { s.end(); resolve(true); });
      s.on('error', () => resolve(false));
    });
    if (ok) {
      console.log(`[pg] ready on localhost:${PORT} (socket ${SOCKET_DIR})`);
      return;
    }
    sleep(200);
  }
  console.error('[pg] did not become ready in 15s; see', logFile);
  process.exit(1);
}

function stop() {
  const r = run('pg_ctl', ['-D', PGDATA, '-m', 'fast', 'stop']);
  process.stdout.write(r.stdout || '');
  process.stderr.write(r.stderr || '');
  if (r.status !== 0) process.exit(r.status ?? 1);
  console.log('[pg] stopped');
}

function status() {
  const r = run('pg_isready', ['-h', 'localhost', '-p', String(PORT), '-U', 'postgres']);
  console.log((r.stdout || '').trim() || `pg_isready exit=${r.status}`);
  process.exit(r.status ?? 0);
}

const cmd = process.argv[2];
const cmds = { init, start, stop, status };
if (!cmds[cmd]) {
  console.error('usage: pg-local.mjs init|start|stop|status');
  process.exit(2);
}
await cmds[cmd]();
