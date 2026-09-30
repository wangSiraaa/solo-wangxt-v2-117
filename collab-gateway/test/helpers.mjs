// 测试辅助：建唯一测试文档、裸 WebSocket 收发、等待条件。
import WebSocket from 'ws';
import * as Y from 'yjs';
import { randomUUID } from 'node:crypto';
import { b64encode } from '../src/protocol.mjs';

export async function makeDoc(pool, id, { owners = ['u-alice'], editors = ['u-bob'], viewers = ['u-carol'] } = {}) {
  await pool.query("INSERT INTO tenants (id,name) VALUES ('t-acme','Acme') ON CONFLICT DO NOTHING");
  await pool.query(
    "INSERT INTO documents (id, tenant_id, title) VALUES ($1,'t-acme',$2) ON CONFLICT DO NOTHING",
    [id, `test ${id}`],
  );
  for (const u of owners) {
    await pool.query(
      `INSERT INTO document_members (doc_id,user_id,role) VALUES ($1,$2,'owner')
       ON CONFLICT DO NOTHING`,
      [id, u],
    );
  }
  for (const u of editors) {
    await pool.query(
      `INSERT INTO document_members (doc_id,user_id,role) VALUES ($1,$2,'editor')
       ON CONFLICT (doc_id,user_id) DO NOTHING`,
      [id, u],
    );
  }
  for (const u of viewers) {
    await pool.query(
      `INSERT INTO document_members (doc_id,user_id,role) VALUES ($1,$2,'viewer')
       ON CONFLICT (doc_id,user_id) DO NOTHING`,
      [id, u],
    );
  }
  return id;
}

export const TOKENS = {
  alice: 'tok-alice', bob: 'tok-bob', carol: 'tok-carol', dave: 'tok-dave',
};

export function openRaw(baseUrl, token, docId) {
  const ws = new WebSocket(`${baseUrl}/docs/${encodeURIComponent(docId)}/ws?token=${encodeURIComponent(token)}`);
  const messages = [];
  const waiters = [];
  ws.on('message', (d) => {
    const m = JSON.parse(d.toString('utf8'));
    messages.push(m);
    for (let i = waiters.length - 1; i >= 0; i--) {
      if (waiters[i].pred(m)) { const w = waiters.splice(i, 1)[0]; w.resolve(m); }
    }
  });
  const api = {
    ws, messages,
    send: (obj) => ws.send(JSON.stringify(obj)),
    opened: () => new Promise((res, rej) => {
      ws.once('open', res); ws.once('error', rej);
    }),
    closed: () => new Promise((res) => ws.once('close', (code, reason) => res({ code, reason: reason.toString() }))),
    // 等到满足条件的消息（含历史消息）
    wait: (pred, timeoutMs = 4000) => {
      const hit = messages.find(pred);
      if (hit) return Promise.resolve(hit);
      return new Promise((resolve, reject) => {
        const t = setTimeout(() => reject(new Error(`wait timeout ${pred.name || 'msg'}`)), timeoutMs);
        waiters.push({ pred, resolve: (m) => { clearTimeout(t); resolve(m); } });
      });
    },
    waitAck: (nonce, timeoutMs = 4000) =>
      api.wait((m) => m.type === 'ack' && m.nonce === nonce, timeoutMs),
    // 只等待“调用之后”到达的消息（避免命中历史消息）
    waitNext: (pred, timeoutMs = 4000) => new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('waitNext timeout')), timeoutMs);
      const onMsg = (m) => {
        if (pred(m)) { clearTimeout(t); ws.off('message', wrapped); resolve(m); }
      };
      const wrapped = (d) => {
        try { onMsg(JSON.parse(d.toString('utf8'))); } catch {}
      };
      ws.on('message', wrapped);
    }),
    close: () => new Promise((res) => { ws.once('close', res); ws.close(); }),
  };
  return api;
}

// 在离线 Y.Doc 上做编辑，返回 { bytes } 列表（不经过网络）
export function localEdit(fn, clientId) {
  const doc = new Y.Doc();
  if (clientId != null) doc.clientID = clientId;
  const captured = [];
  doc.on('update', (u) => captured.push(u));
  doc.transact(() => fn(doc.getText('content')), 'local');
  return captured;
}

export function syncMsg(sv = new Uint8Array(0), nonce = randomUUID()) {
  return { v: 1, type: 'sync', sv: b64encode(sv), nonce };
}

export function updateMsg(bytes, nonce = randomUUID(), clientId = 1) {
  return { v: 1, type: 'update', nonce, update: b64encode(bytes), clientId };
}

export async function waitUntil(pred, { timeout = 5000, interval = 30, label = 'condition' } = {}) {
  const deadline = Date.now() + timeout;
  let lastErr;
  while (Date.now() < deadline) {
    try { if (await pred()) return; } catch (e) { lastErr = e; }
    await new Promise((r) => setTimeout(r, interval));
  }
  throw new Error(`waitUntil timed out: ${label}${lastErr ? ` (${lastErr.message})` : ''}`);
}

export async function countUpdates(pool, docId) {
  const { rows } = await pool.query('SELECT count(*)::int n FROM doc_updates WHERE doc_id=$1', [docId]);
  return rows[0].n;
}

export async function listErrors(pool, docId) {
  const { rows } = await pool.query('SELECT kind, nonce, detail FROM doc_errors WHERE doc_id=$1 ORDER BY id', [docId]);
  return rows;
}
