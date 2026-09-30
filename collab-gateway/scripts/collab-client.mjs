// 脚本用协作客户端：无浏览器，ws + Yjs。
// 行为要点：
//   - 每条本地更新带全局唯一 nonce；未收到 ack 前重连同一条（幂等重放）；
//   - 收到 sync/update 用 Yjs 标准方式应用，不做字符串拼接；
//   - 断线指数退避重连，重连后发自己的状态向量做差异补齐；
//   - 不信任服务端以外的房间信息，URL 即房间。
import WebSocket from 'ws';
import * as Y from 'yjs';
import { randomUUID } from 'node:crypto';
import { b64encode, b64decode } from '../src/protocol.mjs';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export class CollabClient {
  constructor({ url, token, docId, clientId, name, log = () => {} }) {
    this.url = `${url.replace(/\/$/, '')}/docs/${encodeURIComponent(docId)}/ws?token=${encodeURIComponent(token)}`;
    this.docId = docId;
    this.name = name;
    this.log = log;
    this.doc = new Y.Doc();
    if (Number.isInteger(clientId)) this.doc.clientID = clientId;
    this.ws = null;
    this.connected = false;
    this.closed = false;
    this.acks = new Map();          // nonce -> {resolve, reject, timer}
    this.pending = new Map();       // nonce -> bytes  （已发未 ack）
    this.nonceBytes = new Map();    // nonce -> bytes（供重连同字节重放）
    this.reconnectDelay = 50;
    this.maxReconnectDelay = 2000;
    this.reconnectAttempts = 0;
    this.updateHandler = null;
    this.syncedResolvers = [];
    this.syncedOnce = false;
    this.hello = null;
    this.errors = [];

    // 本地编辑 -> 抓 update（一个 Yjs 事务产生一条 update 消息）。
    // 只忽略“来自服务端”的应用；本地事务（origin === this 或用户自定义 origin）必须捕获。
    this.doc.on('update', (u, origin) => {
      if (origin === 'remote' || origin === 'sync' || origin === 'snapshot'
          || origin === 'replay' || origin === 'reconnect' || origin === 'server') return;
      this._capture(u, origin);
    });
  }

  _capture(updateBytes, origin) {
    if (!this.connected) {
      // 离线编辑也会产生 update；先缓存，连接建立后按 nonce 发送
    }
    const nonce = randomUUID();
    this.nonceBytes.set(nonce, updateBytes);
    this.pending.set(nonce, updateBytes);
    this.log?.(`[${this.name}] local update nonce=${nonce.slice(0, 8)} bytes=${updateBytes.length} origin=${origin ?? 'txn'}`);
    if (this.connected) this._sendUpdate(nonce, updateBytes);
  }

  async connect() {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(this.url);
      this.ws = ws;
      const onOpen = () => {
        ws.off('error', onErr);
        this.connected = true;
        this.reconnectAttempts = 0;
        this.reconnectDelay = 50;
        this.log?.(`[${this.name}] connected ${this.url.split('?')[0]}`);
        // 连接建立：发状态向量，服务端回差异
        this._sendSync();
        // 重放未确认更新（同一 nonce、同字节）
        for (const [nonce, bytes] of this.pending) this._sendUpdate(nonce, bytes);
        resolve();
      };
      const onErr = (err) => { ws.off('open', onOpen); reject(err); };
      ws.once('open', onOpen);
      ws.once('error', onErr);

      ws.on('message', (data) => this._onMessage(data));
      ws.on('close', (code, reason) => {
        this.connected = false;
        this.hello = null;
        this.log?.(`[${this.name}] closed code=${code} reason=${reason || ''}`);
        if (!this.closed) this._scheduleReconnect();
      });
      ws.on('error', () => { /* close 会接管；首次握手错误已在 onErr */ });
    });
  }

  _scheduleReconnect() {
    if (this.closed) return;
    this.reconnectAttempts += 1;
    const delay = Math.min(this.maxReconnectDelay, this.reconnectDelay * 2 ** Math.min(this.reconnectAttempts, 8));
    this.log?.(`[${this.name}] reconnect in ${delay}ms (attempt ${this.reconnectAttempts})`);
    sleep(delay).then(() => {
      if (!this.closed && !this.connected) {
        this.connect().catch((e) => this.log?.(`[${this.name}] reconnect failed: ${e.message}`));
      }
    });
  }

  _sendSync() {
    const sv = Y.encodeStateVector(this.doc);
    this.wsSend({ v: 1, type: 'sync', sv: b64encode(sv), nonce: randomUUID() });
  }

  _sendUpdate(nonce, bytes) {
    this.wsSend({ v: 1, type: 'update', nonce, update: b64encode(bytes), clientId: this.doc.clientID });
    // ack 超时（仅诊断；重连才是真正的重发机制）。pending 保留到 ack。
  }

  wsSend(obj) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(obj));
  }

  _onMessage(data) {
    let msg;
    try { msg = JSON.parse(data.toString('utf8')); } catch { return; }
    switch (msg.type) {
      case 'hello':
        this.hello = msg;
        break;
      case 'sync':
        try {
          // 每次 sync 响应都要应用：首次连接和每次重连的差异补齐走的是同一路径
          Y.applyUpdate(this.doc, b64decode(msg.diff), 'sync');
          if (!this.syncedOnce) {
            this.syncedOnce = true;
            const rs = this.syncedResolvers.splice(0);
            rs.forEach((r) => r());
          }
        } catch (e) {
          this.errors.push({ where: 'sync', e: e.message });
        }
        break;
      case 'update':
        try {
          Y.applyUpdate(this.doc, b64decode(msg.update), 'remote');
        } catch (e) {
          this.errors.push({ where: 'update', e: e.message });
        }
        break;
      case 'ack': {
        // 幂等确认：重复 ack / duplicated=true 都安全
        this.pending.delete(msg.nonce);
        this.nonceBytes.delete(msg.nonce);
        const w = this.acks.get(msg.nonce);
        if (w) { this.acks.delete(msg.nonce); w.resolve(msg); }
        this.log?.(`[${this.name}] ack nonce=${String(msg.nonce).slice(0, 8)} dup=${msg.duplicated} id=${msg.updateId}`);
        break;
      }
      case 'pong': break;
      case 'error':
        this.errors.push({ where: 'server', code: msg.code, message: msg.message, nonce: msg.nonce, errorId: msg.errorId });
        this.log?.(`[${this.name}] SERVER-ERROR ${msg.code}: ${msg.message}${msg.errorId ? ` (errorId=${msg.errorId})` : ''}`);
        break;
    }
  }

  // 释放：终止底层连接且不再重连（区别于优雅 close 的可重连语义）
  dispose() {
    this.closed = true;
    if (this.ws) this.ws.terminate();
  }

  // 测试/断网模拟：直接切断底层 TCP 并走与真实断线相同的重连路径
  forceDisconnect() {
    this.connected = false;
    this.hello = null;
    this.ws.terminate(); // 原有 close 监听器会触发 _scheduleReconnect
  }

  // 在 Y.Doc 上做一批编辑（一个事务），返回 nonce，可等待 ack
  transact(fn, label) {
    const before = new Set(this.pending.keys());
    this.doc.transact(() => fn(this.doc.getText('content'), this.doc), this);
    let nonce = null;
    for (const k of this.pending.keys()) if (!before.has(k)) nonce = k;
    this.log?.(`[${this.name}] edit "${label ?? ''}" nonce=${nonce?.slice(0, 8)}`);
    return { nonce, waitAck: (timeoutMs = 4000) => this.waitAck(nonce, timeoutMs) };
  }

  waitAck(nonce, timeoutMs = 4000) {
    if (!nonce) return Promise.resolve(null);
    if (!this.pending.has(nonce)) return Promise.resolve({ nonce, cached: true });
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.acks.delete(nonce);
        reject(new Error(`ack timeout nonce=${nonce.slice(0, 8)}`));
      }, timeoutMs);
      this.acks.set(nonce, {
        resolve: (m) => { clearTimeout(timer); resolve(m); },
        reject: (m) => { clearTimeout(timer); reject(m); },
      });
    });
  }

  // 等首次同步完成（服务端差异已应用）
  waitSynced(timeoutMs = 4000) {
    if (this.syncedOnce) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('sync timeout')), timeoutMs);
      this.syncedResolvers.push(() => { clearTimeout(timer); resolve(); });
    });
  }

  get text() { return this.doc.getText('content').toString(); }
  get length() { return this.doc.getText('content').length; }
  stateBytes() { return Y.encodeStateAsUpdate(this.doc); }
  stateVector() { return Y.encodeStateVector(this.doc); }

  async close() {
    this.closed = true;
    if (!this.ws) return;
    if (this.ws.readyState !== WebSocket.CLOSED) {
      await new Promise((resolve) => {
        this.ws.once('close', resolve);
        this.ws.close(1000, 'bye');
      });
    }
  }
}
