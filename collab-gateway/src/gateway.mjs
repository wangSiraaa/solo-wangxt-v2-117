// WebSocket 网关：认证/鉴权、Yjs 同步、更新幂等持久化、坏消息隔离。
import { authenticate, authorizeDoc, insertUpdate, insertError } from './repo.mjs';
import { validateUpdate } from './yjs-util.mjs';
import * as P from './protocol.mjs';

const MAX_PAYLOAD = 1024 * 1024; // 单帧 1MB，演示足够；坏消息可定位而不是炸进程

// 每个连接的上下文。ws 挂接在 Fastify websocket handler 里。
async function handleConnection(fastify, socket, req) {
  const { pool, rooms } = fastify.collab;

  // 认证/房间加载是异步的，且单条消息处理也是异步的（查库/校验/持久化）。
  // 必须按“到达顺序”逐条串行处理，否则早期 sync 在 await 让出时，
  // 后续坏帧会插到前面被先处理（响应与消息错位）。
  // 做法：每条帧（含认证期间到达的）都进同一条 per-connection FIFO；
  // ctx 就绪前任务在队首等待，就绪后按序执行。
  let chain = Promise.resolve();
  let ctx = null;
  let readyResolve, readyReject;
  const ready = new Promise((res, rej) => { readyResolve = res; readyReject = rej; });
  ready.catch(() => {}); // 认证失败路径会 reject；此处消费，避免 unhandledRejection
  const enqueueFrame = (raw) => {
    const task = (async () => {
      let c;
      try {
        c = await ready; // ctx 就绪前排队等待；认证失败时拒绝（此时连接即将关闭）
      } catch {
        return; // 未认证/未授权：丢弃排队帧，连接已在关闭流程中
      }
      await handleMessage(fastify, c, raw);
    })();
    task.catch(() => {}); // 独立 Promise 必须先消费 rejection，避免 unhandledRejection
    // 串行化但不让单个任务的拒绝中断整条链
    chain = chain.then(() => task).catch((err) => {
      fastify.log.error({ err, docId: ctx?.docId, userId: ctx?.user.id }, 'message handler failed');
      if (ctx) safeError(ctx, P.ERROR_CODE.INTERNAL, 'internal error');
    });
  };
  socket.on('message', enqueueFrame);

  // 1) 认证：token 只来自 ?token= / Authorization，不接受消息体里的身份
  const url = new URL(req.url, 'http://localhost');
  const token = url.searchParams.get('token')
    || (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
  const user = await authenticate(pool, token);
  if (!user) {
    await insertError(pool, { kind: 'unauthorized', detail: `auth failed ${req.socket.remoteAddress}` });
    readyReject(new Error('unauthorized'));
    socket.close(P.CLOSE.UNAUTHORIZED, 'unauthorized');
    return;
  }

  // 2) 房间号只取路径参数 /docs/:docId/ws；消息体自报房间号被忽略
  const docId = req.params.docId;
  const auth = await authorizeDoc(pool, user.tenant_id, user.id, docId);
  if (!auth.ok) {
    await insertError(pool, {
      kind: 'unauthorized', docId, tenantId: user.tenant_id,
      detail: `forbidden user=${user.id} doc=${docId}`,
    });
    readyReject(new Error('forbidden'));
    socket.close(P.CLOSE.FORBIDDEN, 'forbidden');
    return;
  }

  const room = await rooms.get(user.tenant_id, docId);
  ctx = {
    socket, user, docId, tenantId: user.tenant_id, role: auth.role,
    room, closed: false,
  };
  room.add(ctx);

  socket.on('close', () => {
    ctx.closed = true;
    room.remove(ctx);
    // 最后一个人离开即卸载；下一个连接从 PG（快照+日志）重建
    if (room.empty) rooms.rooms.delete(room.key);
  });

  socket.on('error', () => { /* 对端重置等；close 事件负责清理 */ });

  // ctx 就绪：FIFO 里的帧（含认证期间到达的）自此严格按到达顺序处理。
  // 先 resolve 再发 hello：客户端收到 hello 后发出的帧天然排在早期帧之后。
  readyResolve(ctx);
  P.send(socket, { type: 'hello', docId, role: auth.role, serverId: P.serverId() });
}

async function handleMessage(fastify, ctx, raw) {
  const { pool } = fastify.collab;
  if (raw.length > MAX_PAYLOAD) {
    await logBad(ctx, 'bad_envelope', 'frame too large', raw.subarray(0, 256));
    safeError(ctx, P.ERROR_CODE.BAD_ENVELOPE, `frame exceeds ${MAX_PAYLOAD} bytes`);
    return;
  }

  let msg;
  try {
    msg = JSON.parse(raw.toString('utf8'));
  } catch {
    await logBad(ctx, 'bad_envelope', 'invalid JSON frame', raw.subarray(0, 256));
    safeError(ctx, P.ERROR_CODE.BAD_JSON, 'frame is not valid JSON');
    return;
  }
  if (!msg || typeof msg !== 'object' || msg.v !== 1 || typeof msg.type !== 'string') {
    await logBad(ctx, 'bad_envelope', 'missing v:1 or type', raw.subarray(0, 256));
    safeError(ctx, P.ERROR_CODE.BAD_ENVELOPE, 'expected {v:1,type,...}');
    return;
  }

  switch (msg.type) {
    case 'ping':
      P.send(ctx.socket, { type: 'pong', nonce: msg.nonce ?? null });
      return;

    case 'sync':
      return handleSync(ctx, msg);

    case 'update':
      return handleUpdate(fastify, ctx, msg);

    default: {
      // 未知类型：隔离落库（可定位），单条拒绝，不影响其他客户端
      const eid = await insertError(pool, {
        kind: 'unknown_type', docId: ctx.docId, tenantId: ctx.tenantId,
        nonce: typeof msg.nonce === 'string' ? msg.nonce : null,
        detail: `unknown message type=${JSON.stringify(msg.type)}`,
        payload: raw.subarray(0, 256),
      });
      safeError(ctx, P.ERROR_CODE.UNKNOWN_TYPE, `unknown type: ${msg.type}`, {
        nonce: msg.nonce ?? null, errorId: eid.id,
      });
      return;
    }
  }
}

// 重连/首连：客户端上报自己的状态向量，服务端返回差异（断线期间的提交全在里面）
async function handleSync(ctx, msg) {
  let sv = null;
  try {
    sv = msg.sv ? P.b64decode(msg.sv) : new Uint8Array(0);
  } catch {
    await insertError(ctx.room.pool, {
      kind: 'bad_envelope', docId: ctx.docId, tenantId: ctx.tenantId,
      nonce: msg.nonce ?? null, detail: 'sync: state vector not base64',
    });
    safeError(ctx, P.ERROR_CODE.BAD_STATE_VECTOR, 'state vector must be base64');
    return;
  }
  // 差异计算走房间队列：确保不会与“提交后广播”交错漏掉最新提交
  const diff = await ctx.room.enqueue(() => ctx.room.diffFor(sv));
  P.send(ctx.socket, { type: 'sync', diff: P.b64encode(diff), nonce: msg.nonce ?? null });
}

async function handleUpdate(fastify, ctx, msg) {
  const { pool, rooms } = fastify.collab;
  const nonce = msg.nonce;

  // —— 每条更新都重新鉴权：角色以服务端数据库为准（viewer 被降权后不能再写）——
  const fresh = await authorizeDoc(pool, ctx.tenantId, ctx.user.id, ctx.docId);
  if (!fresh.ok) {
    ctx.socket.close(P.CLOSE.FORBIDDEN, 'forbidden');
    return;
  }
  if (fresh.role === 'viewer') {
    await insertError(pool, {
      kind: 'unauthorized', docId: ctx.docId, tenantId: ctx.tenantId,
      nonce: String(nonce ?? null), detail: `viewer write denied user=${ctx.user.id}`,
    });
    safeError(ctx, P.ERROR_CODE.NOT_ALLOWED, 'read-only membership', { nonce: nonce ?? null });
    return;
  }

  if (typeof nonce !== 'string' || !/^[A-Za-z0-9_-]{8,128}$/.test(nonce)) {
    await insertError(pool, {
      kind: 'bad_envelope', docId: ctx.docId, tenantId: ctx.tenantId,
      detail: 'update missing valid nonce', payload: JSON.stringify(msg).slice(0, 512),
    });
    safeError(ctx, P.ERROR_CODE.BAD_ENVELOPE, 'update requires string nonce (8-128 chars)');
    return;
  }

  let bytes;
  try {
    bytes = P.b64decode(msg.update);
  } catch {
    await insertError(pool, {
      kind: 'corrupt_update', docId: ctx.docId, tenantId: ctx.tenantId, nonce,
      detail: 'update payload not base64', payload: String(msg.update).slice(0, 256),
    });
    safeError(ctx, P.ERROR_CODE.CORRUPT_UPDATE, 'update must be base64 binary', { nonce });
    return;
  }

  // —— 损坏字节在持久化之前拦截，绝不进 doc_updates ——
  try {
    validateUpdate(bytes);
  } catch (e) {
    const eid = await insertError(pool, {
      kind: 'corrupt_update', docId: ctx.docId, tenantId: ctx.tenantId, nonce,
      detail: `validateUpdate: ${e.message}`, payload: bytes.subarray(0, 256),
    });
    safeError(ctx, P.ERROR_CODE.CORRUPT_UPDATE, `update rejected: ${e.message}`,
      { nonce, errorId: eid.id });
    return;
  }

  // —— 串行化：插库（幂等）→ 入内存 → 广播。ack 只在提交后发，这是持久化边界 ——
  const result = await ctx.room.enqueue(async () => {
    const ins = await insertUpdate(pool, {
      docId: ctx.docId, tenantId: ctx.tenantId, userId: ctx.user.id,
      nonce, clientId: Number.isInteger(msg.clientId) ? msg.clientId : 0, update: Buffer.from(bytes),
    });

    if (!ins.duplicated) {
      // 已提交的字节才能进入服务端状态并广播给其他人
      ctx.room.applyCommitted(bytes, ins.id);
      for (const peer of ctx.room.conns) {
        if (peer === ctx || peer.closed) continue;
        P.send(peer.socket, {
          type: 'update',
          update: P.b64encode(bytes),
          clientId: Number.isInteger(msg.clientId) ? msg.clientId : 0,
          originNonce: nonce,
        });
      }
      // 持久化边界事件：此刻数据已 fsync 到 PG，ack 尚未发出。
      // 崩溃窗口测试在此刻硬退出，重连后必须靠 nonce 去重 + 状态向量补齐收敛。
      process.emit('update-persisted', {
        docId: ctx.docId, tenantId: ctx.tenantId, nonce,
        updateId: ins.id, userId: ctx.user.id,
      });
    }
    return ins;
  });

  P.send(ctx.socket, {
    type: 'ack',
    nonce,
    updateId: result.id,
    duplicated: result.duplicated,
  });
}

async function logBad(ctx, kind, detail, payload) {
  await insertError(ctx.room.pool, {
    kind, docId: ctx.docId, tenantId: ctx.tenantId, detail, payload,
  });
}

function safeError(ctx, code, message, extra = {}) {
  try {
    P.send(ctx.socket, { type: 'error', code, message, ...extra });
  } catch { /* socket 已关闭 */ }
}

export function registerCollabGateway(fastify, opts, done) {
  fastify.get('/docs/:docId/ws', { websocket: true }, (connection, req) => {
    handleConnection(fastify, connection.socket, req).catch((err) => {
      fastify.log.error({ err }, 'connection setup failed');
      try { connection.socket.close(P.CLOSE.POLICY, 'setup failed'); } catch {}
    });
  });
  done();
}
