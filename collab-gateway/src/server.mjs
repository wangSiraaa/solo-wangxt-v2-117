// Fastify 入口：HTTP 管理面 + WebSocket 协作网关。
import Fastify from 'fastify';
import websocket from '@fastify/websocket';
import { randomUUID } from 'node:crypto';
import { config } from './config.mjs';
import { createPool, authenticate, authorizeDoc, compactDocument, getDocStats, loadDocState } from './repo.mjs';
import { RoomRegistry } from './room.mjs';
import { registerCollabGateway } from './gateway.mjs';
import { buildDoc, docStateBytes, docText, stateHash } from './yjs-util.mjs';

export async function buildServer(overrides = {}) {
  const pool = overrides.pool || createPool(config.pg);
  const rooms = new RoomRegistry(pool);
  const fastify = Fastify({
    logger: { level: process.env.LOG_LEVEL || 'info' },
  });
  fastify.decorate('collab', { pool, rooms });

  await fastify.register(websocket, { options: { maxPayload: 1024 * 1024 } });
  await fastify.register(registerCollabGateway);

  fastify.get('/health', async () => {
    await pool.query('SELECT 1');
    return { ok: true, serverId: process.env.SERVER_ID || `srv-${process.pid}` };
  });

  // HTTP 管理接口用同一套 token 认证
  async function httpAuth(req, reply) {
    const token = req.query.token || (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
    const user = await authenticate(pool, token);
    if (!user) { reply.code(401); return { error: 'unauthorized' }; }
    req.user = user;
    return null;
  }

  // 手动压缩：owner 可调用。压缩后“删除日志也能恢复文档”。
  fastify.post('/admin/docs/:docId/compact', async (req, reply) => {
    const denied = await httpAuth(req, reply);
    if (denied) return denied;
    const auth = await authorizeDoc(pool, req.user.tenant_id, req.user.id, req.params.docId);
    if (!ok(auth)) { reply.code(403); return { error: 'forbidden' }; }
    if (auth.role !== 'owner') { reply.code(403); return { error: 'owner only' }; }

    // 压缩前快照的“现场状态”，用于压缩后校验内容不丢
    const before = await canonicalState(pool, req.params.docId);
    const res = await compactDocument(pool, req.params.docId, req.user.id);
    rooms.invalidate(req.params.docId);
    const after = await canonicalState(pool, req.params.docId);

    if (before.hash !== after.hash) {
      // 绝不该发生；发生了说明压缩/恢复路径有 bug，显式 500 而不是静默成功
      reply.code(500);
      return { error: 'compaction changed document state', before: before.hash, after: after.hash };
    }
    return {
      ok: true,
      compacted: res.compacted,
      snapshotId: res.snapshotId ?? null,
      updatesMerged: res.merged ?? 0,
      hash: after.hash,
      length: after.length,
      stats: await getDocStats(pool, req.params.docId),
    };
  });

  // 从持久层重建文档（快照 + 日志），供测试/运维验证恢复正确性
  fastify.get('/admin/docs/:docId/state', async (req, reply) => {
    const denied = await httpAuth(req, reply);
    if (denied) return denied;
    const auth = await authorizeDoc(pool, req.user.tenant_id, req.user.id, req.params.docId);
    if (!ok(auth)) { reply.code(403); return { error: 'forbidden' }; }
    const state = await canonicalState(pool, req.params.docId);
    return state;
  });

  fastify.get('/admin/docs/:docId/stats', async (req, reply) => {
    const denied = await httpAuth(req, reply);
    if (denied) return denied;
    const auth = await authorizeDoc(pool, req.user.tenant_id, req.user.id, req.params.docId);
    if (!ok(auth)) { reply.code(403); return { error: 'forbidden' }; }
    return { docId: req.params.docId, ...(await getDocStats(pool, req.params.docId)) };
  });

  fastify.addHook('onClose', async () => {
    await rooms.shutdown();
    await pool.end();
  });

  return fastify;
}

function ok(auth) {
  return auth && auth.ok;
}

// 权威恢复路径：最新快照 + 之后全部 update，重建后返回状态字节哈希与文本
async function canonicalState(pool, docId) {
  const { snapshot, updates } = await loadDocState(pool, docId);
  const doc = buildDoc(snapshot?.bytes ?? null, updates);
  const bytes = docStateBytes(doc);
  return {
    docId,
    hash: stateHash(bytes),
    length: doc.getText('content').length,
    text: docText(doc),
    basedOn: {
      snapshotId: snapshot?.id ?? null,
      snapshotLastUpdateId: snapshot?.lastUpdateId ?? null,
      updatesApplied: updates.length,
      updateIds: updates.map((u) => u.id),
    },
  };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const fastify = await buildServer();
  try {
    await fastify.listen({ host: config.http.host, port: config.http.port });
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
  // 便于“持久化后进程退出”测试：给测试一个确定的退出信号
  if (process.env.EXIT_AFTER_PERSIST) {
    let seen = 0;
    const target = Number(process.env.EXIT_AFTER_PERSIST);
    fastify.collab.pool.on('connect', () => {});
    // 在每条 update 提交并广播后由网关回调（见 gateway 中的 process.emit）
    process.on('update-persisted', async (info) => {
      seen += 1;
      if (seen >= target) {
        fastify.log.warn({ info }, 'EXIT_AFTER_PERSIST reached: hard exit before ack delivery');
        // 硬退出：模拟“已落库但 ack/广播没来得及发”的崩溃窗口
        setTimeout(() => process.exit(0), Number(process.env.EXIT_DELAY_MS || 15)).unref();
      }
    });
  }
}
