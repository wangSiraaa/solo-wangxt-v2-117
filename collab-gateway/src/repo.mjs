// 数据访问层：认证、权限、更新日志、快照、错误隔离。
// 所有查询显式带 tenant_id —— 服务端不信任客户端自报的房间号/租户。
import pg from 'pg';

// BIGINT/IDENTITY 列在本系统规模内远小于 2^53，按 number 返回，
// 避免 updateId 经 JSON 变成字符串导致客户端无法做整数断言/排序。
pg.types.setTypeParser(20, (v) => (v === null ? null : Number(v)));

export function createPool(cfg) {
  const pool = new pg.Pool(cfg);
  return pool;
}

// 通过静态 token 找到用户及其租户（生产可替换为 OIDC/JWT，接口不变）
export async function authenticate(pool, token) {
  if (!token || typeof token !== 'string') return null;
  const { rows } = await pool.query(
    `SELECT id, tenant_id, display_name
       FROM app_users WHERE token = $1`,
    [token],
  );
  return rows[0] ?? null;
}

// 每次连接、每次更新都调用：服务端按 URL 上的 docId + token 对应用户判定，
// 忽略消息体中任何房间号字段。
// 返回 { ok, role, docId, tenantId }；失败原因供错误落库。
export async function authorizeDoc(pool, tenantId, userId, docId) {
  const { rows } = await pool.query(
    `SELECT d.tenant_id, m.role
       FROM documents d
       JOIN document_members m ON m.doc_id = d.id
      WHERE d.id = $1 AND d.tenant_id = $2 AND m.user_id = $3`,
    [docId, tenantId, userId],
  );
  if (rows.length === 0) return { ok: false, reason: 'not_found_or_forbidden' };
  return { ok: true, role: rows[0].role, docId, tenantId: rows[0].tenant_id };
}

// 幂等插入一条 Yjs update。
// UNIQUE(doc_id, nonce) 保证同一客户端重连重发不会产生第二条。
// 返回 { duplicated, id, createdAt } —— 重复时返回既有行，ack 仍可正常发出。
export async function insertUpdate(pool, { docId, tenantId, userId, nonce, clientId, update }) {
  try {
    const { rows } = await pool.query(
      `INSERT INTO doc_updates (doc_id, tenant_id, nonce, client_id, update, size_bytes, created_by)
       VALUES ($1,$2,$3,$4,$5,$6,$7)
       RETURNING id, created_at`,
      [docId, tenantId, nonce, clientId, update, update.length ?? update.byteLength, userId],
    );
    return { duplicated: false, id: rows[0].id, createdAt: rows[0].created_at };
  } catch (e) {
    if (e.code === '23505') {
      const { rows } = await pool.query(
        'SELECT id, created_at FROM doc_updates WHERE doc_id = $1 AND nonce = $2',
        [docId, nonce],
      );
      return { duplicated: true, id: rows[0]?.id ?? null, createdAt: rows[0]?.created_at ?? null };
    }
    throw e;
  }
}

// 装载文档：最新快照 + 快照之后的所有 update，保证“压缩后仍可完整恢复”。
export async function loadDocState(pool, docId) {
  const snap = await pool.query(
    `SELECT id, snapshot, state_vector, last_update_id, created_at
       FROM doc_snapshots WHERE doc_id = $1 ORDER BY id DESC LIMIT 1`,
    [docId],
  );
  const snapshot = snap.rows[0] || null;
  const sinceId = snapshot?.last_update_id ?? 0;
  const upd = await pool.query(
    `SELECT id, update AS update_bytes, nonce, client_id, created_at
       FROM doc_updates WHERE doc_id = $1 AND id > $2 ORDER BY id ASC`,
    [docId, sinceId],
  );
  return {
    snapshot: snapshot ? {
      id: snapshot.id,
      bytes: snapshot.snapshot,
      sv: snapshot.state_vector,
      lastUpdateId: snapshot.last_update_id,
      createdAt: snapshot.created_at,
    } : null,
    updates: upd.rows.map((r) => ({
      id: r.id,
      bytes: r.update_bytes,
      nonce: r.nonce,
      clientId: Number(r.client_id),
      createdAt: r.created_at,
    })),
  };
}

export async function listUpdateIds(pool, docId, maxId = null) {
  const { rows } = await pool.query(
    `SELECT id FROM doc_updates
      WHERE doc_id = $1 ${maxId != null ? 'AND id <= $2' : ''}
      ORDER BY id ASC`,
    maxId != null ? [docId, maxId] : [docId],
  );
  return rows.map((r) => r.id);
}

// 压缩：把截至当前的全部 update 合并成一个快照并落库，同一事务内删除已合并的日志。
// 与 insertUpdate 并发时靠事务 + 重新取边界保证不漏不重（见 FOR UPDATE / 串行化）。
export async function compactDocument(pool, docId, userId) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ');
    // 锁定文档行，与并发压缩互斥；update 插入走的是另一张表，不会被长时间阻塞
    await client.query('SELECT id FROM documents WHERE id = $1 FOR UPDATE', [docId]);

    // 事务内重新取边界：只合并本事务快照时已经存在的 update
    const idsRes = await client.query(
      'SELECT id, update AS update_bytes FROM doc_updates WHERE doc_id = $1 ORDER BY id ASC',
      [docId],
    );
    const snapRes = await client.query(
      `SELECT id, snapshot, last_update_id FROM doc_snapshots
        WHERE doc_id = $1 ORDER BY id DESC LIMIT 1`,
      [docId],
    );

    const { mergeToStateUpdate } = await import('./yjs-util.mjs');
    const mergedBytes = mergeToStateUpdate(
      snapRes.rows[0]?.snapshot ?? null,
      idsRes.rows.map((r) => r.update_bytes),
    );
    if (!mergedBytes) {
      await client.query('ROLLBACK');
      return { compacted: false, reason: 'empty' };
    }
    const Y = await import('yjs');
    const doc = new Y.Doc();
    Y.applyUpdate(doc, mergedBytes, 'compact-state');
    const sv = Y.encodeStateVector(doc);

    const maxId = idsRes.rows.length ? idsRes.rows[idsRes.rows.length - 1].id
      : (snapRes.rows[0]?.last_update_id ?? 0);

    const ins = await client.query(
      `INSERT INTO doc_snapshots (doc_id, state_vector, snapshot, updates_merged, last_update_id, created_by)
       VALUES ($1,$2,$3,$4,$5,$6) RETURNING id`,
      [docId, sv, mergedBytes, idsRes.rows.length, maxId, userId],
    );
    if (idsRes.rows.length) {
      await client.query('DELETE FROM doc_updates WHERE doc_id = $1 AND id <= $2', [docId, maxId]);
    }
    await client.query('COMMIT');
    return {
      compacted: true,
      snapshotId: ins.rows[0].id,
      merged: idsRes.rows.length,
      lastUpdateId: maxId,
      bytes: mergedBytes,
    };
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    throw e;
  } finally {
    client.release();
  }
}

export async function insertError(pool, { kind, docId = null, tenantId = null, nonce = null, detail, payload = null }) {
  let hex = null;
  if (payload != null) {
    let buf;
    if (Buffer.isBuffer(payload)) buf = payload;
    else if (payload instanceof Uint8Array) {
      // 不能 Buffer.from(String(u8))：Uint8Array.toString() 是逗号十进制
      buf = Buffer.from(payload.buffer, payload.byteOffset, payload.byteLength);
    } else buf = Buffer.from(String(payload));
    hex = buf.subarray(0, 8192).toString('hex');
  }
  const { rows } = await pool.query(
    `INSERT INTO doc_errors (kind, doc_id, tenant_id, nonce, detail, payload_hex)
     VALUES ($1,$2,$3,$4,$5,$6) RETURNING id, created_at`,
    [kind, docId, tenantId, nonce, String(detail).slice(0, 2000), hex],
  );
  return { id: rows[0].id, createdAt: rows[0].created_at };
}

export async function getDocStats(pool, docId) {
  const u = await pool.query('SELECT count(*)::int AS n, COALESCE(sum(size_bytes),0)::bigint AS bytes FROM doc_updates WHERE doc_id=$1', [docId]);
  const s = await pool.query('SELECT count(*)::int AS n FROM doc_snapshots WHERE doc_id=$1', [docId]);
  return { updates: u.rows[0], snapshots: s.rows[0].n };
}
