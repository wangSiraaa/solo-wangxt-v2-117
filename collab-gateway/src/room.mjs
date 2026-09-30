// 文档房间：服务端内存里的 Y.Doc 是 PG 中已提交数据的缓存，可随时丢弃重建。
// 关键不变量：
//   1) 只有“数据库已提交”的 update 才能 apply 到房间并广播（持久化边界）；
//   2) 每个房间用队列串行化写入，使直播顺序与 doc_updates.id 顺序一致；
//   3) 最后一个连接离开即卸载房间；重连/重启都从“快照 + 增量日志”重建。
import * as Y from 'yjs';
import { loadDocState } from './repo.mjs';
import { buildDoc, docStateBytes, diffForStateVector } from './yjs-util.mjs';

class Room {
  constructor(key, docId, pool) {
    this.key = key;
    this.docId = docId;
    this.pool = pool;
    this.doc = null;
    this.conns = new Set();
    this.lastAppliedId = 0;
    this.chain = Promise.resolve();
    this.loading = null;
  }

  async ensureLoaded() {
    if (this.doc) return this.doc;
    if (!this.loading) {
      this.loading = (async () => {
        const { snapshot, updates } = await loadDocState(this.pool, this.docId);
        const doc = buildDoc(snapshot?.bytes ?? null, updates);
        this.lastAppliedId = updates.length ? updates[updates.length - 1].id : (snapshot?.lastUpdateId ?? 0);
        this.doc = doc;
        return doc;
      })();
    }
    await this.loading;
    return this.doc;
  }

  // 串行执行：避免两条 update 的“插库 → 入内存 → 广播”交错导致直播顺序与日志顺序不一致
  enqueue(task) {
    const run = this.chain.then(() => task());
    // 单个任务失败不应毒死整条链
    this.chain = run.catch(() => {});
    return run;
  }

  // 必须在 enqueue 内、且 DB 已提交后调用
  applyCommitted(updateBytes, dbId) {
    Y.applyUpdate(this.doc, updateBytes, 'server');
    if (dbId != null) this.lastAppliedId = Math.max(this.lastAppliedId, dbId);
  }

  diffFor(clientSV) {
    return diffForStateVector(this.doc, clientSV);
  }

  stateBytes() {
    return docStateBytes(this.doc);
  }

  // 压缩提交后，在串行队列内安全重建状态（可能仍有活跃连接）。
  // 经由 enqueue 保证：重建期间到达的新提交排在“加载”之后，不会 apply 到 null，
  // 也不会漏应用快照之后新插入的 update。
  async reload() {
    return this.enqueue(async () => {
      const { snapshot, updates } = await loadDocState(this.pool, this.docId);
      this.doc = buildDoc(snapshot?.bytes ?? null, updates);
      this.lastAppliedId = updates.length
        ? updates[updates.length - 1].id
        : (snapshot?.lastUpdateId ?? 0);
      this.loading = null;
    });
  }

  add(conn) { this.conns.add(conn); }
  remove(conn) { this.conns.delete(conn); }
  get empty() { return this.conns.size === 0; }
}

export class RoomRegistry {
  constructor(pool) {
    this.pool = pool;
    this.rooms = new Map();
  }

  static key(tenantId, docId) {
    return `${tenantId}::${docId}`;
  }

  async get(tenantId, docId) {
    const k = RoomRegistry.key(tenantId, docId);
    let room = this.rooms.get(k);
    if (!room) {
      room = new Room(k, docId, this.pool);
      this.rooms.set(k, room);
      await room.ensureLoaded();
    }
    return room;
  }

  // 压缩提交后：空房间直接丢弃（下次连接重建）；活跃房间在串行队列内重载
  invalidate(docId) {
    for (const [k, room] of this.rooms) {
      if (room.docId !== docId) continue;
      if (room.empty) this.rooms.delete(k);
      else room.reload().catch(() => { /* 下次 enqueue/ensureLoaded 会重试 */ });
    }
  }

  async shutdown() { /* 房间无后台任务；PG 池由调用方关闭 */ }
}
