// Yjs 相关纯函数：更新校验、状态恢复、状态向量差异。
import * as Y from 'yjs';

function toU8(b) {
  if (b instanceof Uint8Array) return b;
  return new Uint8Array(b);
}

// 校验一条二进制 update 是否能被独立解码。
// 损坏/截断/伪造字节在这里抛错，由调用方落 doc_errors，绝不写进更新日志。
export function validateUpdate(bytes) {
  if (!Buffer.isBuffer(bytes) && !(bytes instanceof Uint8Array)) {
    throw new Error('update is not binary');
  }
  if (bytes.length === 0) throw new Error('empty update');
  // Yjs 解码 update 结构；垃圾输入会抛 RangeError/Error 而不是崩溃进程
  Y.decodeUpdate(toU8(bytes));
  return true;
}

// 用“快照 + 增量”恢复一个 Doc（内存态）。
export function buildDoc(snapshotBytes, updates) {
  const doc = new Y.Doc();
  if (snapshotBytes) Y.applyUpdate(doc, toU8(snapshotBytes), 'snapshot');
  for (const u of updates) {
    Y.applyUpdate(doc, toU8(u.bytes ?? u), 'replay');
  }
  return doc;
}

// 把快照与若干 update 合并为一条规范化的 state-as-update（用于压缩落库）。
// 快照本身就是一条 state update，与后续增量可直接 mergeUpdates。
export function mergeToStateUpdate(snapshotBytes, updateBytesList) {
  const list = [];
  if (snapshotBytes) list.push(toU8(snapshotBytes));
  for (const b of updateBytesList) list.push(toU8(b));
  if (list.length === 0) return null;
  const doc = new Y.Doc();
  Y.applyUpdate(doc, Y.mergeUpdates(list), 'merge');
  return Y.encodeStateAsUpdate(doc);
}

// repo.compactDocument 用这个名字调用
export const encodeMergedState = mergeToStateUpdate;

// 断线重连补齐：给定对端状态向量，编码服务端当前完整状态的差异。
export function diffForStateVector(doc, stateVector) {
  return Y.encodeStateAsUpdate(doc, stateVector && stateVector.length ? toU8(stateVector) : undefined);
}

export function encodeSV(doc) {
  return Y.encodeStateVector(doc);
}

// 收敛断言用：状态字节相同比“显示字符串相同”更强
// —— 两边字符串可能暂时相同但 CRDT 结构分叉。
export function stateHash(bytes) {
  const u8 = toU8(bytes);
  let h = 0x811c9dc5;
  for (let i = 0; i < u8.length; i++) {
    h ^= u8[i];
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h.toString(16).padStart(8, '0');
}

export function docStateBytes(doc) {
  return Y.encodeStateAsUpdate(doc);
}

// 便于日志/测试阅读：导出共享文本
export function docText(doc, name = 'content') {
  return doc.getText(name).toString();
}
