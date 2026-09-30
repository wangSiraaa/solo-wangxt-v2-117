// 线路协议（所有消息为 JSON 文本帧；Yjs 二进制用 base64 承载）
//
// client -> server:
//   { v:1, type:'sync',   sv: <b64 stateVector>, nonce?: <idempotency key for hello sync, optional> }
//   { v:1, type:'update', nonce:<uuid>, update:<b64>, clientId:<number> }
//   { v:1, type:'ping', nonce? }
//
// server -> client:
//   { type:'hello',  docId, role, serverId }           —— 房间号由服务端按 URL 回执
//   { type:'sync',   diff:<b64>, nonce? }
//   { type:'ack',    nonce, updateId, duplicated }     —— 仅在 DB 提交后发出
//   { type:'update', update:<b64>, clientId, originNonce }  —— 其他客户端已提交的更新
//   { type:'pong',   nonce? }
//   { type:'error',  code, message, nonce?, errorId? }
//
// 注意：服务端从不读取客户端消息体中的 doc/tenant 字段；房间完全来自 URL。

export const CLOSE = {
  UNAUTHORIZED: 4401,
  FORBIDDEN: 4403,
  // 可恢复错误（坏消息）不断连接，只回 error；协议级不可恢复才关连接
  POLICY: 4418,
};

export const ERROR_CODE = {
  BAD_JSON: 'bad_json',
  BAD_ENVELOPE: 'bad_envelope',
  UNKNOWN_TYPE: 'unknown_type',
  CORRUPT_UPDATE: 'corrupt_update',
  NOT_ALLOWED: 'not_allowed',           // viewer 试图写入
  BAD_STATE_VECTOR: 'bad_state_vector',
  INTERNAL: 'internal',
};

export function b64encode(u8) {
  return Buffer.from(u8).toString('base64');
}

export function b64decode(s) {
  if (typeof s !== 'string' || s.length === 0) throw new Error('not base64 text');
  return new Uint8Array(Buffer.from(s, 'base64'));
}

export function serverId() {
  return process.env.SERVER_ID || `srv-${process.pid}`;
}

export function send(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}
