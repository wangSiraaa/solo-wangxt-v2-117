-- 协作文档网关 schema
-- 多租户：所有业务表带 tenant_id，权限校验落在 (tenant_id, doc_id)。

CREATE TABLE IF NOT EXISTS tenants (
  id          TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 用户（演示用静态 token 认证；token 只在服务端持有）
CREATE TABLE IF NOT EXISTS app_users (
  id          TEXT PRIMARY KEY,
  tenant_id   TEXT NOT NULL REFERENCES tenants(id),
  display_name TEXT NOT NULL,
  token       TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS documents (
  id          TEXT PRIMARY KEY,
  tenant_id   TEXT NOT NULL REFERENCES tenants(id),
  title       TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS documents_tenant_idx ON documents(tenant_id);

-- 文档成员：role = owner | editor | viewer
CREATE TABLE IF NOT EXISTS document_members (
  doc_id     TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  user_id    TEXT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
  role       TEXT NOT NULL CHECK (role IN ('owner','editor','viewer')),
  PRIMARY KEY (doc_id, user_id)
);

-- Yjs 文档更新日志（追加写入，不做原地修改）。
-- (doc_id, nonce) 唯一约束即“重复消息去重”的持久化边界。
CREATE TABLE IF NOT EXISTS doc_updates (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  doc_id      TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  tenant_id   TEXT NOT NULL REFERENCES tenants(id),
  nonce       TEXT NOT NULL,                 -- 客户端为每条更新生成的幂等键
  client_id   BIGINT NOT NULL,               -- Yjs clientID（仅诊断用）
  update      BYTEA NOT NULL,                -- Yjs 二进制 update
  size_bytes  INTEGER NOT NULL,
  created_by  TEXT NOT NULL REFERENCES app_users(id),
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (doc_id, nonce)
);
CREATE INDEX IF NOT EXISTS doc_updates_doc_idx ON doc_updates(doc_id, id);

-- 压缩快照：compress(doc) 的结果；恢复文档 = 快照 + 快照之后的全部 update
CREATE TABLE IF NOT EXISTS doc_snapshots (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  doc_id          TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  state_vector    BYTEA NOT NULL,
  snapshot        BYTEA NOT NULL,           -- Y.encodeStateAsUpdate(compressed)
  updates_merged  BIGINT NOT NULL,          -- 合并的 update 数量（诊断）
  last_update_id  BIGINT,                   -- 合并区间 [.., <= 此 id]
  created_by      TEXT NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS doc_snapshots_doc_idx ON doc_snapshots(doc_id, id);

-- 未知/损坏消息隔离区：不中断其他客户端，事后可按 doc/nonce/id 定位
CREATE TABLE IF NOT EXISTS doc_errors (
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  kind         TEXT NOT NULL CHECK (kind IN ('corrupt_update','unknown_type','bad_envelope','unauthorized','duplicate_attempt')),
  doc_id       TEXT,
  tenant_id    TEXT,
  nonce        TEXT,
  detail       TEXT NOT NULL,
  payload_hex  TEXT,                         -- 原始消息（截断）
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS doc_errors_doc_idx ON doc_errors(doc_id, created_at);
