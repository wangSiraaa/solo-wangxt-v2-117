# Collab Gateway — 服务端协作文档网关

Fastify + WebSocket 承载 **Yjs** 更新，PostgreSQL 保存更新日志与压缩快照。
本仓库只包含**服务端网关 + 两个脚本客户端**，不包含前端。

核心问题：多人并发编辑时不能“最后一次保存覆盖其他人的修改”。网关把每条
Yjs 更新作为 CRDT 操作持久化并重放，天然合并并发、消解乱序，并在断线重连时
按**状态向量**补齐差异。

---

## 它保证什么

| 需求 | 实现位置 | 保证方式 |
|------|----------|----------|
| 不覆盖他人修改 | `src/gateway.mjs` | 每条更新是 Yjs CRDT 操作，服务端只 `applyUpdate`，从不整体替换文档 |
| **持久化边界** | `src/gateway.mjs` `handleUpdate` | 顺序固定为 `INSERT ... 提交 → apply 到内存 → 广播 → 回 ack`；ack 只在数据库事务提交后发出 |
| 重复消息去重 | `sql/schema.sql` `doc_updates` | `UNIQUE(doc_id, nonce)`；重发同 nonce 返回已有行并回 `ack.duplicated=true`，绝不产生第二条日志 |
| 断线按状态向量补齐 | `handleSync` + `yjs-util.diffForStateVector` | 重连发 `{type:'sync', sv}`，服务端用 `Y.encodeStateAsUpdate(doc, sv)` 只回缺失部分 |
| 租户/文档权限 | `authenticate` / `authorizeDoc` | 连接时鉴权；**每条 update 重新查库鉴权**（viewer 被降权立即生效）。房间号只取 URL `/docs/:docId`，忽略消息体里的任何房间字段 |
| 压缩后仍可恢复 | `repo.compactDocument` + `loadDocState` | 恢复路径恒为 `最新快照 + 快照(id > last_update_id)之后的全部增量`；压缩在一个 `REPEATABLE READ` 事务内“写快照 + 删已并日志”，崩溃也不会同时丢两者 |
| 未知/损坏更新可定位 | `doc_errors` 表 | 坏字节在**入库前**被 `Y.decodeUpdate` 拦截，写一条带 `kind/doc_id/nonce/detail/payload_hex` 的错误记录，回 error 帧但**不断开连接、不污染日志、不影响房间内他人** |
| 重连收敛而非“暂时字符串相同” | 全部测试 | 断言比较 `Y.encodeStateAsUpdate` 的**状态字节哈希**（FNV），并与从 PG 独立重建的权威状态比对 |

### 关键不变量

1. **内存可丢，磁盘为准**：房间里的 `Y.Doc` 只是已提交数据的缓存。最后一个连接离开就卸载房间；任何重启都从“快照 + 增量日志”重建。
2. **提交后才可见**：只有数据库已提交的字节才能进入服务端 `Y.Doc` 并广播。
3. **每房间写串行**：房间维护一条 FIFO 队列，使直播应用顺序与 `doc_updates.id` 顺序一致；连接级另有一条 FIFO，保证认证期间到达的早期帧不与后续帧乱序。

---

## 目录结构

```
sql/schema.sql           租户/用户/文档/成员/更新日志/快照/错误隔离表
sql/seed.sql             演示用租户、token、文档、成员
src/config.mjs           环境变量配置
src/repo.mjs             认证、鉴权、幂等插入、快照、错误落库（全部带 tenant_id）
src/yjs-util.mjs         更新校验、状态恢复、状态向量差异、状态哈希
src/room.mjs             房间缓存 + 每房间串行队列 + 压缩后安全重载
src/protocol.mjs         WS 线路协议、关闭码、base64
src/gateway.mjs          WS 处理器（鉴权/校验/持久化边界/广播）
src/server.mjs           Fastify 入口：/health、/admin 压缩与状态恢复接口
scripts/pg-local.mjs     免安装 PostgreSQL(init/start/stop/status)
scripts/db-setup.mjs     建表 + 种子（--fresh 重建）
scripts/collab-client.mjs 脚本客户端库：nonce、自动重放、指数退避重连、SV 补齐
scripts/client.mjs       交互式 CLI 客户端（手工演示并发）
scripts/demo.mjs         两客户端并发插入/删除并校验收敛
test/                    6 组 22 个 node:test 用例
```

---

## 快速开始

需要 Node ≥ 18。仓库附带免安装 PostgreSQL（无需 root/Docker）。

```bash
npm install

npm run pg:init       # 首次：初始化本地 PG 数据目录 data/pgdata
npm run pg:start      # 启动 PG（localhost:55432，trust 认证，仅开发用）
npm run db:setup      # 建表 + 种子（可重复执行；--fresh 删表重建）

npm run server        # 启动网关 ws://127.0.0.1:8080（另一个终端）
npm run demo          # 两个脚本客户端并发编辑，自动校验收敛与 PG 重建一致
```

`demo` 也会在检测不到在线网关时自动临时拉起一个。

生产环境用外部 PostgreSQL：`DATABASE_URL=postgres://... npm run server`。

### 手工双客户端演示（两个终端）

```bash
# 终端 A
npm run client -- --as alice --doc doc-note
# 终端 B
npm run client -- --as bob --doc doc-note
# 然后交错输入：  i 0 hello      d 0 2      s（查看状态哈希）
```

种子里的身份（静态 token，仅演示）：

| 用户 | token | doc-note 权限 |
|------|-------|---------------|
| alice | `tok-alice` | owner |
| bob | `tok-bob` | editor |
| carol | `tok-carol` | viewer（只读，写入会被逐条拒绝并记错误） |
| dave (Globex 租户) | `tok-dave` | 无权访问（跨租户 4403） |

---

## WebSocket 协议

连接：`GET ws://host/docs/:docId/ws?token=...`（也支持 `Authorization: Bearer`）。
所有帧为 JSON 文本，Yjs 二进制用 base64 承载。

```text
client → server
  {v:1,type:'sync',  sv:<b64 状态向量>, nonce?}
  {v:1,type:'update',nonce:<幂等键>, update:<b64>, clientId:<number>}
  {v:1,type:'ping',  nonce?}

server → client
  {type:'hello', docId, role, serverId}   // 房间号由服务端按 URL 回执
  {type:'sync',  diff:<b64>, nonce?}
  {type:'ack',   nonce, updateId, duplicated}   // 仅在持久化提交后
  {type:'update',update:<b64>, clientId, originNonce}
  {type:'error', code, message, nonce?, errorId?}
```

关闭码：`4401` 未认证、`4403` 未授权。坏消息（可恢复错误）只回 `error`，不关连接。

---

## 管理接口（同一套 token 鉴权）

```bash
# 从“快照 + 增量日志”权威重建文档，返回状态哈希/文本/依据
curl "$BASE/admin/docs/doc-note/state?token=tok-alice"

# owner 手动压缩：压缩前后自动比对状态哈希，哈希不一致返回 500 而不是静默成功
curl -X POST "$BASE/admin/docs/doc-note/compact?token=tok-alice"

curl "$BASE/admin/docs/doc-note/stats?token=tok-alice"
```

---

## 崩溃窗口与恢复语义

服务端支持 `EXIT_AFTER_PERSIST=N`：在第 N 条 update **数据库已提交、ack 尚未发出**
的窗口里硬退出（`process.exit`）。这正是最危险的崩溃点：

1. 数据已落库，但客户端没收到确认、其他客户端没收到广播；
2. 客户端重连后用**同一 nonce、同一字节**重放；
3. 服务端命中 `UNIQUE(doc_id,nonce)`，回 `duplicated=true`，日志仍是同一条；
4. 重连发自己的状态向量，服务端只回缺失差异；
5. 双方与从 PG 重建的状态做到字节级一致。

---

## 测试

```bash
npm test          # node --test test/  （22 个用例）
```

| 文件 | 覆盖点 |
|------|--------|
| `01-authz` | 坏 token、跨租户、非成员 4401/4403；viewer 逐条写入被拒；消息体伪造房间号无效（落库按 URL 文档） |
| `02-persistence` | ack 前行已存在；同 nonce 重发只存一条；同因因果更新乱序（删除先于其依赖插入）收敛；重连状态向量只回差异并补到字节一致 |
| `03-errors` | 随机字节/截断/非 base64 更新被拒并落 `doc_errors`（含 `payload_hex`）；未知类型/坏信封逐条隔离；坏流量不影响房间内他人 |
| `04-compaction` | 压缩删日志后单靠快照字节级恢复；压缩后再编辑、二次压缩不丢；压缩与在线写入并发不丢；非 owner 禁止压缩 |
| `05-crash` | 持久化后、ack 前进程硬退出 → 重启 → nonce 重放去重 → SV 补齐 → 双方与 PG 字节级收敛，且恢复后可继续协作 |
| `06-reconnect` | 两脚本客户端并发插入/删除收敛并与 PG 重建一致；`terminate()` 强杀 TCP 模拟真实断线，离线期间他人持续编辑，重连补齐后继续协作仍收敛 |

测试比较的是 CRDT **状态字节哈希**而不是显示字符串，因此能抓住“字符串暂时相同、
结构却分叉”的假收敛；并额外从 PostgreSQL 独立重建一份文档作为权威对照。

> 说明：测试用唯一 doc id 直接写入种子租户，互不共享数据；可反复运行。
