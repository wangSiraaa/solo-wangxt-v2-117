// 集中配置：所有外部依赖通过环境变量注入，提供开发默认值。
export const config = {
  http: {
    host: process.env.HOST || '127.0.0.1',
    port: Number(process.env.PORT || 8080),
  },
  pg: {
    connectionString: process.env.DATABASE_URL
      || 'postgres://postgres@localhost:55432/postgres',
    max: Number(process.env.PGPOOL || 10),
  },
  // 服务端在“持久化已提交”之后才 ack；ack 前连接被断开时客户端重发同一 nonce
  persistence: {
    // 压缩快照保留策略（delete-after-snapshot 只删除已并入最新快照的 update）
    keepUpdatesAfterSnapshot: process.env.KEEP_OLD_UPDATES === '1',
  },
};
