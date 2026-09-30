#!/usr/bin/env node
// 初始化/刷新数据库结构并写入种子数据：
//   node scripts/db-setup.mjs           # 建表 + seed（幂等）
//   node scripts/db-setup.mjs --fresh   # 删表重建（清空文档数据）
import pg from 'pg';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { config } from '../src/config.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const sqlDir = path.join(here, '..', 'sql');

async function main() {
  const client = new pg.Client(config.pg);
  await client.connect();
  try {
    if (process.argv.includes('--fresh')) {
      console.log('[db] dropping tables...');
      await client.query(`
        DROP TABLE IF EXISTS doc_errors, doc_snapshots, doc_updates,
          document_members, documents, app_users, tenants CASCADE;
      `);
    }
    console.log('[db] applying schema...');
    await client.query(readFileSync(path.join(sqlDir, 'schema.sql'), 'utf8'));
    console.log('[db] applying seed...');
    await client.query(readFileSync(path.join(sqlDir, 'seed.sql'), 'utf8'));
    const r = await client.query('select count(*)::int as n from documents');
    console.log(`[db] ready, documents=${r.rows[0].n}`);
  } finally {
    await client.end();
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
