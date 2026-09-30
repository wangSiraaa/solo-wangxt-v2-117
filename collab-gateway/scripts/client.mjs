#!/usr/bin/env node
// 脚本客户端 CLI —— 可开两个终端各跑一个，手工演示并发编辑：
//   node scripts/client.mjs --as alice --doc doc-note
//   node scripts/client.mjs --as bob   --doc doc-note
//
// 进入交互后输入：
//   i <pos> <text>     在 pos 处插入
//   d <pos> <len>      删除 len 个字符
//   s                  打印当前文本/状态哈希
//   r                  打印本地未 ack 队列
//   quit
import { CollabClient } from './collab-client.mjs';
import { stateHash } from '../src/yjs-util.mjs';
import readline from 'node:readline';

// 同时支持 --flag value 与 --flag=value
const FLAG_NAMES = new Set(['as', 'doc', 'cid', 'token']);
const argv = {};
const rawArgs = process.argv.slice(2);
for (let i = 0; i < rawArgs.length; i++) {
  const m = rawArgs[i].match(/^--([\w-]+)(?:=(.*))?$/);
  if (!m) continue;
  const name = m[1];
  if (m[2] !== undefined) argv[name] = m[2];
  else if (FLAG_NAMES.has(name) && rawArgs[i + 1] && !rawArgs[i + 1].startsWith('--')) argv[name] = rawArgs[++i];
  else argv[name] = true;
}

const TOKENS = { alice: 'tok-alice', bob: 'tok-bob', carol: 'tok-carol', dave: 'tok-dave' };
const url = process.env.GATEWAY_URL || 'ws://127.0.0.1:8080';
const token = TOKENS[argv.as] || argv.token;
if (!token) { console.error('--as alice|bob|carol|dave or --token required'); process.exit(2); }

const client = new CollabClient({
  url, token,
  docId: argv.doc || 'doc-note',
  clientId: argv.cid != null ? Number(argv.cid) : undefined,
  name: String(argv.as || 'client'),
  log: (m) => console.log(m),
});

await client.connect().catch((e) => { console.error('connect failed:', e.message); process.exit(1); });
await client.waitSynced().catch(() => {});
console.log(`\n=== joined doc-note as ${argv.as}, current text: ${JSON.stringify(client.text)} ===\n`);

const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
const prompt = () => rl.question('> ', async (line) => {
  const [cmd, ...rest] = line.trim().split(/\s+/);
  try {
    if (cmd === 'i') {
      const pos = Number(rest[0]); const t = rest.slice(1).join(' ');
      const { waitAck } = client.transact((ytext) => ytext.insert(pos, t), `insert@${pos}`);
      await waitAck(5000);
    } else if (cmd === 'd') {
      const pos = Number(rest[0]); const len = Number(rest[1]);
      const { waitAck } = client.transact((ytext) => ytext.delete(pos, len), `delete@${pos}`);
      await waitAck(5000);
    } else if (cmd === 's') {
      console.log('text :', JSON.stringify(client.text));
      console.log('hash :', stateHash(client.stateBytes()), 'len:', client.length);
    } else if (cmd === 'r') {
      console.log('pending nonces:', [...client.pending.keys()].map((n) => n.slice(0, 8)));
      console.log('server errors :', client.errors.slice(-3));
    } else if (cmd === 'quit' || cmd === 'q') {
      await client.close(); process.exit(0);
    } else if (cmd) {
      console.log('commands: i <pos> <text> | d <pos> <len> | s | r | quit');
    }
  } catch (e) { console.log('operation failed:', e.message); }
  prompt();
});
prompt();
