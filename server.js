// ═══════════════════════════════════════════════════════════
//  Claude Hub v6 — 飞书 + 钉钉集成
// ═══════════════════════════════════════════════════════════

const express = require('express');
const { WebSocketServer } = require('ws');
const http = require('http');
const https = require('https');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const os = require('os');

const CONFIG = {
  port: parseInt(process.env.PORT) || 3800,
  host: '0.0.0.0',
  claudeBin: process.env.CLAUDE_BIN || 'D:\\theapps\\vue\\node_global\\claude.cmd',
  maxConcurrent: parseInt(process.env.MAX_CONCURRENT) || 3,
  baseDir: __dirname,
  tasksDir: path.join(__dirname, 'tasks'),
  rolesDir: path.join(__dirname, 'roles'),
  templatesFile: path.join(__dirname, 'task-templates.json'),
  publicUrl: process.env.PUBLIC_URL || '', // 公网访问地址，用于飞书卡片链接
};

fs.mkdirSync(CONFIG.tasksDir, { recursive: true });

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const tasks = new Map();
const clients = new Set();
const queue = [];
let runningCount = 0;

// ═══════════════════════════════════════════════════════════
//  飞书配置
// ═══════════════════════════════════════════════════════════
const FEISHU = {
  appId: process.env.FEISHU_APP_ID || '',
  appSecret: process.env.FEISHU_APP_SECRET || '',
  verificationToken: process.env.FEISHU_VERIFICATION_TOKEN || '',
  encryptKey: process.env.FEISHU_ENCRYPT_KEY || '',
  enabled: false,
};

let feishuToken = '';
let feishuTokenExpiry = 0;

function initFeishu() {
  if (FEISHU.appId && FEISHU.appSecret) {
    FEISHU.enabled = true;
    console.log('  飞书 Bot: 已启用');
    console.log(`  Webhook: ${CONFIG.publicUrl || 'http://你的地址:' + CONFIG.port}/feishu/webhook`);
  } else {
    console.log('  飞书 Bot: 未配置');
  }
}

async function getFeishuToken() {
  if (Date.now() < feishuTokenExpiry) return feishuToken;
  try {
    const resp = await fetch('https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ app_id: FEISHU.appId, app_secret: FEISHU.appSecret }),
    });
    const data = await resp.json();
    if (data.tenant_access_token) {
      feishuToken = data.tenant_access_token;
      feishuTokenExpiry = Date.now() + (data.expire - 60) * 1000;
    }
  } catch (e) {
    console.error('飞书 Token 获取失败:', e.message);
  }
  return feishuToken;
}

async function sendFeishuMessage(chatId, text) {
  if (!FEISHU.enabled) return;
  try {
    const token = await getFeishuToken();
    await fetch('https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
      body: JSON.stringify({
        receive_id: chatId,
        msg_type: 'text',
        content: JSON.stringify({ text }),
      }),
    });
  } catch (e) {
    console.error('飞书消息发送失败:', e.message);
  }
}

async function sendFeishuCard(chatId, title, content, status) {
  if (!FEISHU.enabled) return;
  try {
    const token = await getFeishuToken();
    const color = status === 'completed' ? 'green' : status === 'failed' ? 'red' : 'blue';
    const card = {
      config: { wide_screen_mode: true },
      header: { title: { tag: 'plain_text', content: title }, template: color },
      elements: [{ tag: 'div', text: { tag: 'lark_md', content } }],
    };
    if (CONFIG.publicUrl) {
      card.elements.push({
        tag: 'action',
        actions: [{
          tag: 'button',
          text: { tag: 'plain_text', content: '查看详情' },
          url: CONFIG.publicUrl,
          type: 'primary',
        }],
      });
    }
    await fetch('https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
      body: JSON.stringify({
        receive_id: chatId,
        msg_type: 'interactive',
        content: JSON.stringify(card),
      }),
    });
  } catch (e) {
    console.error('飞书卡片发送失败:', e.message);
  }
}

// 飞书 Webhook
app.post('/feishu/webhook', async (req, res) => {
  const body = req.body;

  // URL 验证
  if (body.type === 'url_verification') {
    return res.json({ challenge: body.challenge });
  }

  // 消息事件
  if (body.header && body.header.event_type === 'im.message.receive_v1') {
    const event = body.event;
    const msg = event.message;
    if (msg.message_type !== 'text') return res.json({ ok: true });

    const content = JSON.parse(msg.content);
    const text = (content.text || '').replace(/@_user_\d+/g, '').trim();
    const chatId = msg.chat_id;
    if (!text) return res.json({ ok: true });

    await handleBotMessage('feishu', chatId, text);
  }

  res.json({ ok: true });
});

// ═══════════════════════════════════════════════════════════
//  钉钉配置
// ═══════════════════════════════════════════════════════════
const DINGTALK = {
  webhookToken: process.env.DINGTALK_WEBHOOK_TOKEN || '',
  webhookSecret: process.env.DINGTALK_WEBHOOK_SECRET || '',
  appKey: process.env.DINGTALK_APP_KEY || '',
  appSecret: process.env.DINGTALK_APP_SECRET || '',
  enabled: false,
};

function initDingTalk() {
  if (DINGTALK.webhookToken) {
    DINGTALK.enabled = true;
    console.log('  钉钉 Bot: 已启用');
  } else {
    console.log('  钉钉 Bot: 未配置');
  }
}

async function sendDingTalkMessage(title, text) {
  if (!DINGTALK.enabled) return;
  try {
    let url = `https://oapi.dingtalk.com/robot/send?access_token=${DINGTALK.webhookToken}`;
    if (DINGTALK.webhookSecret) {
      const timestamp = Date.now();
      const stringToSign = `${timestamp}\n${DINGTALK.webhookSecret}`;
      const sign = encodeURIComponent(
          crypto.createHmac('sha256', DINGTALK.webhookSecret).update(stringToSign).digest('base64')
      );
      url += `&timestamp=${timestamp}&sign=${sign}`;
    }
    await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        msgtype: 'markdown',
        markdown: { title, text: `Claude Hub ${title}\n\n${text}` },
      }),
    });
  } catch (e) {
    console.error('钉钉消息发送失败:', e.message);
  }
}

// 钉钉 Webhook（企业内部应用）
app.post('/dingtalk/webhook', async (req, res) => {
  const body = req.body;

  // 验证
  if (body.challenge) {
    return res.json({ challenge: body.challenge });
  }

  // 消息处理
  if (body.msgtype === 'text') {
    const text = (body.text && body.text.content) ? body.text.content.trim() : '';
    const conversationId = body.conversationId || '';
    const senderId = body.senderStaffId || '';
    if (text) {
      await handleBotMessage('dingtalk', conversationId, text);
    }
  }

  res.json({ success: true });
});

// ═══════════════════════════════════════════════════════════
//  统一消息处理（飞书 + 钉钉共用）
// ═══════════════════════════════════════════════════════════
async function handleBotMessage(platform, chatId, text) {
  // 解析命令格式：
  // /task <名称> | <路径> | <描述>   → 结构化任务
  // /status                          → 查看运行中的任务
  // /help                            → 帮助
  // 其他文字                         → 当作快速任务

  if (text === '/help' || text === '帮助') {
    const helpText = `Claude Hub 使用指南：

/task <名称> | <路径> | <描述>
  创建结构化任务
  示例：/task 用户管理 | D:\\projects\\user-mgr | 创建用户CRUD接口

/status
  查看当前运行中的任务

其他任何文字
  直接当作快速任务执行`;

    if (platform === 'feishu') await sendFeishuMessage(chatId, helpText);
    else await sendDingTalkMessage('帮助', helpText);
    return;
  }

  if (text === '/status' || text === '状态') {
    const running = Array.from(tasks.values()).filter(t => t.status === 'running');
    const queued = Array.from(tasks.values()).filter(t => t.status === 'queued');
    let statusText = `当前状态：\n运行中：${running.length}\n等待中：${queued.length}\n总计：${tasks.size}`;
    if (running.length > 0) {
      statusText += '\n\n运行中的任务：';
      for (const t of running) statusText += `\n- ${t.name} (${t.phaseName})`;
    }
    if (platform === 'feishu') await sendFeishuMessage(chatId, statusText);
    else await sendDingTalkMessage('状态', statusText);
    return;
  }

  // 创建任务
  const parts = text.startsWith('/task') ? text.replace('/task', '').trim().split('|').map(s => s.trim()) : text.split('|').map(s => s.trim());

  let name, projectPath, plan;

  if (parts.length >= 3) {
    name = parts[0] || 'Bot 任务';
    projectPath = parts[1];
    plan = parts[2];
  } else if (parts.length === 2) {
    name = parts[0] || 'Bot 任务';
    projectPath = parts[1];
    plan = parts[0];
  } else {
    name = text.substring(0, 50);
    projectPath = path.join(CONFIG.tasksDir, '_bot', Date.now().toString());
    plan = text;
  }

  // 确保项目路径存在
  try { fs.mkdirSync(projectPath, { recursive: true }); } catch {}

  const id = genId();
  const taskDir = path.join(CONFIG.tasksDir, id);
  fs.mkdirSync(taskDir, { recursive: true });

  const task = {
    id, name, type: 'single',
    roleId: null, taskTypeId: null,
    projectPath, taskDir,
    status: 'queued', phase: 0, phaseName: '等待中',
    createdAt: Date.now(), startedAt: null, completedAt: null,
    logs: [], error: null, currentProcess: null, results: null,
    plan, followUps: [], parentId: null, childIds: [],
    retryCount: 0,
    botPlatform: platform,
    botChatId: chatId,
  };

  tasks.set(id, task);
  saveIndex();
  broadcast({ type: 'task_created', data: serializeTask(task) });
  queue.push(id);
  tryExecuteNext();

  const confirmMsg = `🚀 任务已创建\n\n名称：${name}\n路径：${projectPath}\n状态：等待执行...`;
  if (platform === 'feishu') await sendFeishuCard(chatId, `🚀 ${name}`, confirmMsg, 'running');
  else await sendDingTalkMessage(`🚀 ${name}`, confirmMsg);
}

// 任务完成后通知 Bot
function notifyBot(task) {
  if (!task.botPlatform || !task.botChatId) return;

  const statusEmoji = task.status === 'completed' ? '✅' : '❌';
  const statusText = task.status === 'completed' ? '已完成' : '失败';
  let msg = `${statusEmoji} ${statusText}：${task.name}\n`;
  msg += `耗时：${fmtDuration(task.completedAt - task.startedAt)}`;
  if (task.error) msg += `\n错误：${task.error}`;
  if (task.results && task.results.files && task.results.files.length) {
    msg += `\n\n生成文件：${task.results.files.length} 个`;
  }

  if (task.botPlatform === 'feishu') {
    sendFeishuCard(task.botChatId, `${statusEmoji} ${task.name}`, msg, task.status).catch(() => {});
  } else if (task.botPlatform === 'dingtalk') {
    sendDingTalkMessage(`${statusEmoji} ${task.name}`, msg).catch(() => {});
  }
}

function fmtDuration(ms) {
  if (!ms) return '未知';
  const s = Math.floor(ms / 1000), m = Math.floor(s / 60), h = Math.floor(m / 60);
  if (h > 0) return `${h}小时${m % 60}分钟`;
  if (m > 0) return `${m}分钟${s % 60}秒`;
  return `${s}秒`;
}

// ═══════════════════════════════════════════════════════════
//  Roles
// ═══════════════════════════════════════════════════════════
let roles = [];

function loadRoles() {
  try {
    const files = fs.readdirSync(CONFIG.rolesDir).filter(f => f.endsWith('.json'));
    roles = files.map(f => {
      try { return JSON.parse(fs.readFileSync(path.join(CONFIG.rolesDir, f), 'utf-8')); }
      catch { return null; }
    }).filter(Boolean);
    console.log(`  ${roles.length} 个角色`);
  } catch {}
}

function saveRole(roleData) {
  const filePath = path.join(CONFIG.rolesDir, `${roleData.id}.json`);
  fs.writeFileSync(filePath, JSON.stringify(roleData, null, 2), 'utf-8');
  loadRoles();
  broadcast({ type: 'roles_updated', data: { roles } });
}

// ═══════════════════════════════════════════════════════════
//  Task Templates
// ═══════════════════════════════════════════════════════════
let taskTemplates = [];

function loadTaskTemplates() {
  try {
    if (fs.existsSync(CONFIG.templatesFile)) {
      taskTemplates = JSON.parse(fs.readFileSync(CONFIG.templatesFile, 'utf-8'));
      console.log(`  ${taskTemplates.length} 个任务模板`);
    }
  } catch {}
}

function saveTaskTemplates() {
  fs.writeFileSync(CONFIG.templatesFile, JSON.stringify(taskTemplates, null, 2), 'utf-8');
}

function addTaskTemplate(data) {
  const tpl = {
    id: genId(), name: data.name || '未命名',
    roleId: data.roleId || null, taskTypeId: data.taskTypeId || null,
    projectPath: data.projectPath || '', plan: data.plan || '',
    createdAt: Date.now(),
  };
  taskTemplates.push(tpl);
  saveTaskTemplates();
  broadcast({ type: 'templates_updated', data: { taskTemplates } });
  return tpl;
}

function deleteTaskTemplate(id) {
  taskTemplates = taskTemplates.filter(t => t.id !== id);
  saveTaskTemplates();
  broadcast({ type: 'templates_updated', data: { taskTemplates } });
}

// ═══════════════════════════════════════════════════════════
//  持久化
// ═══════════════════════════════════════════════════════════
const INDEX_FILE = path.join(CONFIG.tasksDir, 'index.json');

function saveIndex() {
  const list = [];
  for (const [, t] of tasks) {
    list.push({
      id: t.id, name: t.name, type: t.type,
      roleId: t.roleId, taskTypeId: t.taskTypeId,
      projectPath: t.projectPath, taskDir: t.taskDir,
      status: t.status, phase: t.phase, phaseName: t.phaseName,
      createdAt: t.createdAt, startedAt: t.startedAt,
      completedAt: t.completedAt, error: t.error, results: t.results,
      followUps: t.followUps || [], parentId: t.parentId || null,
      childIds: t.childIds || [], retryCount: t.retryCount || 0,
      botPlatform: t.botPlatform || null, botChatId: t.botChatId || null,
    });
  }
  fs.writeFileSync(INDEX_FILE, JSON.stringify(list, null, 2), 'utf-8');
}

function loadIndex() {
  if (!fs.existsSync(INDEX_FILE)) return;
  try {
    const list = JSON.parse(fs.readFileSync(INDEX_FILE, 'utf-8'));
    for (const t of list) {
      if (t.status === 'running' || t.status === 'queued') {
        t.status = 'failed'; t.error = '服务重启中断'; t.completedAt = Date.now();
      }
      t.logs = []; t.currentProcess = null;
      t.followUps = t.followUps || [];
      t.childIds = t.childIds || [];
      t.retryCount = t.retryCount || 0;
      tasks.set(t.id, t);
    }
    console.log(`  ${list.length} 个历史任务`);
  } catch {}
}

// ═══════════════════════════════════════════════════════════
//  工具函数
// ═══════════════════════════════════════════════════════════
function genId() { return crypto.randomBytes(8).toString('hex'); }

function broadcast(msg) {
  const data = JSON.stringify(msg);
  for (const c of clients) { if (c.readyState === 1) c.send(data); }
}

function stripAnsi(str) {
  return str.replace(/\x1B$$[0-9;]*[a-zA-Z]/g, '').replace(/\x1B$$.*?\x07/g, '');
}

function serializeTask(t) {
  return {
    id: t.id, name: t.name, type: t.type,
    roleId: t.roleId, taskTypeId: t.taskTypeId,
    projectPath: t.projectPath, status: t.status,
    phase: t.phase, phaseName: t.phaseName,
    createdAt: t.createdAt, startedAt: t.startedAt,
    completedAt: t.completedAt, error: t.error,
    logCount: t.logs.length, results: t.results,
    followUps: (t.followUps || []).map(f => ({ prompt: f.prompt, time: f.time })),
    parentId: t.parentId || null, childIds: t.childIds || [],
    retryCount: t.retryCount || 0,
    botPlatform: t.botPlatform || null,
    duration: t.startedAt ? (t.completedAt || Date.now()) - t.startedAt : 0,
  };
}

function appendLog(task, text, stream) {
  const entry = { text, stream, time: Date.now() };
  task.logs.push(entry);
  try { fs.appendFileSync(path.join(task.taskDir, 'output.log'), text, 'utf-8'); } catch {}
  if (task.logs.length > 10000) task.logs = task.logs.slice(-8000);
  broadcast({ type: 'task_log', data: { taskId: task.id, entry } });
}

function loadLogs(taskId) {
  const task = tasks.get(taskId);
  if (!task) return [];
  if (task.logs.length > 0) return task.logs;
  const logFile = path.join(task.taskDir, 'output.log');
  if (fs.existsSync(logFile)) {
    try { return [{ text: fs.readFileSync(logFile, 'utf-8'), stream: 'stdout', time: task.createdAt }]; } catch {}
  }
  return [];
}

// ═══════════════════════════════════════════════════════════
//  WebSocket
// ═══════════════════════════════════════════════════════════
wss.on('connection', (ws) => {
  clients.add(ws);
  ws.send(JSON.stringify({
    type: 'init',
    data: { tasks: Array.from(tasks.values()).map(serializeTask), roles, taskTemplates },
  }));
  ws.on('message', (raw) => {
    try { handleMessage(ws, JSON.parse(raw)); } catch {}
  });
  ws.on('close', () => clients.delete(ws));
});

function handleMessage(ws, msg) {
  switch (msg.type) {
    case 'create_task': createTask(msg.data); break;
    case 'follow_up': handleFollowUp(msg.data); break;
    case 'cancel_task': cancelTask(msg.data.taskId); break;
    case 'delete_task': deleteTask(msg.data.taskId); break;
    case 'save_role': saveRole(msg.data); break;
    case 'save_template': {
      const tpl = addTaskTemplate(msg.data);
      ws.send(JSON.stringify({ type: 'template_saved', data: { template: tpl } }));
      break;
    }
    case 'delete_template': deleteTaskTemplate(msg.data.templateId); break;
    case 'plan_chat': handlePlanChat(ws, msg.data); break;
    case 'get_logs': {
      ws.send(JSON.stringify({ type: 'task_logs', data: { taskId: msg.data.taskId, logs: loadLogs(msg.data.taskId) } }));
      break;
    }
    case 'refresh_roles': {
      loadRoles();
      broadcast({ type: 'roles_updated', data: { roles } });
      break;
    }
  }
}

// ═══════════════════════════════════════════════════════════
//  AI 规划助手
// ═══════════════════════════════════════════════════════════
function handlePlanChat(ws, data) {
  const { message, history, roleId, taskTypeId } = data;
  const role = roles.find(r => r.id === roleId);
  const taskType = role ? role.tasks.find(t => t.id === taskTypeId) : null;

  const parts = [];
  if (role && role.persona) parts.push(`## 你的角色\n\n${role.persona}\n`);
  if (taskType) parts.push(`## 任务类型\n\n${taskType.icon} ${taskType.name}：${taskType.description}\n`);
  if (history && history.length > 0) {
    parts.push('## 对话历史\n');
    for (const h of history) parts.push(`**${h.role === 'user' ? '用户' : '助手'}**：${h.content}\n`);
  }
  parts.push(`## 用户最新消息\n\n${message}\n`);
  parts.push(`## 你的任务

你是一个任务规划助手。用户正在描述一个他想做的任务。

1. 如果需求模糊，提出具体问题帮用户理清
2. 如果需求清晰，输出结构化计划

如果需求已清晰，输出以下格式：

### 任务名称
（简洁的任务名）

### 项目路径
（建议路径）

### 技术方案
（技术选型和理由）

### 实现步骤
1. ...
2. ...

### 验收标准
- ...

用中文回复。`);

  const tempDir = path.join(CONFIG.tasksDir, '_planning');
  fs.mkdirSync(tempDir, { recursive: true });
  const planFile = path.join(tempDir, `plan_${Date.now()}.md`);
  fs.writeFileSync(planFile, parts.join('\n---\n\n'), 'utf-8');

  const escapedBin = `"${CONFIG.claudeBin}"`;
  const cmd = `${escapedBin} -p "读取 ${planFile} 并按照指令回复。直接输出，不要创建文件。" --max-turns 10`;

  let output = '';
  const proc = spawn(cmd, [], { cwd: tempDir, env: { ...process.env }, stdio: ['pipe', 'pipe', 'pipe'], shell: true });

  proc.stdout.on('data', (d) => {
    const text = stripAnsi(d.toString());
    output += text;
    ws.send(JSON.stringify({ type: 'plan_chat_stream', data: { text } }));
  });

  proc.on('close', () => {
    try { fs.unlinkSync(planFile); } catch {}
    ws.send(JSON.stringify({ type: 'plan_chat_done', data: { fullText: output } }));
  });

  proc.on('error', () => {
    try { fs.unlinkSync(planFile); } catch {}
    ws.send(JSON.stringify({ type: 'plan_chat_error', data: { message: '规划助手启动失败' } }));
  });
}

// ═══════════════════════════════════════════════════════════
//  任务创建 & 执行（与 v5 相同，省略重复部分）
// ═══════════════════════════════════════════════════════════
function createTask(data) {
  const id = genId();
  const taskDir = path.join(CONFIG.tasksDir, id);
  fs.mkdirSync(taskDir, { recursive: true });
  const projectPath = (data.projectPath || process.cwd()).replace(/\//g, '\\');
  fs.mkdirSync(projectPath, { recursive: true });

  const task = {
    id, name: data.name || '未命名任务', type: data.type || 'single',
    roleId: data.roleId || null, taskTypeId: data.taskTypeId || null,
    projectPath, taskDir, status: 'queued', phase: 0, phaseName: '等待中',
    createdAt: Date.now(), startedAt: null, completedAt: null,
    logs: [], error: null, currentProcess: null, results: null,
    plan: data.plan || '', followUps: [],
    parentId: null, childIds: [], retryCount: 0,
    botPlatform: null, botChatId: null,
  };

  if (data.type === 'parallel' && data.subTasks && data.subTasks.length > 0) {
    task.childIds = [];
    for (const sub of data.subTasks) {
      const subId = genId();
      const subDir = path.join(CONFIG.tasksDir, subId);
      fs.mkdirSync(subDir, { recursive: true });
      tasks.set(subId, {
        id: subId, name: sub.name || '子任务', type: 'single',
        roleId: task.roleId, taskTypeId: task.taskTypeId,
        projectPath, taskDir: subDir, status: 'queued', phase: 0, phaseName: '等待中',
        createdAt: Date.now(), startedAt: null, completedAt: null,
        logs: [], error: null, currentProcess: null, results: null,
        plan: sub.plan || '', followUps: [],
        parentId: id, childIds: [], retryCount: 0,
        botPlatform: null, botChatId: null,
      });
      task.childIds.push(subId);
    }
  }

  tasks.set(id, task);
  saveIndex();
  broadcast({ type: 'task_created', data: serializeTask(task) });
  if (task.childIds.length > 0) for (const cid of task.childIds) queue.push(cid);
  queue.push(id);
  tryExecuteNext();
}

const MAX_RETRY = 1;

function tryExecuteNext() {
  const pending = [];
  while (queue.length > 0) {
    const id = queue.shift();
    const task = tasks.get(id);
    if (!task) continue;
    if (task.type === 'parallel' && task.childIds && task.childIds.length > 0) {
      const allDone = task.childIds.every(cid => {
        const ct = tasks.get(cid);
        return ct && (ct.status === 'completed' || ct.status === 'failed' || ct.status === 'cancelled');
      });
      if (!allDone) { pending.push(id); continue; }
    }
    if (task.status === 'queued') {
      if (runningCount < CONFIG.maxConcurrent) {
        runningCount++;
        executeTask(task).finally(() => { runningCount--; tryExecuteNext(); });
      } else { pending.push(id); }
    }
  }
  for (const id of pending) queue.push(id);
}

async function executeTask(task) {
  task.status = 'running';
  task.startedAt = Date.now();
  saveIndex();
  broadcast({ type: 'task_updated', data: serializeTask(task) });

  try {
    if (task.type === 'parallel' && task.childIds && task.childIds.length > 0) {
      task.phase = 1; task.phaseName = '合并检查中';
      broadcast({ type: 'task_updated', data: serializeTask(task) });
      appendLog(task, '═══════════════════════════════════════\n  合并子任务结果\n═══════════════════════════════════════\n\n', 'system');
      const childSummary = task.childIds.map(cid => {
        const ct = tasks.get(cid);
        const s = ct.status === 'completed' ? '✓' : ct.status === 'failed' ? '✗' : '⚠';
        return `- ${ct.name}: ${s} ${ct.status}${ct.error ? ' → ' + ct.error : ''}`;
      }).join('\n');
      appendLog(task, childSummary + '\n\n', 'system');
      fs.writeFileSync(path.join(task.projectPath, 'PLAN.md'), buildMergePrompt(task, childSummary), 'utf-8');
      await runClaude(task, '读取 PLAN.md，按指令执行合并和检查。', 150);
      if (task.status !== 'cancelled') { task.status = 'completed'; task.results = collectResults(task); appendLog(task, '\n✓ 并行任务完成\n', 'system'); }
    } else {
      task.phase = 1; task.phaseName = '自主执行中';
      broadcast({ type: 'task_updated', data: serializeTask(task) });
      fs.writeFileSync(path.join(task.projectPath, 'PLAN.md'), buildPrompt(task), 'utf-8');
      appendLog(task, '═══════════════════════════════════════\n  开始自主执行\n═══════════════════════════════════════\n\n', 'system');
      await runClaude(task, '读取 PLAN.md，按指令逐步执行。直接开始。', 300);
      if (task.status !== 'cancelled') {
        task.phase = 2; task.phaseName = '检查结果';
        broadcast({ type: 'task_updated', data: serializeTask(task) });
        const rp = path.join(task.projectPath, 'REPORT_PLAN.md');
        fs.writeFileSync(rp, buildReportPrompt(task), 'utf-8');
        await runClaude(task, '读取 REPORT_PLAN.md，按指令生成报告。', 80);
        try { fs.unlinkSync(rp); } catch {}
      }
      if (task.status !== 'cancelled') { task.status = 'completed'; task.results = collectResults(task); appendLog(task, '\n✓ 任务完成\n', 'system'); }
    }
  } catch (err) {
    if (task.status === 'cancelled') {
      // 取消
    } else if (task.parentId && (task.retryCount || 0) < MAX_RETRY) {
      task.retryCount = (task.retryCount || 0) + 1;
      appendLog(task, `\n⚠ 失败，自动重试 (${task.retryCount}/${MAX_RETRY})...\n\n`, 'system');
      task.status = 'queued'; task.phase = 0; task.phaseName = '重试中';
      task.startedAt = null; task.completedAt = null;
      task.plan += `\n\n## 上次失败\n\n${err.message}\n请避免同样问题。`;
      saveIndex();
      broadcast({ type: 'task_updated', data: serializeTask(task) });
      queue.push(task.id);
      tryExecuteNext();
      return;
    } else {
      task.status = 'failed'; task.error = err.message;
      appendLog(task, `\n✗ 失败: ${err.message}\n`, 'system');
    }
  }

  task.completedAt = Date.now();
  if (task.currentProcess) { try { task.currentProcess.kill(); } catch {} task.currentProcess = null; }
  saveIndex();
  broadcast({ type: 'task_updated', data: serializeTask(task) });

  // 通知 Bot
  notifyBot(task);
}

// ═══════════════════════════════════════════════════════════
//  Claude 执行器
// ═══════════════════════════════════════════════════════════
function runClaude(task, shortPrompt, maxTurns) {
  return new Promise((resolve, reject) => {
    const baseTools = ['Read', 'Write', 'Edit', 'Bash', 'Glob', 'Grep', 'WebFetch'];
    if (task.roleId) {
      const role = roles.find(r => r.id === task.roleId);
      if (role && role.mcpServers) {
        for (const s of role.mcpServers) {
          if (s === 'github') baseTools.push('mcp__github__*');
          if (s === 'mysql') baseTools.push('mcp__mysql__*');
        }
      }
    }
    const toolsStr = baseTools.join(',');

    let skillHint = '';
    if (task.roleId) {
      const role = roles.find(r => r.id === task.roleId);
      if (role && role.skills && role.skills.length > 0) {
        skillHint = '\n\n可用技能：' + role.skills.join(', ') + '\n请在合适时主动使用。';
      }
    }

    const escapedBin = `"${CONFIG.claudeBin}"`;
    const escapedPrompt = (shortPrompt + skillHint).replace(/"/g, '\\"');
    const cmd = `${escapedBin} -p "${escapedPrompt}" --allowedTools "${toolsStr}" --max-turns ${maxTurns}`;

    appendLog(task, `[执行] ${cmd.substring(0, 200)}...\n\n`, 'system');

    let proc;
    try {
      proc = spawn(cmd, [], { cwd: task.projectPath, env: { ...process.env }, stdio: ['pipe', 'pipe', 'pipe'], shell: true });
    } catch (err) { return reject(new Error(`启动失败: ${err.message}`)); }

    task.currentProcess = proc;
    proc.stdout.on('data', (d) => appendLog(task, stripAnsi(d.toString()), 'stdout'));
    proc.stderr.on('data', (d) => appendLog(task, stripAnsi(d.toString()), 'stderr'));
    proc.on('close', (code) => {
      task.currentProcess = null;
      if (task.status === 'cancelled' || code === 0) resolve();
      else reject(new Error(`退出码 ${code}`));
    });
    proc.on('error', (err) => {
      task.currentProcess = null;
      if (err.code === 'ENOENT') reject(new Error('找不到 claude'));
      else reject(new Error(`进程错误: ${err.message}`));
    });
  });
}

// ═══════════════════════════════════════════════════════════
//  Prompt 构建器
// ═══════════════════════════════════════════════════════════
function getRoleContext(task) {
  if (!task.roleId) return '';
  const role = roles.find(r => r.id === task.roleId);
  if (!role || !role.persona) return '';
  return `## 你的身份\n\n${role.persona}\n`;
}

function getTaskTypeContext(task) {
  if (!task.roleId || !task.taskTypeId) return '';
  const role = roles.find(r => r.id === task.roleId);
  if (!role) return '';
  const tt = role.tasks.find(t => t.id === task.taskTypeId);
  if (!tt) return '';
  return `## 任务类型：${tt.icon} ${tt.name}\n\n${tt.description || ''}\n${tt.promptSuffix || ''}\n`;
}

function buildPrompt(task) {
  const parts = [];
  const rc = getRoleContext(task); if (rc) parts.push(rc);
  const tc = getTaskTypeContext(task); if (tc) parts.push(tc);
  parts.push(`## 工作目录\n\n${task.projectPath}\n所有文件都在这个目录下。\n`);
  parts.push(`## 任务需求\n\n${task.plan}\n`);

  if (task.roleId) {
    const role = roles.find(r => r.id === task.roleId);
    if (role && role.skills && role.skills.length > 0) {
      let guide = '## 技能指南\n\n你有以下技能，请在合适时主动使用：\n';
      for (const s of role.skills) {
        if (s.includes('brainstorming')) guide += '- brainstorming：动手前先头脑风暴\n';
        else if (s.includes('writing-plans') || s.includes('write-plan')) guide += '- writing-plans：先编写开发计划\n';
        else if (s.includes('executing-plans')) guide += '- executing-plans：按计划执行\n';
        else if (s.includes('subagent')) guide += '- subagent-driven：复杂任务拆分子任务\n';
        else if (s.includes('code-review')) guide += '- code-review：完成后做代码审查\n';
        else if (s.includes('exa-search')) guide += '- exa-search：搜索最新信息\n';
        else if (s.includes('browser')) guide += '- browser-automation：浏览器抓取\n';
        else if (s.includes('python')) guide += '- python-review：Python 代码审查\n';
        else if (s.includes('docs-consistency')) guide += '- docs-consistency：检查文档一致性\n';
      }
      parts.push(guide);
    }
  }

  parts.push(`## 执行规则（必须遵守）

1. 仔细阅读「任务需求」
2. 如果要求创建新子目录，创建后在里面工作
3. 需要外部信息就用 WebFetch
4. 自主完成，不要等确认
5. 遇到错误自行修复
6. 确保产出物真实可用
7. 完成后在 docs/ 下生成 completed.md、issues.md、remaining.md
8. 不要编造内容`);

  return parts.join('\n---\n\n');
}

function buildReportPrompt(task) {
  return `检查 ${task.projectPath}/docs/ 下的报告是否真实准确。如果不存在或不准确，基于实际文件重新生成。不要编造。`;
}

function buildMergePrompt(task, childSummary) {
  const rc = getRoleContext(task);
  return `${rc ? rc + '\n---\n\n' : ''}## 子任务结果\n\n${childSummary}\n\n## 合并规则\n\n1. 检查产出文件\n2. 合并冲突\n3. 生成 docs/ 报告\n4. 直接执行`;
}

// ── 迭代 ──
function handleFollowUp(data) {
  const task = tasks.get(data.taskId);
  if (!task || task.status === 'running') return;
  task.followUps = task.followUps || [];
  task.followUps.push({ prompt: data.prompt, time: Date.now() });
  task.status = 'running'; task.phaseName = '迭代中'; task.completedAt = null;
  saveIndex();
  broadcast({ type: 'task_updated', data: serializeTask(task) });

  const history = (task.followUps || []).map((f, i) => `- 第${i + 1}次：${f.prompt}`).join('\n');
  const rc = getRoleContext(task);
  fs.writeFileSync(path.join(task.projectPath, 'PLAN.md'),
      `${rc ? rc + '\n---\n\n' : ''}## 背景\n\n你之前在 ${task.projectPath} 完成了工作。\n\n${history ? `## 追加记录\n\n${history}\n\n` : ''}## 本次需求\n\n${data.prompt}\n\n## 规则\n\n1. 查看现有文件\n2. 执行新需求\n3. 不破坏已有功能\n4. 更新 docs/\n5. 直接执行`, 'utf-8');

  runningCount++;
  runClaude(task, '读取 PLAN.md，按指令执行。', 200)
      .then(() => { if (task.status !== 'cancelled') { task.status = 'completed'; task.results = collectResults(task); } })
      .catch(err => { if (task.status !== 'cancelled') { task.status = 'failed'; task.error = err.message; } })
      .finally(() => {
        task.completedAt = Date.now();
        if (task.currentProcess) { try { task.currentProcess.kill(); } catch {} task.currentProcess = null; }
        runningCount--;
        saveIndex();
        broadcast({ type: 'task_updated', data: serializeTask(task) });
        notifyBot(task);
        tryExecuteNext();
      });
}

// ═══════════════════════════════════════════════════════════
//  任务控制
// ═══════════════════════════════════════════════════════════
function cancelTask(id) {
  const task = tasks.get(id);
  if (!task || task.status === 'completed' || task.status === 'failed') return;
  task.status = 'cancelled';
  for (const cid of (task.childIds || [])) { const ct = tasks.get(cid); if (ct && (ct.status === 'running' || ct.status === 'queued')) cancelTask(cid); }
  if (task.currentProcess) { try { task.currentProcess.kill('SIGTERM'); } catch {} task.currentProcess = null; }
  task.completedAt = Date.now();
  appendLog(task, '\n⚠ 已取消\n', 'system');
  saveIndex();
  broadcast({ type: 'task_updated', data: serializeTask(task) });
}

function deleteTask(id) {
  const task = tasks.get(id);
  if (!task) return;
  for (const cid of (task.childIds || [])) tasks.delete(cid);
  cancelTask(id);
  tasks.delete(id);
  saveIndex();
  broadcast({ type: 'task_deleted', data: { taskId: id } });
}

function collectResults(task) {
  const docsDir = path.join(task.projectPath, 'docs');
  const files = [];
  if (fs.existsSync(docsDir)) {
    for (const entry of fs.readdirSync(docsDir)) {
      const fp = path.join(docsDir, entry);
      try { const stat = fs.statSync(fp); if (stat.isFile()) files.push({ name: entry, path: fp, size: stat.size, modified: stat.mtimeMs }); } catch {}
    }
  }
  return { files };
}

// ═══════════════════════════════════════════════════════════
//  API
// ═══════════════════════════════════════════════════════════
app.get('/api/file', (req, res) => {
  const filePath = req.query.path;
  if (!filePath) return res.status(400).json({ error: 'Missing path' });
  try { res.json({ content: fs.readFileSync(filePath, 'utf-8') }); }
  catch { res.status(404).json({ error: 'File not found' }); }
});

// ═══════════════════════════════════════════════════════════
//  启动
// ═══════════════════════════════════════════════════════════
loadRoles();
loadTaskTemplates();
loadIndex();
initFeishu();
initDingTalk();

server.listen(CONFIG.port, CONFIG.host, () => {
  const interfaces = os.networkInterfaces();
  const ips = [];
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) ips.push(iface.address);
    }
  }
  console.log('');
  console.log('  ┌──────────────────────────────────────┐');
  console.log('  │       CLAUDE HUB v6 · Ready          │');
  console.log('  ├──────────────────────────────────────┤');
  console.log(`  │  Local:   http://localhost:${CONFIG.port}      │`);
  for (const ip of ips) console.log(`  │  Network: http://${ip}:${CONFIG.port} │`);
  console.log('  └──────────────────────────────────────┘');
  console.log('');
});

process.on('SIGINT', () => {
  console.log('\n正在关闭...');
  for (const [, task] of tasks) { if (task.currentProcess) { try { task.currentProcess.kill('SIGTERM'); } catch {} } }
  server.close();
  process.exit(0);
});
