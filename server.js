// ═══════════════════════════════════════════════════════════
//  Claude Hub v8.1 — 双引擎：Chat + Task
//  Chat 引擎：DeepSeek API → 飞书单聊快速对话
//  Task 引擎：Claude Code CLI → Web 界面自主执行
//  新增：项目记忆 · 智能调度 · 飞书去重 · UI 优化
// ═══════════════════════════════════════════════════════════

const express = require('express');
const { WebSocketServer } = require('ws');
const http = require('http');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const os = require('os');

// ── 配置 ─────────────────────────────────────────────────
const CONFIG = {
  port: parseInt(process.env.PORT) || 3800,
  host: '0.0.0.0',
  claudeBin: process.env.CLAUDE_BIN || 'D:\\theapps\\vue\\node_global\\claude.cmd',
  maxConcurrent: parseInt(process.env.MAX_CONCURRENT) || 3,
  baseDir: __dirname,
  tasksDir: path.join(__dirname, 'tasks'),
  rolesDir: path.join(__dirname, 'roles'),
  templatesFile: path.join(__dirname, 'task-templates.json'),
  publicUrl: process.env.PUBLIC_URL || '',

  // DeepSeek API
  deepseekApiKey: process.env.DEEPSEEK_API_KEY || '',
  deepseekModel: process.env.DEEPSEEK_MODEL || 'deepseek-chat',

  // 飞书
  feishuAppId: process.env.FEISHU_APP_ID || '',
  feishuAppSecret: process.env.FEISHU_APP_SECRET || '',
  feishuVerificationToken: process.env.FEISHU_VERIFICATION_TOKEN || '',
  feishuEncryptKey: process.env.FEISHU_ENCRYPT_KEY || '',
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
//  Chat 引擎 — DeepSeek API
// ═══════════════════════════════════════════════════════════

const chatSessions = new Map();

function getChatSession(userId) {
  if (!chatSessions.has(userId)) chatSessions.set(userId, { messages: [], createdAt: Date.now() });
  return chatSessions.get(userId);
}

function trimChatHistory(session, maxMessages = 20) {
  if (session.messages.length > maxMessages) session.messages = session.messages.slice(-maxMessages);
}

function detectIntent(text) {
  const taskKeywords = ['帮我创建','帮我开发','帮我写一个','帮我做一个','新建项目','创建项目','开发一个','搭建','/task','执行任务','运行任务'];
  for (const kw of taskKeywords) { if (text.includes(kw)) return 'task'; }
  return 'chat';
}

async function callDeepSeek(messages, systemPrompt) {
  if (!CONFIG.deepseekApiKey) return 'DeepSeek API Key 未配置。请设置 DEEPSEEK_API_KEY 环境变量。';
  const apiMessages = [];
  if (systemPrompt) apiMessages.push({ role: 'system', content: systemPrompt });
  for (const m of messages) apiMessages.push({ role: m.role, content: m.content });
  try {
    const resp = await fetch('https://api.deepseek.com/v1/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${CONFIG.deepseekApiKey}` },
      body: JSON.stringify({ model: CONFIG.deepseekModel, messages: apiMessages, temperature: 0.7, max_tokens: 2048 }),
    });
    const data = await resp.json();
    if (data.choices && data.choices[0] && data.choices[0].message) return data.choices[0].message.content;
    if (data.error) return `DeepSeek API 错误: ${data.error.message || '未知错误'}`;
    return '抱歉，AI 暂时无法回复。';
  } catch (err) {
    console.error('DeepSeek API 调用失败:', err.message);
    return `AI 服务暂时不可用：${err.message}`;
  }
}

async function handleChatMessage(userId, userName, text) {
  const session = getChatSession(userId);
  const intent = detectIntent(text);
  if (intent === 'task') {
    const webUrl = CONFIG.publicUrl || `http://localhost:${CONFIG.port}`;
    return `这是一个完整的开发任务，建议到 Web 界面执行：\n\n${webUrl}\n\n在 Web 界面你可以：\n- 选择角色和任务类型\n- 自主执行完整开发流程\n- 查看实时日志和进度\n- 任务完成后迭代修改\n\n如果你想让我先分析需求，直接说就好。`;
  }
  session.messages.push({ role: 'user', content: text });
  trimChatHistory(session);
  const systemPrompt = `你是 Claude Hub 的 AI 助手。你是一个技术顾问和助手。\n\n能力：回答技术问题、梳理方案、解释代码、提供建议。\n规则：用中文回复，技术术语保留英文，回答简洁直接。`;
  const reply = await callDeepSeek(session.messages, systemPrompt);
  session.messages.push({ role: 'assistant', content: reply });
  trimChatHistory(session);
  return reply;
}

function clearChatSession(userId) { chatSessions.delete(userId); }

// ═══════════════════════════════════════════════════════════
//  飞书集成（含消息去重）
// ═══════════════════════════════════════════════════════════
let feishuToken = '';
let feishuTokenExpiry = 0;

// 飞书消息去重（防止重复处理同一条消息）
const feishuProcessedMsgs = new Set();

function isDuplicateMessage(messageId) {
  if (feishuProcessedMsgs.has(messageId)) return true;
  feishuProcessedMsgs.add(messageId);
  // 定期清理，防止内存泄漏
  if (feishuProcessedMsgs.size > 1000) {
    const arr = [...feishuProcessedMsgs];
    for (let i = 0; i < 500; i++) feishuProcessedMsgs.delete(arr[i]);
  }
  return false;
}

function isFeishuEnabled() { return !!(CONFIG.feishuAppId && CONFIG.feishuAppSecret); }

async function getFeishuToken() {
  if (Date.now() < feishuTokenExpiry) return feishuToken;
  try {
    const resp = await fetch('https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ app_id: CONFIG.feishuAppId, app_secret: CONFIG.feishuAppSecret }),
    });
    const data = await resp.json();
    if (data.tenant_access_token) { feishuToken = data.tenant_access_token; feishuTokenExpiry = Date.now() + (data.expire - 60) * 1000; }
  } catch (e) { console.error('飞书 Token 获取失败:', e.message); }
  return feishuToken;
}

async function replyFeishuMessage(messageId, text) {
  if (!isFeishuEnabled()) return;
  try {
    const token = await getFeishuToken();
    await fetch(`https://open.feishu.cn/open-apis/im/v1/messages/${messageId}/reply`, {
      method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
      body: JSON.stringify({ msg_type: 'text', content: JSON.stringify({ text }) }),
    });
  } catch (e) { console.error('飞书回复失败:', e.message); }
}

async function sendFeishuMessage(chatId, text) {
  if (!isFeishuEnabled()) return;
  try {
    const token = await getFeishuToken();
    await fetch('https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id', {
      method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
      body: JSON.stringify({ receive_id: chatId, msg_type: 'text', content: JSON.stringify({ text }) }),
    });
  } catch (e) { console.error('飞书发送失败:', e.message); }
}

app.get('/feishu/webhook', (req, res) => {
  res.json({ status: 'ok', service: 'Claude Hub', feishuEnabled: isFeishuEnabled(), chatEngine: CONFIG.deepseekApiKey ? 'DeepSeek' : '未配置' });
});

app.post('/feishu/webhook', async (req, res) => {
  const body = req.body;
  if (body.type === 'url_verification') return res.json({ challenge: body.challenge });
  if (body.header && body.header.event_type === 'im.message.receive_v1') {
    const event = body.event, msg = event.message, sender = event.sender;
    if (msg.message_type !== 'text') return res.json({ ok: true });
    const content = JSON.parse(msg.content);
    let text = (content.text || '').trim().replace(/@_user_\d+/g, '').trim();
    if (!text) return res.json({ ok: true });

    const messageId = msg.message_id;

    // 去重：如果已经处理过这条消息，直接返回（不浪费 API 调用）
    if (isDuplicateMessage(messageId)) {
      console.log(`[飞书] 跳过重复消息: ${messageId}`);
      return res.json({ ok: true });
    }

    const userId = sender.sender_id.user_id || sender.sender_id.open_id || 'unknown';
    console.log(`[飞书] ${msg.chat_type === 'p2p' ? '单聊' : '群聊'} ${userId}: ${text.substring(0, 50)}`);

    if (text === '/clear' || text === '清除记忆' || text === '新对话') {
      clearChatSession(userId);
      await replyFeishuMessage(messageId, '对话已清空。');
      return res.json({ ok: true });
    }
    if (text === '/help' || text === '帮助') {
      await replyFeishuMessage(messageId, `Claude Hub 助手\n\n直接发消息即可对话。\n/clear — 清除对话\n/help — 帮助\n\nWeb 界面：${CONFIG.publicUrl || 'http://localhost:' + CONFIG.port}`);
      return res.json({ ok: true });
    }
    try {
      const reply = await handleChatMessage(userId, '用户', text);
      await replyFeishuMessage(messageId, reply);
    } catch (err) {
      console.error('[飞书] 处理失败:', err.message);
      await replyFeishuMessage(messageId, '处理消息时出错了，请稍后再试。');
    }
  }
  res.json({ ok: true });
});

// ═══════════════════════════════════════════════════════════
//  钉钉集成
// ═══════════════════════════════════════════════════════════
const DINGTALK = { webhookToken: process.env.DINGTALK_WEBHOOK_TOKEN || '', webhookSecret: process.env.DINGTALK_WEBHOOK_SECRET || '', enabled: false };

function initDingTalk() {
  DINGTALK.enabled = !!DINGTALK.webhookToken;
  console.log(`  钉钉通知: ${DINGTALK.enabled ? '已启用' : '未配置'}`);
}

async function sendDingTalkMessage(title, text) {
  if (!DINGTALK.enabled) return;
  try {
    let url = `https://oapi.dingtalk.com/robot/send?access_token=${DINGTALK.webhookToken}`;
    if (DINGTALK.webhookSecret) {
      const timestamp = Date.now();
      const stringToSign = `${timestamp}\n${DINGTALK.webhookSecret}`;
      const sign = encodeURIComponent(crypto.createHmac('sha256', DINGTALK.webhookSecret).update(stringToSign).digest('base64'));
      url += `&timestamp=${timestamp}&sign=${sign}`;
    }
    await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ msgtype: 'markdown', markdown: { title, text: `Claude Hub ${title}\n\n${text}` } }) });
  } catch (e) { console.error('钉钉发送失败:', e.message); }
}

// ═══════════════════════════════════════════════════════════
//  Roles
// ═══════════════════════════════════════════════════════════
let roles = [];

function loadRoles() {
  try {
    const files = fs.readdirSync(CONFIG.rolesDir).filter(f => f.endsWith('.json'));
    roles = files.map(f => { try { return JSON.parse(fs.readFileSync(path.join(CONFIG.rolesDir, f), 'utf-8')); } catch { return null; } }).filter(Boolean);
    console.log(`  ${roles.length} 个角色`);
  } catch {}
}

function saveRole(roleData) {
  fs.writeFileSync(path.join(CONFIG.rolesDir, `${roleData.id}.json`), JSON.stringify(roleData, null, 2), 'utf-8');
  loadRoles();
  broadcast({ type: 'roles_updated', data: { roles } });
}

// ═══════════════════════════════════════════════════════════
//  Task Templates
// ═══════════════════════════════════════════════════════════
let taskTemplates = [];

function loadTaskTemplates() {
  try {
    if (fs.existsSync(CONFIG.templatesFile)) { taskTemplates = JSON.parse(fs.readFileSync(CONFIG.templatesFile, 'utf-8')); console.log(`  ${taskTemplates.length} 个模板`); }
  } catch {}
}

function saveTaskTemplates() { fs.writeFileSync(CONFIG.templatesFile, JSON.stringify(taskTemplates, null, 2), 'utf-8'); }

function addTaskTemplate(data) {
  const tpl = { id: genId(), name: data.name || '未命名', roleId: data.roleId || null, taskTypeId: data.taskTypeId || null, projectPath: data.projectPath || '', plan: data.plan || '', createdAt: Date.now() };
  taskTemplates.push(tpl); saveTaskTemplates();
  broadcast({ type: 'templates_updated', data: { taskTemplates } });
  return tpl;
}

function deleteTaskTemplate(id) {
  taskTemplates = taskTemplates.filter(t => t.id !== id);
  saveTaskTemplates();
  broadcast({ type: 'templates_updated', data: { taskTemplates } });
}

// ═══════════════════════════════════════════════════════════
//  项目记忆系统
// ═══════════════════════════════════════════════════════════

function getMemoryPath(projectPath) {
  return path.join(projectPath, '.claude-hub', 'memory.json');
}

function loadProjectMemory(projectPath) {
  try {
    const p = getMemoryPath(projectPath);
    if (fs.existsSync(p)) return JSON.parse(fs.readFileSync(p, 'utf-8'));
  } catch {}
  return null;
}

function saveProjectMemory(projectPath, memory) {
  const dir = path.join(projectPath, '.claude-hub');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(getMemoryPath(projectPath), JSON.stringify(memory, null, 2), 'utf-8');
}

async function extractTaskMemory(task) {
  if (!CONFIG.deepseekApiKey) {
    const memory = loadProjectMemory(task.projectPath) || { projectPath: task.projectPath, createdAt: Date.now(), techStack: [], architecture: '', decisions: [], patterns: [], tasks: [] };
    memory.tasks.push({ id: task.id, name: task.name, completedAt: Date.now(), summary: task.plan.substring(0, 100), issues: [] });
    if (memory.tasks.length > 20) memory.tasks = memory.tasks.slice(-20);
    memory.lastUpdated = Date.now();
    saveProjectMemory(task.projectPath, memory);
    broadcast({ type: 'memory_updated', data: { projectPath: task.projectPath, memory } });
    return;
  }

  const logText = task.logs.slice(-200).map(l => l.text).join('\n');
  if (logText.length < 200) return;

  try {
    const reply = await callDeepSeek([{
      role: 'user',
      content: `分析以下任务执行日志，提取项目关键信息。只输出JSON，不要其他内容。

任务名称：${task.name}
任务需求：${task.plan.substring(0, 500)}

日志摘要：
${logText.substring(0, 3000)}

输出格式（严格JSON）：
{"techStack":["技术1","技术2"],"architecture":"架构描述","keyFiles":["重要文件"],"decisions":["关键决策"],"patterns":["代码模式"],"issues":["问题和解决方案"],"summary":"一句话总结"}`
    }], '你是代码分析助手。只输出JSON，不要输出任何其他文字或markdown标记。');

    const match = reply.match(/\{[\s\S]*\}/);
    if (!match) return;

    const extracted = JSON.parse(match[0]);
    const memory = loadProjectMemory(task.projectPath) || { projectPath: task.projectPath, createdAt: Date.now(), techStack: [], architecture: '', decisions: [], patterns: [], tasks: [] };

    for (const t of (extracted.techStack || [])) { if (!memory.techStack.includes(t)) memory.techStack.push(t); }
    if (extracted.architecture) memory.architecture = extracted.architecture;
    for (const d of (extracted.decisions || [])) memory.decisions.push({ date: Date.now(), text: d });
    for (const p of (extracted.patterns || [])) { if (!memory.patterns.includes(p)) memory.patterns.push(p); }

    memory.tasks.push({ id: task.id, name: task.name, completedAt: Date.now(), summary: extracted.summary || '', issues: extracted.issues || [] });
    if (memory.tasks.length > 20) memory.tasks = memory.tasks.slice(-20);
    memory.lastUpdated = Date.now();
    saveProjectMemory(task.projectPath, memory);
    broadcast({ type: 'memory_updated', data: { projectPath: task.projectPath, memory } });
    console.log(`[Memory] 已更新: ${task.projectPath}`);
  } catch (err) {
    console.error('[Memory] 提取失败:', err.message);
  }
}

function getMemoryContext(projectPath) {
  const memory = loadProjectMemory(projectPath);
  if (!memory || !memory.tasks || memory.tasks.length === 0) return '';

  const parts = ['## 项目历史（自动注入，请基于此继续工作）\n'];
  if (memory.techStack.length) parts.push(`技术栈：${memory.techStack.join(', ')}`);
  if (memory.architecture) parts.push(`架构：${memory.architecture}`);
  if (memory.patterns.length) parts.push(`代码模式：${memory.patterns.join('; ')}`);
  if (memory.decisions.length) parts.push(`近期决策：${memory.decisions.slice(-5).map(d => d.text).join('; ')}`);

  parts.push('\n近期完成的任务：');
  for (const t of memory.tasks.slice(-3)) parts.push(`- ${t.name}: ${t.summary}${t.issues && t.issues.length ? ' (问题: ' + t.issues.slice(0, 2).join(';') + ')' : ''}`);
  parts.push('\n请基于以上历史继续工作，避免重复已完成的内容。如果之前有未解决的问题，请一并处理。');

  return parts.join('\n');
}

// ═══════════════════════════════════════════════════════════
//  智能角色推荐
// ═══════════════════════════════════════════════════════════

function suggestRoles(planText) {
  if (!planText || planText.length < 5) return [];
  const text = planText.toLowerCase();
  const results = [];

  const keywordMap = {
    developer: ['开发','代码','编程','实现','创建','搭建','功能','api','接口','前端','后端','全栈','app','网站','页面','组件','模块','crud','登录','注册','管理后台'],
    researcher: ['研究','调研','分析','对比','评估','技术选型','benchmark','性能测试','方案对比','选型'],
    writer: ['文档','写作','文章','教程','博客','readme','说明','手册','报告','总结'],
    'project-owner': ['接手','重构','优化','改进','二次开发','维护','升级','迁移','改造'],
    'data-analyst': ['数据','sql','报表','分析','统计','可视化','图表','dashboard','大屏','etl'],
    devops: ['部署','运维','docker','ci/cd','nginx','服务器','监控','pipeline','容器','k8s','自动化部署'],
  };

  for (const role of roles) {
    let score = 0;
    const kws = keywordMap[role.id] || [];
    for (const kw of kws) { if (text.includes(kw)) score += 3; }
    for (const tt of (role.tasks || [])) {
      const ttName = (tt.name || '').toLowerCase();
      const ttDesc = (tt.description || '').toLowerCase();
      if (text.includes(ttName)) score += 5;
      for (const word of text.split(/[\s,，。、；;]+/)) {
        if (word.length > 1 && (ttName.includes(word) || ttDesc.includes(word))) score += 1;
      }
    }
    if (score > 0) results.push({ roleId: role.id, name: role.name, icon: role.icon, score });
  }

  return results.sort((a, b) => b.score - a.score).slice(0, 3);
}

// ═══════════════════════════════════════════════════════════
//  持久化
// ═══════════════════════════════════════════════════════════
const INDEX_FILE = path.join(CONFIG.tasksDir, 'index.json');

function saveIndex() {
  const list = [];
  for (const [, t] of tasks) {
    list.push({
      id: t.id, name: t.name, type: t.type, roleId: t.roleId, taskTypeId: t.taskTypeId,
      projectPath: t.projectPath, taskDir: t.taskDir, status: t.status, phase: t.phase, phaseName: t.phaseName,
      createdAt: t.createdAt, startedAt: t.startedAt, completedAt: t.completedAt, error: t.error, results: t.results,
      followUps: t.followUps || [], parentId: t.parentId || null, childIds: t.childIds || [], retryCount: t.retryCount || 0,
    });
  }
  fs.writeFileSync(INDEX_FILE, JSON.stringify(list, null, 2), 'utf-8');
}

function loadIndex() {
  if (!fs.existsSync(INDEX_FILE)) return;
  try {
    const list = JSON.parse(fs.readFileSync(INDEX_FILE, 'utf-8'));
    for (const t of list) {
      if (t.status === 'running' || t.status === 'queued') { t.status = 'failed'; t.error = '服务重启中断'; t.completedAt = Date.now(); }
      t.logs = []; t.currentProcess = null; t.followUps = t.followUps || []; t.childIds = t.childIds || []; t.retryCount = t.retryCount || 0;
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

function stripAnsi(str) { return str.replace(/\x1B$$[0-9;]*[a-zA-Z]/g, '').replace(/\x1B$$.*?\x07/g, ''); }

function serializeTask(t) {
  return {
    id: t.id, name: t.name, type: t.type, roleId: t.roleId, taskTypeId: t.taskTypeId,
    projectPath: t.projectPath, status: t.status, phase: t.phase, phaseName: t.phaseName,
    createdAt: t.createdAt, startedAt: t.startedAt, completedAt: t.completedAt, error: t.error,
    logCount: t.logs.length, results: t.results,
    followUps: (t.followUps || []).map(f => ({ prompt: f.prompt, time: f.time })),
    parentId: t.parentId || null, childIds: t.childIds || [], retryCount: t.retryCount || 0,
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
  if (fs.existsSync(logFile)) { try { return [{ text: fs.readFileSync(logFile, 'utf-8'), stream: 'stdout', time: task.createdAt }]; } catch {} }
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
  ws.on('message', (raw) => { try { handleMessage(ws, JSON.parse(raw)); } catch {} });
  ws.on('close', () => clients.delete(ws));
});

function handleMessage(ws, msg) {
  switch (msg.type) {
    case 'create_task': createTask(msg.data); break;
    case 'follow_up': handleFollowUp(msg.data); break;
    case 'cancel_task': cancelTask(msg.data.taskId); break;
    case 'delete_task': deleteTask(msg.data.taskId); break;
    case 'save_role': saveRole(msg.data); break;
    case 'save_template': { const tpl = addTaskTemplate(msg.data); ws.send(JSON.stringify({ type: 'template_saved', data: { template: tpl } })); break; }
    case 'delete_template': deleteTaskTemplate(msg.data.templateId); break;
    case 'plan_chat': handlePlanChat(ws, msg.data); break;
    case 'get_logs': ws.send(JSON.stringify({ type: 'task_logs', data: { taskId: msg.data.taskId, logs: loadLogs(msg.data.taskId) } })); break;
    case 'refresh_roles': loadRoles(); broadcast({ type: 'roles_updated', data: { roles } }); break;
    case 'get_memory': {
      const memory = loadProjectMemory(msg.data.projectPath);
      ws.send(JSON.stringify({ type: 'memory_data', data: { projectPath: msg.data.projectPath, memory } }));
      break;
    }
    case 'clear_memory': {
      try { fs.unlinkSync(getMemoryPath(msg.data.projectPath)); } catch {}
      ws.send(JSON.stringify({ type: 'memory_data', data: { projectPath: msg.data.projectPath, memory: null } }));
      break;
    }
    case 'suggest_role': {
      const suggestions = suggestRoles(msg.data.plan || '');
      ws.send(JSON.stringify({ type: 'role_suggestions', data: { suggestions } }));
      break;
    }
  }
}

// ═══════════════════════════════════════════════════════════
//  AI 规划助手（模拟流式输出）
// ═══════════════════════════════════════════════════════════
function handlePlanChat(ws, data) {
  const { message, history, roleId, taskTypeId } = data;
  const role = roles.find(r => r.id === roleId);
  const taskType = role ? role.tasks.find(t => t.id === taskTypeId) : null;

  const systemParts = ['你是一个任务规划助手。帮用户理清需求并生成结构化计划。'];
  if (role && role.persona) systemParts.push(`角色背景：${role.persona}`);
  if (taskType) systemParts.push(`任务类型：${taskType.name} — ${taskType.description}`);
  systemParts.push(`如果需求清晰，输出以下格式：\n### 任务名称\n### 项目路径\n### 技术方案\n### 实现步骤\n### 验收标准\n\n如果需求模糊，提出问题帮用户理清。用中文回复。`);

  const messages = [];
  if (history) for (const h of history) messages.push({ role: h.role, content: h.content });
  messages.push({ role: 'user', content: message });

  callDeepSeek(messages, systemParts.join('\n')).then(async reply => {
    const chars = [...reply];
    for (let i = 0; i < chars.length; i += 3) {
      const chunk = chars.slice(i, i + 3).join('');
      ws.send(JSON.stringify({ type: 'plan_chat_stream', data: { text: chunk } }));
      await new Promise(r => setTimeout(r, 15));
    }
    ws.send(JSON.stringify({ type: 'plan_chat_done', data: { fullText: reply } }));
  }).catch(err => {
    ws.send(JSON.stringify({ type: 'plan_chat_error', data: { message: err.message } }));
  });
}

// ═══════════════════════════════════════════════════════════
//  Task 引擎 — Claude Code CLI
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
    plan: data.plan || '', followUps: [], parentId: null, childIds: [], retryCount: 0,
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
        plan: sub.plan || '', followUps: [], parentId: id, childIds: [], retryCount: 0,
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
      const allDone = task.childIds.every(cid => { const ct = tasks.get(cid); return ct && (ct.status === 'completed' || ct.status === 'failed' || ct.status === 'cancelled'); });
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
  task.status = 'running'; task.startedAt = Date.now();
  saveIndex();
  broadcast({ type: 'task_updated', data: serializeTask(task) });

  try {
    if (task.type === 'parallel' && task.childIds && task.childIds.length > 0) {
      task.phase = 1; task.phaseName = '合并检查中';
      broadcast({ type: 'task_updated', data: serializeTask(task) });
      appendLog(task, '══════ 合并子任务结果 ══════\n\n', 'system');
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
      appendLog(task, '══════ 开始自主执行 ══════\n\n', 'system');
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
    if (task.status === 'cancelled') { /* 取消 */ }
    else if (task.parentId && (task.retryCount || 0) < MAX_RETRY) {
      task.retryCount = (task.retryCount || 0) + 1;
      appendLog(task, `\n⚠ 失败，自动重试 (${task.retryCount}/${MAX_RETRY})...\n\n`, 'system');
      task.status = 'queued'; task.phase = 0; task.phaseName = '重试中';
      task.startedAt = null; task.completedAt = null;
      task.plan += `\n\n## 上次失败\n\n${err.message}\n请避免同样问题。`;
      saveIndex(); broadcast({ type: 'task_updated', data: serializeTask(task) });
      queue.push(task.id); tryExecuteNext(); return;
    } else {
      task.status = 'failed'; task.error = err.message;
      appendLog(task, `\n✗ 失败: ${err.message}\n`, 'system');
    }
  }

  task.completedAt = Date.now();
  if (task.currentProcess) { try { task.currentProcess.kill(); } catch {} task.currentProcess = null; }
  saveIndex();
  broadcast({ type: 'task_updated', data: serializeTask(task) });

  // 任务完成后提取项目记忆
  if (task.status === 'completed') {
    extractTaskMemory(task).catch(err => console.error('[Memory] 后台提取失败:', err.message));
  }

  // 钉钉通知
  if (DINGTALK.enabled) {
    const emoji = task.status === 'completed' ? '✅' : '❌';
    sendDingTalkMessage(`${emoji} ${task.name}`, `${task.status === 'completed' ? '已完成' : '失败'}\n${task.error || ''}`).catch(() => {});
  }
}

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

    let skillHint = '';
    if (task.roleId) {
      const role = roles.find(r => r.id === task.roleId);
      if (role && role.skills && role.skills.length > 0) {
        skillHint = '\n\n可用技能：' + role.skills.join(', ') + '\n请在合适时主动使用。';
      }
    }

    const escapedBin = `"${CONFIG.claudeBin}"`;
    const escapedPrompt = (shortPrompt + skillHint).replace(/"/g, '\\"');
    const cmd = `${escapedBin} -p "${escapedPrompt}" --allowedTools "${baseTools.join(',')}" --max-turns ${maxTurns}`;

    appendLog(task, `[执行] ${cmd.substring(0, 200)}...\n\n`, 'system');

    let proc;
    try { proc = spawn(cmd, [], { cwd: task.projectPath, env: { ...process.env }, stdio: ['pipe', 'pipe', 'pipe'], shell: true }); }
    catch (err) { return reject(new Error(`启动失败: ${err.message}`)); }

    task.currentProcess = proc;
    proc.stdout.on('data', (d) => appendLog(task, stripAnsi(d.toString()), 'stdout'));
    proc.stderr.on('data', (d) => appendLog(task, stripAnsi(d.toString()), 'stderr'));
    proc.on('close', (code) => { task.currentProcess = null; if (task.status === 'cancelled' || code === 0) resolve(); else reject(new Error(`退出码 ${code}`)); });
    proc.on('error', (err) => { task.currentProcess = null; if (err.code === 'ENOENT') reject(new Error('找不到 claude')); else reject(new Error(`进程错误: ${err.message}`)); });
  });
}

// ═══════════════════════════════════════════════════════════
//  Prompt 构建器（注入项目记忆）
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
  const mc = getMemoryContext(task.projectPath); if (mc) parts.push(mc);
  parts.push(`## 工作目录\n\n${task.projectPath}\n所有文件都在这个目录下。\n`);
  parts.push(`## 任务需求\n\n${task.plan}\n`);

  if (task.roleId) {
    const role = roles.find(r => r.id === task.roleId);
    if (role && role.skills && role.skills.length > 0) {
      let guide = '## 技能指南\n\n你有以下技能，请在合适时主动使用：\n';
      for (const s of role.skills) {
        if (s.includes('brainstorming')) guide += '- brainstorming：动手前先头脑风暴\n';
        else if (s.includes('writing-plans') || s.includes('write-plan')) guide += '- writing-plans：先编写开发计划\n';
        else if (s.includes('subagent')) guide += '- subagent-driven：复杂任务拆分子任务\n';
        else if (s.includes('code-review')) guide += '- code-review：完成后做代码审查\n';
        else if (s.includes('exa-search')) guide += '- exa-search：搜索最新信息\n';
        else if (s.includes('browser')) guide += '- browser-automation：浏览器抓取\n';
        else if (s.includes('python')) guide += '- python-review：Python 代码审查\n';
      }
      parts.push(guide);
    }
  }

  parts.push(`## 执行规则（必须遵守）

1. 仔细阅读「任务需求」和「项目历史」
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
  const mc = getMemoryContext(task.projectPath);
  return `${rc ? rc + '\n---\n\n' : ''}${mc ? mc + '\n---\n\n' : ''}## 子任务结果\n\n${childSummary}\n\n## 合并规则\n\n1. 检查产出文件\n2. 合并冲突\n3. 生成 docs/ 报告\n4. 直接执行`;
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
  const mc = getMemoryContext(task.projectPath);
  fs.writeFileSync(path.join(task.projectPath, 'PLAN.md'),
      `${rc ? rc + '\n---\n\n' : ''}${mc ? mc + '\n---\n\n' : ''}## 背景\n\n你之前在 ${task.projectPath} 完成了工作。\n\n${history ? `## 追加记录\n\n${history}\n\n` : ''}## 本次需求\n\n${data.prompt}\n\n## 规则\n\n1. 查看现有文件\n2. 执行新需求\n3. 不破坏已有功能\n4. 更新 docs/\n5. 直接执行`, 'utf-8');

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
        if (task.status === 'completed') extractTaskMemory(task).catch(() => {});
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

app.post('/api/chat', async (req, res) => {
  const { message, userId } = req.body;
  if (!message) return res.status(400).json({ error: 'Missing message' });
  try { const reply = await handleChatMessage(userId || 'web-user', 'Web用户', message); res.json({ reply }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/memory', (req, res) => {
  const projectPath = req.query.path;
  if (!projectPath) return res.status(400).json({ error: 'Missing path' });
  const memory = loadProjectMemory(projectPath);
  res.json({ memory });
});

app.get('/api/suggest-role', (req, res) => {
  const plan = req.query.plan || '';
  res.json({ suggestions: suggestRoles(plan) });
});

// ═══════════════════════════════════════════════════════════
//  启动
// ═══════════════════════════════════════════════════════════
loadRoles();
loadTaskTemplates();
loadIndex();
initDingTalk();

server.listen(CONFIG.port, CONFIG.host, () => {
  const interfaces = os.networkInterfaces();
  const ips = [];
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) { if (iface.family === 'IPv4' && !iface.internal) ips.push(iface.address); }
  }
  console.log('');
  console.log('  ┌──────────────────────────────────────┐');
  console.log('  │       CLAUDE HUB v8.1 · Ready        │');
  console.log('  ├──────────────────────────────────────┤');
  console.log(`  │  Local:   http://localhost:${CONFIG.port}      │`);
  for (const ip of ips) console.log(`  │  Network: http://${ip}:${CONFIG.port} │`);
  console.log('  ├──────────────────────────────────────┤');
  console.log(`  │  Chat:    ${CONFIG.deepseekApiKey ? 'DeepSeek ✓' : '未配置 ✗'}`);
  console.log(`  │  飞书:    ${isFeishuEnabled() ? '已启用 ✓' : '未配置 ✗'}`);
  console.log(`  │  钉钉:   ${DINGTALK.enabled ? '已启用 ✓' : '未配置 ✗'}`);
  console.log(`  │  Memory:  项目记忆系统 ✓`);
  console.log('  └──────────────────────────────────────┘');
  console.log('');
});

process.on('SIGINT', () => {
  console.log('\n正在关闭...');
  for (const [, task] of tasks) { if (task.currentProcess) { try { task.currentProcess.kill('SIGTERM'); } catch {} } }
  server.close();
  process.exit(0);
});
