// ═══════════════════════════════════════════════════════════
//  Claude Hub v5 — 完整版
//  角色系统 + 任务模板 + AI规划 + 并行执行 + 失败重试
// ═══════════════════════════════════════════════════════════

const express = require('express');
const { WebSocketServer } = require('ws');
const http = require('http');
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
//  Task Templates — 任务模板
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
    id: genId(),
    name: data.name || '未命名模板',
    roleId: data.roleId || null,
    taskTypeId: data.taskTypeId || null,
    projectPath: data.projectPath || '',
    plan: data.plan || '',
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
    data: {
      tasks: Array.from(tasks.values()).map(serializeTask),
      roles, taskTemplates,
    },
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
    for (const h of history) {
      parts.push(`**${h.role === 'user' ? '用户' : '助手'}**：${h.content}\n`);
    }
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
//  任务创建
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
  };

  if (data.type === 'parallel' && data.subTasks && data.subTasks.length > 0) {
    task.childIds = [];
    for (const sub of data.subTasks) {
      const subId = genId();
      const subDir = path.join(CONFIG.tasksDir, subId);
      fs.mkdirSync(subDir, { recursive: true });
      const subTask = {
        id: subId, name: sub.name || '子任务', type: 'single',
        roleId: task.roleId, taskTypeId: task.taskTypeId,
        projectPath, taskDir: subDir, status: 'queued', phase: 0, phaseName: '等待中',
        createdAt: Date.now(), startedAt: null, completedAt: null,
        logs: [], error: null, currentProcess: null, results: null,
        plan: sub.plan || '', followUps: [],
        parentId: id, childIds: [], retryCount: 0,
      };
      tasks.set(subId, subTask);
      task.childIds.push(subId);
    }
  }

  tasks.set(id, task);
  saveIndex();
  broadcast({ type: 'task_created', data: serializeTask(task) });

  if (task.childIds.length > 0) {
    for (const cid of task.childIds) queue.push(cid);
  }
  queue.push(id);
  tryExecuteNext();
}

// ═══════════════════════════════════════════════════════════
//  执行引擎（并行 + 失败重试）
// ═══════════════════════════════════════════════════════════
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
      } else {
        pending.push(id);
      }
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
        const r = ct.retryCount > 0 ? ` (重试${ct.retryCount}次)` : '';
        return `- ${ct.name}: ${s} ${ct.status}${r}${ct.error ? ' → ' + ct.error : ''}`;
      }).join('\n');
      appendLog(task, childSummary + '\n\n', 'system');

      const planFile = path.join(task.projectPath, 'PLAN.md');
      fs.writeFileSync(planFile, buildMergePrompt(task, childSummary), 'utf-8');
      await runClaude(task, '读取当前目录下的 PLAN.md，按指令执行合并和检查。', 150);

      if (task.status !== 'cancelled') {
        task.status = 'completed';
        task.results = collectResults(task);
        appendLog(task, '\n✓ 并行任务完成\n', 'system');
      }
    } else {
      task.phase = 1; task.phaseName = '自主执行中';
      broadcast({ type: 'task_updated', data: serializeTask(task) });

      const fullPrompt = buildPrompt(task);
      const planFile = path.join(task.projectPath, 'PLAN.md');
      fs.writeFileSync(planFile, fullPrompt, 'utf-8');
      appendLog(task, `[指令] ${planFile}\n\n`, 'system');
      appendLog(task, '═══════════════════════════════════════\n  开始自主执行\n═══════════════════════════════════════\n\n', 'system');
      await runClaude(task, '读取当前目录下的 PLAN.md，按指令逐步执行。直接开始。', 300);

      if (task.status !== 'cancelled') {
        task.phase = 2; task.phaseName = '检查结果';
        broadcast({ type: 'task_updated', data: serializeTask(task) });
        appendLog(task, '\n═══════════════════════════════════════\n  检查结果\n═══════════════════════════════════════\n\n', 'system');
        const rp = path.join(task.projectPath, 'REPORT_PLAN.md');
        fs.writeFileSync(rp, buildReportPrompt(task), 'utf-8');
        await runClaude(task, '读取当前目录下的 REPORT_PLAN.md，按指令生成报告。', 80);
        try { fs.unlinkSync(rp); } catch {}
      }

      if (task.status !== 'cancelled') {
        task.status = 'completed';
        task.results = collectResults(task);
        appendLog(task, '\n✓ 任务完成\n', 'system');
      }
    }
  } catch (err) {
    if (task.status === 'cancelled') {
      // 用户取消
    } else if (task.parentId && (task.retryCount || 0) < MAX_RETRY) {
      task.retryCount = (task.retryCount || 0) + 1;
      appendLog(task, `\n⚠ 失败，自动重试 (${task.retryCount}/${MAX_RETRY})...\n\n`, 'system');
      task.status = 'queued';
      task.phase = 0; task.phaseName = '重试中';
      task.startedAt = null; task.completedAt = null;
      task.plan += `\n\n## 上次执行失败\n\n错误：${err.message}\n请避免同样的问题。`;
      saveIndex();
      broadcast({ type: 'task_updated', data: serializeTask(task) });
      queue.push(task.id);
      tryExecuteNext();
      return;
    } else {
      task.status = 'failed';
      task.error = err.message;
      appendLog(task, `\n✗ 失败: ${err.message}\n`, 'system');
    }
  }

  task.completedAt = Date.now();
  if (task.currentProcess) { try { task.currentProcess.kill(); } catch {} task.currentProcess = null; }
  saveIndex();
  broadcast({ type: 'task_updated', data: serializeTask(task) });
}

// ═══════════════════════════════════════════════════════════
//  Claude 执行器（技能注入）
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
        skillHint = '\n\n可用技能：' + role.skills.join(', ') + '\n请在合适的时候主动使用这些技能。';
      }
    }

    const escapedBin = `"${CONFIG.claudeBin}"`;
    const escapedPrompt = (shortPrompt + skillHint).replace(/"/g, '\\"');
    const cmd = `${escapedBin} -p "${escapedPrompt}" --allowedTools "${toolsStr}" --max-turns ${maxTurns}`;

    appendLog(task, `[执行] ${cmd.substring(0, 200)}...\n\n`, 'system');

    let proc;
    try {
      proc = spawn(cmd, [], { cwd: task.projectPath, env: { ...process.env }, stdio: ['pipe', 'pipe', 'pipe'], shell: true });
    } catch (err) {
      return reject(new Error(`启动失败: ${err.message}`));
    }

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
  const rc = getRoleContext(task);
  if (rc) parts.push(rc);
  const tc = getTaskTypeContext(task);
  if (tc) parts.push(tc);

  parts.push(`## 工作目录\n\n${task.projectPath}\n所有文件都在这个目录下。\n`);
  parts.push(`## 任务需求\n\n${task.plan}\n`);

  // 技能指南
  if (task.roleId) {
    const role = roles.find(r => r.id === task.roleId);
    if (role && role.skills && role.skills.length > 0) {
      let guide = '## 技能指南\n\n你有以下技能，请在合适时主动使用：\n';
      for (const s of role.skills) {
        if (s.includes('brainstorming')) guide += '- brainstorming：动手前先头脑风暴分析方案\n';
        else if (s.includes('writing-plans') || s.includes('write-plan')) guide += '- writing-plans：先编写详细开发计划\n';
        else if (s.includes('executing-plans')) guide += '- executing-plans：按计划逐步执行\n';
        else if (s.includes('subagent')) guide += '- subagent-driven：复杂任务拆分子任务分别执行\n';
        else if (s.includes('code-review')) guide += '- code-review：完成后做代码审查\n';
        else if (s.includes('exa-search')) guide += '- exa-search：搜索最新信息\n';
        else if (s.includes('browser')) guide += '- browser-automation：需要时用浏览器抓取网页\n';
        else if (s.includes('python')) guide += '- python-review：Python 代码专项审查\n';
        else if (s.includes('docs-consistency')) guide += '- docs-consistency：检查文档一致性\n';
      }
      parts.push(guide);
    }
  }

  parts.push(`## 执行规则（必须遵守）

1. 仔细阅读「任务需求」，理解用户到底要什么
2. 如果要求创建新子目录，创建后在里面工作
3. 需要外部信息就用 WebFetch 抓取
4. 自主完成，不要等确认
5. 遇到错误自行修复
6. 需要安装依赖就安装
7. 确保产出物真实可用
8. 完成后在 docs/ 下生成：
   - docs/completed.md — 完成了什么
   - docs/issues.md — 遇到的问题
   - docs/remaining.md — 遗留问题
9. 不要编造内容。没实现的功能不要写在报告里。
10. 每个需求都要实际执行。`);

  return parts.join('\n---\n\n');
}

function buildReportPrompt(task) {
  return `你在 ${task.projectPath} 中刚完成了一些工作。

检查 docs/ 下的报告是否真实准确：
1. docs/completed.md
2. docs/issues.md
3. docs/remaining.md

如果不存在或与实际不符，基于实际文件重新生成。
如果已经准确，直接确认。不要编造。`;
}

function buildMergePrompt(task, childSummary) {
  const rc = getRoleContext(task);
  return `${rc ? rc + '\n---\n\n' : ''}## 背景

你将大任务拆分为多个子任务并行执行，现在所有子任务已完成。

## 子任务结果

${childSummary}

## 工作目录

${task.projectPath}

## 合并规则

1. 检查所有子任务产出文件
2. 如有冲突，以功能完整为准合并
3. 确保合并后项目可运行
4. 生成 docs/completed.md、docs/issues.md、docs/remaining.md
5. 直接执行`;
}

// ── 迭代 ──
function handleFollowUp(data) {
  const task = tasks.get(data.taskId);
  if (!task || task.status === 'running') return;
  task.followUps = task.followUps || [];
  task.followUps.push({ prompt: data.prompt, time: Date.now() });
  task.status = 'running';
  task.phaseName = '迭代中';
  task.completedAt = null;
  saveIndex();
  broadcast({ type: 'task_updated', data: serializeTask(task) });

  const history = (task.followUps || []).map((f, i) => `- 第${i + 1}次：${f.prompt}`).join('\n');
  const rc = getRoleContext(task);
  const content = `${rc ? rc + '\n---\n\n' : ''}## 背景\n\n你之前在 ${task.projectPath} 中完成了工作。\n\n${history ? `## 追加记录\n\n${history}\n\n` : ''}## 本次需求\n\n${data.prompt}\n\n## 执行规则\n\n1. 先查看现有文件\n2. 执行新需求\n3. 不破坏已有功能\n4. 更新 docs/ 报告\n5. 直接执行`;

  fs.writeFileSync(path.join(task.projectPath, 'PLAN.md'), content, 'utf-8');

  runningCount++;
  runClaude(task, '读取当前目录下的 PLAN.md，按指令执行。', 200)
      .then(() => { if (task.status !== 'cancelled') { task.status = 'completed'; task.results = collectResults(task); } })
      .catch(err => { if (task.status !== 'cancelled') { task.status = 'failed'; task.error = err.message; } })
      .finally(() => {
        task.completedAt = Date.now();
        if (task.currentProcess) { try { task.currentProcess.kill(); } catch {} task.currentProcess = null; }
        runningCount--;
        saveIndex();
        broadcast({ type: 'task_updated', data: serializeTask(task) });
        tryExecuteNext();
      });
}

// ═══════════════════════════════════════════════════════════
//  任务控制 & 结果
// ═══════════════════════════════════════════════════════════
function cancelTask(id) {
  const task = tasks.get(id);
  if (!task || task.status === 'completed' || task.status === 'failed') return;
  task.status = 'cancelled';
  for (const cid of (task.childIds || [])) {
    const ct = tasks.get(cid);
    if (ct && (ct.status === 'running' || ct.status === 'queued')) cancelTask(cid);
  }
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
      try {
        const stat = fs.statSync(fp);
        if (stat.isFile()) files.push({ name: entry, path: fp, size: stat.size, modified: stat.mtimeMs });
      } catch {}
    }
  }
  return { files };
}

// ═══════════════════════════════════════════════════════════
//  API & 启动
// ═══════════════════════════════════════════════════════════
app.get('/api/file', (req, res) => {
  const filePath = req.query.path;
  if (!filePath) return res.status(400).json({ error: 'Missing path' });
  try { res.json({ content: fs.readFileSync(filePath, 'utf-8') }); }
  catch { res.status(404).json({ error: 'File not found' }); }
});

loadRoles();
loadTaskTemplates();
loadIndex();

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
  console.log('  │       CLAUDE HUB v5 · Ready          │');
  console.log('  ├──────────────────────────────────────┤');
  console.log(`  │  Local:   http://localhost:${CONFIG.port}      │`);
  for (const ip of ips) console.log(`  │  Network: http://${ip}:${CONFIG.port} │`);
  console.log('  └──────────────────────────────────────┘');
  console.log('');
});

process.on('SIGINT', () => {
  console.log('\n正在关闭...');
  for (const [, task] of tasks) {
    if (task.currentProcess) { try { task.currentProcess.kill('SIGTERM'); } catch {} }
  }
  server.close();
  process.exit(0);
});
