// ═══════════════════════════════════════════════════════════
//  Claude Hub v4 — 角色系统 + 并行执行
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
//  Role System — 角色系统
// ═══════════════════════════════════════════════════════════
let roles = [];

function loadRoles() {
  try {
    const files = fs.readdirSync(CONFIG.rolesDir).filter(f => f.endsWith('.json'));
    roles = files.map(f => {
      try { return JSON.parse(fs.readFileSync(path.join(CONFIG.rolesDir, f), 'utf-8')); }
      catch { return null; }
    }).filter(Boolean);
    console.log(`  已加载 ${roles.length} 个角色`);
  } catch {}
}

function saveRole(roleData) {
  const filePath = path.join(CONFIG.rolesDir, `${roleData.id}.json`);
  fs.writeFileSync(filePath, JSON.stringify(roleData, null, 2), 'utf-8');
  loadRoles();
  broadcast({ type: 'roles_updated', data: { roles } });
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
      followUps: t.followUps || [],
      parentId: t.parentId || null,
      childIds: t.childIds || [],
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
      tasks.set(t.id, t);
    }
    console.log(`  已加载 ${list.length} 个历史任务`);
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
    parentId: t.parentId || null,
    childIds: t.childIds || [],
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
    data: { tasks: Array.from(tasks.values()).map(serializeTask), roles },
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
//  任务创建
// ═══════════════════════════════════════════════════════════
function createTask(data) {
  const id = genId();
  const taskDir = path.join(CONFIG.tasksDir, id);
  fs.mkdirSync(taskDir, { recursive: true });

  const projectPath = (data.projectPath || process.cwd()).replace(/\//g, '\\');
  fs.mkdirSync(projectPath, { recursive: true });

  const task = {
    id, name: data.name || '未命名任务',
    type: data.type || 'single',
    roleId: data.roleId || null,
    taskTypeId: data.taskTypeId || null,
    projectPath, taskDir,
    status: 'queued', phase: 0, phaseName: '等待中',
    createdAt: Date.now(), startedAt: null, completedAt: null,
    logs: [], error: null, currentProcess: null, results: null,
    plan: data.plan || '',
    followUps: [],
    parentId: data.parentId || null,
    childIds: [],
  };

  // 如果是并行拆分模式，创建子任务
  if (data.type === 'parallel' && data.subTasks && data.subTasks.length > 0) {
    task.childIds = [];
    for (const sub of data.subTasks) {
      const subId = genId();
      const subDir = path.join(CONFIG.tasksDir, subId);
      fs.mkdirSync(subDir, { recursive: true });

      const subTask = {
        id: subId, name: sub.name || '子任务',
        type: 'single', roleId: task.roleId, taskTypeId: task.taskTypeId,
        projectPath, taskDir: subDir,
        status: 'queued', phase: 0, phaseName: '等待中',
        createdAt: Date.now(), startedAt: null, completedAt: null,
        logs: [], error: null, currentProcess: null, results: null,
        plan: sub.plan || '', followUps: [],
        parentId: id, childIds: [],
      };
      tasks.set(subId, subTask);
      task.childIds.push(subId);
    }
  }

  tasks.set(id, task);
  saveIndex();
  broadcast({ type: 'task_created', data: serializeTask(task) });

  // 把主任务和子任务都加入队列
  if (task.childIds.length > 0) {
    for (const cid of task.childIds) queue.push(cid);
  }
  queue.push(id);
  tryExecuteNext();
}

// ═══════════════════════════════════════════════════════════
//  执行引擎
// ═══════════════════════════════════════════════════════════
function tryExecuteNext() {
  while (runningCount < CONFIG.maxConcurrent && queue.length > 0) {
    const id = queue.shift();
    const task = tasks.get(id);
    if (!task) continue;

    // 并行主任务：等所有子任务完成后再执行
    if (task.type === 'parallel' && task.childIds.length > 0) {
      const allDone = task.childIds.every(cid => {
        const ct = tasks.get(cid);
        return ct && (ct.status === 'completed' || ct.status === 'failed' || ct.status === 'cancelled');
      });
      if (!allDone) {
        // 还有子任务没完成，放回队列末尾
        queue.push(id);
        break;
      }
    }

    if (task.status === 'queued') {
      runningCount++;
      executeTask(task).finally(() => {
        runningCount--;
        tryExecuteNext();
      });
    }
  }
}

async function executeTask(task) {
  task.status = 'running';
  task.startedAt = Date.now();
  task.phase = 1;

  // 并行主任务：进入合并检查阶段
  if (task.type === 'parallel' && task.childIds.length > 0) {
    task.phaseName = '合并检查中';
    saveIndex();
    broadcast({ type: 'task_updated', data: serializeTask(task) });

    try {
      appendLog(task, '═══════════════════════════════════════\n  合并子任务结果\n═══════════════════════════════════════\n\n', 'system');

      // 收集子任务结果摘要
      const childSummary = task.childIds.map(cid => {
        const ct = tasks.get(cid);
        return `- ${ct.name}: ${ct.status}${ct.error ? ' (错误: ' + ct.error + ')' : ''}`;
      }).join('\n');

      const mergePrompt = buildMergePrompt(task, childSummary);
      const planFile = path.join(task.projectPath, 'PLAN.md');
      fs.writeFileSync(planFile, mergePrompt, 'utf-8');

      await runClaude(task, '读取当前目录下的 PLAN.md 文件，按照其中的指令执行合并和检查。', 150);

      task.status = 'completed';
      task.results = collectResults(task);
      appendLog(task, '\n✓ 并行任务完成\n', 'system');
    } catch (err) {
      task.status = 'failed';
      task.error = err.message;
      appendLog(task, `\n✗ 合并失败: ${err.message}\n`, 'system');
    }
  } else {
    // 普通任务：自主执行
    task.phaseName = '自主执行中';
    saveIndex();
    broadcast({ type: 'task_updated', data: serializeTask(task) });

    try {
      const fullPrompt = buildPrompt(task);
      const planFile = path.join(task.projectPath, 'PLAN.md');
      fs.writeFileSync(planFile, fullPrompt, 'utf-8');
      appendLog(task, `[指令文件] ${planFile}\n\n`, 'system');

      appendLog(task, '═══════════════════════════════════════\n  开始自主执行\n═══════════════════════════════════════\n\n', 'system');
      await runClaude(task, '读取当前目录下的 PLAN.md 文件，按照其中的指令逐步执行。直接开始，不要等待确认。', 300);

      if (task.status !== 'cancelled') {
        task.phase = 2;
        task.phaseName = '检查结果';
        broadcast({ type: 'task_updated', data: serializeTask(task) });
        appendLog(task, '\n═══════════════════════════════════════\n  检查结果\n═══════════════════════════════════════\n\n', 'system');

        const rp = path.join(task.projectPath, 'REPORT_PLAN.md');
        fs.writeFileSync(rp, buildReportPrompt(task), 'utf-8');
        await runClaude(task, '读取当前目录下的 REPORT_PLAN.md 文件，按照其中的指令生成报告。', 80);
        try { fs.unlinkSync(rp); } catch {}
      }

      if (task.status !== 'cancelled') {
        task.status = 'completed';
        task.results = collectResults(task);
        appendLog(task, '\n✓ 任务完成\n', 'system');
      }
    } catch (err) {
      if (task.status !== 'cancelled') {
        task.status = 'failed';
        task.error = err.message;
        appendLog(task, `\n✗ 失败: ${err.message}\n`, 'system');
      }
    }
  }

  task.completedAt = Date.now();
  if (task.currentProcess) { try { task.currentProcess.kill(); } catch {} task.currentProcess = null; }
  saveIndex();
  broadcast({ type: 'task_updated', data: serializeTask(task) });
}

// ═══════════════════════════════════════════════════════════
//  Claude 执行器
// ═══════════════════════════════════════════════════════════
function runClaude(task, shortPrompt, maxTurns) {
  return new Promise((resolve, reject) => {
    const escapedBin = `"${CONFIG.claudeBin}"`;
    const escapedPrompt = shortPrompt.replace(/"/g, '\\"');
    const cmd = `${escapedBin} -p "${escapedPrompt}" --allowedTools "Read,Write,Edit,Bash,Glob,Grep,WebFetch" --max-turns ${maxTurns}`;

    appendLog(task, `[执行] ${cmd}\n\n`, 'system');

    let proc;
    try {
      proc = spawn(cmd, [], {
        cwd: task.projectPath,
        env: { ...process.env },
        stdio: ['pipe', 'pipe', 'pipe'],
        shell: true,
      });
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
  return `## 你的身份和工作方式\n\n${role.persona}\n`;
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
  const roleCtx = getRoleContext(task);
  if (roleCtx) parts.push(roleCtx);
  const taskCtx = getTaskTypeContext(task);
  if (taskCtx) parts.push(taskCtx);

  parts.push(`## 工作目录\n\n${task.projectPath}\n所有文件都在这个目录下创建和操作。\n`);

  parts.push(`## 任务需求\n\n${task.plan}\n`);

  parts.push(`## 执行规则（必须遵守）

1. 仔细阅读上面的「任务需求」，理解用户到底要什么
2. 如果需求要求创建新子目录，创建后在那个目录里工作
3. 如果需要获取外部信息，使用 WebFetch 抓取真实网页
4. 自主完成所有步骤，不要停下来问用户确认
5. 遇到错误自行修复，不要放弃
6. 需要安装依赖就安装
7. 确保最终产出物是真实可用的
8. 完成后在工作目录下创建 docs/ 文件夹，生成：
   - docs/completed.md — 完成了什么（基于实际文件，不要编造）
   - docs/issues.md — 遇到的问题和解决方案
   - docs/remaining.md — 遗留问题

重要：不要编造内容。没有真正实现的功能不要写在报告里。
重要：每个需求都要实际执行，不是写在报告里就算完成。`);

  return parts.join('\n---\n\n');
}

function buildReportPrompt(task) {
  return `你在项目 ${task.projectPath} 中刚完成了一些工作。

请检查项目目录下的实际文件，确认 docs/ 下的三份报告是否真实准确：
1. docs/completed.md
2. docs/issues.md
3. docs/remaining.md

如果报告不存在或内容与实际代码不符，请基于实际文件重新生成。
如果已经准确，直接确认即可。不要编造内容。`;
}

function buildMergePrompt(task, childSummary) {
  const roleCtx = getRoleContext(task);
  return `${roleCtx ? roleCtx + '\n---\n\n' : ''}## 背景

你之前将一个大任务拆分成了多个子任务并行执行。现在所有子任务已完成，你需要合并结果。

## 子任务执行情况

${childSummary}

## 工作目录

${task.projectPath}

## 合并规则

1. 检查所有子任务的产出文件
2. 如果有冲突，以功能完整为准进行合并
3. 确保合并后的项目整体可运行
4. 生成 docs/completed.md — 总结所有完成的工作
5. 生成 docs/issues.md — 汇总所有问题
6. 生成 docs/remaining.md — 汇总遗留问题

直接执行，不要等待确认。`;
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
  const roleCtx = getRoleContext(task);

  const content = `${roleCtx ? roleCtx + '\n---\n\n' : ''}## 背景\n\n你之前在项目 ${task.projectPath} 中已完成了一些工作。\n\n${history ? `## 追加记录\n\n${history}\n\n` : ''}## 本次需求\n\n${data.prompt}\n\n## 执行规则\n\n1. 先查看项目现有文件，了解当前状态\n2. 执行上面的新需求\n3. 不要破坏已有功能\n4. 更新 docs/ 下的报告\n5. 直接执行，不要等待确认`;

  const planFile = path.join(task.projectPath, 'PLAN.md');
  fs.writeFileSync(planFile, content, 'utf-8');

  runningCount++;
  runClaude(task, '读取当前目录下的 PLAN.md 文件，按照其中的指令执行。', 200)
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
  // 取消子任务
  for (const cid of (task.childIds || [])) {
    const ct = tasks.get(cid);
    if (ct && (ct.status === 'running' || ct.status === 'queued')) {
      cancelTask(cid);
    }
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
  // 删除子任务
  for (const cid of (task.childIds || [])) {
    tasks.delete(cid);
  }
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
  console.log('  │       CLAUDE HUB v4 · Ready          │');
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
