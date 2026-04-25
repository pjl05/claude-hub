// ═══════════════════════════════════════════════════════════
//  Claude Hub — Autonomous Task Orchestrator
//  把 Claude Code headless 模式包装成可视化自主开发平台
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
  claudeBin: process.env.CLAUDE_BIN || 'claude',
  maxConcurrent: parseInt(process.env.MAX_CONCURRENT) || 2,
  tasksDir: path.join(__dirname, 'tasks'),
  phaseMaxTurns: [80, 300, 80],  // 规划、开发、报告
};

fs.mkdirSync(CONFIG.tasksDir, { recursive: true });

// ── Express + HTTP + WebSocket ───────────────────────────
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// ── 状态 ─────────────────────────────────────────────────
const tasks = new Map();
const clients = new Set();
const queue = [];
let runningCount = 0;

// ── 工具函数 ─────────────────────────────────────────────
function genId() {
  return crypto.randomBytes(8).toString('hex');
}

function broadcast(msg) {
  const data = JSON.stringify(msg);
  for (const c of clients) {
    if (c.readyState === 1) c.send(data);
  }
}

function stripAnsi(str) {
  return str.replace(/\x1B\[[0-9;]*[a-zA-Z]/g, '').replace(/\x1B\].*?\x07/g, '');
}

function serializeTask(task) {
  return {
    id: task.id,
    name: task.name,
    type: task.type,
    projectPath: task.projectPath,
    status: task.status,
    phase: task.phase,
    phaseName: task.phaseName,
    createdAt: task.createdAt,
    startedAt: task.startedAt,
    completedAt: task.completedAt,
    error: task.error,
    logCount: task.logs.length,
    results: task.results,
    duration: task.startedAt
      ? (task.completedAt || Date.now()) - task.startedAt
      : 0,
  };
}

// ── WebSocket 处理 ───────────────────────────────────────
wss.on('connection', (ws) => {
  clients.add(ws);

  ws.send(JSON.stringify({
    type: 'init',
    data: { tasks: Array.from(tasks.values()).map(serializeTask) },
  }));

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      handleMessage(ws, msg);
    } catch {
      ws.send(JSON.stringify({ type: 'error', data: { message: 'Invalid message' } }));
    }
  });

  ws.on('close', () => clients.delete(ws));
});

function handleMessage(ws, msg) {
  switch (msg.type) {
    case 'create_task':
      createTask(msg.data);
      break;
    case 'cancel_task':
      cancelTask(msg.data.taskId);
      break;
    case 'delete_task':
      deleteTask(msg.data.taskId);
      break;
    case 'get_logs': {
      const task = tasks.get(msg.data.taskId);
      if (task) {
        ws.send(JSON.stringify({
          type: 'task_logs',
          data: { taskId: task.id, logs: task.logs },
        }));
      }
      break;
    }
  }
}

// ── 任务创建 ─────────────────────────────────────────────
function createTask(data) {
  const id = genId();
  const taskDir = path.join(CONFIG.tasksDir, id);
  fs.mkdirSync(taskDir, { recursive: true });

  const planFile = path.join(taskDir, 'plan.md');
  fs.writeFileSync(planFile, data.plan || data.prompt || '');

  const projectPath = data.projectPath || process.cwd();
  fs.mkdirSync(projectPath, { recursive: true });
  fs.mkdirSync(path.join(projectPath, 'docs'), { recursive: true });

  const task = {
    id,
    name: data.name || '未命名任务',
    type: data.type || 'full',
    projectPath,
    plan: data.plan || '',
    planFile,
    taskDir,
    status: 'queued',
    phase: 0,
    phaseName: '等待中',
    createdAt: Date.now(),
    startedAt: null,
    completedAt: null,
    logs: [],
    error: null,
    currentProcess: null,
    results: null,
    prompt: data.prompt || '',
  };

  tasks.set(id, task);
  broadcast({ type: 'task_created', data: serializeTask(task) });

  queue.push(id);
  tryExecuteNext();
}

// ── 任务执行引擎 ─────────────────────────────────────────
function tryExecuteNext() {
  while (runningCount < CONFIG.maxConcurrent && queue.length > 0) {
    const id = queue.shift();
    const task = tasks.get(id);
    if (task && task.status === 'queued') {
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
  broadcast({ type: 'task_updated', data: serializeTask(task) });

  try {
    if (task.type === 'quick') {
      task.phase = 1;
      task.phaseName = '执行中';
      broadcast({ type: 'task_updated', data: serializeTask(task) });
      await runClaude(task, task.prompt || task.plan, 200);
    } else {
      const phases = [
        { name: '生成开发文档', turns: CONFIG.phaseMaxTurns[0], prompt: buildPlanningPrompt(task) },
        { name: '自主开发', turns: CONFIG.phaseMaxTurns[1], prompt: buildDevPrompt(task) },
        { name: '生成报告', turns: CONFIG.phaseMaxTurns[2], prompt: buildReportPrompt(task) },
      ];

      for (let i = 0; i < phases.length; i++) {
        if (task.status === 'cancelled') break;

        task.phase = i + 1;
        task.phaseName = phases[i].name;
        broadcast({ type: 'task_updated', data: serializeTask(task) });

        appendLog(task, `\n═══════════════════════════════════════\n  阶段 ${i + 1}/3: ${phases[i].name}\n═══════════════════════════════════════\n\n`, 'system');

        await runClaude(task, phases[i].prompt, phases[i].turns);
      }
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
      appendLog(task, `\n✗ 任务失败: ${err.message}\n`, 'system');
    }
  }

  task.completedAt = Date.now();
  if (task.currentProcess) {
    try { task.currentProcess.kill(); } catch {}
    task.currentProcess = null;
  }

  broadcast({ type: 'task_updated', data: serializeTask(task) });

  try {
    fs.writeFileSync(
      path.join(task.taskDir, 'output.log'),
      task.logs.map(l => l.text).join('')
    );
  } catch {}
}

function runClaude(task, prompt, maxTurns) {
  return new Promise((resolve, reject) => {
    // 把 prompt 写到临时文件，避免命令行传参被截断
    const promptFile = path.join(task.taskDir, `prompt_${Date.now()}.md`);
    fs.writeFileSync(promptFile, prompt, 'utf-8');

    const args = [
      '-p', `请先读取以下指令文件，然后按照文件内容执行：${promptFile}`,
      '--allowedTools', 'Read,Write,Edit,Bash,Glob,Grep,WebFetch',
      '--max-turns', String(maxTurns),
    ];

    let proc;
    try {
      proc = spawn(CONFIG.claudeBin, args, {
        cwd: task.projectPath,
        env: { ...process.env },
        stdio: ['pipe', 'pipe', 'pipe'],
        shell: true,
      });
    } catch (err) {
      return reject(new Error(`启动 Claude Code 失败: ${err.message}`));
    }

    task.currentProcess = proc;

    proc.stdout.on('data', (data) => {
      appendLog(task, stripAnsi(data.toString()), 'stdout');
    });

    proc.stderr.on('data', (data) => {
      appendLog(task, stripAnsi(data.toString()), 'stderr');
    });

    proc.on('close', (code) => {
      task.currentProcess = null;
      // 清理临时文件
      try { fs.unlinkSync(promptFile); } catch {}
      if (task.status === 'cancelled' || code === 0) {
        resolve();
      } else {
        reject(new Error(`进程退出，代码 ${code}`));
      }
    });

    proc.on('error', (err) => {
      task.currentProcess = null;
      try { fs.unlinkSync(promptFile); } catch {}
      if (err.code === 'ENOENT') {
        reject(new Error('找不到 claude 命令。请确认已安装 Claude Code CLI，或设置 CLAUDE_BIN 环境变量指向其路径。'));
      } else {
        reject(new Error(`进程错误: ${err.message}`));
      }
    });
  });
}


function appendLog(task, text, stream) {
  const entry = { text, stream, time: Date.now() };
  task.logs.push(entry);

  if (task.logs.length > 10000) {
    task.logs = task.logs.slice(-8000);
  }

  broadcast({ type: 'task_log', data: { taskId: task.id, entry } });
}

// ── Prompt 构建器 ────────────────────────────────────────
function buildPlanningPrompt(task) {
  return `你是一个资深全栈开发者和技术架构师。

请先读取开发计划文件：${task.planFile}

然后执行以下任务：
1. 分析需求，确定最佳技术选型
2. 设计清晰的项目目录结构
3. 将项目分解为可执行的开发阶段
4. 为每个阶段定义明确的任务、涉及文件和验收标准
5. 将完整的开发架构文档写入 ${task.projectPath}/docs/development-plan.md

确保文档详细、可执行，后续可以直接按照文档逐步实现。`;
}

function buildDevPrompt(task) {
  return `你是一个资深全栈开发者，正在进行自主开发。

请先读取 ${task.projectPath}/docs/development-plan.md 了解完整计划，然后按以下规则自主执行：

1. 按阶段顺序逐一实现每个功能模块
2. 每完成一个阶段后运行测试确保代码正确
3. 遇到错误或bug立即修复，不要等待确认
4. 需要安装的依赖直接安装
5. 需要创建的目录直接创建
6. 每完成一个阶段，在 ${task.projectPath}/docs/progress.md 追加进度记录

你拥有完全的自主执行权限，不要等待任何人工确认。直接开始执行。`;
}

function buildReportPrompt(task) {
  return `你刚刚完成了一个项目的自主开发。请基于项目代码和进度记录生成最终报告。

进度记录：${task.projectPath}/docs/progress.md
项目目录：${task.projectPath}

请生成以下三份详细报告：

1. **完成情况报告** → ${task.projectPath}/docs/report-completed.md
   - 所有已完成的功能模块及关键实现说明
   - 项目的运行方式和使用说明

2. **遇到问题报告** → ${task.projectPath}/docs/report-issues.md
   - 开发过程中遇到的所有问题
   - 每个问题的解决方案及妥协策略

3. **遗留问题报告** → ${task.projectPath}/docs/report-remaining.md
   - 尚未解决的问题
   - 需要人工介入的部分
   - 建议的后续优化方向

每份报告要详细、具体、可操作。`;
}

// ── 任务控制 ─────────────────────────────────────────────
function cancelTask(id) {
  const task = tasks.get(id);
  if (!task || task.status === 'completed' || task.status === 'failed') return;

  task.status = 'cancelled';
  if (task.currentProcess) {
    try { task.currentProcess.kill('SIGTERM'); } catch {}
    task.currentProcess = null;
  }
  task.completedAt = Date.now();
  appendLog(task, '\n⚠ 任务已取消\n', 'system');
  broadcast({ type: 'task_updated', data: serializeTask(task) });
}

function deleteTask(id) {
  const task = tasks.get(id);
  if (task) {
    cancelTask(id);
    tasks.delete(id);
    broadcast({ type: 'task_deleted', data: { taskId: id } });
  }
}

// ── 结果收集 ─────────────────────────────────────────────
function collectResults(task) {
  const docsDir = path.join(task.projectPath, 'docs');
  const files = [];

  if (fs.existsSync(docsDir)) {
    for (const entry of fs.readdirSync(docsDir)) {
      const fp = path.join(docsDir, entry);
      try {
        const stat = fs.statSync(fp);
        if (stat.isFile()) {
          files.push({ name: entry, path: fp, size: stat.size, modified: stat.mtimeMs });
        }
      } catch {}
    }
  }

  return { files };
}

// ── API 路由 ─────────────────────────────────────────────
app.get('/api/file', (req, res) => {
  const filePath = req.query.path;
  if (!filePath) return res.status(400).json({ error: 'Missing path' });
  try {
    const content = fs.readFileSync(filePath, 'utf-8');
    res.json({ content });
  } catch {
    res.status(404).json({ error: 'File not found' });
  }
});

// ── 启动 ─────────────────────────────────────────────────
server.listen(CONFIG.port, CONFIG.host, () => {
  const interfaces = os.networkInterfaces();
  const ips = [];
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        ips.push(iface.address);
      }
    }
  }

  console.log('');
  console.log('  ┌──────────────────────────────────────┐');
  console.log('  │         CLAUDE HUB · Ready           │');
  console.log('  ├──────────────────────────────────────┤');
  console.log(`  │  Local:   http://localhost:${CONFIG.port}      │`);
  for (const ip of ips) {
    console.log(`  │  Network: http://${ip}:${CONFIG.port} │`);
  }
  console.log('  └──────────────────────────────────────┘');
  console.log('');
  console.log('  手机访问请使用 Network 地址（同一 WiFi）');
  console.log('  Ctrl+C 停止服务');
  console.log('');
});

process.on('SIGINT', () => {
  console.log('\n正在关闭...');
  for (const [, task] of tasks) {
    if (task.currentProcess) {
      try { task.currentProcess.kill('SIGTERM'); } catch {}
    }
  }
  server.close();
  process.exit(0);
});
