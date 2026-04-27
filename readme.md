
<p align="center">
  <h1 align="center">Claude Hub</h1>
  <p align="center">让每个开发者都有自己的 AI 员工</p>
  <p align="center">
    <img src="https://img.shields.io/badge/version-v8.1-blue" alt="version">
    <img src="https://img.shields.io/badge/node-%3E%3D18-green" alt="node">
    <img src="https://img.shields.io/badge/license-MIT-yellow" alt="license">
  </p>
</p>

---

## 这是什么

Claude Hub 是一个**开源的本地自主任务编排平台**，把 [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) 包装成可视化的 AI 员工管理系统。

你定义角色、描述需求、点击执行——Claude 自主完成整个开发流程，你只需要看结果。

```txt
你（飞书/Web）→ 选角色 + 描述需求 → Claude 自主执行 → 实时日志 → 完成 → 迭代
```



## 核心特性

| 特性 | 说明 |
|---|---|
| **角色系统** | 预设 6 个角色（全栈开发、技术研究、文档写作、项目接手、数据分析、DevOps），每个有独立人设、技能、MCP 工具链，支持自定义 |
| **自主执行** | 创建任务后 Claude 自主完成，不需要逐步确认，实时日志流推送 |
| **多轮迭代** | 任务完成后随时追加需求，Claude 在已有代码基础上继续工作，不破坏已有功能 |
| **并行子任务** | 复杂任务拆分为多个子任务并发执行，自动合并检查 |
| **项目记忆** | 跨任务记住技术栈、架构决策、代码模式，同一个项目不会从零开始 |
| **智能调度** | 输入任务描述自动推荐最匹配的角色 |
| **飞书集成** | 飞书单聊做轻量对话（技术问答、方案梳理），复杂任务自动引导到 Web 界面 |
| **钉钉通知** | 任务完成/失败自动推送钉钉群通知 |
| **任务模板** | 常用任务存为模板，一键复用 |
| **AI 规划助手** | 创建任务前用 AI 聊天理清思路，自动生成结构化计划 |

## 

| | Claude Hub |
|---|---|
| **定位** | 本地 AI 员工管理平台 |
| **代码位置** | 你的本地电脑 |
| **交互方式** | 描述需求 → AI 自主执行 |
| **角色定制** | 支持，可自定义人设和技能 |
| **多轮迭代** | 支持，保留项目上下文 |
| **飞书/钉钉** | 原生支持 |
| **项目记忆** | 跨任务自动注入历史上下文 |
| **开源** | 完全开源，本地部署 |



## 架构

```
┌─────────────────────────────────────────┐
			│ 用户入口 │
│ 📱 飞书单聊         💻 Web 界面（PC/手机） │
└──────────┬──────────────┬───────────────┘
           │ 			  │
		   ▼              ▼
┌────────────┐     ┌──────────────┐
│ Chat 引擎  │     │  Task 引擎 │
│ DeepSeek  │        │ Claude Code │
│ API │                │ CLI │
│ (2-5秒回复)│       │ (自主执行) │
└────────────┘     └──────┬───────┘
						  │
				   ┌──────┴───────┐
			          │ 项目记忆 │
                   │ .claude-hub/ │
                   │ memory.json │
                   └──────────────┘
```

- **Chat 引擎**：DeepSeek API，处理轻量对话，飞书单聊 2-5 秒响应

- **Task 引擎**：Claude Code CLI，处理完整开发任务，自主执行 10-30 分钟

- **意图识别**：自动判断用户是想聊天还是想创建任务

- **项目记忆**：任务完成后自动分析日志，提取关键信息存入项目目录

  

## 快速开始

### 环境要求

- Node.js >= 18
- [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) 已安装
- Windows / macOS / Linux

### 安装

```bash
git clone https://github.com/your-username/claude-hub.git
cd claude-hub
npm install
```

### 配置

复制环境变量模板：
cp .env.example .env

编辑 .env：

```
必须配置
DEEPSEEK_API_KEY=sk-xxxxx
CLAUDE_BIN=/path/to/claude

可选：飞书集成
FEISHU_APP_ID=cli_xxxxx
FEISHU_APP_SECRET=xxxxx

可选：钉钉通知
DINGTALK_WEBHOOK_TOKEN=xxxxx
DINGTALK_WEBHOOK_SECRET=xxxxx

可选：公网访问（Tailscale Funnel）
PUBLIC_URL=https://your-machine.tailnet.ts.net
```



### 启动

#### Linux / macOS

```
DEEPSEEK_API_KEY=sk-xxxxx CLAUDE_BIN=/path/to/claude npm start
```

#### Windows PowerShell

```
$env:DEEPSEEK_API_KEY="sk-xxxxx"
$env:CLAUDE_BIN="D:\path\to\claude.cmd"
npm start
```

启动后访问 http://localhost:3800



## 使用指南

### Web 界面

1. 1.选择角色 — 根据任务类型选择合适的角色（支持智能推荐）
2. 2.选择任务类型 — 新建项目、修复 Bug、优化性能等
3. 3.描述需求 — 可以直接写，也可以用 AI 规划助手帮你整理
4. 4.创建并执行 — Claude 自主完成，实时查看日志
5. 5.迭代优化 — 任务完成后追加需求，继续对话



### 飞书集成

1. 1.在飞书开放平台创建应用，获取 App ID 和 App Secret
2. 2.配置事件订阅，请求地址：`https://your-url/feishu/webhook`
3. 3.订阅 `im.message.receive_v1` 事件
4. 4.启动 Claude Hub，飞书单聊机器人即可使用

飞书支持的命令：

| 命令            | 说明                    |
| --------------- | ----------------------- |
| 直接发消息      | 快速对话，DeepSeek 回答 |
| /clear          | 清除对话记忆            |
| /help           | 显示帮助                |
| 提到"开发/创建" | 自动引导到 Web 界面     |



### 项目记忆

项目记忆是按项目路径隔离的，存储在每个项目目录下：

```your-project/
├── .claude-hub/
│   └── memory.json    ← 项目记忆（自动管理）
├── src/
└── ...
```

- 任务完成后自动提取：技术栈、架构、决策、代码模式
- 新任务自动注入历史上下文
- 在 Web 界面可以查看和清除记忆
- 不同项目完全隔离，互不干扰



### 角色系统

预设角色：

| 角色       | 技能                                 | MCP    | 适用场景       |
| ---------- | ------------------------------------ | ------ | -------------- |
| 全栈开发者 | brainstorming, subagent, code-review | github | 开发完整项目   |
| 技术研究员 | brainstorming, exa-search, browser   | github | 技术调研和选型 |
| 技术写作者 | brainstorming, writing-plans         | —      | 文档和教程     |
| 项目接手者 | brainstorming, subagent, code-review | github | 二次开发和重构 |
| 数据分析师 | brainstorming, python-review         | mysql  | 数据分析和报表 |
| DevOps     | brainstorming, code-review           | github | 部署和运维     |

支持自定义角色：修改 `roles/` 目录下的 JSON 文件，或在 Web 界面的角色管理中编辑。



### 项目结构

```
claude-hub/
├── server.js              ← 后端主文件（Express + WebSocket）
├── package.json
├── task-templates.json    ← 任务模板
├── roles/                 ← 角色定义
│   ├── developer.json
│   ├── researcher.json
│   ├── writer.json
│   ├── project-owner.json
│   ├── data-analyst.json
│   ├── devops.json
│   └── custom.json
├── public/
│   └── index.html         ← 前端界面（单文件，无构建工具）
├── tasks/
│   └── index.json         ← 任务持久化
└── README.md
```



### 远程访问

推荐使用 Tailscale Funnel 暴露公网地址：

```
# 安装 Tailscale 后
tailscale funnel 3800
```

即可通过 https://your-machine.tailnet.ts.net 在手机或任何设备上访问。



## 已知限制

- Windows 下 `.cmd` 文件需要 `shell: true`（已处理）

- Claude Code CLI 的执行时间取决于任务复杂度，通常 10-30 分钟

- 项目记忆依赖 DeepSeek API 分析日志，无 API Key 时只存基本信息

- 并行子任务最多 3 个（可通过 `MAX_CONCURRENT` 环境变量调整）

  

## 开发路线

-  基础任务执行（v1-v3）

-  角色系统 + 并行执行（v4）

-  角色预览 + AI 规划助手（v5）

-  飞书钉钉集成（v6）

-  双引擎架构（v7）

-  项目记忆 + 智能调度（v8）

-  Web 界面内嵌对话（Chat 引擎也接入 Web）

-  工作流模板市场（用户分享角色和模板）

-  多模型支持（Gemini CLI、Codex CLI）

-  团队协作（多人共享任务队列）

-  VS Code 插件

  

## 贡献

欢迎提交 Issue 和 Pull Request。

1.Fork 本仓库

2.创建特性分支：`git checkout -b feature/xxx`

3.提交更改：`git commit -m 'Add xxx'`

4.推送分支：`git push origin feature/xxx`

5.创建 Pull Request



## 许可证

MIT License




Built with Claude Code CLI + DeepSeek + MiniMax

