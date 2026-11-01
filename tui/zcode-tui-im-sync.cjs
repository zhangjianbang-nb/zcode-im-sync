/*
 * zcode-tui-im-sync —— 让微信/飞书与 zcode TUI **双向、全过程**同步（内存注入，不改 bundle）。
 *
 * 安装（可拆卸，install.sh 会自动做）：
 *   export NODE_OPTIONS="--require /path/to/zcode-tui-im-sync.cjs"
 * 卸载：删掉上面这行即可，bundle 一个字节都没动。
 *
 * 做三件事：
 *   1) 微信/飞书 → TUI：轮询 state/im-inbox/*.json，空闲时调用 TUI 自己的
 *      `tui.submit(text)` —— 和用户在键盘上敲回车完全同一条路径，消息会出现在
 *      TUI 会话里并触发回合。原子 rename 认领，绝不和桥的兜底重复执行。
 *   2) TUI → 微信/飞书：包装 `tui.onEvent`，把**块级**事件（用户输入、思考块、
 *      工具调用/结果、回答块、结束/出错）写进 state/im-events/<sess>.ndjson。
 *      不是每 token 写盘：delta 只在内存里累积，块结束才落一行；队列有界、
 *      异步 append、4MB 轮转。TUI 事件循环里只做字符串拼接 + 入队。
 *   3) `/wechat` `/feishu`：拦截提交，spawn `im-bridge login <platform> --bind-latest`
 *      （有 DISPLAY 弹 PNG 二维码；无 DISPLAY 把字符二维码写日志），成功后自动
 *      绑定当前 TUI 会话并重启常驻桥。
 *
 * 安全边界：只在本进程是 TUI（stdin/stdout 都是 TTY、非 app-server/--prompt）时启用；
 * 任何形状不匹配、任何异常都原样放行（宁可不修，不弄坏 TUI）。
 */

'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const Module = require('module');
const { spawn } = require('child_process');

const BRIDGE_HOME =
  process.env.IM_BRIDGE_HOME || path.join(os.homedir(), '.local/share/zcode-im-sync');
const STATE_DIR = path.join(BRIDGE_HOME, 'state');
const INBOX_DIR = path.join(STATE_DIR, 'im-inbox');
const EVENTS_DIR = path.join(STATE_DIR, 'im-events');
const HEARTBEAT = path.join(STATE_DIR, 'tui-heartbeat.json');
const LOG = path.join(BRIDGE_HOME, 'logs', 'tui-im-sync.log');

const TARGET_RE = /@zcode[\/\\]tui[\/\\]dist[\/\\]index\.js$/;
const TUI_URL_RE = /@zcode\/tui\/dist\/index\.js$/;
const ANCHOR = 'editor.onSubmit = (text) => void this.submit(text);';
const MARKER = 'globalThis.__zcImStart';
const POLL_MS = 2000;
const HEARTBEAT_MS = 2000;
const MAX_ATTEMPTS = 3;
const MAX_BLOCK_CHARS = 6000;
const MAX_TOOL_CHARS = 3000;
const MAX_TOOL_INPUT_CHARS = 800;
const EVENT_FILE_MAX = 8 * 1024 * 1024;
const QUEUE_MAX = 512;
const LOG_MAX = 4 * 1024 * 1024;

/** 只在 TUI 进程里干活：app-server / headless prompt / 子命令一律放行。 */
function isTuiProcess() {
  const argv = process.argv.slice(2).join(' ');
  if (/app-server/.test(argv)) return false;
  if (/--prompt|(^|\s)-p(\s|$)/.test(argv)) return false;
  if (/^(doctor|login|logout|version|plugins|skills|commands)\b/.test(argv)) return false;
  return Boolean(process.stdin && process.stdin.isTTY && process.stdout && process.stdout.isTTY);
}

function rotateLog() {
  try {
    const st = fs.statSync(LOG);
    if (st.size > LOG_MAX) fs.renameSync(LOG, `${LOG}.1`);
  } catch {}
}

function log(msg) {
  try {
    fs.mkdirSync(path.dirname(LOG), { recursive: true });
    rotateLog();
    fs.appendFileSync(LOG, `${new Date().toISOString()} [pid ${process.pid}] ${msg}\n`);
  } catch {
    /* 日志失败不影响主流程 */
  }
}

/** IM_SYNC_DEBUG=1 时逐条打印原始事件元信息，排查采集缺口用；默认关闭。 */
function debugLog(msg) {
  if (process.env.IM_SYNC_DEBUG !== '1') return;
  log(`[debug] ${msg}`);
}

function ensureDirs() {
  for (const d of [INBOX_DIR, EVENTS_DIR, path.dirname(LOG)]) {
    try {
      fs.mkdirSync(d, { recursive: true });
    } catch {}
  }
}

// ---------------------------------------------------------------------------
// 事件提取 / 块级采集（纯函数，便于单测）
// ---------------------------------------------------------------------------

function isRecord(v) {
  return v !== null && typeof v === 'object';
}

function str(v) {
  return typeof v === 'string' ? v : '';
}

/** 兼容 zcode 的三种信封：value.payload.event / value.payload / value.params.payload。 */
function bodyOf(value) {
  if (!isRecord(value)) return {};
  const params = value.params;
  const payload = value.payload || (isRecord(params) ? params.payload : undefined);
  const body =
    (isRecord(payload) && (payload.event || payload.streamEvent)) ||
    payload ||
    value;
  return isRecord(body) ? body : {};
}

function typeOf(value) {
  const body = bodyOf(value);
  const params = isRecord(value) ? value.params : undefined;
  return str(body.type) || str(value && value.type) || str(params && params.type);
}

function sessionOf(value) {
  const body = bodyOf(value);
  const params = isRecord(value) ? value.params : undefined;
  const payload = isRecord(value) ? value.payload : undefined;
  return (
    str(value && (value.sessionId || value.sessionID)) ||
    str(params && (params.sessionId || params.sessionID)) ||
    str(payload && (payload.sessionId || payload.sessionID)) ||
    str(body.sessionId || body.sessionID)
  );
}

function eventIdOf(value) {
  const body = bodyOf(value);
  const params = isRecord(value) ? value.params : undefined;
  return (
    str(value && value.eventId) ||
    str(params && params.eventId) ||
    str(value && value.id) ||
    str(params && params.id) ||
    str(body.eventId)
  );
}

/** 原始 body 常常没有 kind（工具事件靠 type 表达），按 TUI 的 runtimeToolKind 映射补齐。 */
function kindOf(body, type) {
  const k = str(body.kind);
  if (k) return k;
  switch (type) {
    case 'tool_call_scheduled':
      return 'scheduled';
    case 'tool_call_started':
      return 'started';
    case 'tool_call_progress':
      return 'progress';
    case 'tool_call_result':
      return 'result';
    case 'tool_call_error':
      return 'error';
    case 'tool_call_closed':
      return 'closed';
    default:
      return '';
  }
}

/** 按字符上限截断（JS 字符串按 UTF-16 code unit，够用；Rust 侧还会按字符再截）。 */
function truncate(s, max) {
  const t = String(s == null ? '' : s);
  if (t.length <= max) return t;
  return `${t.slice(0, max)}\n…（截断，共 ${t.length} 字符；完整内容见 TUI）`;
}

function appendBounded(cur, delta, max) {
  if (!delta) return cur;
  if (cur.length >= max) return cur;
  return (cur + String(delta)).slice(0, max);
}

function extractError(body) {
  const err = body.error != null ? body.error : body.message != null ? body.message : body.reason;
  if (typeof err === 'string') return truncate(err, MAX_TOOL_CHARS);
  if (isRecord(err)) {
    const m = str(err.message) || str(err.error);
    if (m) return truncate(m, MAX_TOOL_CHARS);
    try {
      return truncate(JSON.stringify(err), MAX_TOOL_CHARS);
    } catch {}
  }
  try {
    return truncate(JSON.stringify(body), MAX_TOOL_CHARS);
  } catch {
    return '未知错误';
  }
}

function makeCaptureState() {
  return {
    seq: 0,
    thinking: '',
    thinkingTruncated: false,
    answer: '',
    answerTruncated: false,
    tools: new Map(),
    toolInputs: new Map(),
    turnActive: false,
    turnEnded: false,
    lastStatusAt: 0,
    seen: new Set(),
    queue: [],
    flushing: false,
    dropped: 0,
    // 会话 ID 还没建立时（刚敲回车那一刻）先缓冲，拿到 sid 后按顺序补写
    pending: [],
    pendingDropped: 0,
  };
}

const PENDING_MAX = 300;

function bufferPending(state, events) {
  for (const ev of events) {
    if (state.pending.length >= PENDING_MAX) {
      state.pending.shift();
      state.pendingDropped += 1;
      if (state.pendingDropped === 1 || state.pendingDropped % 50 === 0) {
        log(`会话未建立时缓冲溢出，已丢弃 ${state.pendingDropped} 条`);
      }
    }
    state.pending.push(ev);
  }
}

function remember(state, id) {
  if (!id) return true;
  if (state.seen.has(id)) return false;
  state.seen.add(id);
  if (state.seen.size > 2000) {
    const first = state.seen.values().next().value;
    state.seen.delete(first);
  }
  return true;
}

/**
 * 吃一条 onEvent 原始事件，返回要写盘的事件对象数组（不写盘，便于测试）。
 * 只采主回合的块级事件；跳过 part.* 快照、子代理事件、其它会话事件。
 */
function captureEvent(state, tui, value, turnEpoch) {
  const out = [];
  const sid = tui && tui.sessionId;
  const body = bodyOf(value);
  const type = typeOf(value);
  const kind = kindOf(body, type);
  if (!kind && !type) return out;
  if (kind.startsWith('part.') || type.startsWith('part.')) return out;
  // 子代理/后台任务事件不混进主会话过程（TUI 自己也有子代理面板）
  if (body.taskId || body.taskKind || body.taskType || body.agentId) return out;
  const sess = sessionOf(value);
  if (sid && sid !== 'unknown' && sess && sess !== sid) return out;
  const isTurnStart = type === 'turn_started' || type === 'turn.started';
  const isTurnEnd =
    type === 'turn_complete' ||
    type === 'turn.completed' ||
    type === 'turn_error' ||
    type === 'turn.failed';
  // turn_started 往往先于 primaryTurnActive 置位；turn_end 也可能晚于它复位，
  // 这两个边界事件按 state.turnActive 放行，否则会丢掉回合头尾。
  if (!isTurnStart && !isTurnEnd && !tui.primaryTurnActive) return out;
  if (
    turnEpoch !== undefined &&
    tui.activeTurnEpoch !== undefined &&
    turnEpoch !== tui.activeTurnEpoch &&
    !isTurnStart &&
    !(isTurnEnd && state.turnActive)
  ) {
    return out;
  }
  if (!remember(state, eventIdOf(value))) return out;

  const push = (ev) => out.push(ev);
  const now = Date.now();

  // —— 回合生命周期 ——
  if (isTurnStart) {
    state.thinking = '';
    state.answer = '';
    state.tools.clear();
    state.toolInputs.clear();
    state.turnActive = true;
    state.turnEnded = false;
    push({ t: 'turn_start' });
    return out;
  }
  if (type === 'turn_complete' || type === 'turn.completed') {    const tokens =
      (isRecord(body.tokens) && body.tokens.total) || body.tokenCount || body.totalTokens;
    push({
      t: 'turn_end',
      tokens: typeof tokens === 'number' ? tokens : undefined,
      tools: typeof body.toolCallCount === 'number' ? body.toolCallCount : undefined,
    });
    state.turnActive = false;
    state.turnEnded = true;
    state.thinking = '';
    state.answer = '';
    state.tools.clear();
    return out;
  }
  if (type === 'turn_error' || type === 'turn.failed') {
    push({ t: 'turn_error', message: extractError(body) });
    state.turnActive = false;
    state.turnEnded = true;
    state.thinking = '';
    state.answer = '';
    return out;
  }
  if (type === 'model_request_started') {
    if (now - state.lastStatusAt > 3000) {
      state.lastStatusAt = now;
      push({ t: 'status', text: '模型请求中…' });
    }
    return out;
  }
  if (type === 'model_retry_scheduled') {
    push({ t: 'status', text: '模型重试中…' });
    return out;
  }
  if (type === 'compact_boundary') {
    push({ t: 'notice', text: '上下文已压缩' });
    return out;
  }
  if (type === 'permission_request' || type === 'ask_user_question') {
    push({ t: 'notice', text: 'TUI 正在等待确认（请在电脑前操作）' });
    return out;
  }

  // —— 流式块 ——
  const delta = str(body.delta);
  const toolCallId = str(
    body.toolCallId ||
      (isRecord(body.toolCall) && (body.toolCall.id || body.toolCall.toolCallId)) ||
      (isRecord(body.part) && body.part.toolCallId)
  );
  const toolName =
    str(body.toolName) ||
    str(isRecord(body.toolCall) && (body.toolCall.name || body.toolCall.toolName)) ||
    str(isRecord(body.part) && body.part.toolName);

  if (kind === 'reasoning_start') {
    state.thinking = '';
    state.thinkingTruncated = false;
    push({ t: 'reasoning_start' });
    return out;
  }
  if (kind === 'reasoning_delta' && delta) {
    if (state.thinking.length >= MAX_BLOCK_CHARS) state.thinkingTruncated = true;
    state.thinking = appendBounded(state.thinking, delta, MAX_BLOCK_CHARS);
    return out;
  }
  if (kind === 'reasoning_end') {
    if (state.thinking.trim()) {
      push({
        t: 'reasoning',
        text: state.thinkingTruncated
          ? `${state.thinking}\n…（TUI 思考块超过 ${MAX_BLOCK_CHARS} 字符，已截断）`
          : state.thinking,
      });
    }
    state.thinking = '';
    state.thinkingTruncated = false;
    return out;
  }
  if (kind === 'text_start') {
    state.answer = '';
    state.answerTruncated = false;
    push({ t: 'text_start' });
    return out;
  }
  if (kind === 'text_delta' && delta) {
    if (state.answer.length >= MAX_BLOCK_CHARS) state.answerTruncated = true;
    state.answer = appendBounded(state.answer, delta, MAX_BLOCK_CHARS);
    return out;
  }
  if (kind === 'text_end') {
    if (state.answer.trim()) {
      push({
        t: 'text',
        text: state.answerTruncated
          ? `${state.answer}\n…（TUI 回答块超过 ${MAX_BLOCK_CHARS} 字符，已截断）`
          : state.answer,
      });
    }
    state.answer = '';
    state.answerTruncated = false;
    return out;
  }

  // —— 工具调用 ——
  const ensureTool = (id) => {
    if (!id) return null;
    let tool = state.tools.get(id);
    if (!tool) {
      tool = { name: toolName || '', start: now, input: '', emitted: false, ended: false };
      state.tools.set(id, tool);
    }
    if (toolName && !tool.name) tool.name = toolName;
    return tool;
  };
  const emitStart = (tool, id, input) => {
    if (!tool || tool.emitted) return;
    tool.emitted = true;
    tool.start = now;
    const raw = input !== undefined ? input : tool.input;
    let bounded;
    if (typeof raw === 'string') {
      bounded = truncate(raw, MAX_TOOL_INPUT_CHARS);
    } else {
      try {
        const json = JSON.stringify(raw);
        bounded = json.length <= MAX_TOOL_INPUT_CHARS ? raw : truncate(json, MAX_TOOL_INPUT_CHARS);
      } catch {
        bounded = truncate(String(raw), MAX_TOOL_INPUT_CHARS);
      }
    }
    push({ t: 'tool_start', id, name: tool.name, input: bounded });
  };

  if (kind === 'tool_input_start') {
    if (toolCallId) state.toolInputs.set(toolCallId, '');
    ensureTool(toolCallId);
    return out;
  }
  if (kind === 'tool_input_delta' && delta) {
    if (toolCallId) {
      state.toolInputs.set(
        toolCallId,
        appendBounded(state.toolInputs.get(toolCallId) || '', delta, MAX_TOOL_INPUT_CHARS)
      );
    }
    ensureTool(toolCallId);
    return out;
  }
  if (kind === 'tool_input_end') {
    const tool = ensureTool(toolCallId);
    if (tool) tool.input = state.toolInputs.get(toolCallId) || '';
    // 不在这里 emit：tool_call 通常紧随其后，带解析好的对象入参；
    // 万一没有 tool_call，result/error 分支会补发 tool_start。
    return out;
  }
  if (kind === 'tool_call' || kind === 'scheduled' || kind === 'started') {
    const tool = ensureTool(toolCallId);
    if (!tool) return out;
    const input = body.input !== undefined ? body.input : tool.input;
    if (input !== undefined) tool.input = input;
    emitStart(tool, toolCallId, input);
    return out;
  }
  if (kind === 'progress') {
    // 进度只更新 TUI 视图，不往 IM 刷（防刷屏）
    ensureTool(toolCallId);
    return out;
  }
  if (kind === 'result') {
    const tool = ensureTool(toolCallId);
    if (!tool || tool.ended) return out;
    if (!tool.emitted) emitStart(tool, toolCallId, tool.input);
    const result = body.result !== undefined ? body.result : body.output;
    const ok = !(isRecord(result) && (result.success === false || result.error));
    const output = isRecord(result)
      ? result.content !== undefined
        ? result.content
        : result.output !== undefined
          ? result.output
          : result.text !== undefined
            ? result.text
            : result
      : result;
    let outText;
    if (typeof output === 'string') {
      outText = truncate(output, MAX_TOOL_CHARS);
    } else {
      try {
        outText = truncate(JSON.stringify(output), MAX_TOOL_CHARS);
      } catch {
        outText = truncate(String(output), MAX_TOOL_CHARS);
      }
    }
    push({
      t: 'tool_end',
      id: toolCallId,
      name: tool.name,
      ok,
      output: outText,
      ms: Math.max(0, now - tool.start),
    });
    tool.ended = true;
    return out;
  }
  if (kind === 'error' && toolCallId) {
    const tool = ensureTool(toolCallId);
    if (!tool || tool.ended) return out;
    if (!tool.emitted) emitStart(tool, toolCallId, tool.input);
    push({
      t: 'tool_end',
      id: toolCallId,
      name: tool.name,
      ok: false,
      output: extractError(body),
      ms: Math.max(0, now - tool.start),
    });
    tool.ended = true;
    return out;
  }
  if (kind === 'closed' && toolCallId) {
    const tool = ensureTool(toolCallId);
    if (!tool || tool.ended) return out;
    if (!tool.emitted) emitStart(tool, toolCallId, tool.input);
    const result = body.result !== undefined ? body.result : body.output;
    let text;
    if (typeof result === 'string') text = truncate(result, MAX_TOOL_CHARS);
    else if (result !== undefined) {
      try {
        text = truncate(JSON.stringify(result), MAX_TOOL_CHARS);
      } catch {
        text = truncate(String(result), MAX_TOOL_CHARS);
      }
    } else text = '（工具已关闭，无输出）';
    push({
      t: 'tool_end',
      id: toolCallId,
      name: tool.name,
      ok: true,
      output: text,
      ms: Math.max(0, now - tool.start),
    });
    tool.ended = true;
    return out;
  }
  return out;
}

// ---------------------------------------------------------------------------
// 写盘：有界队列 + 异步 append + 轮转
// ---------------------------------------------------------------------------

function eventFileFor(tui) {
  const sid = tui && tui.sessionId;
  if (!sid || sid === 'unknown') return null;
  return path.join(EVENTS_DIR, `${sid}.ndjson`);
}

function maybeRotate(file) {
  try {
    if (fs.statSync(file).size > EVENT_FILE_MAX) {
      fs.renameSync(file, `${file}.${Date.now()}`);
      log(`事件文件轮转 ${path.basename(file)}`);
    }
  } catch {}
}

function enqueueLine(state, file, line) {
  if (state.queue.length >= QUEUE_MAX) {
    state.queue.shift();
    state.dropped += 1;
    if (state.dropped === 1 || state.dropped % 100 === 0) {
      log(`事件队列溢出，已丢弃 ${state.dropped} 条`);
    }
  }
  state.queue.push({ file, line });
  if (!state.flushing) flushQueue(state);
}

function flushQueue(state) {
  if (state.flushing) return;
  state.flushing = true;
  (async () => {
    while (state.queue.length) {
      const batch = state.queue.splice(0, 64);
      for (const item of batch) {
        try {
          await fs.promises.appendFile(item.file, item.line, { mode: 0o600 });
        } catch (e) {
          log(`事件写入失败 ${item.file}: ${e && e.message}`);
        }
      }
    }
    state.flushing = false;
  })();
}

function flushSync(state) {
  while (state.queue.length) {
    const item = state.queue.shift();
    try {
      fs.appendFileSync(item.file, item.line, { mode: 0o600 });
    } catch {}
  }
}

function flushPending(state, tui) {
  if (!state.pending.length) return;
  const file = eventFileFor(tui);
  if (!file) return;
  const sid = tui.sessionId;
  const ts = Date.now();
  const batch = state.pending.splice(0);
  ensureDirs();
  maybeRotate(file);
  for (const ev of batch) {
    state.seq += 1;
    const doc = { v: 1, seq: state.seq, ts, sid, ...ev };
    try {
      enqueueLine(state, file, `${JSON.stringify(doc)}\n`);
    } catch {}
  }
  log(`会话 ${sid} 建立，补写缓冲事件 ${batch.length} 条`);
}

function emitEvents(state, tui, events) {
  if (!events.length) return;
  const file = eventFileFor(tui);
  if (!file) {
    bufferPending(state, events);
    return;
  }
  flushPending(state, tui);
  ensureDirs();
  maybeRotate(file);
  const sid = tui.sessionId;
  const ts = Date.now();
  for (const ev of events) {
    state.seq += 1;
    const doc = { v: 1, seq: state.seq, ts, sid, ...ev };
    let line;
    try {
      line = JSON.stringify(doc);
    } catch {
      continue;
    }
    enqueueLine(state, file, `${line}\n`);
  }
}

// ---------------------------------------------------------------------------
// 心跳 / 收件箱 / 登录
// ---------------------------------------------------------------------------

function writeHeartbeat(tui) {
  try {
    ensureDirs();
    const sid = tui && tui.sessionId;
    if (sid && sid !== 'unknown') {
      process.env.ZCODE_SESSION_ID = sid;
      const st = globalThis.__zcImSyncState;
      if (st) flushPending(st, tui);
    }
    const doc = JSON.stringify({
      pid: process.pid,
      session_id: sid || null,
      cwd:
        process.env.ZCODE_PROJECT_DIR ||
        (tui && tui.options && tui.options.workspaceDirectory) ||
        process.cwd(),
      busy: Boolean(tui && tui.primaryTurnActive),
      events: true,
      ts: Date.now(),
    });
    const tmp = `${HEARTBEAT}.${process.pid}.tmp`;
    fs.writeFileSync(tmp, doc);
    fs.renameSync(tmp, HEARTBEAT);
  } catch {
    /* 心跳失败只影响分流，不影响 TUI */
  }
}

/** 按文件名排序（桥用 <ts>-<id>.json 命名）取第一条并原子认领。 */
function claimFirst(tuiSessionId) {
  let names;
  try {
    names = fs.readdirSync(INBOX_DIR).filter((n) => n.endsWith('.json')).sort();
  } catch {
    return null;
  }
  for (const name of names) {
    const file = path.join(INBOX_DIR, name);
    let msg;
    try {
      msg = JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch (e) {
      log(`收件箱文件损坏，丢弃 ${name}: ${e && e.message}`);
      try {
        fs.unlinkSync(file);
      } catch {}
      continue;
    }
    const target = msg && msg.target_session;
    if (target) {
      if (!tuiSessionId || tuiSessionId === 'unknown' || target !== tuiSessionId) continue;
    }
    const claimed = file.replace(/\.json$/, '.tui-running');
    try {
      fs.renameSync(file, claimed);
    } catch (e) {
      if (e && e.code === 'ENOENT') continue;
      log(`认领失败 ${name}: ${e && e.message}`);
      continue;
    }
    return { file: claimed, msg };
  }
  return null;
}

function findBridgeBin() {
  const candidates = [
    process.env.IM_BRIDGE_BIN,
    path.join(os.homedir(), '.local/bin/im-bridge'),
    path.join(__dirname, '..', 'target', 'release', 'im-bridge'),
    path.join(__dirname, 'target', 'release', 'im-bridge'),
  ].filter(Boolean);
  for (const c of candidates) {
    try {
      if (fs.existsSync(c)) return c;
    } catch {}
  }
  return null;
}

function notice(tui, text) {
  log(`notice: ${text}`);
  try {
    if (tui && typeof tui.addNotice === 'function') {
      tui.addNotice(text, 'muted');
      if (tui.ui && typeof tui.ui.requestRender === 'function') tui.ui.requestRender();
    }
  } catch {}
}

function startLogin(tui, platform) {
  const bin = findBridgeBin();
  if (!bin) {
    notice(tui, '❌ 未找到 im-bridge：请先运行 zcode-im-sync 的 install.sh');
    return;
  }
  const display = process.env.DISPLAY || '';
  const args = ['login', platform, '--bind-latest'];
  if (display) args.push('--qr-open');
  else args.push('--qr-inline');

  const env = {
    ...process.env,
    IM_BRIDGE_HOME: BRIDGE_HOME,
    // 告诉桥这是 TUI 发起的登录：没有 ZCODE_SESSION_ID 时写占位绑定，
    // 等心跳报出真实会话后自动补绑，避免绑到同目录的旧会话。
    ZCODE_TUI: '1',
    ZCODE_SESSION_ID: (tui && tui.sessionId) || '',
    ZCODE_PROJECT_DIR:
      (tui && tui.options && tui.options.workspaceDirectory) || process.cwd(),
  };
  const loginLog = path.join(STATE_DIR, `login-${platform}.log`);
  let out = '';
  let err = '';
  let child;
  try {
    child = spawn(bin, args, { env, stdio: ['ignore', 'pipe', 'pipe'] });
  } catch (e) {
    notice(tui, `❌ 启动登录失败：${e && e.message}`);
    return;
  }
  const tail = (s, n) => (s.length > n ? s.slice(-n) : s);
  child.stdout.on('data', (d) => {
    out = tail(out + d.toString(), 8192);
    if (!display) {
      try {
        fs.appendFileSync(loginLog, d.toString(), { mode: 0o600 });
      } catch {}
    }
  });
  child.stderr.on('data', (d) => {
    err = tail(err + d.toString(), 4096);
  });
  child.on('error', (e) => notice(tui, `❌ ${platform} 登录进程错误：${e && e.message}`));
  child.on('exit', (code) => {
    if (code === 0) {
      const sid = tui && tui.sessionId;
      const bound =
        sid && sid !== 'unknown'
          ? `已绑定当前会话 ${sid}`
          : '当前 TUI 还没发过消息，发出第一条后会自动补绑';
      notice(tui, `✅ ${platform} 登录成功（${bound}），双向同步已开启`);
    } else {
      const detail = tail((err || out).trim(), 300);
      notice(tui, `❌ ${platform} 登录失败（exit ${code}）：${detail || '见 ' + loginLog}`);
    }
  });

  if (display) {
    notice(tui, `📱 正在生成 ${platform} 登录二维码，窗口即将弹出，请用手机扫码…`);
  } else {
    notice(tui, `📱 正在生成 ${platform} 登录二维码，字符码写入 ${loginLog}，请查看后扫码…`);
  }
}

function installSlashCommands(tui) {
  try {
    const options = tui && tui.options;
    if (!options) return;
    const list = Array.isArray(options.slashCommands) ? options.slashCommands : [];
    for (const [name, desc] of [
      ['wechat', '扫码登录微信并绑定当前 TUI 会话（双向全过程同步）'],
      ['feishu', '扫码登录飞书并绑定当前 TUI 会话（双向全过程同步）'],
    ]) {
      if (!list.some((c) => (c.name || '').replace(/^\//, '') === name)) {
        list.push({ name, description: desc, argumentHint: '' });
      }
    }
    options.slashCommands = list;
  } catch {}
}

// ---------------------------------------------------------------------------
// 启动：包装 submit / onEvent / finishTurn + 心跳 + 收件箱轮询
// ---------------------------------------------------------------------------

function startSync(tui) {
  if (globalThis.__zcImSyncStarted) return;
  globalThis.__zcImSyncStarted = true;
  globalThis.__zcTui = tui;
  ensureDirs();

  const state = makeCaptureState();
  globalThis.__zcImSyncState = state;

  // 1) 用户输入（本机 / 微信注入）与 /wechat /feishu 拦截
  const origSubmit = tui.submit.bind(tui);
  tui.submit = async function patchedSubmit(rawInput, queuedSubmission) {
    const input = String(
      (queuedSubmission && queuedSubmission.input) || rawInput || ''
    ).trim();
    if (/^\/wechat(?:\s|$)/i.test(input)) {
      startLogin(tui, 'wechat');
      return true;
    }
    if (/^\/feishu(?:\s|$)/i.test(input)) {
      startLogin(tui, 'feishu');
      return true;
    }
    const inj = globalThis.__zcImInjecting;
    const fromIm = Boolean(inj && inj.text === input);
    debugLog(`submit len=${input.length} sid=${tui.sessionId} fromIm=${fromIm}`);
    if (!fromIm && input && !input.startsWith('/')) {
      emitEvents(state, tui, [{ t: 'user', src: 'tui', text: truncate(input, MAX_BLOCK_CHARS) }]);
    }
    try {
      return await origSubmit(rawInput, queuedSubmission);
    } finally {
      if (fromIm) globalThis.__zcImInjecting = null;
    }
  };

  // 2) 过程事件（块级）
  const origOnEvent = tui.onEvent.bind(tui);
  tui.onEvent = function patchedOnEvent(value, turnEpoch) {
    const result = origOnEvent(value, turnEpoch);
    try {
      debugLog(
        `ev type=${typeOf(value)} kind=${str(bodyOf(value).kind)} sid=${tui.sessionId} ` +
          `active=${tui.primaryTurnActive} epoch=${turnEpoch}/${tui.activeTurnEpoch}`
      );
      if (process.env.IM_SYNC_DEBUG === '1' && typeOf(value).startsWith('tool_')) {
        try {
          debugLog(`  body=${truncate(JSON.stringify(bodyOf(value)), 800)}`);
        } catch {}
      }
      const events = captureEvent(state, tui, value, turnEpoch);
      if (events.length) emitEvents(state, tui, events);
    } catch (e) {
      log(`onEvent 采集异常（忽略）: ${e && e.message}`);
    }
    return result;
  };

  // 3) 回合结束兜底（abort/stop 等没有 turn_complete 的路径）
  const origFinish = tui.finishTurn;
  if (typeof origFinish === 'function') {
    tui.finishTurn = function patchedFinishTurn(...args) {
      try {
        if (state.turnActive && !state.turnEnded) {
          state.turnActive = false;
          state.turnEnded = true;
          emitEvents(state, tui, [{ t: 'turn_end', reason: 'finish' }]);
        }
      } catch {}
      return origFinish.apply(this, args);
    };
  }

  installSlashCommands(tui);

  // 4) 心跳 + 收件箱轮询（2s 一拍；只在空闲且输入框为空时注入）
  const attempts = Object.create(null);
  const giveUp = (claimed, why) => {
    const failed = claimed.replace(/\.tui-running$/, '.failed');
    try {
      fs.renameSync(claimed, failed);
    } catch {}
    log(`放弃 ${path.basename(claimed)} 原因=${why}（交还桥兜底）`);
    delete attempts[claimed];
  };

  const tick = () => {
    try {
      writeHeartbeat(tui);
      if (tui.primaryTurnActive) return;
      const editor = tui.editor;
      if (editor && typeof editor.getText === 'function' && editor.getText().trim() !== '') {
        return;
      }
      const picked = claimFirst(tui.sessionId);
      if (!picked) return;
      const text = String((picked.msg && picked.msg.text) || '');
      const id = (picked.msg && picked.msg.id) || path.basename(picked.file, '.tui-running');
      if (!text.trim()) {
        try {
          fs.unlinkSync(picked.file);
        } catch {}
        return;
      }
      attempts[picked.file] = (attempts[picked.file] || 0) + 1;
      const n = attempts[picked.file];
      log(`注入 id=${id} chars=${text.length} attempt=${n}`);
      globalThis.__zcImInjecting = { id, text: text.trim() };
      Promise.resolve(tui.submit(text))
        .then((result) => {
          globalThis.__zcImInjecting = null;
          if (result === false) {
            log(`注入被拒 id=${id} attempt=${n}`);
            if (n >= MAX_ATTEMPTS) return giveUp(picked.file, 'submit=false');
            try {
              fs.renameSync(picked.file, picked.file.replace(/\.tui-running$/, '.json'));
            } catch {}
            return;
          }
          try {
            fs.unlinkSync(picked.file);
          } catch {}
          delete attempts[picked.file];
          log(`已注入并完成 id=${id} chars=${text.length}`);
        })
        .catch((e) => {
          globalThis.__zcImInjecting = null;
          log(`注入异常 id=${id} attempt=${n} err=${e && e.message}`);
          if (n >= MAX_ATTEMPTS) return giveUp(picked.file, 'throw');
          try {
            fs.renameSync(picked.file, picked.file.replace(/\.tui-running$/, '.json'));
          } catch {}
        });
    } catch (e) {
      log(`tick 异常: ${e && e.message}`);
    }
  };

  const timer = setInterval(tick, POLL_MS);
  if (timer && typeof timer.unref === 'function') timer.unref();
  tick();
  log(`sync start session=${(tui && tui.sessionId) || '?'} pid=${process.pid}`);

  process.on('exit', () => {
    try {
      flushSync(state);
    } catch {}
  });
}

// ---------------------------------------------------------------------------
// 模块加载钩子（ESM registerHooks + CJS 兜底）
// ---------------------------------------------------------------------------

globalThis.__zcImStart = startSync;

function patchSource(src) {
  if (src.includes(MARKER)) return src;
  const hits = src.split(ANCHOR).length - 1;
  if (hits !== 1) return src;
  return src.replace(
    ANCHOR,
    `${ANCHOR}\ntry{globalThis.__zcTui=this;if(globalThis.__zcImStart)globalThis.__zcImStart(this)}catch(__zcImErr){}`
  );
}

function install() {
  let esm = false;
  try {
    const { registerHooks } = require('node:module');
    if (typeof registerHooks === 'function') {
      registerHooks({
        load(url, context, nextLoad) {
          const result = nextLoad(url, context);
          if (!TUI_URL_RE.test(url)) return result;
          try {
            const src =
              typeof result.source === 'string'
                ? result.source
                : result.source
                  ? Buffer.from(result.source).toString('utf8')
                  : null;
            if (!src) return result;
            const out = patchSource(src);
            if (out === src) return result;
            return { ...result, source: out };
          } catch (e) {
            log(`ESM 注入失败，回退原模块: ${e && e.message}`);
            return result;
          }
        },
      });
      esm = true;
      debugLog('ESM 钩子已安装（registerHooks）');
    }
  } catch (e) {
    log(`registerHooks 不可用（忽略）: ${e && e.message}`);
  }

  for (const ext of ['.js', '.cjs']) {
    const orig = Module._extensions[ext];
    if (!orig) continue;
    Module._extensions[ext] = function patchedLoader(mod, filename) {
      if (!TARGET_RE.test(filename)) return orig.call(this, mod, filename);
      let src;
      try {
        src = fs.readFileSync(filename, 'utf8');
      } catch {
        return orig.call(this, mod, filename);
      }
      const out = patchSource(src);
      if (out === src) return orig.call(this, mod, filename);
      try {
        return mod._compile(out, filename);
      } catch (e) {
        log(`编译注入后源码失败，回退原文件: ${e && e.message}`);
        return orig.call(this, mod, filename);
      }
    };
  }
  return esm;
}

try {
  if (process.env.ZC_IM_INJECT !== '0' && isTuiProcess()) install();
} catch (e) {
  try {
    log(`install 异常（忽略）: ${e && e.message}`);
  } catch {}
}

module.exports = {
  patchSource,
  install,
  isTuiProcess,
  TARGET_RE,
  ANCHOR,
  __test: { bodyOf, typeOf, sessionOf, captureEvent, makeCaptureState, truncate },
};

// 自检：node zcode-tui-im-sync.cjs --check [target]
// 不给 target 时从 `zcode` 可执行文件反推 npm 包内的 TUI bundle 路径。
if (process.argv.includes('--check')) {
  function resolveTarget() {
    const fromEnv = process.env.ZCODE_TUI_INDEX;
    if (fromEnv) return fromEnv;
    const candidates = [];
    const zcodeBin = process.env.ZCODE_BIN || 'zcode';
    try {
      const which = require('node:child_process').execFileSync('sh', ['-c', `command -v ${zcodeBin}`], {
        encoding: 'utf8',
      }).trim();
      if (which) {
        const real = fs.realpathSync(which);
        // <pkg>/bin/zcode.js → <pkg>/vendor/node_modules/@zcode/tui/dist/index.js
        candidates.push(path.join(path.dirname(path.dirname(real)), 'vendor/node_modules/@zcode/tui/dist/index.js'));
        candidates.push(path.join(path.dirname(path.dirname(real)), 'node_modules/@zcode/tui/dist/index.js'));
      }
    } catch {}
    candidates.push(
      path.join(process.env.HOME || '', '.npm-global/lib/node_modules/zcode-app-cli/vendor/node_modules/@zcode/tui/dist/index.js'),
      '/usr/local/lib/node_modules/zcode-app-cli/vendor/node_modules/@zcode/tui/dist/index.js',
      '/usr/lib/node_modules/zcode-app-cli/vendor/node_modules/@zcode/tui/dist/index.js'
    );
    return candidates.find((p) => p && fs.existsSync(p)) || candidates[0];
  }
  const target = process.argv[process.argv.indexOf('--check') + 1] || resolveTarget();
  const src = fs.readFileSync(target, 'utf8');
  const out = patchSource(src);
  console.log('target   :', target);
  console.log('size     :', src.length);
  console.log('already  :', src.includes(MARKER));
  console.log('patched? :', out !== src);
  console.log('anchor   :', src.split(ANCHOR).length - 1, 'hit(s)');
  process.exit(out !== src || src.includes(MARKER) ? 0 : 1);
}
