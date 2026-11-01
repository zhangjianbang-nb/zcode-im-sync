'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { __test } = require('../zcode-tui-im-sync.cjs');
const { captureEvent, makeCaptureState, bodyOf, truncate } = __test;

function tui(overrides = {}) {
  return {
    sessionId: 'sess_test',
    primaryTurnActive: true,
    activeTurnEpoch: 1,
    ...overrides,
  };
}

function ev(payload, envelope = {}) {
  return { ...envelope, params: { payload } };
}

test('bodyOf 兼容 params.payload / payload.event / 裸对象', () => {
  assert.deepEqual(bodyOf({ params: { payload: { kind: 'text_delta' } } }), {
    kind: 'text_delta',
  });
  assert.deepEqual(bodyOf({ payload: { event: { type: 'turn_started' } } }), {
    type: 'turn_started',
  });
  assert.deepEqual(bodyOf({ kind: 'tool_call' }), { kind: 'tool_call' });
});

test('完整一轮：用户消息→思考→工具→回答→结束', () => {
  const state = makeCaptureState();
  const t = tui();
  const all = [];
  const feed = (payload, envelope) => {
    const out = captureEvent(state, t, ev(payload, envelope), 1);
    all.push(...out);
    return out;
  };

  feed({ type: 'turn_started' });
  feed({ kind: 'reasoning_start' });
  feed({ kind: 'reasoning_delta', delta: '先看看目录' });
  feed({ kind: 'reasoning_delta', delta: '，再改文件' });
  feed({ kind: 'reasoning_end' });

  feed({ kind: 'tool_input_start', toolCallId: 'c1', toolName: 'Bash' });
  feed({ kind: 'tool_input_delta', toolCallId: 'c1', delta: 'ls -la' });
  feed({ kind: 'tool_input_end', toolCallId: 'c1' });
  feed({ kind: 'tool_call', toolCallId: 'c1', toolName: 'Bash', input: { command: 'ls -la' } });
  feed({
    kind: 'result',
    toolCallId: 'c1',
    result: { content: 'total 8', success: true },
  });

  feed({ kind: 'text_start' });
  feed({ kind: 'text_delta', delta: '目录里有 3 个文件' });
  feed({ kind: 'text_end' });
  feed({ type: 'turn_complete', tokenCount: 1234, toolCallCount: 1 });

  const types = all.map((e) => e.t);
  assert.deepEqual(types, [
    'turn_start',
    'reasoning_start',
    'reasoning',
    'tool_start',
    'tool_end',
    'text_start',
    'text',
    'turn_end',
  ]);
  assert.equal(all.find((e) => e.t === 'reasoning').text, '先看看目录，再改文件');
  assert.equal(all.find((e) => e.t === 'tool_start').input.command, 'ls -la');
  assert.equal(all.find((e) => e.t === 'tool_end').ok, true);
  assert.match(all.find((e) => e.t === 'tool_end').output, /total 8/);
  assert.equal(all.find((e) => e.t === 'text').text, '目录里有 3 个文件');
  assert.equal(all.find((e) => e.t === 'turn_end').tokens, 1234);
  assert.equal(all.find((e) => e.t === 'turn_end').tools, 1);
});

test('tool_input_end 不抢发，tool_call 带对象入参只发一次', () => {
  const state = makeCaptureState();
  const t = tui();
  const out1 = captureEvent(state, t, ev({ kind: 'tool_input_end', toolCallId: 'c1', toolName: 'Read' }), 1);
  const out2 = captureEvent(
    state,
    t,
    ev({ kind: 'tool_call', toolCallId: 'c1', toolName: 'Read', input: { file_path: '/a' } }),
    1
  );
  assert.equal(out1.filter((e) => e.t === 'tool_start').length, 0);
  assert.equal(out2.filter((e) => e.t === 'tool_start').length, 1);
  assert.equal(out2.find((e) => e.t === 'tool_start').input.file_path, '/a');
});

test('tool_call_result 用 type 表达 kind，也能落 tool_end', () => {
  const state = makeCaptureState();
  const t = tui();
  const out = captureEvent(
    state,
    t,
    ev({ type: 'tool_call_result', toolCallId: 'c9', toolName: 'Bash', result: { content: 'ok', success: true } }),
    1
  );
  const end = out.find((e) => e.t === 'tool_end');
  assert.ok(end);
  assert.equal(end.ok, true);
  assert.match(end.output, /ok/);
});

test('跳过 part.* 快照、子代理事件、其它会话', () => {
  const state = makeCaptureState();
  const t = tui();
  assert.equal(captureEvent(state, t, ev({ type: 'part.updated', part: {} }), 1).length, 0);
  assert.equal(captureEvent(state, t, ev({ kind: 'part.delta' }), 1).length, 0);
  assert.equal(captureEvent(state, t, ev({ kind: 'tool_call', taskId: 'task1' }), 1).length, 0);
  assert.equal(
    captureEvent(state, t, ev({ kind: 'text_delta', delta: 'x' }, { sessionId: 'sess_other' }), 1).length,
    0
  );
});

test('非当前回合/非忙碌时不采集', () => {
  const state = makeCaptureState();
  assert.equal(captureEvent(state, tui(), ev({ kind: 'text_delta', delta: 'x' }), 2).length, 0);
  assert.equal(
    captureEvent(state, tui({ primaryTurnActive: false }), ev({ kind: 'text_delta', delta: 'x' }), 1)
      .length,
    0
  );
});

test('同一 eventId 只采一次', () => {
  const state = makeCaptureState();
  const t = tui();
  const e = ev({ kind: 'text_delta', delta: 'x' }, { eventId: 'e1' });
  assert.equal(captureEvent(state, t, e, 1).length, 0); // text_delta 没有 text_start 也不落块
  const e2 = ev({ kind: 'text_end' }, { eventId: 'e2' });
  captureEvent(state, t, e2, 1);
  assert.equal(captureEvent(state, t, e2, 1).length, 0);
});

test('长文本按上限截断并标注', () => {
  const s = truncate('汉'.repeat(100), 10);
  assert.match(s, /截断/);
  assert.equal(s.slice(0, 10), '汉'.repeat(10));
});

test('patchSource 锚点恰好命中一次', () => {
  const fs = require('node:fs');
  const path = require('node:path');
  const target =
    process.env.ZCODE_TUI_INDEX ||
    '/home/whu/.npm-global/lib/node_modules/zcode-app-cli/vendor/node_modules/@zcode/tui/dist/index.js';
  if (!fs.existsSync(target)) {
    return; // 非本机 CI 跳过
  }
  const src = fs.readFileSync(target, 'utf8');
  const out = require('../zcode-tui-im-sync.cjs').patchSource(src);
  assert.ok(out.includes('globalThis.__zcImStart'));
  assert.equal(out.split('globalThis.__zcImStart').length - 1, 2); // 注入一行 + MARKER 检查
});
