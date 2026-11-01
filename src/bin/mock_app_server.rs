//! mock zcode app-server（测试专用二进制，脚本化反向请求 / -32010 / 事件回放）。
//!
//! 语义与真机对齐的部分（回归护栏）：
//! - 启动发 `startup/storageState` 噪音通知（客户端必须容忍）；
//! - 服务端反向请求用**字符串 id**（`server-N`，真机就是字符串），且
//!   create/resume 的回包以「反向请求被应答」为前置 gate——不应答就不回包
//!   （真机等 15s 后回 `-32022`；mock 用 gate 语义等价，测试断言更确定）；
//! - `session/send` 按消息文本脚本化：
//!   含 `BUSY` → 前 2 次回 `-32010`（测 500ms 重试）；含 `FAIL` → 发 turn error 事件；
//!   含 `HANG` → 收下永不发完成事件（测 turn 超时路径，手动场景）；
//!   默认 → text_delta 流 + `response`+`tokenCount` 完成事件；
//! - `session/close` 回包后进程退出。
//!
//! 协议形状参照 zcode-remote bridge.py（MIT）事实对齐，代码自写。

use std::io::{BufRead, BufReader, Write};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

use serde_json::{json, Value};

static SESSION_N: AtomicU32 = AtomicU32::new(0);
static SERVER_ID: AtomicU32 = AtomicU32::new(0);
static SEND_N: AtomicUsize = AtomicUsize::new(0);

fn main() {
    // argv[1] 可能是 "app-server"（ZcodeHandle::spawn 拼的），忽略即可
    let stdin = std::io::stdin();
    let mut out = std::io::stdout();

    // 启动噪音通知（真机会发，客户端必须容忍）
    emit(
        &mut out,
        &json!({"method":"startup/storageState","params":{"phase":"ready","sequence":1}}),
    );
    emit(
        &mut out,
        &json!({"method":"startup/storageState","params":{"phase":"ready","sequence":2}}),
    );

    // 挂起的客户端请求：回包要等反向请求被应答（gate 语义）
    struct PendingReq {
        id: Value,
        method: String,
    }
    let mut waiting: Option<PendingReq> = None;
    // 服务端反向请求 id → 等其应答（与 waiting 配对）
    let mut expect_answer: Option<String> = None;

    let reader = BufReader::new(stdin.lock());
    for line in reader.lines() {
        let Ok(line) = line else { break };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(msg) = serde_json::from_str::<Value>(line) else {
            continue;
        };

        // 反向请求的应答：{"id":"server-N","result":...}（无 method）→ 放行挂起请求
        if let (Some(id), None) = (msg.get("id").cloned(), msg.get("method").as_ref()) {
            let key = id_key(&id);
            if expect_answer.as_deref() == Some(key.as_str()) {
                expect_answer = None;
                if let Some(req) = waiting.take() {
                    answer_open_session(&mut out, &req.id, &req.method);
                }
            }
            continue;
        }

        let Some(method) = msg.get("method").and_then(|x| x.as_str()) else {
            continue;
        };
        let id = msg.get("id").cloned().unwrap_or(Value::Null);
        let params = msg.get("params").cloned().unwrap_or(json!({}));

        match method {
            "session/create" | "session/resume" => {
                // 先发反向请求（字符串 id），应答到了才回包——真机行为的确定性化
                let rid = format!("server-{}", SERVER_ID.fetch_add(1, Ordering::SeqCst) + 1);
                let rmethod = if method == "session/resume" {
                    "session/requestRuntimePreferences"
                } else {
                    "interaction/requestOfficialMcpAuthHeaders"
                };
                emit(
                    &mut out,
                    &json!({"id": rid, "method": rmethod, "params": {"scope":"mock"}}),
                );
                expect_answer = Some(rid);
                waiting = Some(PendingReq {
                    id,
                    method: method.to_string(),
                });
            }
            "session/subscribe" => {
                emit(&mut out, &json!({"id": id, "result": {}}));
                // 回放一条带 eventId 的标题事件（验证去重/回放容忍）
                emit(
                    &mut out,
                    &json!({"method":"session/event",
                        "params":{"eventId":"mock-ev-title",
                        "payload":{"title":"mock session","source":"auto"}}}),
                );
            }
            "session/send" => {
                // ZcodeHandle::send 的正文参数名是 content（message/text 作兼容兜底）
                let text = params
                    .get("content")
                    .or_else(|| params.get("message"))
                    .or_else(|| params.get("text"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let n = SEND_N.fetch_add(1, Ordering::SeqCst);
                if text.contains("BUSY") && n < 2 {
                    emit(
                        &mut out,
                        &json!({"id": id, "error": {"code": -32010,
                            "message": "A prompt is already running in this session"}}),
                    );
                    continue;
                }
                emit(&mut out, &json!({"id": id, "result": {"ok": true}}));
                if text.contains("HANG") {
                    continue; // 收下永不完成（测 turn 超时路径，手动场景）
                }
                if text.contains("FAIL") {
                    emit(
                        &mut out,
                        &json!({"method":"session/event","params":{"eventId":
                            format!("mock-ev-err-{n}"),
                            "payload":{"kind":"error","error":"mock turn failed"}}}),
                    );
                    continue;
                }
                emit(
                    &mut out,
                    &json!({"method":"session/event","params":{"eventId":
                        format!("mock-ev-delta1-{n}"),
                        "payload":{"kind":"text_delta","delta":"MOCK:","done":false}}}),
                );
                emit(
                    &mut out,
                    &json!({"method":"session/event","params":{"eventId":
                        format!("mock-ev-delta2-{n}"),
                        "payload":{"kind":"text_delta","delta":text,"done":true}}}),
                );
                emit(
                    &mut out,
                    &json!({"method":"session/event","params":{"eventId":
                        format!("mock-ev-done-{n}"),
                        "payload":{"response":format!("MOCK-REPLY:{text}"),
                        "tokenCount":42,"toolCallCount":1}}}),
                );
            }
            "session/stop" => {
                emit(&mut out, &json!({"id": id, "result": {"ok": true}}));
            }
            "session/close" => {
                emit(&mut out, &json!({"id": id, "result": {"ok": true}}));
                let _ = out.flush();
                std::process::exit(0);
            }
            "workspace/updateProviderRegistry" => {
                emit(&mut out, &json!({"id": id, "result": {"ok": true}}));
            }
            other => {
                eprintln!("mock-app-server 未实现方法 {other}，空对象兜底");
                emit(&mut out, &json!({"id": id, "result": {}}));
            }
        }
        let _ = out.flush();
    }
}

/// create/resume 的回包（gate 放行后的正常路径）。
fn answer_open_session(out: &mut impl Write, id: &Value, method: &str) {
    let sid = format!("sess_mock-{}", SESSION_N.fetch_add(1, Ordering::SeqCst) + 1);
    let ws = json!({"workspaceKey": "/tmp/mock", "workspacePath": "/tmp/mock"});
    if method == "session/resume" {
        emit(
            out,
            &json!({"id": id, "result": {
                "sessionId": sid,
                "session": {"sessionId": sid, "workspace": ws},
                "messages": [],
            }}),
        );
    } else {
        emit(out, &json!({"id": id, "result": {"sessionId": sid}}));
    }
}

fn emit(out: &mut impl Write, v: &Value) {
    let _ = writeln!(out, "{v}");
    let _ = out.flush();
}

/// JSON-RPC id 键：字符串取裸串（与 zcode_rpc::id_key 同语义）。
fn id_key(v: &Value) -> String {
    v.as_str()
        .map(String::from)
        .unwrap_or_else(|| v.to_string())
}
