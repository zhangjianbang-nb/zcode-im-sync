//! zcode app-server JSON-RPC 行协议的事件侧：原始 payload → 规范化事件。
//!
//! 语义与 zcode-remote bridge.py（协议参考，MIT，不复制代码）对齐：
//! `session/event` 的 payload.kind 决定事件种类；`response`+`tokenCount` 为 turn 完成。

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 规范化后的会话事件（IM 回程只关心 TurnDone/TurnError，其余用于状态/日志）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ZEvent {
    /// 用户输入回显（IM 自发消息会回显——不能镜像回 IM，防自回声）
    UserEcho { text: String },
    /// 助手流式文本增量
    AssistantText { text: String, done: bool },
    /// 思考增量（默认不回程）
    Reasoning { text: String },
    /// 工具调用开始
    ToolStart {
        call_id: String,
        tool: String,
        input: String,
    },
    /// 工具调用结束
    ToolEnd {
        call_id: String,
        success: bool,
        content: String,
    },
    /// 一轮完成（payload.response = 完整回复）
    TurnDone {
        response: Option<String>,
        token_count: Option<u64>,
        tool_call_count: Option<u64>,
    },
    /// 一轮出错
    TurnError { message: String },
    /// 会话标题
    Title { title: String },
}

/// 把 zcode 原始 payload 规范化；不认识的返回 None。
pub fn normalize_event(p: &Value) -> Option<ZEvent> {
    let kind = p.get("kind").and_then(|x| x.as_str()).unwrap_or("");
    match kind {
        "text_delta" => {
            return Some(ZEvent::AssistantText {
                text: str_of(p, "delta"),
                done: p.get("done").and_then(|x| x.as_bool()).unwrap_or(false),
            })
        }
        "reasoning_delta" => {
            return Some(ZEvent::Reasoning {
                text: str_of(p, "delta"),
            })
        }
        "tool_call" => {
            return Some(ZEvent::ToolStart {
                call_id: str_of(p, "toolCallId"),
                tool: str_of(p, "toolName"),
                input: p.get("input").map(compact).unwrap_or_default(),
            })
        }
        "result" if p.get("toolCallId").is_some() => {
            let r = p.get("result");
            // content 优先取 result.content 字符串（人读），否则整块紧凑 JSON
            let content = r
                .and_then(|x| x.get("content"))
                .and_then(|x| x.as_str())
                .map(String::from)
                .or_else(|| r.map(compact))
                .unwrap_or_default();
            return Some(ZEvent::ToolEnd {
                call_id: str_of(p, "toolCallId"),
                success: r
                    .and_then(|x| x.get("success"))
                    .and_then(|x| x.as_bool())
                    .unwrap_or(false),
                content,
            });
        }
        "tool_error" => {
            return Some(ZEvent::ToolEnd {
                call_id: str_of(p, "toolCallId"),
                success: false,
                content: str_of(p, "error"),
            })
        }
        "error" => {
            let m = p
                .get("error")
                .or_else(|| p.get("message"))
                .map(compact)
                .unwrap_or_default();
            return Some(ZEvent::TurnError {
                message: if m.is_empty() {
                    "未知错误".into()
                } else {
                    m
                },
            });
        }
        _ => {}
    }
    // 无 kind 的完成/回显/标题事件（bridge.py 同序判定）
    if p.get("response").is_some() && p.get("tokenCount").is_some() {
        return Some(ZEvent::TurnDone {
            response: p.get("response").and_then(|x| x.as_str()).map(String::from),
            token_count: p.get("tokenCount").and_then(|x| x.as_u64()),
            tool_call_count: p.get("toolCallCount").and_then(|x| x.as_u64()),
        });
    }
    if p.get("input").is_some() && p.get("turnNumber").is_some() {
        return Some(ZEvent::UserEcho {
            text: str_of(p, "input"),
        });
    }
    if p.get("title").is_some() && p.get("source").is_some() {
        return Some(ZEvent::Title {
            title: str_of(p, "title"),
        });
    }
    let ty = p.get("type").and_then(|x| x.as_str()).unwrap_or("");
    if ty.ends_with("_failed") || ty == "model_request_failed" {
        return Some(ZEvent::TurnError {
            message: compact(p),
        });
    }
    None
}

fn str_of(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string()
}

fn compact(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// eventId 去重集合（cap 5000，裁到 2000，防止 resume 回放风暴重复渲染）。
#[derive(Debug, Default)]
pub struct SeenSet {
    keys: HashSet<String>,
}

impl SeenSet {
    /// 返回 true 表示新事件（首次见到）；false 表示重复。
    pub fn insert(&mut self, key: &str) -> bool {
        if self.keys.contains(key) {
            return false;
        }
        self.keys.insert(key.to_string());
        if self.keys.len() > 5000 {
            let keep: Vec<String> = self.keys.iter().take(2000).cloned().collect();
            self.keys = keep.into_iter().collect();
        }
        true
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn text_delta_maps() {
        let e = normalize_event(&json!({"kind":"text_delta","delta":"你好","done":true})).unwrap();
        match e {
            ZEvent::AssistantText { text, done } => {
                assert_eq!(text, "你好");
                assert!(done);
            }
            other => panic!("wrong: {other:?}"),
        }
    }

    #[test]
    fn turn_done_needs_token_count() {
        let e =
            normalize_event(&json!({"response":"答案","tokenCount":42,"toolCallCount":2})).unwrap();
        match e {
            ZEvent::TurnDone {
                response,
                token_count,
                tool_call_count,
            } => {
                assert_eq!(response.as_deref(), Some("答案"));
                assert_eq!(token_count, Some(42));
                assert_eq!(tool_call_count, Some(2));
            }
            other => panic!("wrong: {other:?}"),
        }
        // 只有 response 不算完成
        assert!(normalize_event(&json!({"response":"x"})).is_none());
    }

    #[test]
    fn tool_lifecycle_maps() {
        let s = normalize_event(&json!({"kind":"tool_call","toolCallId":"c1","toolName":"Bash","input":{"command":"ls"}}))
            .unwrap();
        assert!(matches!(s, ZEvent::ToolStart { .. }));
        let r = normalize_event(
            &json!({"kind":"result","toolCallId":"c1","result":{"success":true,"content":"ok"}}),
        )
        .unwrap();
        match r {
            ZEvent::ToolEnd {
                call_id,
                success,
                content,
            } => {
                assert_eq!(call_id, "c1");
                assert!(success);
                assert_eq!(content, "ok");
            }
            other => panic!("wrong: {other:?}"),
        }
        let err = normalize_event(&json!({"kind":"tool_error","toolCallId":"c1","error":"boom"}))
            .unwrap();
        assert!(matches!(err, ZEvent::ToolEnd { success: false, .. }));
    }

    #[test]
    fn failed_types_are_turn_error() {
        let e = normalize_event(&json!({"type":"model_request_failed","x":1})).unwrap();
        assert!(matches!(e, ZEvent::TurnError { .. }));
        let e2 = normalize_event(&json!({"type":"something_failed"})).unwrap();
        assert!(matches!(e2, ZEvent::TurnError { .. }));
    }

    #[test]
    fn user_echo_and_title() {
        let u = normalize_event(&json!({"input":"hi","turnNumber":3})).unwrap();
        assert!(matches!(u, ZEvent::UserEcho { .. }));
        let t = normalize_event(&json!({"title":"会话标题","source":"auto"})).unwrap();
        assert!(matches!(t, ZEvent::Title { .. }));
    }

    #[test]
    fn unknown_event_none() {
        assert!(normalize_event(&json!({"kind":"telemetry"})).is_none());
        assert!(normalize_event(&json!({})).is_none());
    }

    #[test]
    fn seen_set_dedups() {
        let mut s = SeenSet::default();
        assert!(s.insert("a"));
        assert!(!s.insert("a"));
        assert!(s.insert("b"));
        assert_eq!(s.len(), 2);
    }
}
