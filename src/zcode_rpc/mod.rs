//! zcode app-server 驱动（stdio JSON-RPC 2.0，换行分隔）。
//!
//! 协议事实参照 zcode-remote bridge.py（MIT，仅借协议语义，代码 Rust 重写）：
//! - 服务端会发反向请求（`session/requestRuntimePreferences` 等），**必须先应答**，
//!   否则 resume 15s 超时；应答不占 pending 表，直接写本 handle 的 stdin。
//! - `session/send` fire-and-accept，回复走 `session/event` 流直到 turn 完成。
//! - 同会话忙返回 -32010 → 500ms 重试（上限 120 次 ≈60s，容忍 TUI 长轮次）。
//! - close/stop/退出 **一律 killpg(SIGKILL)** 杀整组（实测不杀每会话泄漏 ~600MB）。

pub mod protocol;

use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{broadcast, oneshot, Mutex};

pub use protocol::{normalize_event, SeenSet, ZEvent};

/// 会话忙（-32010：A prompt is already running）。
pub const BUSY_CODE: i64 = -32010;

const CREATE_TIMEOUT_S: u64 = 90;
const REGISTRY_TIMEOUT_S: u64 = 20;
const SUBSCRIBE_TIMEOUT_S: u64 = 30;
const SEND_TIMEOUT_S: u64 = 60;
const CLOSE_TIMEOUT_S: u64 = 5;
const BUSY_RETRY_MAX: u32 = 120;
const BUSY_RETRY_INTERVAL: Duration = Duration::from_millis(500);

/// app-server 请求失败（区分忙/超时/远端错误/进程死亡）。
#[derive(Debug, thiserror::Error)]
pub enum RpcFail {
    #[error("zcode 会话忙（-32010），重试 {retried} 次仍未成功")]
    Busy { retried: u32 },
    #[error("zcode {method} 超时（{timeout_s}s）")]
    Timeout { method: String, timeout_s: u64 },
    #[error("zcode {method} 错误 {code}: {message}")]
    Remote {
        method: String,
        code: i64,
        message: String,
    },
    #[error("zcode app-server 进程已退出")]
    Dead,
    #[error("zcode app-server I/O 错误: {0}")]
    Io(#[from] std::io::Error),
    #[error("zcode app-server JSON 错误: {0}")]
    Json(#[from] serde_json::Error),
}

/// zcode app-server 子进程句柄（每个绑定会话一个）。
pub struct ZcodeHandle {
    stdin: Arc<Mutex<ChildStdin>>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<Value>>>>,
    events: broadcast::Sender<ZEvent>,
    alive: Arc<AtomicBool>,
    pid: u32,
    child: Child,
    /// `open_session` 之后填充
    pub session_id: String,
}

impl ZcodeHandle {
    /// 拉起 `zcode app-server`（独立进程组，便于整组回收）。
    pub async fn spawn(bin: &str, cwd: &Path) -> Result<Self, RpcFail> {
        let mut cmd = Command::new(bin);
        cmd.arg("app-server")
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .env("TERM", "dumb")
            .kill_on_drop(true);
        // 独立进程组（setpgid(0,0)，pgid == pid），退出时 killpg 整组回收（含 MCP 子进程）
        cmd.process_group(0);
        let mut child = cmd.spawn().map_err(RpcFail::Io)?;
        let pid = child.id().unwrap_or(0);
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| RpcFail::Io(std::io::Error::other("app-server stdin 未获取")))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| RpcFail::Io(std::io::Error::other("app-server stdout 未获取")))?;

        let pending: Arc<Mutex<HashMap<String, oneshot::Sender<Value>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (events, _) = broadcast::channel(512);
        let alive = Arc::new(AtomicBool::new(true));
        let stdin = Arc::new(Mutex::new(stdin));

        // 读循环：反向请求（id+method）优先应答 → 回包投 pending → session/event 规范化广播
        {
            let pending = pending.clone();
            let events = events.clone();
            let alive = alive.clone();
            let stdin = stdin.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stdout).lines();
                let mut seen = SeenSet::default();
                while let Ok(Some(line)) = lines.next_line().await {
                    let line = line.trim().to_string();
                    if line.is_empty() {
                        continue;
                    }
                    let Ok(obj) = serde_json::from_str::<Value>(&line) else {
                        tracing::debug!(
                            "app-server 非 JSON 行忽略: {}",
                            &line[..line.len().min(120)]
                        );
                        continue;
                    };
                    let id = obj.get("id").cloned();
                    let method = obj.get("method").and_then(|x| x.as_str()).map(String::from);
                    // 服务端→客户端请求：带 id+method，必须应答（先于 pending 回包判定）
                    if let (Some(rid), Some(m)) = (id.clone(), method.clone()) {
                        let answer = reverse_answer(&m);
                        let out = json!({"id": rid, "result": answer}).to_string();
                        tracing::debug!("应答反向请求 {m}");
                        let mut w = stdin.lock().await;
                        let _ = w.write_all(out.as_bytes()).await;
                        let _ = w.write_all(b"\n").await;
                        let _ = w.flush().await;
                        continue;
                    }
                    if obj.get("result").is_some() || obj.get("error").is_some() {
                        if let Some(rid) = &id {
                            let tx = pending.lock().await.remove(&id_key(rid));
                            match tx {
                                Some(tx) => {
                                    let _ = tx.send(obj);
                                }
                                None => {
                                    tracing::debug!("回包 id 未命中 pending: {}", id_key(rid));
                                }
                            }
                            continue;
                        }
                    }
                    if method.as_deref() == Some("session/event") {
                        let params = obj.get("params").cloned().unwrap_or(Value::Null);
                        let payload = params.get("payload").cloned().unwrap_or(Value::Null);
                        if payload.is_null() {
                            continue;
                        }
                        let eid = params
                            .get("eventId")
                            .and_then(|x| x.as_str())
                            .map(String::from)
                            .unwrap_or_default();
                        let key = if eid.is_empty() {
                            serde_json::to_string(&payload)
                                .unwrap_or_default()
                                .chars()
                                .take(200)
                                .collect::<String>()
                        } else {
                            eid
                        };
                        if !seen.insert(&key) {
                            continue;
                        }
                        if let Some(ev) = normalize_event(&payload) {
                            let _ = events.send(ev);
                        }
                    }
                    // 其余通知（telemetry 等）忽略
                }
                alive.store(false, Ordering::SeqCst);
                tracing::warn!("app-server stdout EOF，进程已退出");
            });
        }

        Ok(Self {
            stdin,
            pending,
            events,
            alive,
            pid,
            child,
            session_id: String::new(),
        })
    }

    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    /// 订阅会话事件流（单消费者；lag 时调用方告警续读）。
    pub fn subscribe_events(&self) -> broadcast::Receiver<ZEvent> {
        self.events.subscribe()
    }

    /// 建会话：create 或 resume →（resume 时推 provider registry）→ subscribe。
    /// 返回 sessionId。
    pub async fn open_session(
        &mut self,
        resume_sid: Option<&str>,
        cwd: &Path,
    ) -> Result<String, RpcFail> {
        let cwd_str = cwd.to_string_lossy().to_string();
        let resp = match resume_sid {
            Some(sid) => {
                self.request(
                    "session/resume",
                    json!({"sessionId": sid}),
                    CREATE_TIMEOUT_S,
                )
                .await?
            }
            None => {
                self.request(
                    "session/create",
                    json!({"workspace": {"workspacePath": cwd_str, "workspaceKey": cwd_str}}),
                    CREATE_TIMEOUT_S,
                )
                .await?
            }
        };
        let res = resp.get("result").cloned().unwrap_or(Value::Null);
        let sid = res
            .get("sessionId")
            .and_then(|x| x.as_str())
            .map(String::from)
            .or_else(|| {
                res.get("session")
                    .and_then(|s| s.get("sessionId"))
                    .and_then(|x| x.as_str())
                    .map(String::from)
            })
            .unwrap_or_default();
        if sid.is_empty() {
            return Err(RpcFail::Remote {
                method: "session/create|resume".into(),
                code: -1,
                message: "响应缺少 sessionId".into(),
            });
        }
        self.session_id = sid.clone();

        if resume_sid.is_some() {
            // cold resume 的 deferred adapter 会让 send 被「模型不可用」拦截，
            // 推一次 provider registry 让 providerRevision 就位（失败仅告警）
            let ws = res
                .get("session")
                .and_then(|s| s.get("workspace"))
                .cloned()
                .unwrap_or_else(|| json!({"workspaceKey": cwd_str, "workspacePath": cwd_str}));
            if let Err(e) = self.push_provider_registry(&ws).await {
                tracing::warn!("updateProviderRegistry 失败（继续）: {e}");
            }
        }

        self.request(
            "session/subscribe",
            json!({"sessionId": sid, "deliveryKind": "web-remote-replayable"}),
            SUBSCRIBE_TIMEOUT_S,
        )
        .await?;
        Ok(sid)
    }

    /// 发一条消息（fire-and-accept）；-32010 忙重试 500ms×20。
    pub async fn send(&self, text: &str) -> Result<Value, RpcFail> {
        let mut retried = 0u32;
        loop {
            match self
                .request(
                    "session/send",
                    json!({"sessionId": self.session_id, "content": text}),
                    SEND_TIMEOUT_S,
                )
                .await
            {
                Ok(v) => return Ok(v),
                Err(RpcFail::Remote { code, message, .. }) if code == BUSY_CODE => {
                    retried += 1;
                    if retried >= BUSY_RETRY_MAX {
                        return Err(RpcFail::Busy { retried });
                    }
                    tracing::debug!(
                        "session/send 忙（{message}），重试 {retried}/{BUSY_RETRY_MAX}"
                    );
                    tokio::time::sleep(BUSY_RETRY_INTERVAL).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// 打断当前 turn。
    pub async fn stop_turn(&self) -> Result<(), RpcFail> {
        self.request("session/stop", json!({"sessionId": self.session_id}), 15)
            .await?;
        Ok(())
    }

    /// 关闭会话并回收进程组：session/close（5s，尽力）→ killpg(SIGKILL)。
    pub async fn close(mut self) {
        if self.is_alive() && !self.session_id.is_empty() {
            let close = self.request(
                "session/close",
                json!({"sessionId": self.session_id}),
                CLOSE_TIMEOUT_S,
            );
            if let Err(e) = close.await {
                tracing::debug!("session/close 未成功（忽略）: {e}");
            }
        }
        self.kill_group();
        // 等待子进程真正退出，避免僵尸
        let _ = self.child.wait().await;
    }

    fn kill_group(&mut self) {
        if self.pid == 0 {
            let _ = self.child.start_kill();
            return;
        }
        // process_group(0) 后 pgid == pid；killpg 杀整棵（app-server + MCP 子进程）
        let rc = unsafe { libc::killpg(self.pid as i32, libc::SIGKILL) };
        if rc != 0 {
            tracing::warn!("killpg({}) 失败，回退 kill 主进程", self.pid);
            let _ = self.child.start_kill();
        }
        self.alive.store(false, Ordering::SeqCst);
    }

    /// 底层请求：写一行 JSON-RPC，等回包（超时按方法给）。
    async fn request(&self, method: &str, params: Value, timeout_s: u64) -> Result<Value, RpcFail> {
        if !self.is_alive() {
            return Err(RpcFail::Dead);
        }
        let rid = uuid::Uuid::new_v4().simple().to_string()[..16].to_string();
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(rid.clone(), tx);
        let line = json!({"id": rid, "method": method, "params": params}).to_string();
        {
            let mut stdin = self.stdin.lock().await;
            stdin
                .write_all(line.as_bytes())
                .await
                .map_err(RpcFail::Io)?;
            stdin.write_all(b"\n").await.map_err(RpcFail::Io)?;
            stdin.flush().await.map_err(RpcFail::Io)?;
        }
        match tokio::time::timeout(Duration::from_secs(timeout_s), rx).await {
            Ok(Ok(resp)) => {
                if let Some(err) = resp.get("error") {
                    let code = err.get("code").and_then(|x| x.as_i64()).unwrap_or(-1);
                    let message = err
                        .get("message")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string();
                    return Err(RpcFail::Remote {
                        method: method.to_string(),
                        code,
                        message,
                    });
                }
                Ok(resp)
            }
            Ok(Err(_)) => Err(RpcFail::Dead),
            Err(_) => {
                self.pending.lock().await.remove(&rid);
                Err(RpcFail::Timeout {
                    method: method.to_string(),
                    timeout_s,
                })
            }
        }
    }

    /// resume 后推 provider registry（解除 deferred model adapter 的模型不可用拦截）。
    async fn push_provider_registry(&self, workspace: &Value) -> Result<(), RpcFail> {
        let providers = load_providers_from_config();
        let registry = json!({
            "revision": "1",
            "generatedAt": now_millis(),
            "providers": providers,
        });
        self.request(
            "workspace/updateProviderRegistry",
            json!({"workspace": workspace, "registry": registry}),
            REGISTRY_TIMEOUT_S,
        )
        .await?;
        Ok(())
    }
}

/// JSON-RPC id 的 pending 表键：字符串 id 取裸串，数字等取 JSON 形态。
/// `Value::to_string()` 对 String 会带引号，直接用会与写入侧裸键永不匹配。
fn id_key(v: &Value) -> String {
    v.as_str()
        .map(String::from)
        .unwrap_or_else(|| v.to_string())
}

/// 服务端反向请求应答：prefs 给具体对象，其余空对象兜底（漏答会让 resume 15s 超时）。
fn reverse_answer(method: &str) -> Value {
    match method {
        "session/requestRuntimePreferences" => json!({
            "nativeSearchEnhancementsEnabled": true,
            "memoryEnabled": false,
            "askUserQuestionAutoResolutionEnabled": true,
        }),
        _ => {
            tracing::debug!("app-server 反向请求 {method}，空对象兜底应答");
            json!({})
        }
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 从 `~/.zcode/cli/config.json` 抽 provider registry 条目（失败返回空）。
fn load_providers_from_config() -> Vec<Value> {
    let path = dirs::home_dir()
        .unwrap_or_default()
        .join(".zcode/cli/config.json");
    let Ok(raw) = std::fs::read_to_string(&path) else {
        return Vec::new();
    };
    let Ok(cfg) = serde_json::from_str::<Value>(&raw) else {
        return Vec::new();
    };
    let mut providers = Vec::new();
    let Some(map) = cfg.get("provider").and_then(|x| x.as_object()) else {
        return providers;
    };
    for (pid, p) in map {
        let opts = p.get("options").cloned().unwrap_or(Value::Null);
        let mut models = Vec::new();
        if let Some(mmap) = p.get("models").and_then(|x| x.as_object()) {
            for (mid, m) in mmap {
                let lim = m.get("limit").cloned().unwrap_or(Value::Null);
                let inputs = m
                    .get("modalities")
                    .and_then(|x| x.get("input"))
                    .and_then(|x| x.as_array())
                    .cloned()
                    .unwrap_or_default();
                models.push(json!({
                    "modelId": mid,
                    "label": m.get("name").and_then(|x| x.as_str()).unwrap_or(mid),
                    "contextWindow": lim.get("context").and_then(|x| x.as_u64()).unwrap_or(524288),
                    "maxOutputTokens": lim.get("output").and_then(|x| x.as_u64()).unwrap_or(32768),
                    "supportsImages": inputs.iter().any(|v| v.as_str() == Some("image")),
                    "supportsPdf": inputs.iter().any(|v| v.as_str() == Some("pdf")),
                    "supportsVideo": inputs.iter().any(|v| v.as_str() == Some("video")),
                    "reasoning": {
                        "enabled": true,
                        "levels": [
                            {"value": "low", "label": "low"},
                            {"value": "high", "label": "high"},
                            {"value": "max", "label": "max"}
                        ],
                        "defaultLevel": "max",
                    },
                }));
            }
        }
        providers.push(json!({
            "providerId": pid,
            "kind": p.get("kind").and_then(|x| x.as_str()).unwrap_or("openai-compatible"),
            "label": p.get("name").and_then(|x| x.as_str()).unwrap_or(pid),
            "baseURL": opts.get("baseURL"),
            "apiKeyRequired": opts.get("apiKeyRequired"),
            "models": models,
        }));
    }
    providers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_key_string_id_no_quotes() {
        // 回归：Value::String.to_string() 带引号会与 pending 裸键永不匹配（曾致 create 90s 超时）
        assert_eq!(id_key(&json!("server-1")), "server-1");
        assert_eq!(id_key(&json!(1)), "1");
        assert_eq!(id_key(&json!(true)), "true");
    }

    #[test]
    fn reverse_answer_prefs() {
        let v = reverse_answer("session/requestRuntimePreferences");
        assert_eq!(v["memoryEnabled"], json!(false));
        let other = reverse_answer("some/unknown");
        assert_eq!(other, json!({}));
    }

    #[test]
    fn providers_parse_tolerant() {
        // 配置文件不存在/损坏时返回空，不 panic
        let _ = load_providers_from_config();
    }
}
