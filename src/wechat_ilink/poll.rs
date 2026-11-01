//! 微信收消息：getupdates 长轮询 + sync buf 续传 + 双去重 + 错误恢复。
//!
//! 语义（Hermes-CN-Core weixin.py〔MIT〕）：
//! - `getupdates` body = `{"get_updates_buf": buf}`，长轮询 35s；超时=空结果（buf 不变）；
//!   新 buf 必须持久化（`state/weixin.sync.json`）才能断点续传；
//! - 入站跳过自发（from_user_id 空或 == account_id）；message_id + 内容指纹双去重
//!   （TTL 300s）；message 的 context_token 更新 per-peer token store；
//! - 错误恢复：-14 → sleep 600s 续询；连续 3 次错误 → 30s 退避并重建 HTTP client。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::sync::mpsc;

use super::client::{classify, is_timeout, ApiOutcome, IlinkClient};
use super::tokens::TokenStore;
use super::{write_secret_file, WeixinCreds, EP_GET_UPDATES, LONG_POLL_TIMEOUT_MS};
use crate::error::BridgeError;

/// 入站去重 TTL。
const DEDUP_TTL: Duration = Duration::from_secs(300);
/// 长轮询客户端超时（服务端 35s + 5s 网络余量）。
fn poll_timeout() -> Duration {
    Duration::from_millis(LONG_POLL_TIMEOUT_MS + 5_000)
}
/// -14 后的挂起续询时长。
const SESSION_EXPIRED_SLEEP: Duration = Duration::from_secs(600);
/// 连续失败上限 → 长退避 + 重建 client。
const MAX_CONSECUTIVE_FAILURES: u32 = 3;
const FAILURE_BACKOFF: Duration = Duration::from_secs(30);
const SHORT_BACKOFF: Duration = Duration::from_secs(2);

/// 一条入站消息（已过滤自发/重复）。
#[derive(Debug, Clone)]
pub struct InboundMessage {
    pub from_user_id: String,
    pub to_user_id: String,
    pub message_id: String,
    pub msg_type: i64,
    /// 对方最新 context_token（发消息时必须回显；同时更新 token store）
    pub context_token: Option<String>,
    /// 文本（纯媒体消息为空）
    pub text: String,
    /// 含媒体 item（媒体处理 M4；文本路由先行）
    pub has_media: bool,
    pub raw: Value,
}

/// TTL 去重集合（首见返回 true）。
#[derive(Debug, Default)]
pub struct TtlDedup {
    map: HashMap<String, Instant>,
}

impl TtlDedup {
    /// 返回 true = 首次见到（已记录）；false = TTL 内重复。
    pub fn first_seen(&mut self, key: &str) -> bool {
        let now = Instant::now();
        self.map.retain(|_, at| now.duration_since(*at) < DEDUP_TTL);
        if self.map.contains_key(key) {
            return false;
        }
        self.map.insert(key.to_string(), now);
        true
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// 轮询错误（run_loop 据此分级恢复）。
#[derive(Debug, thiserror::Error)]
pub enum PollError {
    #[error("iLink 会话过期（-14/stale）")]
    SessionExpired,
    #[error(transparent)]
    Other(#[from] BridgeError),
}

/// 微信长轮询器（每账号一个；共享 TokenStore 与发送侧）。
pub struct WeixinPoller {
    client: IlinkClient,
    http: reqwest::Client,
    creds: WeixinCreds,
    tokens: Arc<TokenStore>,
    sync_path: PathBuf,
    sync_buf: String,
    dedup: TtlDedup,
}

impl WeixinPoller {
    /// `sync_path` = `state/weixin.sync.json`（get_updates_buf 断点续传）。
    pub fn new(
        http: reqwest::Client,
        creds: WeixinCreds,
        tokens: Arc<TokenStore>,
        sync_path: PathBuf,
    ) -> Self {
        let sync_buf = load_sync_buf(&sync_path);
        let client = IlinkClient::new(http.clone(), creds.clone());
        Self {
            client,
            http,
            creds,
            tokens,
            sync_path,
            sync_buf,
            dedup: TtlDedup::default(),
        }
    }

    /// 拉一轮消息（一次 getupdates 长轮询）。超时/空 = `Ok(vec![])`。
    pub async fn poll_once(&mut self) -> std::result::Result<Vec<InboundMessage>, PollError> {
        let payload = serde_json::json!({ "get_updates_buf": self.sync_buf });
        let resp = match self
            .client
            .post(EP_GET_UPDATES, payload, poll_timeout())
            .await
        {
            Ok(v) => v,
            Err(e) if is_timeout(&e) => return Ok(Vec::new()),
            Err(e) => return Err(PollError::Other(e)),
        };
        match classify(&resp) {
            ApiOutcome::Ok => {}
            ApiOutcome::SessionExpired => return Err(PollError::SessionExpired),
            ApiOutcome::RateLimited | ApiOutcome::Failed { .. } => {
                return Err(PollError::Other(BridgeError::other(format!(
                    "getupdates 失败: {}",
                    err_hint(&resp)
                ))));
            }
        }
        // 新 sync buf 立即持久化（崩溃/重启后断点续传）
        if let Some(buf) = resp.get("get_updates_buf").and_then(|x| x.as_str()) {
            if buf != self.sync_buf {
                self.sync_buf = buf.to_string();
                save_sync_buf(&self.sync_path, &self.sync_buf);
            }
        }
        let account = self.client.account_id().to_string();
        let mut out = Vec::new();
        if let Some(msgs) = resp.get("msgs").and_then(|x| x.as_array()) {
            for m in msgs {
                if let Some(inb) = self.take_message(m, &account) {
                    out.push(inb);
                }
            }
        }
        Ok(out)
    }

    /// 常驻轮询循环：消息经 mpsc 交给路由层；错误分级恢复（-14 挂 600s、3 连败重建 client）。
    pub async fn run_loop(mut self, out: mpsc::Sender<InboundMessage>) {
        let mut failures = 0u32;
        loop {
            match self.poll_once().await {
                Ok(msgs) => {
                    failures = 0;
                    for m in msgs {
                        if out.send(m).await.is_err() {
                            tracing::info!("微信轮询：路由接收端已关闭，退出");
                            return;
                        }
                    }
                }
                Err(PollError::SessionExpired) => {
                    failures = 0;
                    tracing::warn!(
                        "微信 getupdates 会话过期，{}s 后续询",
                        SESSION_EXPIRED_SLEEP.as_secs()
                    );
                    tokio::time::sleep(SESSION_EXPIRED_SLEEP).await;
                }
                Err(e) => {
                    failures += 1;
                    if failures >= MAX_CONSECUTIVE_FAILURES {
                        tracing::warn!(
                            "微信轮询连续 {failures} 次失败（{e}），{FAILURE_BACKOFF:?} 后重建 client"
                        );
                        tokio::time::sleep(FAILURE_BACKOFF).await;
                        self.client = IlinkClient::new(self.http.clone(), self.creds.clone());
                        failures = 0;
                    } else {
                        tracing::warn!("微信轮询失败 {failures}/{MAX_CONSECUTIVE_FAILURES}: {e}");
                        tokio::time::sleep(SHORT_BACKOFF).await;
                    }
                }
            }
        }
    }

    /// 单条消息 → 入站（自发/重复/无内容返回 None；context_token 同步进 token store）。
    fn take_message(&mut self, m: &Value, account_id: &str) -> Option<InboundMessage> {
        let from = str_of(m, "from_user_id");
        if from.is_empty() || from == account_id {
            return None;
        }
        let mid = str_of(m, "message_id");
        let (text, has_media) = extract_text(m.get("item_list"));
        if text.is_empty() && !has_media {
            return None;
        }
        // 双去重：message_id + 内容指纹（TTL 300s）
        if !mid.is_empty() && !self.dedup.first_seen(&format!("id:{mid}")) {
            return None;
        }
        let fp = content_fingerprint(&from, &text);
        if !self.dedup.first_seen(&fp) {
            return None;
        }
        // 入站 context_token 是发送侧 token 的主源
        let token = m
            .get("context_token")
            .and_then(|x| x.as_str())
            .filter(|t| !t.is_empty())
            .map(String::from);
        if let Some(t) = &token {
            self.tokens.set(&from, t);
        }
        Some(InboundMessage {
            from_user_id: from,
            to_user_id: str_of(m, "to_user_id"),
            message_id: mid,
            msg_type: m.get("msg_type").and_then(|x| x.as_i64()).unwrap_or(0),
            context_token: token,
            text,
            has_media,
            raw: m.clone(),
        })
    }
}

/// 首个文本 item 的 text；无文本时用语音 ASR 兜底（无 media 的 voice）。
/// 媒体判定：media 在类型化子对象里（`image_item.media` 等，Hermes `_media_item` 同款），
/// 空对象/缺失/None 均视为无媒体。
fn extract_text(item_list: Option<&Value>) -> (String, bool) {
    let Some(items) = item_list.and_then(|x| x.as_array()) else {
        return (String::new(), false);
    };
    let mut text = String::new();
    let mut voice_fallback = String::new();
    let mut has_media = false;
    for item in items {
        if item_has_media(item) {
            has_media = true;
        }
        match item.get("type").and_then(|x| x.as_i64()) {
            Some(1) if text.is_empty() => {
                text = item
                    .get("text_item")
                    .and_then(|x| x.get("text"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
            }
            Some(3) if voice_fallback.is_empty() => {
                // ITEM_VOICE=3（item_list 口径；getuploadurl 的 4 是 FILE）：无 media 时 text 是 ASR 转写
                let vi = item.get("voice_item");
                let has_vmedia = vi.map(item_has_media).unwrap_or(false);
                let t = vi
                    .and_then(|x| x.get("text"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("");
                if !t.is_empty() && !has_vmedia {
                    voice_fallback = format!("[Voice transcription provided by Weixin]\n{t}");
                }
            }
            _ => {}
        }
    }
    if text.is_empty() {
        text = voice_fallback;
    }
    (text, has_media)
}

/// item（或其类型化子对象）是否携带 media 载荷。
fn item_has_media(item: &Value) -> bool {
    let media = |v: &Value| match v.get("media") {
        None | Some(Value::Null) => false,
        Some(Value::Object(o)) => !o.is_empty(),
        Some(_) => true,
    };
    if media(item) {
        return true;
    }
    // 媒体挂在子对象上：{type, image_item: {media: {...}}}
    item.as_object()
        .map(|o| o.values().any(|sub| sub.is_object() && media(sub)))
        .unwrap_or(false)
}

/// 内容指纹：`content:<sender>:<hash(text)>`（DefaultHasher 固定密钥，进程内稳定）。
fn content_fingerprint(sender: &str, text: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    text.hash(&mut h);
    format!("content:{sender}:{:016x}", h.finish())
}

fn str_of(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string()
}

fn err_hint(resp: &Value) -> String {
    resp.get("errmsg")
        .or_else(|| resp.get("msg"))
        .and_then(|x| x.as_str())
        .map(String::from)
        .unwrap_or_else(|| "未知错误".into())
}

fn load_sync_buf(path: &Path) -> String {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return String::new();
    };
    serde_json::from_str::<Value>(&raw)
        .ok()
        .and_then(|v| {
            v.get("get_updates_buf")
                .and_then(|x| x.as_str())
                .map(String::from)
        })
        .unwrap_or_default()
}

fn save_sync_buf(path: &Path, buf: &str) {
    let doc = serde_json::json!({ "get_updates_buf": buf });
    let text = serde_json::to_string(&doc).unwrap_or_default();
    if let Err(e) = write_secret_file(path, text.as_bytes()) {
        tracing::warn!("get_updates_buf 落盘失败（仅影响断点续传）: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn msg(id: &str, from: &str, text: &str) -> Value {
        json!({
            "message_id": id,
            "from_user_id": from,
            "to_user_id": "bot-account",
            "msg_type": 2,
            "context_token": format!("tok-{id}"),
            "item_list": [{"type": 1, "text_item": {"text": text}}],
        })
    }

    fn poller() -> WeixinPoller {
        let dir = std::env::temp_dir().join(format!("im-bridge-poll-test-{}", std::process::id()));
        let creds = WeixinCreds {
            account_id: "bot-account".into(),
            token: "t".into(),
            base_url: super::super::ILINK_BASE_URL.into(),
            user_id: "bot-account".into(),
            saved_at: String::new(),
        };
        WeixinPoller::new(
            reqwest::Client::new(),
            creds,
            Arc::new(TokenStore::empty(dir.join("tok.json"))),
            dir.join("weixin.sync.json"),
        )
    }

    #[test]
    fn skips_self_and_empty_sender() {
        let mut p = poller();
        assert!(p
            .take_message(&msg("m0", "bot-account", "hi"), "bot-account")
            .is_none());
        let mut m = msg("m1", "", "hi");
        m["from_user_id"] = json!(null);
        assert!(p.take_message(&m, "bot-account").is_none());
    }

    #[test]
    fn dedups_by_message_id_and_content() {
        let mut p = poller();
        assert!(p
            .take_message(&msg("m1", "u1", "hi"), "bot-account")
            .is_some());
        assert!(p
            .take_message(&msg("m1", "u1", "hi"), "bot-account")
            .is_none());
        // 同内容不同 id 也去重（防服务端重推）
        assert!(p
            .take_message(&msg("m2", "u1", "hi"), "bot-account")
            .is_none());
        // 不同内容通过
        assert!(p
            .take_message(&msg("m3", "u1", "hi again"), "bot-account")
            .is_some());
    }

    #[test]
    fn inbound_context_token_updates_store() {
        let mut p = poller();
        let inb = p
            .take_message(&msg("m1", "u1", "hi"), "bot-account")
            .unwrap();
        assert_eq!(inb.context_token.as_deref(), Some("tok-m1"));
        assert_eq!(p.tokens.get("u1").as_deref(), Some("tok-m1"));
    }

    #[test]
    fn voice_asr_fallback() {
        let m = json!({
            "message_id": "v1", "from_user_id": "u1", "to_user_id": "bot-account",
            "item_list": [{"type": 3, "voice_item": {"text": "语音转写"}}],
        });
        let (text, has_media) = extract_text(m.get("item_list"));
        assert!(text.contains("Voice transcription"));
        assert!(text.contains("语音转写"));
        assert!(!has_media);
    }

    #[test]
    fn media_flag_detected() {
        let items = json!([
            {"type": 2, "image_item": {"media": {"x": 1}}},
            {"type": 1, "text_item": {"text": "看图"}}
        ]);
        let (text, has_media) = extract_text(Some(&items));
        assert_eq!(text, "看图");
        assert!(has_media);
    }

    #[test]
    fn ttl_dedup_first_seen() {
        let mut d = TtlDedup::default();
        assert!(d.first_seen("a"));
        assert!(!d.first_seen("a"));
        assert!(d.first_seen("b"));
        assert_eq!(d.len(), 2);
    }

    #[test]
    fn sync_buf_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("weixin.sync.json");
        assert_eq!(load_sync_buf(&path), "");
        save_sync_buf(&path, "buf-123");
        assert_eq!(load_sync_buf(&path), "buf-123");
    }
}
