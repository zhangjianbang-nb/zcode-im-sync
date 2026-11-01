//! 微信发送通道：context_token 状态机 + 分段 + 限流熔断 + typing。
//!
//! 语义（Hermes-CN-Core weixin.py〔MIT〕逐行定案，代码自写）：
//! - `sendmessage` body = `{"msg":{from_user_id:"",to_user_id,client_id:uuid,message_type:2,
//!   message_state:2,item_list:[{type:1,text_item:{text}}][,context_token]}}`；
//! - 成功判定 = ret/errcode 均 0/缺失；-14 或 stale(-2+unknown error/prepare failed) =
//!   会话过期 → evict 后**无 token 重试一次**（iLink 接受无 token 发送降级），再败给
//!   `SessionNotReady` 确定性回执，不无限重试；
//! - 真限流（-2 非 stale）= 30s 熔断 + 退避 0.5/2/6s，上限 4 次；
//! - 全 adapter 串行发闩；段间 800ms；分段软 1800 / 硬 2000（UTF-8 边界、``` 围栏感知）；
//! - typing：getconfig（peer 的 ilink_user_id+context_token）→ typing_ticket（TTL 600s）
//!   → sendtyping {ilink_user_id:chat_id, typing_ticket, status:1|2}，失败静默。

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::sync::Mutex;

use super::client::{classify, ApiOutcome, IlinkClient};
use super::tokens::TokenStore;
use super::{API_TIMEOUT_MS, CONFIG_TIMEOUT_MS, EP_GET_CONFIG, EP_SEND_MESSAGE, EP_SEND_TYPING};
use crate::error::BridgeError;

/// 分段软阈值（Hermes `_SPLIT_THRESHOLD`）。
pub const SPLIT_THRESHOLD: usize = 1800;
/// 单条硬上限（Hermes `MAX_MESSAGE_LENGTH`，"iLink chunks at ~2048 chars"）。
pub const MAX_MESSAGE_LENGTH: usize = 2000;
/// 段间间隔。
const SEGMENT_GAP: Duration = Duration::from_millis(800);
/// 发送尝试上限（含首发）。
const SEND_ATTEMPTS: usize = 4;
/// 限流退避表。
const RATE_LIMIT_BACKOFF: [Duration; 3] = [
    Duration::from_millis(500),
    Duration::from_millis(2000),
    Duration::from_millis(6000),
];
/// 限流熔断窗口。
const CIRCUIT_OPEN: Duration = Duration::from_secs(30);
/// typing 票据 TTL。
const TYPING_TICKET_TTL: Duration = Duration::from_secs(600);

pub const TYPING_START: i64 = 1;
pub const TYPING_STOP: i64 = 2;

/// 发送失败（供路由层映射确定性回执）。
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("context_token 已失效且无 token 重试仍失败")]
    SessionNotReady,
    #[error("触发微信限流冷却")]
    RateLimited,
    #[error("iLink 业务错误: {0}")]
    Remote(String),
    #[error(transparent)]
    Bridge(#[from] BridgeError),
}

/// 微信发送器（每账号一个；发送串行化，typing 尽力而为）。
pub struct WeixinSender {
    client: IlinkClient,
    tokens: Arc<TokenStore>,
    /// 全 adapter 串行发闩（Hermes `_send_latch` 同语义，防并发触发限流）。
    gate: Mutex<()>,
    /// chat_id → (typing_ticket, fetched_at)
    typing: StdMutex<HashMap<String, (String, Instant)>>,
    circuit_until: StdMutex<Option<Instant>>,
}

impl WeixinSender {
    pub fn new(client: IlinkClient, tokens: Arc<TokenStore>) -> Self {
        Self {
            client,
            tokens,
            gate: Mutex::new(()),
            typing: StdMutex::new(HashMap::new()),
            circuit_until: StdMutex::new(None),
        }
    }

    pub fn tokens(&self) -> Arc<TokenStore> {
        Arc::clone(&self.tokens)
    }

    pub fn client(&self) -> &IlinkClient {
        &self.client
    }

    /// 发文本（自动分段）；空/纯空白直接忽略。
    pub async fn send_text(&self, peer: &str, text: &str) -> Result<(), SendError> {
        if text.trim().is_empty() {
            tracing::debug!("send_text 空文本忽略 peer={peer}");
            return Ok(());
        }
        let chunks = split_text(text);
        let last = chunks.len().saturating_sub(1);
        for (i, chunk) in chunks.iter().enumerate() {
            self.send_chunk(peer, chunk).await?;
            if i < last {
                tokio::time::sleep(SEGMENT_GAP).await;
            }
        }
        Ok(())
    }

    /// 单段发送状态机。
    pub async fn send_chunk(&self, peer: &str, chunk: &str) -> Result<(), SendError> {
        let _latch = self.gate.lock().await;
        if let Some(until) = *self.circuit_until.lock().unwrap() {
            if Instant::now() < until {
                return Err(SendError::RateLimited);
            }
        }
        let mut token = self.tokens.get(peer);
        // 无 token 起手时「重试一次」机会已用掉，-14 直接确定性回执
        let mut retried_without_token = token.is_none();
        let mut attempt = 0usize;
        loop {
            let msg = build_msg(peer, chunk, token.as_deref());
            let resp = match self
                .client
                .post(EP_SEND_MESSAGE, json!({"msg": msg}), api_timeout())
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    // 传输层瞬时故障：退避重试，上限同 SEND_ATTEMPTS
                    attempt += 1;
                    if attempt >= SEND_ATTEMPTS {
                        return Err(SendError::Bridge(e));
                    }
                    tracing::warn!(
                        "sendmessage 传输失败（{e}），退避重试 {attempt}/{SEND_ATTEMPTS}"
                    );
                    tokio::time::sleep(RATE_LIMIT_BACKOFF[(attempt - 1).min(2)]).await;
                    continue;
                }
            };
            match classify(&resp) {
                ApiOutcome::Ok => {
                    // 防御性：响应带 context_token 也更新（入站是主源）
                    if let Some(t) = resp.get("context_token").and_then(|x| x.as_str()) {
                        if !t.is_empty() {
                            self.tokens.set(peer, t);
                        }
                    }
                    *self.circuit_until.lock().unwrap() = None;
                    // 成功也留痕：没有这条日志时，只能靠「没有 warn」反推，无法核实送达
                    tracing::info!("微信已发送 peer={peer} 字符数={}", chunk.chars().count());
                    return Ok(());
                }
                ApiOutcome::SessionExpired => {
                    if !retried_without_token {
                        tracing::debug!(
                            "sendmessage 会话过期，evict token 后无 token 重试 peer={peer}"
                        );
                        self.tokens.evict(peer);
                        token = None;
                        retried_without_token = true;
                        continue;
                    }
                    return Err(SendError::SessionNotReady);
                }
                ApiOutcome::RateLimited => {
                    // 真限流：开 30s 熔断（挡后续调用），本次按 0.5/2/6s 退避，上限 4 次
                    {
                        let mut circuit = self.circuit_until.lock().unwrap();
                        if !matches!(*circuit, Some(u) if Instant::now() < u) {
                            *circuit = Some(Instant::now() + CIRCUIT_OPEN);
                        }
                    }
                    attempt += 1;
                    if attempt >= SEND_ATTEMPTS {
                        return Err(SendError::RateLimited);
                    }
                    let wait = RATE_LIMIT_BACKOFF[(attempt - 1).min(2)];
                    tracing::warn!(
                        "sendmessage 限流，{wait:?} 后重试 {attempt}/{SEND_ATTEMPTS} peer={peer}"
                    );
                    tokio::time::sleep(wait).await;
                }
                ApiOutcome::Failed { errmsg } => {
                    return Err(SendError::Remote(errmsg));
                }
            }
        }
    }

    /// typing 开始（失败静默）。
    pub async fn send_typing(&self, peer: &str) {
        self.set_typing(peer, TYPING_START).await;
    }

    /// typing 结束（失败静默；票据过期会自动续取，否则客户端指示灯常亮）。
    pub async fn stop_typing(&self, peer: &str) {
        self.set_typing(peer, TYPING_STOP).await;
    }

    /// 入站消息时预取 typing 票据（Hermes `_process_message` 同款；失败静默）。
    pub async fn prefetch_typing_ticket(&self, peer: &str, context_token: Option<&str>) {
        if self.typing.lock().unwrap().contains_key(peer) {
            return;
        }
        let _ = self.fetch_typing_ticket(peer, context_token).await;
    }

    async fn set_typing(&self, peer: &str, status: i64) {
        let Some(ticket) = self.ensure_typing_ticket(peer).await else {
            return;
        };
        let payload = json!({
            "ilink_user_id": peer,
            "typing_ticket": ticket,
            "status": status,
        });
        if let Err(e) = self
            .client
            .post(EP_SEND_TYPING, payload, config_timeout())
            .await
        {
            tracing::debug!("typing status={status} peer={peer} 失败（静默）: {e}");
        }
    }

    async fn ensure_typing_ticket(&self, peer: &str) -> Option<String> {
        // TTL 内直接用；过期重新取（stop_typing 依赖有效票据，否则灯常亮）
        if let Some((ticket, at)) = self.typing.lock().unwrap().get(peer).cloned() {
            if at.elapsed() < TYPING_TICKET_TTL {
                return Some(ticket);
            }
            self.typing.lock().unwrap().remove(peer);
        }
        self.fetch_typing_ticket(peer, self.tokens.get(peer).as_deref())
            .await
    }

    async fn fetch_typing_ticket(&self, peer: &str, context_token: Option<&str>) -> Option<String> {
        let mut payload = json!({ "ilink_user_id": peer });
        if let Some(t) = context_token {
            if !t.is_empty() {
                payload["context_token"] = json!(t);
            }
        }
        match self
            .client
            .post(EP_GET_CONFIG, payload, config_timeout())
            .await
        {
            Ok(resp) => {
                let ticket = resp
                    .get("typing_ticket")
                    .and_then(|x| x.as_str())
                    .unwrap_or("");
                if ticket.is_empty() {
                    tracing::debug!("getconfig 无 typing_ticket peer={peer}");
                    return None;
                }
                self.typing
                    .lock()
                    .unwrap()
                    .insert(peer.to_string(), (ticket.to_string(), Instant::now()));
                Some(ticket.to_string())
            }
            Err(e) => {
                tracing::debug!("getconfig 失败 peer={peer}: {e}");
                None
            }
        }
    }
}

fn api_timeout() -> Duration {
    Duration::from_millis(API_TIMEOUT_MS)
}

fn config_timeout() -> Duration {
    Duration::from_millis(CONFIG_TIMEOUT_MS)
}

fn build_msg(peer: &str, text: &str, token: Option<&str>) -> Value {
    let mut msg = json!({
        "from_user_id": "",
        "to_user_id": peer,
        "client_id": format!("im-bridge-{}", uuid::Uuid::new_v4().simple()),
        "message_type": 2,
        "message_state": 2,
        "item_list": [{"type": 1, "text_item": {"text": text}}],
    });
    if let Some(t) = token {
        if !t.is_empty() {
            msg["context_token"] = json!(t);
        }
    }
    msg
}

/// 分段：软阈值 `SPLIT_THRESHOLD`（1800 字符）按行切、``` 围栏内不切（硬上限 2000）、
/// 超长单行按字符硬切。返回非空段（空输入返回空 Vec）。
pub fn split_text(text: &str) -> Vec<String> {
    if text.trim().is_empty() {
        return Vec::new();
    }
    let total = text.chars().count();
    if total <= SPLIT_THRESHOLD {
        return vec![text.to_string()];
    }
    let mut chunks: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut cur_len = 0usize;
    let mut in_fence = false;

    let flush = |cur: &mut String, cur_len: &mut usize, chunks: &mut Vec<String>| {
        if !cur.is_empty() {
            chunks.push(std::mem::take(cur));
            *cur_len = 0;
        }
    };

    for line in text.split_inclusive('\n') {
        let line_len = line.chars().count();
        // 单行就超硬上限：先把当前段冲掉，再按字符硬切该行
        if line_len > MAX_MESSAGE_LENGTH {
            flush(&mut cur, &mut cur_len, &mut chunks);
            let mut piece = String::new();
            let mut piece_len = 0usize;
            for ch in line.chars() {
                piece.push(ch);
                piece_len += 1;
                if piece_len == MAX_MESSAGE_LENGTH {
                    chunks.push(std::mem::take(&mut piece));
                    piece_len = 0;
                }
            }
            if !piece.is_empty() {
                // 尾段并入下一段的开头（保持行连续性意义不大，直接成段）
                chunks.push(piece);
            }
            in_fence = fence_state(line, in_fence);
            continue;
        }
        let soft_cap = if in_fence {
            MAX_MESSAGE_LENGTH
        } else {
            SPLIT_THRESHOLD
        };
        if cur_len + line_len > soft_cap && !cur.is_empty() {
            flush(&mut cur, &mut cur_len, &mut chunks);
        }
        cur.push_str(line);
        cur_len += line_len;
        in_fence = fence_state(line, in_fence);
    }
    flush(&mut cur, &mut cur_len, &mut chunks);
    chunks
}

/// 围栏状态推进：行内出现 ``` 翻转（Hermes 同款粗粒度判定）。
fn fence_state(line: &str, in_fence: bool) -> bool {
    let ticks = line.matches("```").count();
    if ticks % 2 == 1 {
        !in_fence
    } else {
        in_fence
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_short_passthrough() {
        assert_eq!(split_text("hello"), vec!["hello"]);
        assert_eq!(split_text(&"x".repeat(1800)).len(), 1);
        assert!(split_text("   \n  ").is_empty());
    }

    #[test]
    fn split_long_by_lines_soft_limit() {
        // 每行 100 字符 × 30 行 = 3000+，应切成 ≤1800 的段且行边界完整
        let line = "a".repeat(99) + "\n";
        let text = line.repeat(30);
        let chunks = split_text(&text);
        assert!(chunks.len() >= 2);
        for c in &chunks {
            assert!(
                c.chars().count() <= SPLIT_THRESHOLD,
                "{}",
                c.chars().count()
            );
        }
        assert_eq!(chunks.concat().chars().count(), text.chars().count());
    }

    #[test]
    fn split_fence_aware_up_to_hard_cap() {
        // 围栏内一个 1900 字符长行：软阈值 1800 装不下，但围栏内不切 → 单段 ≤2000
        let text = format!("```\n{}\n```\n", "b".repeat(1900));
        let chunks = split_text(&text);
        // 围栏块保持完整（不被切成两段把 ``` 拆开）
        assert_eq!(chunks.len(), 1, "fence block should stay whole: {chunks:?}");
        assert!(chunks[0].chars().count() <= MAX_MESSAGE_LENGTH + 8);
    }

    #[test]
    fn split_overlong_single_line_hard_split() {
        let text = "c".repeat(4500);
        let chunks = split_text(&text);
        assert_eq!(chunks.len(), 3);
        for c in &chunks {
            assert!(c.chars().count() <= MAX_MESSAGE_LENGTH);
        }
        assert_eq!(chunks.concat(), text);
    }

    #[test]
    fn split_multibyte_safe() {
        let text = "汉".repeat(2200); // 2200 字符 > 2000
        let chunks = split_text(&text);
        for c in &chunks {
            assert!(c.chars().count() <= MAX_MESSAGE_LENGTH);
        }
        assert_eq!(chunks.concat(), text);
    }

    #[test]
    fn build_msg_shape() {
        let m = build_msg("peer1", "你好", Some("tok"));
        assert_eq!(m["from_user_id"], json!(""));
        assert_eq!(m["to_user_id"], json!("peer1"));
        assert_eq!(m["message_type"], json!(2));
        assert_eq!(m["message_state"], json!(2));
        assert_eq!(m["context_token"], json!("tok"));
        assert_eq!(m["item_list"][0]["type"], json!(1));
        assert_eq!(m["item_list"][0]["text_item"]["text"], json!("你好"));
        assert!(m["client_id"].as_str().unwrap().starts_with("im-bridge-"));
        let m2 = build_msg("p", "x", None);
        assert!(m2.get("context_token").is_none());
    }
}
