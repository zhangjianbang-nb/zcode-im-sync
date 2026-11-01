//! 飞书通道：WS 长连接收消息 + 出站发送 + reactions 状态机。
//!
//! 协议层用官方 SDK（larksuite-oapi-sdk-rs 0.3.13，MIT），业务逻辑自写：
//! - 入站只处理 `chat_type == "p2p"`（`ChannelPolicy::default()` 的
//!   `group_allowlist: None` 是放行所有群——群聊必须靠这里的双保险拦截）；
//!   DM 白名单由 SessionRouter 单一事实源判定，Dropped 严禁任何回执/反应。
//!   媒体资源经 `message_resource.get` 下载落盘 `media/<chat>/<date>/`，
//!   绝对路径注入 prompt（与微信通道同策略）。
//! - reactions：Turn 入队即打 ⏳(OnIt)，TurnSignal 到达换 ✅(DONE)/❌(CrossMark)。
//!   Pending/orphans 状态机覆盖「信号早于登记」竞态；per-chat 串行门保证
//!   登记顺序 = 入队顺序 = turn 顺序，FIFO 结算恒正确。
//! - 出站消费 MirrorPoller 产出的 OutboundMsg：首段 reply_text（失败码
//!   {230011,231003} 回退 create），后续段 create；4000 软 / 8000 硬围栏感知分段。
//! - 锁纪律：StdMutex 快锁段绝不跨 await（与出站消费任务零死锁）。

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use larksuite_oapi_sdk_rs::channel::{
    Channel, ChannelPolicy, NormalizedMessage, RejectEvent, SendTarget,
};
use larksuite_oapi_sdk_rs::service::im::v1::{CreateMessageReactionReqBody, Emoji};
use larksuite_oapi_sdk_rs::{EventDispatcher, LarkClient, LarkError, RequestOption};
use tokio::sync::{mpsc, Mutex};

use super::{open_base_url, FeishuCreds};
use crate::media::{self, MediaKind, SavedMedia};
use crate::session_router::{InboundOutcome, OutboundMsg, Platform, SessionRouter, ACK_TEXT};

/// 处理中（⏳）。
pub const EMOJI_BUSY: &str = "OnIt";
/// 完成（✅）。注意不是 "CheckMark" 系；官方 DONE 即绿色对勾。
pub const EMOJI_DONE: &str = "DONE";
/// 失败（❌）。**不用 ERROR**——那是斜眼红纹尴尬脸。
pub const EMOJI_FAIL: &str = "CrossMark";

/// 分段软阈值（字符数，按行切）。
pub const SPLIT_SOFT: usize = 4000;
/// 单条硬上限（字符数，围栏内/超长行才动用）。
pub const SPLIT_HARD: usize = 8000;
/// 段间间隔。
const SEGMENT_GAP: Duration = Duration::from_millis(300);
/// reply_text 不可用需回退 create 的业务码（消息不可回复/已撤回类）。
const REPLY_FALLBACK_CODES: [i64; 2] = [230011, 231003];

/// 已登记 ⏳、待 TurnSignal 结算的 Turn 消息。
#[derive(Debug, Clone)]
struct Pending {
    message_id: String,
    /// ⏳ 的 reaction_id（None = create_reaction 尽力失败，结算时只建不删）
    reaction_id: Option<String>,
}

/// 通道共享状态（StdMutex 快锁；锁段绝不跨 await）。
#[derive(Debug, Default)]
struct SharedState {
    /// chat_key → 最近入站 message_id（出站首段 reply 用）。
    last_inbound: HashMap<String, String>,
    /// chat_key → 待结算 Turn 消息 FIFO（per-chat 串行门保证与 turn 顺序一致）。
    pending: HashMap<String, VecDeque<Pending>>,
    /// chat_key → 先到的 TurnSignal FIFO（覆盖「信号早于登记」竞态）。
    orphans: HashMap<String, VecDeque<bool>>,
    /// per-chat 处理串行门（登记顺序 = 入队顺序 = turn 顺序）。
    gates: HashMap<String, Arc<Mutex<()>>>,
}

impl SharedState {
    fn gate(&mut self, chat_key: &str) -> Arc<Mutex<()>> {
        self.gates.entry(chat_key.to_string()).or_default().clone()
    }

    /// Turn 消息登记：孤儿信号先到 → 返回 Some(ok) 立即结算；否则挂 pending 返回 None。
    fn register_turn(
        &mut self,
        chat_key: &str,
        message_id: &str,
        reaction_id: Option<String>,
    ) -> Option<bool> {
        if let Some(ok) = self
            .orphans
            .entry(chat_key.to_string())
            .or_default()
            .pop_front()
        {
            return Some(ok);
        }
        self.pending
            .entry(chat_key.to_string())
            .or_default()
            .push_back(Pending {
                message_id: message_id.to_string(),
                reaction_id,
            });
        None
    }

    /// TurnSignal 到达：pending 有排队 → 返回结算目标；否则存孤儿返回 None。
    fn on_signal(&mut self, chat_key: &str, ok: bool) -> Option<Pending> {
        match self
            .pending
            .entry(chat_key.to_string())
            .or_default()
            .pop_front()
        {
            Some(p) => Some(p),
            None => {
                self.orphans
                    .entry(chat_key.to_string())
                    .or_default()
                    .push_back(ok);
                None
            }
        }
    }
}

/// 跑飞书通道：WS 长连接（阻塞至断开）+ 出站消费 + TurnSignal→reactions 两个后台任务。
pub async fn run(
    creds: FeishuCreds,
    router: Arc<SessionRouter>,
    outbound: mpsc::Receiver<OutboundMsg>,
    media_root: PathBuf,
) -> anyhow::Result<()> {
    let client = LarkClient::builder(creds.app_id.clone(), creds.app_secret.clone())
        .base_url(open_base_url(&creds.domain))
        .build()
        .map_err(|e| anyhow::anyhow!("飞书客户端构建失败: {e}"))?;
    // WS 长连接模式不校验 verify token / encrypt key（那是 webhook 用的）
    let dispatcher = EventDispatcher::new("", "");
    let state = Arc::new(StdMutex::new(SharedState::default()));

    tokio::spawn(consume_outbound(client.clone(), state.clone(), outbound));
    tokio::spawn(turn_signal_loop(
        router.clone(),
        state.clone(),
        client.clone(),
    ));

    // policy 默认：require_mention=true 辅助拦群；group_allowlist=None=放行所有群，
    // 故群聊拦截以 handle_message 的 chat_type=="p2p" 双保险为准。
    let policy = ChannelPolicy::default();
    // 闭包环境存副本（builder 的 &client 借用不能与 move 捕获冲突）
    let msg_router = router.clone();
    let msg_state = state.clone();
    let msg_client = client.clone();
    let msg_media_root = media_root.clone();
    Channel::builder(&client, dispatcher)
        .policy(policy)
        .on_message(move |msg: NormalizedMessage| {
            let router = msg_router.clone();
            let state = msg_state.clone();
            let client = msg_client.clone();
            let media_root = msg_media_root.clone();
            async move {
                handle_message(msg, router, state, client, media_root).await;
                Ok::<(), LarkError>(())
            }
        })
        .on_rejected(move |ev: RejectEvent| async move {
            tracing::debug!(
                "飞书消息被策略拒绝 {:?}: {} ({})",
                ev.reason,
                ev.event_key,
                ev.detail
            );
            Ok::<(), LarkError>(())
        })
        .build()
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("飞书长连接异常退出: {e}"))
}

/// 入站消息主流程（on_message 回调体；永不 panic、永不 Err——错误只记日志）。
async fn handle_message(
    msg: NormalizedMessage,
    router: Arc<SessionRouter>,
    state: Arc<StdMutex<SharedState>>,
    client: LarkClient,
    media_root: PathBuf,
) {
    // 双保险一：只处理 DM（policy 的 group_allowlist=None 是放行所有群！）
    if msg.chat_type != "p2p" {
        tracing::debug!(
            "飞书非 p2p 消息忽略 chat_type={} chat_id={}",
            msg.chat_type,
            msg.chat_id
        );
        return;
    }
    // 双保险二：缺 open_id 忽略
    let Some(peer) = msg
        .sender
        .user_id
        .as_ref()
        .and_then(|u| u.open_id().map(String::from))
    else {
        tracing::warn!("飞书消息缺 open_id，忽略 message_id={}", msg.message_id);
        return;
    };
    let chat_key = Platform::Feishu.chat_key(&peer);
    let message_id = msg.message_id.clone();

    // 媒体资源：下载 → 落盘 media/<chat>/<date>/ → 绝对路径注入 prompt
    let attachments = collect_media(&client, &msg, &media_root, &chat_key).await;
    let Some(text) = Some(media::compose_prompt(
        msg.text.as_deref().unwrap_or(""),
        &attachments,
    ))
    .filter(|t| !t.trim().is_empty()) else {
        tracing::debug!("飞书无文本且无媒体消息忽略 message_id={}", message_id);
        return;
    };

    // 入站秒回：白名单用户先拿到 10s 内回执，再进 per-chat 串行门等 turn
    // （模型 TTFT 不可控，回程镜像兜不住这个时限）
    if router.is_allowed(Platform::Feishu, &peer).await {
        let target = SendTarget::new("open_id", peer.clone());
        if let Err(e) = client
            .channel_messaging()
            .send_text(&target, ACK_TEXT, &RequestOption::default())
            .await
        {
            tracing::warn!("飞书入站秒回失败 peer={peer}: {e}");
        }
    }

    // per-chat 串行门：同一会话的登记顺序 = 入队顺序 = turn 顺序（FIFO 恒正确）
    let gate = state.lock().unwrap().gate(&chat_key);
    let _guard = gate.lock().await;

    // 快锁段：记 last_inbound（出站首段 reply 引用）
    {
        let mut st = state.lock().unwrap();
        st.last_inbound.insert(chat_key.clone(), message_id.clone());
    }

    // 无锁段：入站处理（内部自管队列/会话）
    let outcome = router.handle_inbound(Platform::Feishu, &peer, &text).await;

    match outcome {
        InboundOutcome::Turn => {
            // ⏳ 尽力打（拿到 reaction_id 才能在结算时删掉）
            let rid = create_reaction(&client, &message_id, EMOJI_BUSY).await;
            // 快锁结算：孤儿信号先到 → 直接换表情；否则挂 pending 等信号
            let settle_now =
                state
                    .lock()
                    .unwrap()
                    .register_turn(&chat_key, &message_id, rid.clone());
            if let Some(ok) = settle_now {
                swap_reaction(&client, &message_id, rid.as_deref(), ok).await;
            }
        }
        InboundOutcome::Failed => {
            // 入队失败不会有 TurnSignal，即刻 ❌
            let _ = create_reaction(&client, &message_id, EMOJI_FAIL).await;
        }
        // Dropped：严禁任何回执/反应（防暴露机器人）；Unbound/Command：router 已发文字回执
        // QueuedToTui：消息已交 TUI 自己跑回合，秒回 ACK 已发；无 TurnSignal 可挂 reaction
        InboundOutcome::Dropped
        | InboundOutcome::Unbound
        | InboundOutcome::Command
        | InboundOutcome::QueuedToTui => {}
    }
}

/// 下载 msg.resources 全部资源并落盘（失败只 warn 不中断；空资源返回空 Vec）。
async fn collect_media(
    client: &LarkClient,
    msg: &NormalizedMessage,
    media_root: &std::path::Path,
    chat_key: &str,
) -> Vec<SavedMedia> {
    let mut out = Vec::new();
    for res in &msg.resources {
        if res.file_key.is_empty() {
            tracing::debug!("飞书资源缺 file_key，跳过 message_id={}", msg.message_id);
            continue;
        }
        let kind = resource_kind(&res.resource_type);
        let api_type = resource_api_type(&res.resource_type);
        let resp = match client
            .im()
            .message_resource
            .get(
                &msg.message_id,
                &res.file_key,
                api_type,
                &RequestOption::default(),
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    "飞书媒体下载异常 message_id={} file_key={}: {e}",
                    msg.message_id,
                    res.file_key
                );
                continue;
            }
        };
        let file_name = if !res.file_name.is_empty() {
            res.file_name.clone()
        } else {
            resp.file_name
                .clone()
                .filter(|n| !n.trim().is_empty())
                .unwrap_or_else(|| kind.default_file_name().to_string())
        };
        match media::save_media(media_root, chat_key, kind, &file_name, &resp.data) {
            Ok(saved) => {
                tracing::info!(
                    "飞书媒体落盘 {}（{}）→ {}",
                    saved.file_name,
                    media::human_size(saved.size as u64),
                    saved.path.display()
                );
                out.push(saved);
            }
            Err(e) => tracing::warn!("飞书媒体落盘失败 {file_name}: {e}"),
        }
    }
    out
}

/// 资源类型 → MediaKind（image/sticker→Image，audio→Voice，video→Video，其余→File）。
fn resource_kind(resource_type: &str) -> MediaKind {
    match resource_type {
        "image" | "sticker" => MediaKind::Image,
        "audio" => MediaKind::Voice,
        "video" => MediaKind::Video,
        _ => MediaKind::File,
    }
}

/// 下载 API 的 resource_type 参数：image/sticker→"image"，其余→"file"（API 只认这两个）。
fn resource_api_type(resource_type: &str) -> &'static str {
    match resource_type {
        "image" | "sticker" => "image",
        _ => "file",
    }
}

/// TurnSignal 消费：把对应 ⏳ 换成 ✅/❌（文本回程走 MirrorPoller，这里只管表情）。
async fn turn_signal_loop(
    router: Arc<SessionRouter>,
    state: Arc<StdMutex<SharedState>>,
    client: LarkClient,
) {
    let mut rx = router.turn_signals();
    loop {
        match rx.recv().await {
            Ok(sig) => {
                if !sig.chat_key.starts_with("feishu:dm:") {
                    continue;
                }
                // 快锁段：FIFO 结算或存孤儿
                let target = state.lock().unwrap().on_signal(&sig.chat_key, sig.ok);
                if let Some(p) = target {
                    swap_reaction(&client, &p.message_id, p.reaction_id.as_deref(), sig.ok).await;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!("TurnSignal 接收滞后 {n} 条（对应 reactions 可能滞留 ⏳）");
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

/// 出站消费：分段 → 首段 reply（可回退 create）→ 后续段 create。
async fn consume_outbound(
    client: LarkClient,
    state: Arc<StdMutex<SharedState>>,
    mut rx: mpsc::Receiver<OutboundMsg>,
) {
    let opt = RequestOption::default();
    while let Some(msg) = rx.recv().await {
        if msg.platform != Platform::Feishu {
            continue;
        }
        let chunks = split_reply(&msg.text);
        if chunks.is_empty() {
            continue;
        }
        let last_mid = state
            .lock()
            .unwrap()
            .last_inbound
            .get(&msg.chat_key)
            .cloned();
        let total = chunks.len();
        for (i, chunk) in chunks.iter().enumerate() {
            if i == 0 {
                if let Some(mid) = &last_mid {
                    match try_reply(&client, mid, chunk, &opt).await {
                        ReplyOutcome::Sent => {
                            // 成功留痕（无此日志时只能靠「没有 warn」反推，无法核实送达）
                            tracing::info!(
                                "飞书已回复 peer={} 字符数={}",
                                msg.peer,
                                chunk.chars().count()
                            );
                            if i + 1 < total {
                                tokio::time::sleep(SEGMENT_GAP).await;
                            }
                            continue;
                        }
                        ReplyOutcome::Fallback => {
                            tracing::debug!("飞书 reply 不可用，回退 create peer={}", msg.peer);
                        }
                        ReplyOutcome::Failed => {
                            // 非回退码错误：只记日志，不重复轰炸
                            if i + 1 < total {
                                tokio::time::sleep(SEGMENT_GAP).await;
                            }
                            continue;
                        }
                    }
                }
            }
            let target = SendTarget::new("open_id", msg.peer.clone());
            match client
                .channel_messaging()
                .send_text(&target, chunk, &opt)
                .await
            {
                Ok(_) => tracing::info!(
                    "飞书已发送 peer={} 字符数={}",
                    msg.peer,
                    chunk.chars().count()
                ),
                Err(e) => tracing::warn!("飞书出站发送失败 peer={}: {e}", msg.peer),
            }
            if i + 1 < total {
                tokio::time::sleep(SEGMENT_GAP).await;
            }
        }
    }
}

/// reply_text 结果三态。
enum ReplyOutcome {
    /// 已成功发出
    Sent,
    /// 消息不可回复（{230011,231003}）→ 上层 create 回退
    Fallback,
    /// 其他失败 → 只记日志
    Failed,
}

async fn try_reply(
    client: &LarkClient,
    message_id: &str,
    text: &str,
    opt: &RequestOption,
) -> ReplyOutcome {
    match client
        .channel_messaging()
        .reply_text(message_id, text, false, opt)
        .await
    {
        Ok(resp) if resp.success() => ReplyOutcome::Sent,
        Ok(resp) => {
            let code = resp.code_error.code;
            if REPLY_FALLBACK_CODES.contains(&code) {
                ReplyOutcome::Fallback
            } else {
                tracing::warn!(
                    "飞书 reply 失败 {code}: {}",
                    resp.code_error.effective_message()
                );
                ReplyOutcome::Failed
            }
        }
        Err(e) => {
            tracing::warn!("飞书 reply 异常: {e}");
            ReplyOutcome::Failed
        }
    }
}

/// 打 reaction；成功返回 reaction_id（失败 None，尽力而为）。
async fn create_reaction(
    client: &LarkClient,
    message_id: &str,
    emoji_type: &str,
) -> Option<String> {
    // Emoji 是 non_exhaustive：外部 crate 禁字面量，只能 Default + 字段赋值
    let mut emoji = Emoji::default();
    emoji.emoji_type = Some(emoji_type.to_string());
    let body = CreateMessageReactionReqBody {
        reaction_type: Some(emoji),
    };
    match client
        .im()
        .message_reaction
        .create(message_id, &body, &RequestOption::default())
        .await
    {
        Ok(resp) if resp.success() => resp.data.and_then(|d| d.reaction_id),
        Ok(resp) => {
            tracing::debug!(
                "create_reaction {emoji_type} 失败 {}: {}",
                resp.code_error.code,
                resp.code_error.effective_message()
            );
            None
        }
        Err(e) => {
            tracing::debug!("create_reaction {emoji_type} 异常: {e}");
            None
        }
    }
}

/// ⏳ → ✅/❌：删旧 reaction + 建新（尽力而为，失败只记日志）。
async fn swap_reaction(
    client: &LarkClient,
    message_id: &str,
    busy_reaction_id: Option<&str>,
    ok: bool,
) {
    if let Some(rid) = busy_reaction_id {
        if let Err(e) = client
            .im()
            .message_reaction
            .delete(message_id, rid, &RequestOption::default())
            .await
        {
            tracing::debug!("delete_reaction {rid} 异常（忽略）: {e}");
        }
    }
    let emoji = if ok { EMOJI_DONE } else { EMOJI_FAIL };
    let _ = create_reaction(client, message_id, emoji).await;
}

/// 分段（飞书 4000 软 / 8000 硬，``` 围栏感知、UTF-8 字符边界）。
/// 空/纯空白返回空 Vec。
pub fn split_reply(text: &str) -> Vec<String> {
    if text.trim().is_empty() {
        return Vec::new();
    }
    let total = text.chars().count();
    if total <= SPLIT_SOFT {
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
        // 单行就超硬上限：先冲当前段，再按字符硬切该行
        if line_len > SPLIT_HARD {
            flush(&mut cur, &mut cur_len, &mut chunks);
            let mut piece = String::new();
            let mut piece_len = 0usize;
            for ch in line.chars() {
                piece.push(ch);
                piece_len += 1;
                if piece_len == SPLIT_HARD {
                    chunks.push(std::mem::take(&mut piece));
                    piece_len = 0;
                }
            }
            if !piece.is_empty() {
                chunks.push(piece);
            }
            in_fence = fence_state(line, in_fence);
            continue;
        }
        let soft_cap = if in_fence { SPLIT_HARD } else { SPLIT_SOFT };
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

/// 围栏状态推进：行内出现 ``` 奇数次翻转。
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
    fn split_reply_short_passthrough() {
        assert_eq!(split_reply("hello"), vec!["hello"]);
        assert_eq!(split_reply(&"x".repeat(SPLIT_SOFT)).len(), 1);
        assert!(split_reply("   \n  ").is_empty());
    }

    #[test]
    fn split_reply_long_by_lines_soft_limit() {
        // 每行 100 字符 × 60 行 = 6000+，应切成 ≤4000 的段且行边界完整
        let line = "a".repeat(99) + "\n";
        let text = line.repeat(60);
        let chunks = split_reply(&text);
        assert!(chunks.len() >= 2);
        for c in &chunks {
            assert!(c.chars().count() <= SPLIT_SOFT, "{}", c.chars().count());
        }
        assert_eq!(chunks.concat().chars().count(), text.chars().count());
    }

    #[test]
    fn split_reply_fence_aware_up_to_hard_cap() {
        // 围栏内一个 5000 字符长行：软阈值 4000 装不下，但围栏内不切 → 单段 ≤8000
        let text = format!("```\n{}\n```\n", "b".repeat(5000));
        let chunks = split_reply(&text);
        assert_eq!(chunks.len(), 1, "fence block should stay whole: {chunks:?}");
        assert!(chunks[0].chars().count() <= SPLIT_HARD + 8);
    }

    #[test]
    fn split_reply_overlong_single_line_hard_split() {
        let text = "c".repeat(9000);
        let chunks = split_reply(&text);
        assert_eq!(chunks.len(), 2);
        for c in &chunks {
            assert!(c.chars().count() <= SPLIT_HARD);
        }
        assert_eq!(chunks.concat(), text);
    }

    #[test]
    fn split_reply_multibyte_safe() {
        let text = "汉".repeat(8100);
        let chunks = split_reply(&text);
        for c in &chunks {
            assert!(c.chars().count() <= SPLIT_HARD);
        }
        assert_eq!(chunks.concat(), text);
    }

    #[test]
    fn gate_same_chat_same_arc() {
        let mut st = SharedState::default();
        let g1 = st.gate("feishu:dm:o1");
        let g2 = st.gate("feishu:dm:o1");
        let g3 = st.gate("feishu:dm:o2");
        assert!(Arc::ptr_eq(&g1, &g2));
        assert!(!Arc::ptr_eq(&g1, &g3));
    }

    #[test]
    fn register_then_signal_settles_fifo() {
        let mut st = SharedState::default();
        let chat = "feishu:dm:o1";
        assert!(st.register_turn(chat, "m1", Some("r1".into())).is_none());
        assert!(st.register_turn(chat, "m2", None).is_none());
        // FIFO：先结算 m1 再 m2
        let p1 = st.on_signal(chat, true).expect("m1 应结算");
        assert_eq!(p1.message_id, "m1");
        assert_eq!(p1.reaction_id.as_deref(), Some("r1"));
        let p2 = st.on_signal(chat, false).expect("m2 应结算");
        assert_eq!(p2.message_id, "m2");
        assert!(p2.reaction_id.is_none());
    }

    #[test]
    fn signal_before_register_goes_orphan_then_settles() {
        let mut st = SharedState::default();
        let chat = "feishu:dm:o1";
        // 信号先到：无 pending → 存孤儿
        assert!(st.on_signal(chat, true).is_none());
        // 登记时发现孤儿 → 立即结算
        assert_eq!(st.register_turn(chat, "m1", Some("r1".into())), Some(true));
        // 后续正常挂 pending
        assert!(st.register_turn(chat, "m2", None).is_none());
        assert_eq!(
            st.on_signal(chat, false).map(|p| p.message_id),
            Some("m2".into())
        );
    }

    #[test]
    fn orphan_fifo_across_races() {
        let mut st = SharedState::default();
        let chat = "feishu:dm:o1";
        assert!(st.on_signal(chat, true).is_none());
        assert!(st.on_signal(chat, false).is_none());
        // 两个孤儿按 FIFO 结算
        assert_eq!(st.register_turn(chat, "m1", None), Some(true));
        assert_eq!(st.register_turn(chat, "m2", None), Some(false));
        // 孤儿清空后：登记正常挂 pending，信号正常结算
        assert!(st.register_turn(chat, "m3", None).is_none());
        assert_eq!(
            st.on_signal(chat, true).map(|p| p.message_id),
            Some("m3".into())
        );
    }

    #[test]
    fn turn_signal_chat_key_filter() {
        // feishu 通道只认 feishu:dm: 前缀（微信信号不打飞书表情）
        assert!("feishu:dm:ou_x".starts_with("feishu:dm:"));
        assert!(!"wechat:dm:u1".starts_with("feishu:dm:"));
    }

    #[test]
    fn resource_kind_and_api_type_mapping() {
        // image/sticker → Image + "image"；audio → Voice；video → Video；file/未知 → File
        for (t, k) in [
            ("image", MediaKind::Image),
            ("sticker", MediaKind::Image),
            ("audio", MediaKind::Voice),
            ("video", MediaKind::Video),
            ("file", MediaKind::File),
            ("weird", MediaKind::File),
        ] {
            assert_eq!(resource_kind(t), k, "kind({t})");
        }
        assert_eq!(resource_api_type("image"), "image");
        assert_eq!(resource_api_type("sticker"), "image");
        assert_eq!(resource_api_type("audio"), "file");
        assert_eq!(resource_api_type("file"), "file");
    }
}
