//! 会话路由：IM 消息 ↔ zcode 会话「双向同步」运行时（登录绑定后的活跃核心）。
//!
//! 设计（回程单一路径防双发）：
//! - **回复文本统一由 MirrorPoller 捕获**：5s 轮询会话库 `~/.zcode/cli/db/db.sqlite`，
//!   把 role=assistant 且 `time.completed` 非空的 text part 镜像回 IM（watermark +
//!   part-id 去重）。zcode TUI 进程里发起的轮次回复**只能经 DB 捕获**，桥发起轮次的
//!   回复也走同一路径——两条来源天然不重复；事件流不直接回程文本。
//! - **事件流只做信号**：TurnDone/TurnError/超时用于结束等待与错误回执（DB 里
//!   没有对应 text part，不会与镜像重复）。
//! - per-chat worker：mpsc 串行喂 `session/send`（同会话不并发，-32010 由
//!   `ZcodeHandle::send` 内重试兜底）；喂前等会话空闲（30 秒静默判定，防抢 TUI）；
//!   turn 超时 → `session/stop` + 错误回执；空闲 10 分钟关 handle（释放 ~600MB/会话）。
//! - 控制命令 /new /stop /cwd /status /help；白名单外静默丢弃（只记日志）。
//! - 出站统一投 `OutboundMsg` 队列，由平台通道任务（wechat/feishu）实际发送并
//!   把 `SendError` 映射为确定性回执（如「请先在微信里给机器人发一条消息」）。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::Connection;
use serde_json::Value;
use tokio::sync::{broadcast, mpsc, RwLock};

use crate::binding::{ChatsFile, BIND_MODE_TUI_SYNC};
use crate::im_inbox::{ImInbox, InboxMsg, TUI_BUSY_HARD_MS};
use crate::process_mirror::tui_owns_session;
use crate::wechat_ilink::poll::TtlDedup;
use crate::whitelist::Whitelist;
use crate::zcode_rpc::{RpcFail, ZEvent, ZcodeHandle};

/// MirrorPoller 轮询间隔。
const MIRROR_INTERVAL: Duration = Duration::from_secs(5);
/// 会话「活跃」判定窗口（此内有写入 = TUI 在用/turn 进行中，IM 消息排队）。
/// 30s 静默即视为空闲：兼顾「不抢 TUI 正在跑的轮次」与「IM 连续对话不吃 3 分钟延迟」；
/// 抢跑的边界情况交给 -32010 重试（60s 预算）兜底。
const ACTIVE_WINDOW_MS: i64 = 30_000;
/// 排队等空闲的硬上限（超时照喂，交给 -32010 重试兜底）。
const IDLE_WAIT_MAX: Duration = Duration::from_secs(180);
/// worker 空闲多久关 zcode handle（app-server 每会话 ~600MB）。
const WORKER_IDLE_CLOSE: Duration = Duration::from_secs(600);
/// turn 完成后等待镜像文本的兜底窗口（仅用于空回复判定）。
const EMPTY_REPLY_GRACE: Duration = Duration::from_secs(12);
/// 新会话初始水位回退量：覆盖「绑定/重启 → 首次轮询」≤1 个轮询间隔的竞态，
/// 防止首条回复落盘早于首次轮询被永久漏掉；代价=重启后最多补发 6s 内的文本。
const STARTUP_GRACE_MS: i64 = 6_000;

/// 入站秒回文案：白名单用户发来任何消息，通道层先直发这一句再喂 zcode，
/// 保证 10s 内必有回执（模型 turn 的 TTFT 不可控，不能靠回程镜像兜这个时限）。
pub const ACK_TEXT: &str = "📥 收到，开始处理…（思考/工具/回答全过程会同步到这里）";

/// 出站消息（平台通道任务消费后实际发送）。
#[derive(Debug, Clone)]
pub struct OutboundMsg {
    /// `wechat:dm:<ilink_user_id>` / `feishu:dm:<open_id>`
    pub chat_key: String,
    /// 平台原生对端 id（微信 ilink_user_id / 飞书 open_id）
    pub peer: String,
    pub platform: Platform,
    pub text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Wechat,
    Feishu,
}

impl Platform {
    pub fn chat_key(self, peer: &str) -> String {
        match self {
            Platform::Wechat => format!("wechat:dm:{peer}"),
            Platform::Feishu => format!("feishu:dm:{peer}"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Platform::Wechat => "wechat",
            Platform::Feishu => "feishu",
        }
    }
}

/// `handle_inbound` 处理结果（供平台通道决定 reactions 等即时反馈）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundOutcome {
    /// 白名单外丢弃（严禁任何回执/反应，防暴露机器人）
    Dropped,
    /// 未绑定 zcode（已发引导回执）
    Unbound,
    /// 控制命令（已入队或已回执）
    Command,
    /// 普通用户文本，已入队等 turn（将有 TurnSignal 收尾）
    Turn,
    /// 普通用户文本，已写入 TUI 收件箱（TUI 在线且正看着绑定会话，由 TUI 自己跑回合；
    /// 桥的兜底扫描只在 TUI 掉线/长忙超时时接管）
    QueuedToTui,
    /// 用户文本入队失败（队满/绑定丢失，已发回执）
    Failed,
}

/// turn 完成信号（供平台通道打 reactions；文本回程仍统一走 MirrorPoller）。
#[derive(Debug, Clone)]
pub struct TurnSignal {
    pub chat_key: String,
    /// true=TurnDone（含空回复兜底）；false=超时/TurnError/事件流关闭
    pub ok: bool,
}

/// 控制命令（`/` 开头才可能命中；其余按普通 prompt 转发）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// 新开会话（create，换绑）
    New,
    /// 打断当前轮次
    Stop,
    /// 切换目录并新开会话
    Cwd(String),
    /// 查询绑定/会话状态
    Status,
    /// 命令帮助
    Help,
}

/// 解析控制命令；非命令（含其他 `/xxx`）返回 None 走普通 prompt。
pub fn parse_command(text: &str) -> Option<Command> {
    let t = text.trim();
    if !t.starts_with('/') {
        return None;
    }
    let (head, rest) = match t.find(char::is_whitespace) {
        Some(i) => (&t[..i], t[i..].trim()),
        None => (t, ""),
    };
    match head {
        "/new" if rest.is_empty() => Some(Command::New),
        "/stop" if rest.is_empty() => Some(Command::Stop),
        "/status" if rest.is_empty() => Some(Command::Status),
        "/help" if rest.is_empty() => Some(Command::Help),
        "/cwd" if !rest.is_empty() => Some(Command::Cwd(rest.to_string())),
        _ => None,
    }
}

pub const HELP_TEXT: &str = "im-bridge 命令：\n\
    /new — 新开会话并绑定（丢弃旧上下文）\n\
    /stop — 打断当前轮次\n\
    /cwd <path> — 切换工作目录并新开会话\n\
    /status — 查看绑定与会话状态\n\
    /help — 本帮助\n\
    其余消息原样转发给 zcode（全能力 yolo 模式）";

/// 回程文案定型（含脚注）；text 为空给占位。
pub fn finalize_reply(text: &str, tool_calls: Option<u64>, tokens: Option<u64>) -> String {
    let body = text.trim_end();
    let mut out = if body.is_empty() {
        String::from("（本轮没有文字回复）")
    } else {
        body.to_string()
    };
    let mut notes: Vec<String> = Vec::new();
    if let Some(n) = tool_calls {
        if n > 0 {
            notes.push(format!("{n} tools"));
        }
    }
    if let Some(tk) = tokens {
        if tk > 0 {
            notes.push(humanize_tokens(tk));
        }
    }
    if !notes.is_empty() {
        out.push_str(&format!("\n[{}]", notes.join(" · ")));
    }
    out
}

fn humanize_tokens(n: u64) -> String {
    if n >= 100_000 {
        format!("{:.0}k", n as f64 / 1000.0)
    } else if n >= 1000 {
        format!("{:.1}k", n as f64 / 1000.0)
    } else {
        format!("{n}")
    }
}

/// MirrorPoller 捕获的一条 assistant 文本。
#[derive(Debug, Clone)]
pub struct MirrorItem {
    pub part_id: String,
    pub time_created: i64,
    pub text: String,
}

/// 查会话库：`watermark`（ms）之后、已完成 assistant 消息的 text part（升序）。
/// role/completed/type 判定在 Rust 侧解析 JSON（SQL 只做粗过滤，防 schema 漂移误伤）。
pub fn fetch_new_assistant_texts(
    conn: &Connection,
    sess_id: &str,
    watermark: i64,
) -> rusqlite::Result<Vec<MirrorItem>> {
    let mut stmt = conn.prepare(
        "SELECT p.id, p.time_created, p.data, m.data \
         FROM part p JOIN message m ON m.id = p.message_id \
         WHERE p.session_id = ?1 AND p.time_created > ?2 \
           AND m.data LIKE '%\"role\":\"assistant\"%' \
           AND m.data LIKE '%\"completed\"%' \
           AND p.data LIKE '%\"type\":\"text\"%' \
         ORDER BY p.time_created ASC",
    )?;
    let rows = stmt.query_map(rusqlite::params![sess_id, watermark], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (part_id, tc, part_data, msg_data) = row?;
        // 精确判定（JSON 解析）
        let (Ok(p), Ok(m)) = (
            serde_json::from_str::<Value>(&part_data),
            serde_json::from_str::<Value>(&msg_data),
        ) else {
            continue;
        };
        let is_assistant = m.get("role").and_then(|x| x.as_str()) == Some("assistant");
        let completed = m
            .get("time")
            .and_then(|t| t.get("completed"))
            .and_then(|x| x.as_i64())
            .is_some();
        let is_text = p.get("type").and_then(|x| x.as_str()) == Some("text");
        if !(is_assistant && completed && is_text) {
            continue;
        }
        let text = p
            .get("text")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        if text.trim().is_empty() {
            continue;
        }
        out.push(MirrorItem {
            part_id,
            time_created: tc,
            text,
        });
    }
    Ok(out)
}

/// 会话最近活跃时间（message.time_updated 最大值，ms）；无消息返回 None。
pub fn session_last_activity_ms(conn: &Connection, sess_id: &str) -> rusqlite::Result<Option<i64>> {
    let v: Option<i64> = conn.query_row(
        "SELECT MAX(time_updated) FROM message WHERE session_id = ?1",
        [sess_id],
        |r| r.get(0),
    )?;
    Ok(v)
}

/// 会话 30 秒内仍有写入（TUI 在用/turn 进行中）→ true。
pub fn is_session_active(last_ms: Option<i64>, now_ms: i64) -> bool {
    matches!(last_ms, Some(t) if now_ms - t < ACTIVE_WINDOW_MS)
}

/// Router 静态配置（从 Config 展开，便于测试注入）。
#[derive(Debug, Clone)]
pub struct RouterConfig {
    pub zcode_bin: String,
    pub turn_timeout_s: u64,
    pub workdir_default: String,
    pub db_path: PathBuf,
    pub chats_path: PathBuf,
    /// 桥的 state 目录（收件箱 `im-inbox/` 与 `tui-heartbeat.json` 都在这里）
    pub state_dir: PathBuf,
}

/// per-chat worker 命令。
#[derive(Debug)]
enum WorkerCmd {
    UserText {
        text: String,
    },
    /// 新开会话（Some(cwd)=切目录；None=沿用当前目录）
    NewSession {
        cwd: Option<String>,
    },
    StopTurn,
    /// 询问状态（worker 回执一文本）
    Status,
}

/// 双向同步路由器：入站分发 + per-chat worker 池 + MirrorPoller 驱动。
pub struct SessionRouter {
    cfg: RouterConfig,
    chats: Arc<RwLock<ChatsFile>>,
    whitelist: Arc<RwLock<Whitelist>>,
    out_tx: mpsc::Sender<OutboundMsg>,
    /// chat_key → worker 入队口（worker 退出后条目失效，enqueue 检测 Closed 后重建）
    workers: StdMutex<HashMap<String, mpsc::Sender<WorkerCmd>>>,
    /// 出站文本指纹（防镜像与「空回复兜底」边缘双发；TTL 300s）
    out_dedup: Arc<StdMutex<TtlDedup>>,
    /// turn 完成广播（平台通道打 reactions 用）
    turn_tx: broadcast::Sender<TurnSignal>,
    /// TUI 收件箱（TUI 在线时消息交给 TUI 自己跑；桥只在 TUI 掉线时兜底）
    inbox: ImInbox,
}

impl SessionRouter {
    pub fn new(
        cfg: RouterConfig,
        chats: Arc<RwLock<ChatsFile>>,
        whitelist: Arc<RwLock<Whitelist>>,
        out_tx: mpsc::Sender<OutboundMsg>,
    ) -> Arc<Self> {
        let (turn_tx, _) = broadcast::channel(64);
        let inbox = ImInbox::new(&cfg.state_dir);
        Arc::new(Self {
            cfg,
            chats,
            whitelist,
            out_tx,
            workers: StdMutex::new(HashMap::new()),
            out_dedup: Arc::new(StdMutex::new(TtlDedup::default())),
            turn_tx,
            inbox,
        })
    }

    /// 启动收件箱兜底扫描（由 main 显式调用；测试里不启动，避免干扰断言）。
    pub fn spawn_inbox_sweeper(self: &Arc<Self>) {
        let me = self.clone();
        tokio::spawn(async move { me.inbox_sweep_loop().await });
    }

    /// 收件箱兜底扫描（每 2s）：
    /// - `.failed`：TUI 三次提交失败 → 立刻认领并 headless 执行；
    /// - `.json`：TUI 心跳不新鲜（掉线）或滞留超硬上限 → 认领并 headless 执行；
    /// - 认领者崩溃留下的 `.tui-running`/`.headless-running` → 归档为 `.orphaned`。
    async fn inbox_sweep_loop(self: Arc<Self>) {
        const SWEEP: Duration = Duration::from_secs(2);
        loop {
            tokio::time::sleep(SWEEP).await;
            let took = self.inbox_sweep_once().await;
            if took > 0 {
                tracing::debug!("收件箱兜底接管 {took} 条");
            }
        }
    }

    /// 扫描一轮，返回本轮接管（headless 执行）的条数。
    pub async fn inbox_sweep_once(&self) -> usize {
        let now = now_ms();
        let fresh = self.inbox.fresh(now);
        let mut took = 0;

        // 1) TUI 提交失败交还的，直接接管
        for path in self.inbox.failed() {
            if self.adopt_inbox_file(&path, "headless-running").await {
                took += 1;
            }
        }

        // 2) 待认领：TUI 掉线 → 立即接管；TUI 在线但滞留超上限 → 也接管
        for path in self.inbox.pending() {
            let stale_tui = fresh.is_none();
            let overdue = self
                .inbox
                .read(&path)
                .map(|m| now.saturating_sub(m.ts) > TUI_BUSY_HARD_MS)
                .unwrap_or(true);
            if !stale_tui && !overdue {
                continue;
            }
            if self.adopt_inbox_file(&path, "headless-running").await {
                tracing::info!(
                    "TUI {}，桥接管收件箱消息 {}",
                    if stale_tui {
                        "不在线"
                    } else {
                        "长忙超时"
                    },
                    path.display()
                );
                took += 1;
            }
        }

        // 3) 孤儿归档
        let orphans = self.inbox.recover_orphans(now);
        if orphans > 0 {
            tracing::warn!("归档 {orphans} 条崩溃遗留的认领文件（不重跑，避免重复执行）");
        }
        took
    }

    /// 认领一条收件箱消息并交给 per-chat worker 执行；认领失败（TUI 抢先）返回 false。
    async fn adopt_inbox_file(&self, path: &Path, tag: &str) -> bool {
        let Some(claimed) = self.inbox.claim(path, tag) else {
            return false; // TUI 已经抢走，桥不重复执行
        };
        let msg = match self.inbox.read(&claimed) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("收件箱文件不可读 {}: {e}", claimed.display());
                self.inbox.complete(&claimed);
                return false;
            }
        };
        let platform = if msg.platform == "feishu" {
            Platform::Feishu
        } else {
            Platform::Wechat
        };
        let queued = self
            .enqueue(
                &msg.chat_key,
                &msg.peer,
                platform,
                WorkerCmd::UserText { text: msg.text },
            )
            .await;
        if !queued {
            let _ = self
                .send_out(
                    &msg.chat_key,
                    &msg.peer,
                    platform,
                    "消息未入队（队列已满或会话未绑定），请稍后再发".to_string(),
                )
                .await;
            self.inbox.complete(&claimed);
            return false;
        }
        self.inbox.complete(&claimed);
        true
    }

    /// 订阅 turn 完成信号（平台通道 reactions 状态机用）。
    pub fn turn_signals(&self) -> broadcast::Receiver<TurnSignal> {
        self.turn_tx.subscribe()
    }

    /// 出站去重指纹共享出口（MirrorPoller 与 router 必须共用同一份）。
    pub fn out_dedup(&self) -> Arc<StdMutex<TtlDedup>> {
        self.out_dedup.clone()
    }

    /// 白名单判定（与 `handle_inbound` 同一事实源）。
    /// 供平台通道在发出任何回执/提示（如 typing）之前自查——
    /// 非白名单发送者必须完全静默，防暴露机器人存在。
    pub async fn is_allowed(&self, platform: Platform, sender: &str) -> bool {
        let wl = self.whitelist.read().await;
        match platform {
            Platform::Wechat => wl.allows_wechat(sender),
            Platform::Feishu => wl.allows_feishu(sender),
        }
    }

    /// 入站消息（平台通道收消息后调用）：白名单 → 控制命令 / 普通文本。
    /// 未命中白名单静默丢弃（只记日志）；未绑定 chat 回执引导。
    pub async fn handle_inbound(
        &self,
        platform: Platform,
        sender: &str,
        text: &str,
    ) -> InboundOutcome {
        let allowed = {
            let wl = self.whitelist.read().await;
            match platform {
                Platform::Wechat => wl.allows_wechat(sender),
                Platform::Feishu => wl.allows_feishu(sender),
            }
        };
        if !allowed {
            tracing::info!(
                target: "whitelist",
                "非白名单 {} sender={} 消息已丢弃（不回执）",
                platform.as_str(),
                sender
            );
            return InboundOutcome::Dropped;
        }
        let chat_key = platform.chat_key(sender);
        let bound = self.chats.read().await.chats.get(&chat_key).cloned();
        let Some(bound) = bound else {
            let _ = self
                .send_out(
                    &chat_key,
                    sender,
                    platform,
                    format!(
                        "本会话尚未绑定 zcode：请在 zcode TUI 里运行 /{} 完成扫码绑定",
                        platform.as_str()
                    ),
                )
                .await;
            return InboundOutcome::Unbound;
        };

        match parse_command(text) {
            Some(Command::Help) => {
                let _ = self
                    .send_out(&chat_key, sender, platform, HELP_TEXT.to_string())
                    .await;
                InboundOutcome::Command
            }
            Some(Command::Status) => {
                // worker 持 handle，状态问 worker（未起 worker 时直接回执绑定信息）
                let queued = self
                    .enqueue(&chat_key, sender, platform, WorkerCmd::Status)
                    .await;
                if !queued {
                    let msg = format!(
                        "chat: {}\nzcode 会话: {}\n工作目录: {}\nworker: 未启动（闲置）",
                        chat_key, bound.sess_id, bound.cwd
                    );
                    let _ = self.send_out(&chat_key, sender, platform, msg).await;
                }
                InboundOutcome::Command
            }
            Some(Command::New) => {
                self.enqueue(
                    &chat_key,
                    sender,
                    platform,
                    WorkerCmd::NewSession { cwd: None },
                )
                .await;
                InboundOutcome::Command
            }
            Some(Command::Cwd(path)) => {
                self.enqueue(
                    &chat_key,
                    sender,
                    platform,
                    WorkerCmd::NewSession { cwd: Some(path) },
                )
                .await;
                InboundOutcome::Command
            }
            Some(Command::Stop) => {
                self.enqueue(&chat_key, sender, platform, WorkerCmd::StopTurn)
                    .await;
                InboundOutcome::Command
            }
            None => {
                // TUI 在线且正看着绑定会话 → 写收件箱，让 TUI 自己跑回合：
                // 消息出现在界面上，回复同时经 DB 镜像回 IM。桥不重复执行。
                let now = now_ms();
                let tui = self.inbox.fresh(now);
                let tui_on_bound_session = tui
                    .as_ref()
                    .and_then(|hb| hb.session_id.as_deref())
                    .map(|sid| sid == bound.sess_id.as_str())
                    .unwrap_or(false);
                if tui_on_bound_session {
                    let msg = InboxMsg {
                        id: uuid::Uuid::new_v4().simple().to_string(),
                        platform: platform.as_str().to_string(),
                        peer: sender.to_string(),
                        chat_key: chat_key.clone(),
                        text: text.to_string(),
                        target_session: bound.sess_id.clone(),
                        ts: now,
                    };
                    match self.inbox.write(&msg) {
                        Ok(path) => {
                            tracing::info!(
                                "IM 消息进 TUI 收件箱 chat={chat_key} sess={} file={}",
                                bound.sess_id,
                                path.display()
                            );
                            return InboundOutcome::QueuedToTui;
                        }
                        Err(e) => {
                            // 写盘失败（磁盘满/权限）退回 headless，保证消息不丢
                            tracing::warn!("写 TUI 收件箱失败，退回 headless: {e}");
                        }
                    }
                }
                let queued = self
                    .enqueue(
                        &chat_key,
                        sender,
                        platform,
                        WorkerCmd::UserText {
                            text: text.to_string(),
                        },
                    )
                    .await;
                if queued {
                    InboundOutcome::Turn
                } else {
                    // 队满（或绑定恰好丢失）：确定性回执，不让用户干等
                    let _ = self
                        .send_out(
                            &chat_key,
                            sender,
                            platform,
                            "消息未入队（队列已满或会话未绑定），请稍后再发".to_string(),
                        )
                        .await;
                    InboundOutcome::Failed
                }
            }
        }
    }

    /// 投递 worker 命令（worker 懒创建）；返回是否已入队。
    async fn enqueue(
        &self,
        chat_key: &str,
        peer: &str,
        platform: Platform,
        cmd: WorkerCmd,
    ) -> bool {
        let mut cmd = cmd;
        // 复用存活 worker；死条目（receiver 已关）清理后重建
        {
            let mut map = self.workers.lock().unwrap();
            if let Some(tx) = map.get(chat_key) {
                match tx.try_send(cmd) {
                    Ok(()) => return true,
                    Err(mpsc::error::TrySendError::Full(_)) => return false,
                    Err(mpsc::error::TrySendError::Closed(back)) => {
                        tracing::debug!("chat {chat_key} worker 已退出，重建");
                        map.remove(chat_key);
                        cmd = back;
                    }
                }
            }
        }
        // 懒创建：取绑定 → spawn worker → 入队
        let binding = self.chats.read().await.chats.get(chat_key).cloned();
        let Some(b) = binding else {
            return false;
        };
        let (tx, rx) = mpsc::channel(8);
        let queued = tx.try_send(cmd).is_ok();
        {
            let mut map = self.workers.lock().unwrap();
            // 并发创建竞态：后到者接管，旧 sender 移除后其 worker 收到 None 自然退出
            map.remove(chat_key);
            map.insert(chat_key.to_string(), tx);
        }
        let worker = ChatWorker {
            chat_key: chat_key.to_string(),
            peer: peer.to_string(),
            platform,
            sess_id: b.sess_id.clone(),
            cwd: PathBuf::from(&b.cwd),
            cfg: self.cfg.clone(),
            out_tx: self.out_tx.clone(),
            out_dedup: Arc::clone(&self.out_dedup),
            chats: Arc::clone(&self.chats),
            turn_tx: self.turn_tx.clone(),
        };
        tokio::spawn(worker.run(rx));
        queued
    }

    /// 出站（带指纹去重；重复返回 false 不发）。
    pub async fn send_out(
        &self,
        chat_key: &str,
        peer: &str,
        platform: Platform,
        text: String,
    ) -> bool {
        if !self
            .out_dedup
            .lock()
            .unwrap()
            .first_seen(&out_fingerprint(chat_key, &text))
        {
            tracing::debug!("出站重复文本去重 chat={chat_key}");
            return false;
        }
        let _ = self
            .out_tx
            .send(OutboundMsg {
                chat_key: chat_key.to_string(),
                peer: peer.to_string(),
                platform,
                text,
            })
            .await;
        true
    }

    pub fn cfg(&self) -> &RouterConfig {
        &self.cfg
    }
}

/// 出站文本指纹（按 chat 隔离 + trim 后哈希）：同一段回复要能分别发到多个通道。
fn out_fingerprint(chat_key: &str, text: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    chat_key.hash(&mut h);
    text.trim().hash(&mut h);
    format!("out:{:016x}", h.finish())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 单 chat 串行 worker：喂 zcode → 等 turn 信号 → （文本由 MirrorPoller 镜像）。
struct ChatWorker {
    chat_key: String,
    peer: String,
    platform: Platform,
    sess_id: String,
    cwd: PathBuf,
    cfg: RouterConfig,
    out_tx: mpsc::Sender<OutboundMsg>,
    out_dedup: Arc<StdMutex<TtlDedup>>,
    chats: Arc<RwLock<ChatsFile>>,
    turn_tx: broadcast::Sender<TurnSignal>,
}

impl ChatWorker {
    async fn run(mut self, mut rx: mpsc::Receiver<WorkerCmd>) {
        let mut handle: Option<ZcodeHandle> = None;
        let mut last_activity = Instant::now();
        loop {
            // 空闲 10 分钟关 handle（省 ~600MB/会话）；期间可被新消息唤醒
            let idle_left = WORKER_IDLE_CLOSE.saturating_sub(last_activity.elapsed());
            let cmd = tokio::select! {
                c = rx.recv() => c,
                _ = tokio::time::sleep(idle_left) => {
                    tracing::info!("chat {} 空闲 {:?}，关闭 zcode handle", self.chat_key, WORKER_IDLE_CLOSE);
                    if let Some(h) = handle.take() {
                        h.close().await;
                    }
                    // 再等一条消息；没有就退出 worker
                    rx.recv().await
                }
            };
            let Some(cmd) = cmd else { break };
            last_activity = Instant::now();
            match cmd {
                WorkerCmd::UserText { text } => {
                    self.handle_user_text(&mut handle, &text).await;
                }
                WorkerCmd::NewSession { cwd } => {
                    if let Some(h) = handle.take() {
                        h.close().await;
                    }
                    self.sess_id.clear();
                    if let Some(c) = cwd {
                        let p = PathBuf::from(&c);
                        self.cwd = if p.is_absolute() {
                            p
                        } else {
                            PathBuf::from(&self.cfg.workdir_default).join(p)
                        };
                    }
                    match self.ensure_handle(&mut handle).await {
                        Ok(sid) => {
                            self.rebind(&sid).await;
                            self.reply(&format!(
                                "已新开 zcode 会话 {sid}（目录 {}）",
                                self.cwd.display()
                            ))
                            .await;
                        }
                        Err(e) => self.reply(&format!("新开会话失败：{e}")).await,
                    }
                }
                WorkerCmd::StopTurn => {
                    if let Some(h) = &handle {
                        match h.stop_turn().await {
                            Ok(()) => self.reply("已打断当前轮次").await,
                            Err(e) => {
                                self.reply(&format!("打断失败：{}", rpc_fail_receipt(&e)))
                                    .await
                            }
                        }
                    } else {
                        self.reply("当前没有进行中的轮次").await;
                    }
                }
                WorkerCmd::Status => {
                    let alive = handle.as_ref().map(|h| h.is_alive()).unwrap_or(false);
                    let msg = format!(
                        "chat: {}\nzcode 会话: {}\n工作目录: {}\napp-server: {}\n\
                         消息会排队串行处理；TUI 与 IM 共享此会话上下文",
                        self.chat_key,
                        if self.sess_id.is_empty() {
                            "(待创建)".to_string()
                        } else {
                            self.sess_id.clone()
                        },
                        self.cwd.display(),
                        if alive { "运行中" } else { "未启动" }
                    );
                    self.reply(&msg).await;
                }
            }
        }
        if let Some(h) = handle.take() {
            h.close().await;
        }
    }

    /// 喂一条用户文本：等会话空闲 → send → 等 turn 信号；末尾广播 TurnSignal。
    async fn handle_user_text(&mut self, handle: &mut Option<ZcodeHandle>, text: &str) {
        // 安全阀：TUI 在用（3 分钟内活跃）→ 排队等空闲（硬上限后照喂）
        self.wait_session_idle(handle).await;

        // turn 起点：空回复兜底判定用（text part 的 time_created ≥ 此刻）
        let turn_start = now_ms();
        let ok = match self.ensure_handle(handle).await {
            Err(e) => {
                self.reply(&format!(
                    "zcode 会话不可用：{e}\n（可用 /new 新开会话，或稍后重试）"
                ))
                .await;
                false
            }
            Ok(_) => match handle.as_ref() {
                None => {
                    // ensure_handle Ok 却无句柄 = 内部不变量破坏；确定性回执，绝不 panic
                    self.reply("zcode 会话句柄异常（内部错误），请重试或 /new 新开会话")
                        .await;
                    false
                }
                Some(h) => {
                    // 先订阅再 send：send 返回到订阅之间的竞态会漏掉 TurnDone
                    // （mock/快速 turn 毫秒级完成），漏掉 = 白等到 turn 超时。
                    let ev = h.subscribe_events();
                    if let Err(e) = h.send(text).await {
                        self.reply(&format!("消息发送到 zcode 失败：{}", rpc_fail_receipt(&e)))
                            .await;
                        false
                    } else {
                        // 等 turn 信号（文本回程走 MirrorPoller，此处只管完成/错误/超时）
                        self.await_turn(h, ev, turn_start).await
                    }
                }
            },
        };
        // 单点广播：飞书 reactions 状态机据此把 ⏳ 换成 ✅/❌
        let _ = self.turn_tx.send(TurnSignal {
            chat_key: self.chat_key.clone(),
            ok,
        });
    }

    /// 等会话空闲：DB 3 分钟无更新，或硬上限到。
    async fn wait_session_idle(&self, handle: &Option<ZcodeHandle>) {
        if handle.is_none() {
            return; // 无 handle = 没有并发 turn
        }
        let deadline = Instant::now() + IDLE_WAIT_MAX;
        loop {
            if !self.db_session_active() {
                return;
            }
            if Instant::now() >= deadline {
                tracing::warn!(
                    "chat {} 会话持续活跃超 {:?}，硬喂（-32010 重试兜底）",
                    self.chat_key,
                    IDLE_WAIT_MAX
                );
                return;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    fn db_session_active(&self) -> bool {
        if self.sess_id.is_empty() {
            return false;
        }
        let Ok(conn) = open_db(&self.cfg.db_path) else {
            return false;
        };
        match session_last_activity_ms(&conn, &self.sess_id) {
            Ok(last) => is_session_active(last, now_ms()),
            Err(_) => false,
        }
    }

    /// 空回复兜底判定：turn 开始后库里是否已落 text part（有=镜像将发文本，不重复回执）。
    fn db_has_new_texts(&self, since_ms: i64) -> bool {
        if self.sess_id.is_empty() {
            return false;
        }
        let Ok(conn) = open_db(&self.cfg.db_path) else {
            return false;
        };
        fetch_new_assistant_texts(&conn, &self.sess_id, since_ms)
            .map(|v| !v.is_empty())
            .unwrap_or(false)
    }

    /// 确保 handle 可用（缺失/死亡 → spawn + resume/create）。
    async fn ensure_handle(&mut self, handle: &mut Option<ZcodeHandle>) -> Result<String, String> {
        let alive = handle.as_ref().map(|h| h.is_alive()).unwrap_or(false);
        if alive {
            return Ok(handle.as_ref().unwrap().session_id.clone());
        }
        // 尸体直接丢（kill_on_drop 兜底）
        let _ = handle.take();
        let cwd = self.cwd.clone();
        let mut h = ZcodeHandle::spawn(&self.cfg.zcode_bin, &cwd)
            .await
            .map_err(|e| format!("app-server 启动失败：{}", rpc_fail_receipt(&e)))?;
        let resume = if self.sess_id.is_empty() {
            None
        } else {
            Some(self.sess_id.as_str())
        };
        match h.open_session(resume, &cwd).await {
            Ok(sid) => {
                self.sess_id = sid.clone();
                // 句柄必须存回 slot：漏存会在作用域末尾被 drop（kill_on_drop
                // 连 app-server 一起回收），调用方 unwrap 取到 None 直接 panic。
                *handle = Some(h);
                Ok(sid)
            }
            Err(e) => {
                h.close().await;
                Err(format!("会话打开失败：{}", rpc_fail_receipt(&e)))
            }
        }
    }

    /// 等 turn 完成信号：TurnDone / TurnError / 超时。文本回程统一走 MirrorPoller。
    /// 返回 true=TurnDone（含空回复兜底）；false=超时/TurnError/事件流关闭。
    /// `ev` 由调用方在 send 之前订阅好（防漏事件），此处只消费。
    async fn await_turn(
        &self,
        h: &ZcodeHandle,
        mut ev: broadcast::Receiver<ZEvent>,
        turn_start_ms: i64,
    ) -> bool {
        let mut buf = String::new();
        let deadline = tokio::time::sleep(Duration::from_secs(self.cfg.turn_timeout_s));
        let mut done_response: Option<Option<String>> = None;
        let mut error: Option<String> = None;
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                _ = &mut deadline => {
                    tracing::warn!(
                        "chat {} turn 超时（{}s），stop_turn",
                        self.chat_key,
                        self.cfg.turn_timeout_s
                    );
                    let _ = h.stop_turn().await;
                    self.reply(&format!(
                        "处理超时（>{}s），已打断。可缩短问题或用 /new 重开会话。",
                        self.cfg.turn_timeout_s
                    )).await;
                    return false;
                }
                item = ev.recv() => {
                    match item {
                        Ok(ZEvent::TurnDone { response, .. }) => {
                            done_response = Some(response);
                            break;
                        }
                        Ok(ZEvent::TurnError { message }) => {
                            error = Some(message);
                            break;
                        }
                        Ok(ZEvent::AssistantText { text, .. }) => buf.push_str(&text),
                        Ok(_) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("事件流滞后 {n} 条，续读");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            error = Some("zcode app-server 事件流关闭".into());
                            break;
                        }
                    }
                }
            }
        }
        if let Some(msg) = error {
            self.reply(&format!("zcode 处理出错：{msg}")).await;
            return false;
        }
        // 文本统一由 MirrorPoller 镜像（防双发）。仅在「response 与缓冲都空、且库里
        // 确实没落 text part」时兜底空回复回执——DB 晚落盘时镜像仍会补文本。
        let response = done_response.flatten().unwrap_or_default();
        if response.trim().is_empty() && buf.trim().is_empty() {
            tokio::time::sleep(EMPTY_REPLY_GRACE).await;
            if !self.db_has_new_texts(turn_start_ms) {
                self.reply("（本轮没有文字回复）").await;
            }
        }
        true
    }

    /// 回程一条（带出站指纹去重）。
    async fn reply(&self, text: &str) {
        if !self
            .out_dedup
            .lock()
            .unwrap()
            .first_seen(&out_fingerprint(&self.chat_key, text))
        {
            return;
        }
        let _ = self
            .out_tx
            .send(OutboundMsg {
                chat_key: self.chat_key.clone(),
                peer: self.peer.clone(),
                platform: self.platform,
                text: text.to_string(),
            })
            .await;
    }

    /// 换绑：把 chat_key 的绑定改到新 sess_id/cwd（原子写 chats.json）。
    async fn rebind(&self, sess_id: &str) {
        let mut chats = self.chats.write().await;
        if let Some(b) = chats.chats.get_mut(&self.chat_key) {
            b.sess_id = sess_id.to_string();
            b.cwd = self.cwd.display().to_string();
            b.bound_at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
        }
        if let Err(e) = chats.save(&self.cfg.chats_path) {
            tracing::warn!("chats.json 换绑落盘失败: {e}");
        }
    }
}

/// MirrorPoller：轮询会话库，把所有绑定会话的新 assistant 文本镜像回 IM。
/// 新会话启动水位 = 当前时间 − 6s（覆盖首拍竞态；重启最多补 6s，不刷整段历史）。
pub struct MirrorPoller {
    cfg: RouterConfig,
    chats: Arc<RwLock<ChatsFile>>,
    out_tx: mpsc::Sender<OutboundMsg>,
    out_dedup: Arc<StdMutex<TtlDedup>>,
    /// TUI 心跳：钩子在线且写过程事件时，DB 文本镜像让位（防双发）。
    inbox: ImInbox,
    /// (sess_id, chat_key) → 水位（part.time_created ms）。按 chat 隔离：多条 chat
    /// 绑同一会话时，各自独立推进水位，否则先遍历到的那条会把新文本吃光。
    watermarks: HashMap<(String, String), i64>,
    /// 已镜像 part id（防同水位重复）
    seen_parts: TtlDedup,
}

impl MirrorPoller {
    pub fn new(
        cfg: RouterConfig,
        chats: Arc<RwLock<ChatsFile>>,
        out_tx: mpsc::Sender<OutboundMsg>,
        out_dedup: Arc<StdMutex<TtlDedup>>,
    ) -> Self {
        let inbox = ImInbox::new(&cfg.state_dir);
        Self {
            cfg,
            chats,
            out_tx,
            out_dedup,
            inbox,
            watermarks: HashMap::new(),
            seen_parts: TtlDedup::default(),
        }
    }

    /// TUI 心跳补绑：`/wechat` 在用户还没发第一条消息时只能写空会话占位绑定；
    /// 心跳报出真实 session_id 后，把同目录的空绑定补上，微信消息才会进这个 TUI。
    async fn rebind_pending_from_heartbeat(&mut self) {
        let Some(hb) = self.inbox.fresh(now_ms()) else {
            return;
        };
        if !hb.events {
            return;
        }
        let Some(sid) = hb
            .session_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        else {
            return;
        };
        let hb_cwd = hb.cwd.as_deref().unwrap_or("").trim_end_matches('/');
        let mut chats = self.chats.write().await;
        let mut changed = false;
        for (key, b) in chats.chats.iter_mut() {
            if b.bind_mode != BIND_MODE_TUI_SYNC || !b.sess_id.is_empty() {
                continue;
            }
            if !hb_cwd.is_empty() && !b.cwd.trim_end_matches('/').eq(hb_cwd) {
                continue;
            }
            b.sess_id = sid.to_string();
            if !hb_cwd.is_empty() {
                b.cwd = hb_cwd.to_string();
            }
            b.bound_at = (now_ms() / 1000) as u64;
            changed = true;
            tracing::info!("TUI 心跳补绑：chat={key} → {sid}");
        }
        if changed {
            if let Err(e) = chats.save(&self.cfg.chats_path) {
                tracing::warn!("补绑后写 chats.json 失败: {e}");
            }
        }
    }

    /// 一轮镜像（可测试入口）：对每个绑定会话抓新文本并出站。返回镜像条数。
    pub async fn poll_round(&mut self) -> usize {
        self.rebind_pending_from_heartbeat().await;
        let snapshot: Vec<(String, String, Platform)> = {
            let chats = self.chats.read().await;
            chats
                .chats
                .iter()
                .map(|(k, b)| {
                    let (platform, peer) = split_chat_key(k);
                    (b.sess_id.clone(), peer, platform)
                })
                .collect()
        };
        let conn = match open_db(&self.cfg.db_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!("MirrorPoller 打不开会话库（{e}），本轮跳过");
                return 0;
            }
        };
        let mut sent = 0usize;
        for (sess_id, peer, platform) in snapshot {
            if sess_id.is_empty() {
                continue;
            }
            // TUI 钩子在线且写过程事件 → 回答由钩子发，DB 镜像必须让位，否则双发。
            // 钩子掉线 6s 后心跳失效，这里自动恢复 DB 兜底。
            if tui_owns_session(&self.inbox, &sess_id, now_ms()) {
                tracing::debug!("会话 {sess_id} 由 TUI 钩子接管，跳过 DB 文本镜像");
                continue;
            }
            let chat_key = platform.chat_key(&peer);
            let wm_key = (sess_id.clone(), chat_key.clone());
            let wm = self
                .watermarks
                .get(&wm_key)
                .copied()
                .unwrap_or_else(|| now_ms() - STARTUP_GRACE_MS);
            let items = match fetch_new_assistant_texts(&conn, &sess_id, wm) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("MirrorPoller 查询会话 {sess_id} 失败: {e}");
                    continue;
                }
            };
            let mut new_wm = wm;
            for it in items {
                if it.time_created > new_wm {
                    new_wm = it.time_created;
                }
                if !self
                    .seen_parts
                    .first_seen(&format!("part:{chat_key}:{}", it.part_id))
                {
                    continue;
                }
                if !self
                    .out_dedup
                    .lock()
                    .unwrap()
                    .first_seen(&out_fingerprint(&chat_key, &it.text))
                {
                    continue;
                }
                let _ = self
                    .out_tx
                    .send(OutboundMsg {
                        chat_key: chat_key.clone(),
                        peer: peer.clone(),
                        platform,
                        text: it.text,
                    })
                    .await;
                sent += 1;
            }
            self.watermarks.insert(wm_key, new_wm);
        }
        sent
    }

    /// 常驻：5s 一轮。
    pub async fn run(mut self) {
        let mut tick = tokio::time::interval(MIRROR_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            self.poll_round().await;
        }
    }
}

/// `wechat:dm:<peer>` / `feishu:dm:<peer>` → (Platform, peer)。
pub fn split_chat_key(key: &str) -> (Platform, String) {
    if let Some(peer) = key.strip_prefix("wechat:dm:") {
        (Platform::Wechat, peer.to_string())
    } else if let Some(peer) = key.strip_prefix("feishu:dm:") {
        (Platform::Feishu, peer.to_string())
    } else {
        (Platform::Wechat, key.to_string())
    }
}

/// 供 main 接线：出站队列（平台通道消费端缓冲）。
pub fn outbound_channel() -> (mpsc::Sender<OutboundMsg>, mpsc::Receiver<OutboundMsg>) {
    mpsc::channel(64)
}

/// 供 main 接线：组装 RouterConfig。
pub fn router_config_from(
    zcode_bin: &str,
    turn_timeout_s: u64,
    workdir_default: &str,
    db_path: PathBuf,
    chats_path: PathBuf,
    state_dir: PathBuf,
) -> RouterConfig {
    RouterConfig {
        zcode_bin: zcode_bin.to_string(),
        turn_timeout_s,
        workdir_default: workdir_default.to_string(),
        db_path,
        chats_path,
        state_dir,
    }
}

/// 打开只读会话库（helper，供 worker/MirrorPoller/doctor 复用）。
pub fn open_db(path: &Path) -> rusqlite::Result<Connection> {
    Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
}

/// RpcFail → 用户可读回执。
pub fn rpc_fail_receipt(e: &RpcFail) -> String {
    match e {
        RpcFail::Busy { retried } => {
            format!("zcode 会话持续繁忙（重试 {retried} 次），请稍后再发或 /stop 后重试")
        }
        RpcFail::Timeout { method, timeout_s } => {
            format!("zcode {method} 超时（{timeout_s}s），请稍后重试")
        }
        RpcFail::Remote { code, message, .. } => format!("zcode 返回错误 {code}: {message}"),
        RpcFail::Dead => "zcode app-server 进程已退出，正在恢复（稍后重发）".into(),
        RpcFail::Io(e) => format!("与 zcode 通信 I/O 错误：{e}"),
        RpcFail::Json(e) => format!("与 zcode 通信协议错误：{e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn parse_command_cases() {
        assert_eq!(parse_command("/new"), Some(Command::New));
        assert_eq!(parse_command("  /stop  "), Some(Command::Stop));
        assert_eq!(parse_command("/status"), Some(Command::Status));
        assert_eq!(parse_command("/help"), Some(Command::Help));
        assert_eq!(
            parse_command("/cwd /tmp/workspace"),
            Some(Command::Cwd("/tmp/workspace".into()))
        );
        // 空参数 / 未知命令 / 普通文本 → None（转普通 prompt）
        assert_eq!(parse_command("/cwd"), None);
        assert_eq!(parse_command("/new now"), None);
        assert_eq!(parse_command("/foo bar"), None);
        assert_eq!(parse_command("你好"), None);
        assert_eq!(parse_command("/"), None);
    }

    #[test]
    fn chat_key_roundtrip() {
        assert_eq!(Platform::Wechat.chat_key("u1"), "wechat:dm:u1");
        assert_eq!(Platform::Feishu.chat_key("ou1"), "feishu:dm:ou1");
        let (p, peer) = split_chat_key("feishu:dm:ou9");
        assert_eq!(p, Platform::Feishu);
        assert_eq!(peer, "ou9");
    }

    #[test]
    fn finalize_reply_footnote() {
        assert_eq!(finalize_reply("你好", None, None), "你好");
        assert_eq!(finalize_reply("  ", None, None), "（本轮没有文字回复）");
        let s = finalize_reply("结果", Some(3), Some(12_300));
        assert!(s.starts_with("结果\n[3 tools · 12.3k]"), "{s}");
        let s2 = finalize_reply("x", Some(0), Some(219_286));
        assert!(s2.ends_with("[219k]"), "{s2}");
    }

    #[test]
    fn humanize_tokens_scale() {
        assert_eq!(humanize_tokens(999), "999");
        assert_eq!(humanize_tokens(12_300), "12.3k");
        assert_eq!(humanize_tokens(219_286), "219k");
    }

    #[test]
    fn active_window_semantics() {
        let now = 1_793_426_000_000;
        assert!(!is_session_active(None, now));
        assert!(is_session_active(Some(now - 20_000), now)); // 20 秒前 → 活跃
        assert!(!is_session_active(Some(now - 60_000), now)); // >30 秒 → 空闲
    }

    /// 建 fixture 会话库（真实 schema 简化版）。
    fn fixture_db() -> (tempfile::TempDir, Connection) {
        let dir = tempdir().unwrap();
        let conn = Connection::open(dir.path().join("db.sqlite")).unwrap();
        conn.execute_batch(
            "CREATE TABLE message (
                id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                time_created INTEGER NOT NULL, time_updated INTEGER NOT NULL,
                data TEXT NOT NULL, sequence INTEGER
            );
            CREATE TABLE part (
                id TEXT PRIMARY KEY, message_id TEXT NOT NULL, session_id TEXT NOT NULL,
                time_created INTEGER NOT NULL, time_updated INTEGER NOT NULL,
                data TEXT NOT NULL, sequence INTEGER
            );",
        )
        .unwrap();
        (dir, conn)
    }

    /// 指定路径建 fixture 库（返回目录 holder 保活 + 连接）。
    fn fixture_db_at(path: &Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(
            "CREATE TABLE message (
                id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                time_created INTEGER NOT NULL, time_updated INTEGER NOT NULL,
                data TEXT NOT NULL, sequence INTEGER
            );
            CREATE TABLE part (
                id TEXT PRIMARY KEY, message_id TEXT NOT NULL, session_id TEXT NOT NULL,
                time_created INTEGER NOT NULL, time_updated INTEGER NOT NULL,
                data TEXT NOT NULL, sequence INTEGER
            );",
        )
        .unwrap();
        conn
    }

    fn insert_message(
        conn: &Connection,
        sid: &str,
        mid: &str,
        role: &str,
        completed: bool,
        t: i64,
    ) {
        let time = if completed {
            format!("{{\"created\":{t},\"completed\":{}}}", t + 500)
        } else {
            format!("{{\"created\":{t}}}")
        };
        let data = format!("{{\"role\":\"{role}\",\"time\":{time}}}");
        conn.execute(
            "INSERT INTO message (id, session_id, time_created, time_updated, data) \
             VALUES (?1,?2,?3,?3,?4)",
            rusqlite::params![mid, sid, t, data],
        )
        .unwrap();
    }

    fn insert_part(
        conn: &Connection,
        sid: &str,
        mid: &str,
        pid: &str,
        ptype: &str,
        text: &str,
        t: i64,
    ) {
        let data = format!(
            "{{\"type\":\"{ptype}\",\"text\":{}}}",
            serde_json::to_string(text).unwrap()
        );
        conn.execute(
            "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) \
             VALUES (?1,?2,?3,?4,?4,?5)",
            rusqlite::params![pid, mid, sid, t, data],
        )
        .unwrap();
    }

    #[test]
    fn fetch_texts_filters_roles_states_types() {
        let (_dir, conn) = fixture_db();
        // m1: assistant 已完成 + text → 命中
        insert_message(&conn, "s1", "m1", "assistant", true, 1000);
        insert_part(&conn, "s1", "m1", "p1", "text", "答案甲", 1100);
        // m2: assistant 未完成 → 不命中
        insert_message(&conn, "s1", "m2", "assistant", false, 2000);
        insert_part(&conn, "s1", "m2", "p2", "text", "半成品", 2100);
        // m3: user → 不命中
        insert_message(&conn, "s1", "m3", "user", true, 3000);
        insert_part(&conn, "s1", "m3", "p3", "text", "用户说", 3100);
        // m4: assistant 已完成但 reasoning part → 不命中
        insert_message(&conn, "s1", "m4", "assistant", true, 4000);
        insert_part(&conn, "s1", "m4", "p4", "reasoning", "内心戏", 4100);
        // m5: assistant 已完成 text 但空文本 → 不命中
        insert_message(&conn, "s1", "m5", "assistant", true, 5000);
        insert_part(&conn, "s1", "m5", "p5", "text", "   ", 5100);
        // m6: 另一会话 → 不命中
        insert_message(&conn, "s2", "m6", "assistant", true, 6000);
        insert_part(&conn, "s2", "m6", "p6", "text", "别人的", 6100);

        let got = fetch_new_assistant_texts(&conn, "s1", 0).unwrap();
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got[0].part_id, "p1");
        assert_eq!(got[0].text, "答案甲");

        // watermark 生效：1100 之后不再有
        let got2 = fetch_new_assistant_texts(&conn, "s1", 1100).unwrap();
        assert!(got2.is_empty(), "{got2:?}");
    }

    #[test]
    fn fetch_texts_orders_ascending() {
        let (_dir, conn) = fixture_db();
        insert_message(&conn, "s1", "m1", "assistant", true, 1000);
        insert_part(&conn, "s1", "m1", "p2", "text", "第二段", 1200);
        insert_part(&conn, "s1", "m1", "p1", "text", "第一段", 1100);
        let got = fetch_new_assistant_texts(&conn, "s1", 0).unwrap();
        let ids: Vec<_> = got.iter().map(|x| x.part_id.as_str()).collect();
        assert_eq!(ids, vec!["p1", "p2"]);
    }

    #[test]
    fn session_last_activity_reads_max() {
        let (_dir, conn) = fixture_db();
        insert_message(&conn, "s1", "m1", "user", false, 1000);
        insert_message(&conn, "s1", "m2", "assistant", true, 5000);
        insert_message(&conn, "s1", "m3", "assistant", true, 3000);
        let t = session_last_activity_ms(&conn, "s1").unwrap();
        assert_eq!(t, Some(5000));
        assert_eq!(session_last_activity_ms(&conn, "none").unwrap(), None);
    }

    #[test]
    fn out_fingerprint_dedups_trimmed() {
        let mut d = TtlDedup::default();
        let k1 = out_fingerprint("wechat:dm:u1", "你好  ");
        let k2 = out_fingerprint("wechat:dm:u1", "  你好");
        assert_eq!(k1, k2);
        assert!(d.first_seen(&k1));
        assert!(!d.first_seen(&k2));
    }

    #[test]
    fn out_fingerprint_is_per_chat() {
        let mut d = TtlDedup::default();
        let wx = out_fingerprint("wechat:dm:u1", "同一段回复");
        let fs = out_fingerprint("feishu:dm:u1", "同一段回复");
        assert_ne!(wx, fs);
        assert!(d.first_seen(&wx));
        assert!(d.first_seen(&fs), "同一文本发往不同通道不应被去重挡掉");
    }

    #[tokio::test]
    async fn mirror_poller_sends_new_texts_only_once() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("db.sqlite");
        {
            let conn = fixture_db_at(&db_path);
            // 10 秒前完成的历史回复：超出 STARTUP_GRACE 宽限，重启不刷历史 → 不镜像
            let t = now_ms() - 10_000;
            insert_message(&conn, "s1", "m1", "assistant", true, t);
            insert_part(&conn, "s1", "m1", "p1", "text", "历史内容", t + 10);
        }
        let chats = Arc::new(RwLock::new(ChatsFile::default()));
        {
            let mut c = chats.write().await;
            c.bind(
                "wechat:dm:u1",
                &crate::binding::SessTarget {
                    sess_id: "s1".into(),
                    cwd: "/tmp".into(),
                    title: None,
                },
            );
        }
        let (tx, mut rx) = outbound_channel();
        let dedup = Arc::new(StdMutex::new(TtlDedup::default()));
        let cfg = RouterConfig {
            zcode_bin: "zcode".into(),
            turn_timeout_s: 60,
            workdir_default: "/tmp".into(),
            db_path: db_path.clone(),
            chats_path: dir.path().join("chats.json"),
            state_dir: dir.path().to_path_buf(),
        };
        let mut poller = MirrorPoller::new(cfg, chats, tx, dedup);

        // 第一轮：水位初始化为 now−6s 宽限，10s 前的历史 part 不镜像
        assert_eq!(poller.poll_round().await, 0);
        assert!(rx.try_recv().is_err());

        // 插入「现在」的新完成文本 → 镜像一条
        let t2 = now_ms() + 5;
        {
            let conn = Connection::open(&db_path).unwrap();
            insert_message(&conn, "s1", "m2", "assistant", true, t2);
            insert_part(&conn, "s1", "m2", "p2", "text", "新回复", t2 + 10);
        }
        assert_eq!(poller.poll_round().await, 1);
        let msg = rx.try_recv().unwrap();
        assert_eq!(msg.text, "新回复");
        assert_eq!(msg.chat_key, "wechat:dm:u1");

        // 再一轮：水位推进 + part-id 去重，无新镜像
        assert_eq!(poller.poll_round().await, 0);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn mirror_poller_sends_same_content_to_each_chat() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("db.sqlite");
        {
            let conn = fixture_db_at(&db_path);
            let t = now_ms() + 5;
            insert_message(&conn, "s1", "m1", "assistant", true, t);
            insert_part(&conn, "s1", "m1", "p1", "text", "重复文案", t + 1);
            insert_message(&conn, "s2", "m2", "assistant", true, t);
            insert_part(&conn, "s2", "m2", "p2", "text", "重复文案", t + 2);
        }
        let chats = Arc::new(RwLock::new(ChatsFile::default()));
        {
            let mut c = chats.write().await;
            c.bind(
                "wechat:dm:u1",
                &crate::binding::SessTarget {
                    sess_id: "s1".into(),
                    cwd: "/tmp".into(),
                    title: None,
                },
            );
            c.bind(
                "feishu:dm:ou1",
                &crate::binding::SessTarget {
                    sess_id: "s2".into(),
                    cwd: "/tmp".into(),
                    title: None,
                },
            );
        }
        let (tx, mut rx) = outbound_channel();
        let cfg = RouterConfig {
            zcode_bin: "zcode".into(),
            turn_timeout_s: 60,
            workdir_default: "/tmp".into(),
            db_path: db_path.clone(),
            chats_path: dir.path().join("chats.json"),
            state_dir: dir.path().to_path_buf(),
        };
        let mut poller =
            MirrorPoller::new(cfg, chats, tx, Arc::new(StdMutex::new(TtlDedup::default())));
        // 指纹按 chat 隔离：同内容发往不同通道各发一条（否则后一条永远收不到回复）
        assert_eq!(poller.poll_round().await, 2);
        let mut got = Vec::new();
        while let Ok(m) = rx.try_recv() {
            got.push(m.chat_key);
        }
        got.sort();
        assert_eq!(
            got,
            vec!["feishu:dm:ou1".to_string(), "wechat:dm:u1".to_string()]
        );
    }

    #[tokio::test]
    async fn mirror_poller_fans_out_to_chats_sharing_one_session() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("db.sqlite");
        {
            let conn = fixture_db_at(&db_path);
            let t = now_ms() + 5;
            insert_message(&conn, "s1", "m1", "assistant", true, t);
            insert_part(&conn, "s1", "m1", "p1", "text", "同会话回复", t + 1);
        }
        let chats = Arc::new(RwLock::new(ChatsFile::default()));
        {
            let mut c = chats.write().await;
            for key in ["wechat:dm:u1", "feishu:dm:ou1"] {
                c.bind(
                    key,
                    &crate::binding::SessTarget {
                        sess_id: "s1".into(),
                        cwd: "/tmp".into(),
                        title: None,
                    },
                );
            }
        }
        let (tx, mut rx) = outbound_channel();
        let cfg = RouterConfig {
            zcode_bin: "zcode".into(),
            turn_timeout_s: 60,
            workdir_default: "/tmp".into(),
            db_path: db_path.clone(),
            chats_path: dir.path().join("chats.json"),
            state_dir: dir.path().to_path_buf(),
        };
        let mut poller =
            MirrorPoller::new(cfg, chats, tx, Arc::new(StdMutex::new(TtlDedup::default())));
        // 两条 chat 绑同一会话：水位与 part-id 均按 chat 隔离 → 两条都要收到
        assert_eq!(poller.poll_round().await, 2);
        let mut got = Vec::new();
        while let Ok(m) = rx.try_recv() {
            got.push(m.chat_key);
        }
        got.sort();
        assert_eq!(
            got,
            vec!["feishu:dm:ou1".to_string(), "wechat:dm:u1".to_string()]
        );
    }

    // —— TUI 收件箱分流 ——

    fn test_router(dir: &Path, sess_id: &str) -> (Arc<SessionRouter>, mpsc::Receiver<OutboundMsg>) {
        let mut cf = ChatsFile::default();
        cf.bind(
            "wechat:dm:u_test",
            &crate::binding::SessTarget {
                sess_id: sess_id.into(),
                cwd: "/tmp".into(),
                title: None,
            },
        );
        let mut wl = Whitelist::default();
        wl.add_runtime_wechat("u_test");
        let (tx, rx) = outbound_channel();
        let cfg = RouterConfig {
            // 不存在的二进制：worker 真跑起来也只会失败，测试不依赖它
            zcode_bin: "/nonexistent/zcode".into(),
            turn_timeout_s: 5,
            workdir_default: "/tmp".into(),
            db_path: dir.join("db.sqlite"),
            chats_path: dir.join("chats.json"),
            state_dir: dir.to_path_buf(),
        };
        let router = SessionRouter::new(
            cfg,
            Arc::new(RwLock::new(cf)),
            Arc::new(RwLock::new(wl)),
            tx,
        );
        (router, rx)
    }

    fn write_heartbeat(dir: &Path, sess_id: &str, ts: i64) {
        std::fs::write(
            ImInbox::new(dir).heartbeat_path(),
            format!(r#"{{"pid":1,"session_id":"{sess_id}","busy":false,"ts":{ts}}}"#),
        )
        .unwrap();
    }

    fn sample_msg(ts: i64) -> InboxMsg {
        InboxMsg {
            id: "msg1".into(),
            platform: "wechat".into(),
            peer: "u_test".into(),
            chat_key: "wechat:dm:u_test".into(),
            text: "写首诗".into(),
            target_session: "sess_tui".into(),
            ts,
        }
    }

    #[tokio::test]
    async fn inbound_queues_to_tui_when_tui_on_bound_session() {
        let dir = tempdir().unwrap();
        let (router, mut rx) = test_router(dir.path(), "sess_tui");
        write_heartbeat(dir.path(), "sess_tui", now_ms());

        let out = router
            .handle_inbound(Platform::Wechat, "u_test", "写首诗")
            .await;
        assert_eq!(out, InboundOutcome::QueuedToTui);

        let inbox = ImInbox::new(dir.path());
        let pending = inbox.pending();
        assert_eq!(pending.len(), 1);
        let msg = inbox.read(&pending[0]).unwrap();
        assert_eq!(msg.text, "写首诗");
        assert_eq!(msg.target_session, "sess_tui");
        assert!(rx.try_recv().is_err(), "TUI 分支不应有任何出站消息");
    }

    #[tokio::test]
    async fn inbound_falls_back_to_headless_when_tui_offline() {
        let dir = tempdir().unwrap();
        let (router, _rx) = test_router(dir.path(), "sess_tui");
        // 没有心跳文件 = TUI 不在线
        let out = router
            .handle_inbound(Platform::Wechat, "u_test", "写首诗")
            .await;
        assert_eq!(out, InboundOutcome::Turn);
        assert!(ImInbox::new(dir.path()).pending().is_empty());
    }

    #[tokio::test]
    async fn inbound_falls_back_when_tui_on_other_session() {
        let dir = tempdir().unwrap();
        let (router, _rx) = test_router(dir.path(), "sess_tui");
        write_heartbeat(dir.path(), "sess_other", now_ms());

        let out = router
            .handle_inbound(Platform::Wechat, "u_test", "写首诗")
            .await;
        assert_eq!(
            out,
            InboundOutcome::Turn,
            "TUI 在看别的会话，不能把消息塞进去"
        );
        assert!(ImInbox::new(dir.path()).pending().is_empty());
    }

    #[tokio::test]
    async fn sweep_takes_over_pending_when_tui_offline() {
        let dir = tempdir().unwrap();
        let (router, _rx) = test_router(dir.path(), "sess_tui");
        let inbox = ImInbox::new(dir.path());
        inbox.write(&sample_msg(now_ms() - 1_000)).unwrap();

        assert_eq!(router.inbox_sweep_once().await, 1);
        assert!(inbox.pending().is_empty(), "认领后不应留在待认领队列");
    }

    #[tokio::test]
    async fn sweep_leaves_pending_alone_while_tui_fresh() {
        let dir = tempdir().unwrap();
        let (router, _rx) = test_router(dir.path(), "sess_tui");
        let inbox = ImInbox::new(dir.path());
        inbox.write(&sample_msg(now_ms())).unwrap();
        write_heartbeat(dir.path(), "sess_tui", now_ms());

        assert_eq!(router.inbox_sweep_once().await, 0, "TUI 在线时不抢");
        assert_eq!(inbox.pending().len(), 1);
    }

    #[tokio::test]
    async fn sweep_takes_failed_files_immediately() {
        let dir = tempdir().unwrap();
        let (router, _rx) = test_router(dir.path(), "sess_tui");
        let inbox = ImInbox::new(dir.path());
        let p = inbox.write(&sample_msg(now_ms())).unwrap();
        let claimed = inbox.claim(&p, "tui-running").unwrap();
        std::fs::rename(&claimed, claimed.with_extension("failed")).unwrap();
        // 心跳仍新鲜：TUI 提交失败交还的消息也必须立刻接管
        write_heartbeat(dir.path(), "sess_tui", now_ms());

        assert_eq!(router.inbox_sweep_once().await, 1);
        assert!(inbox.failed().is_empty());
    }

    #[tokio::test]
    async fn sweep_respects_tui_claim() {
        let dir = tempdir().unwrap();
        let (router, _rx) = test_router(dir.path(), "sess_tui");
        let inbox = ImInbox::new(dir.path());
        let p = inbox.write(&sample_msg(now_ms())).unwrap();
        let claimed = inbox.claim(&p, "tui-running").expect("TUI 先认领");

        assert_eq!(
            router.inbox_sweep_once().await,
            0,
            "已被 TUI 认领，桥不重复执行"
        );
        assert!(claimed.exists());
    }

    #[tokio::test]
    async fn mirror_poller_rebinds_empty_binding_from_tui_heartbeat() {
        let dir = tempdir().unwrap();
        let chats = Arc::new(RwLock::new(ChatsFile::default()));
        {
            let mut c = chats.write().await;
            c.bind(
                "wechat:dm:u1",
                &crate::binding::SessTarget {
                    sess_id: String::new(), // /wechat 在首条消息前写的占位绑定
                    cwd: "/work".into(),
                    title: None,
                },
            );
        }
        std::fs::write(
            ImInbox::new(dir.path()).heartbeat_path(),
            format!(
                r#"{{"pid":1,"session_id":"sess_tui","busy":false,"events":true,"cwd":"/work","ts":{}}}"#,
                now_ms()
            ),
        )
        .unwrap();
        let (tx, _rx) = outbound_channel();
        let cfg = RouterConfig {
            zcode_bin: "zcode".into(),
            turn_timeout_s: 60,
            workdir_default: "/work".into(),
            db_path: dir.path().join("db.sqlite"), // 不存在：poll_round 会在补绑后提前返回
            chats_path: dir.path().join("chats.json"),
            state_dir: dir.path().to_path_buf(),
        };
        let mut poller = MirrorPoller::new(
            cfg,
            chats.clone(),
            tx,
            Arc::new(StdMutex::new(TtlDedup::default())),
        );
        assert_eq!(poller.poll_round().await, 0);

        let c = chats.read().await;
        assert_eq!(c.chats["wechat:dm:u1"].sess_id, "sess_tui");
        assert_eq!(c.chats["wechat:dm:u1"].cwd, "/work");
    }
}
