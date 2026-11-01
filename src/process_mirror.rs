//! 过程镜像：TUI 钩子写入的**块级事件** → IM 过程消息。
//!
//! 为什么不是每 token 流式发：微信/飞书都是"一条消息发出去就不能改"的通道，
//! 每 token 一条会把手机刷爆，还会触发限流。这里按 **块**（思考块、工具调用、
//! 回答块）出站，既有"过程可见"，又把消息量压到每轮个位数~几十条。
//!
//! 数据流（旁路，绝不进 TUI 事件循环）：
//!
//! ```text
//! TUI 钩子 onEvent（块结束时一次 append）
//!   → state/im-events/<sess>.ndjson（有界队列 + 异步写 + 8MB 轮转）
//!   → EventTailer（300ms 增量读，记 offset，重启续读）
//!   → ProcDispatcher（每 chat 1.2s 一条，队列上限 200，低优先级先丢）
//!   → 现有 OutboundMsg → 微信/飞书发送器（自带分段/限流/重试）
//! ```
//!
//! 事件协议（v1，一行一个 JSON）：
//! `{"v":1,"seq":12,"ts":...,"sid":"sess_x","t":"tool_end","name":"Bash","ok":true,...}`

use std::collections::{HashMap, VecDeque};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{mpsc, RwLock};

use crate::binding::ChatsFile;
use crate::im_inbox::ImInbox;
use crate::session_router::{split_chat_key, OutboundMsg, Platform};
use crate::wechat_ilink::poll::TtlDedup;

/// 事件文件轮询间隔。
pub const TAIL_INTERVAL: Duration = Duration::from_millis(300);
/// 单次最多读这么多字节，避免大文件把桥的内存顶起来；剩下的下一拍继续。
pub const MAX_READ_BYTES: usize = 256 * 1024;
/// 偏移落盘间隔。
pub const OFFSET_SAVE_INTERVAL: Duration = Duration::from_secs(5);
/// 事件文件保留时长（轮转后的旧文件不参与读取，只是占盘；超过就删）。
pub const EVENT_FILE_TTL: Duration = Duration::from_secs(7 * 24 * 3600);

/// 单条过程消息的最大字符数（超出截断并标注；微信发送器还会按 1800 字再分段）。
pub const MAX_BLOCK_CHARS: usize = 6000;
/// 工具入参最大字符数。
pub const MAX_TOOL_INPUT_CHARS: usize = 800;
/// 工具输出最大字符数。
pub const MAX_TOOL_OUTPUT_CHARS: usize = 3000;
/// 每 chat 出站最小间隔（微信分段间隔 800ms 之上再留余量）。
pub const PROC_MIN_GAP: Duration = Duration::from_millis(1200);
/// 每 chat 排队上限；超了先丢低优先级。
pub const PROC_MAX_QUEUE: usize = 200;

/// 一条块级事件。字段全部 `default`，上游加字段不会把桥弄挂。
#[derive(Debug, Clone, Deserialize)]
pub struct ProcEvent {
    #[serde(default)]
    pub v: u32,
    #[serde(default)]
    pub seq: u64,
    #[serde(default)]
    pub ts: i64,
    #[serde(default)]
    pub sid: String,
    #[serde(rename = "t")]
    pub t: String,
    #[serde(default)]
    pub src: String,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub input: Value,
    #[serde(default)]
    pub output: Value,
    #[serde(default)]
    pub ok: bool,
    #[serde(default)]
    pub ms: u64,
    #[serde(default)]
    pub tokens: Option<u64>,
    #[serde(default)]
    pub tools: Option<u64>,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub reason: String,
}

/// 过程消息优先级：队列满时先丢 Low。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Low,
    Normal,
    High,
}

/// 待出站的一条过程消息。
#[derive(Debug, Clone)]
pub struct ProcMsg {
    pub chat_key: String,
    pub peer: String,
    pub platform: Platform,
    pub text: String,
    pub priority: Priority,
}

/// 把一条事件格式化成 IM 文案；返回 None = 这条事件不回程（如 IM 自发消息回声）。
pub fn format_event(ev: &ProcEvent) -> Option<(String, Priority)> {
    match ev.t.as_str() {
        "user" => {
            if ev.src == "im" {
                return None; // 微信自己发的消息，通道层已回执，不再回声
            }
            Some((
                format!(
                    "👤 本机输入：\n{}",
                    truncate_chars(&ev.text, MAX_BLOCK_CHARS)
                ),
                Priority::High,
            ))
        }
        "turn_start" => Some(("⏳ 开始处理…".into(), Priority::High)),
        "status" => Some((format!("ℹ️ {}", ev.text.trim()), Priority::Low)),
        "reasoning_start" => Some(("💭 思考中…".into(), Priority::Low)),
        "reasoning" => Some((
            format!("💭 思考：\n{}", truncate_chars(&ev.text, MAX_BLOCK_CHARS)),
            Priority::Normal,
        )),
        "tool_start" => Some((
            format!(
                "🔧 {}：{}",
                if ev.name.is_empty() {
                    "工具"
                } else {
                    &ev.name
                },
                summarize_input(&ev.name, &ev.input)
            ),
            Priority::Low,
        )),
        "tool_end" => {
            let mark = if ev.ok { "✅" } else { "❌" };
            let dur = if ev.ms > 0 {
                format!("（{:.1}s）", ev.ms as f64 / 1000.0)
            } else {
                String::new()
            };
            let body = truncate_chars(&output_text(&ev.output), MAX_TOOL_OUTPUT_CHARS);
            let name = if ev.name.is_empty() {
                "工具"
            } else {
                &ev.name
            };
            Some((format!("{mark} {name}{dur}：\n{body}"), Priority::High))
        }
        "text_start" => Some(("✍️ 正在输出回答…".into(), Priority::Low)),
        "text" => Some((
            format!("🤖 回答：\n{}", truncate_chars(&ev.text, MAX_BLOCK_CHARS)),
            Priority::High,
        )),
        "turn_end" => {
            let mut notes: Vec<String> = Vec::new();
            if let Some(n) = ev.tools {
                if n > 0 {
                    notes.push(format!("{n} 个工具"));
                }
            }
            if let Some(t) = ev.tokens {
                if t > 0 {
                    notes.push(format!("{} tokens", humanize_tokens(t)));
                }
            }
            let suffix = if notes.is_empty() {
                String::new()
            } else {
                format!(" · {}", notes.join(" · "))
            };
            Some((format!("✅ 本轮完成{suffix}"), Priority::High))
        }
        "turn_error" => Some((
            format!("❌ 本轮出错：{}", ev.message.trim()),
            Priority::High,
        )),
        "notice" => Some((format!("ℹ️ {}", ev.text.trim()), Priority::Low)),
        "dropped" => Some((
            format!("⚠️ 过程事件积压，已丢弃 {} 条低优先级消息", ev.text),
            Priority::Low,
        )),
        _ => None,
    }
}

/// 工具入参摘要：Bash 看命令，文件类看路径，其余紧凑 JSON。
fn summarize_input(name: &str, input: &Value) -> String {
    if let Some(s) = input.as_str() {
        return truncate_chars(s, MAX_TOOL_INPUT_CHARS);
    }
    if let Some(obj) = input.as_object() {
        let keys: &[&str] = if name == "Bash" {
            &["command", "description"]
        } else {
            &[
                "file_path",
                "path",
                "pattern",
                "query",
                "url",
                "description",
            ]
        };
        for key in keys {
            if let Some(v) = obj.get(*key).and_then(|x| x.as_str()) {
                if !v.trim().is_empty() {
                    return truncate_chars(v, MAX_TOOL_INPUT_CHARS);
                }
            }
        }
    }
    let raw = serde_json::to_string(input).unwrap_or_default();
    truncate_chars(&raw, MAX_TOOL_INPUT_CHARS)
}

/// 工具结果取人读文本：字符串 / content / output / text，最后退整块 JSON。
fn output_text(output: &Value) -> String {
    if let Some(s) = output.as_str() {
        return s.to_string();
    }
    if let Some(obj) = output.as_object() {
        for key in ["content", "output", "text", "stdout", "stderr"] {
            if let Some(s) = obj.get(key).and_then(|x| x.as_str()) {
                if !s.is_empty() {
                    return s.to_string();
                }
            }
        }
    }
    serde_json::to_string(output).unwrap_or_default()
}

/// 按**字符**截断（不是字节，避免把 UTF-8 切断），并标注原长度。
pub fn truncate_chars(s: &str, max: usize) -> String {
    let count = s.chars().count();
    if count <= max {
        return s.to_string();
    }
    let head: String = s.chars().take(max).collect();
    format!("{head}\n…（截断，共 {count} 字符；完整内容见 TUI）")
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

/// 增量 tail 事件目录。只读文件、只记 offset，不碰 TUI 进程。
pub struct EventTailer {
    events_dir: PathBuf,
    offsets_path: PathBuf,
    chats: Arc<RwLock<ChatsFile>>,
    tx: mpsc::Sender<ProcMsg>,
    /// 文件名 → 已读字节数
    offsets: HashMap<String, u64>,
    seen: TtlDedup,
    last_save: Instant,
    initialized: bool,
}

impl EventTailer {
    pub fn new(state_dir: &Path, chats: Arc<RwLock<ChatsFile>>, tx: mpsc::Sender<ProcMsg>) -> Self {
        Self {
            events_dir: state_dir.join("im-events"),
            offsets_path: state_dir.join("im-events").join("offsets.json"),
            chats,
            tx,
            offsets: HashMap::new(),
            seen: TtlDedup::default(),
            last_save: Instant::now(),
            initialized: false,
        }
    }

    /// 首次启动：有 offsets 就续读；没有就把现存文件读到末尾（不重放历史）。
    fn init_once(&mut self) {
        if self.initialized {
            return;
        }
        self.initialized = true;
        if let Ok(raw) = std::fs::read_to_string(&self.offsets_path) {
            if let Ok(map) = serde_json::from_str::<HashMap<String, u64>>(&raw) {
                self.offsets = map;
                tracing::info!("过程镜像续读 {} 个事件文件", self.offsets.len());
                return;
            }
        }
        if let Ok(rd) = std::fs::read_dir(&self.events_dir) {
            for e in rd.flatten() {
                let p = e.path();
                if p.extension().and_then(|s| s.to_str()) != Some("ndjson") {
                    continue;
                }
                if let Ok(md) = e.metadata() {
                    if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                        self.offsets.insert(name.to_string(), md.len());
                    }
                }
            }
        }
        tracing::info!("过程镜像首次启动：从现有文件末尾开始（不重放历史）");
    }

    /// 一轮：扫目录、增量读、格式化、投递。返回投递条数（测试用）。
    pub async fn poll_round(&mut self) -> usize {
        self.init_once();
        let bindings: HashMap<String, Vec<(String, String, Platform)>> = {
            let chats = self.chats.read().await;
            let mut map: HashMap<String, Vec<(String, String, Platform)>> = HashMap::new();
            for (chat_key, b) in chats.chats.iter() {
                if b.sess_id.is_empty() {
                    continue;
                }
                let (platform, peer) = split_chat_key(chat_key);
                map.entry(b.sess_id.clone())
                    .or_default()
                    .push((chat_key.clone(), peer, platform));
            }
            map
        };
        if bindings.is_empty() {
            return 0;
        }

        let mut names: Vec<String> = match std::fs::read_dir(&self.events_dir) {
            Ok(rd) => rd
                .flatten()
                .filter_map(|e| {
                    let p = e.path();
                    if p.extension().and_then(|s| s.to_str()) == Some("ndjson") {
                        p.file_name()
                            .and_then(|s| s.to_str())
                            .map(|s| s.to_string())
                    } else {
                        None
                    }
                })
                .collect(),
            Err(_) => return 0,
        };
        names.sort();

        let mut sent = 0usize;
        for name in names {
            let sid = name.trim_end_matches(".ndjson").to_string();
            let Some(targets) = bindings.get(&sid) else {
                continue;
            };
            let path = self.events_dir.join(&name);
            let Ok(mut file) = std::fs::File::open(&path) else {
                continue;
            };
            let Ok(md) = file.metadata() else { continue };
            let len = md.len();
            let mut offset = *self.offsets.get(&name).unwrap_or(&0);
            if len < offset {
                // 轮转/截断：从头读新文件
                offset = 0;
            }
            if len == offset {
                continue;
            }
            if file.seek(SeekFrom::Start(offset)).is_err() {
                continue;
            }
            let want = (len - offset).min(MAX_READ_BYTES as u64) as usize;
            let mut buf = vec![0u8; want];
            let n = match file.read(&mut buf) {
                Ok(n) => n,
                Err(_) => continue,
            };
            if n == 0 {
                continue;
            }
            buf.truncate(n);
            let Ok(text) = String::from_utf8(buf) else {
                // 理论上钩子只写完整 UTF-8 行；坏块直接跳过，下一拍重试会重复读到坏块，
                // 因此把 offset 推到末尾避免卡死。
                self.offsets.insert(name.clone(), len);
                continue;
            };
            let Some(last_nl) = text.rfind('\n') else {
                // 半行：等下一拍
                continue;
            };
            let complete = &text[..=last_nl];
            let consumed = complete.len() as u64;
            self.offsets.insert(name.clone(), offset + consumed);

            for line in complete.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let Ok(ev) = serde_json::from_str::<ProcEvent>(line) else {
                    tracing::debug!(
                        "过程事件 JSON 解析失败，跳过: {}",
                        &line[..line.len().min(120)]
                    );
                    continue;
                };
                if !ev.sid.is_empty() && ev.sid != sid {
                    continue;
                }
                let key = format!("{}:{}:{}", sid, ev.seq, ev.ts);
                if !self.seen.first_seen(&key) {
                    continue;
                }
                let Some((msg, priority)) = format_event(&ev) else {
                    continue;
                };
                for (chat_key, peer, platform) in targets {
                    let _ = self
                        .tx
                        .send(ProcMsg {
                            chat_key: chat_key.clone(),
                            peer: peer.clone(),
                            platform: *platform,
                            text: msg.clone(),
                            priority,
                        })
                        .await;
                    sent += 1;
                }
            }
        }

        if self.last_save.elapsed() >= OFFSET_SAVE_INTERVAL {
            self.save_offsets();
            self.last_save = Instant::now();
        }
        self.cleanup_old_files();
        sent
    }

    /// 常驻：300ms 一拍。
    pub async fn run(mut self) {
        let mut tick = tokio::time::interval(TAIL_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            self.poll_round().await;
        }
    }

    fn save_offsets(&self) {
        if let Some(parent) = self.offsets_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let Ok(json) = serde_json::to_string(&self.offsets) else {
            return;
        };
        let tmp = self.offsets_path.with_extension("json.tmp");
        if std::fs::write(&tmp, json.as_bytes()).is_ok() {
            let _ = std::fs::rename(&tmp, &self.offsets_path);
        }
    }

    /// 删 7 天前的旧事件文件（只删已轮转/停写的；活跃文件 mtime 很新）。
    fn cleanup_old_files(&self) {
        let Ok(rd) = std::fs::read_dir(&self.events_dir) else {
            return;
        };
        let now = SystemTime::now();
        for e in rd.flatten() {
            let p = e.path();
            if p == self.offsets_path {
                continue;
            }
            if p.extension().and_then(|s| s.to_str()) != Some("ndjson") {
                continue;
            }
            if let Ok(md) = e.metadata() {
                if let Ok(modified) = md.modified() {
                    if now.duration_since(modified).unwrap_or_default() > EVENT_FILE_TTL {
                        let _ = std::fs::remove_file(&p);
                    }
                }
            }
        }
    }
}

/// 过程消息限速器：每 chat 1.2s 一条，队列有界，低优先级先丢。
pub struct ProcDispatcher {
    rx: mpsc::Receiver<ProcMsg>,
    out_tx: mpsc::Sender<OutboundMsg>,
    queues: HashMap<String, VecDeque<ProcMsg>>,
    last_sent: HashMap<String, Instant>,
}

impl ProcDispatcher {
    pub fn new(rx: mpsc::Receiver<ProcMsg>, out_tx: mpsc::Sender<OutboundMsg>) -> Self {
        Self {
            rx,
            out_tx,
            queues: HashMap::new(),
            last_sent: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        let mut tick = tokio::time::interval(Duration::from_millis(200));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                maybe = self.rx.recv() => {
                    match maybe {
                        Some(msg) => self.enqueue(msg),
                        None => break,
                    }
                }
                _ = tick.tick() => {
                    self.drain().await;
                }
            }
        }
    }

    fn enqueue(&mut self, msg: ProcMsg) {
        let q = self.queues.entry(msg.chat_key.clone()).or_default();
        if q.len() >= PROC_MAX_QUEUE {
            // 队列满：Low 直接丢；否则挤掉一条最老的 Low，没有就丢最老的
            if msg.priority == Priority::Low {
                return;
            }
            if let Some(pos) = q.iter().position(|m| m.priority == Priority::Low) {
                q.remove(pos);
            } else {
                q.pop_front();
            }
        }
        q.push_back(msg);
    }

    async fn drain(&mut self) {
        let now = Instant::now();
        let keys: Vec<String> = self.queues.keys().cloned().collect();
        for key in keys {
            let ready = self
                .last_sent
                .get(&key)
                .map(|t| now.duration_since(*t) >= PROC_MIN_GAP)
                .unwrap_or(true);
            if !ready {
                continue;
            }
            let msg = {
                let Some(q) = self.queues.get_mut(&key) else {
                    continue;
                };
                q.pop_front()
            };
            let Some(msg) = msg else {
                self.queues.remove(&key);
                continue;
            };
            let _ = self
                .out_tx
                .send(OutboundMsg {
                    chat_key: msg.chat_key,
                    peer: msg.peer,
                    platform: msg.platform,
                    text: msg.text,
                })
                .await;
            self.last_sent.insert(key, Instant::now());
        }
    }
}

/// 判断某会话当前是否由 TUI 钩子接管（心跳新鲜 + 同会话 + events=true）。
/// MirrorPoller 用它决定是否跳过 DB 文本镜像，避免"钩子发一份、DB 又发一份"。
pub fn tui_owns_session(inbox: &ImInbox, sess_id: &str, now_ms: i64) -> bool {
    inbox
        .fresh(now_ms)
        .map(|hb| hb.events && hb.session_id.as_deref() == Some(sess_id))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::{ChatBinding, ChatsFile};
    use serde_json::json;
    use tempfile::tempdir;

    fn ev(v: Value) -> ProcEvent {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn user_im_echo_is_suppressed() {
        let e = ev(json!({"t":"user","src":"im","text":"hi"}));
        assert!(format_event(&e).is_none());
        let e = ev(json!({"t":"user","src":"tui","text":"hi"}));
        let (s, p) = format_event(&e).unwrap();
        assert!(s.contains("本机输入"));
        assert_eq!(p, Priority::High);
    }

    #[test]
    fn tool_end_formats_success_and_duration() {
        let e = ev(json!({
            "t":"tool_end","name":"Bash","ok":true,"ms":1234,
            "output":{"content":"done"}
        }));
        let (s, _) = format_event(&e).unwrap();
        assert!(s.starts_with("✅ Bash（1.2s）："));
        assert!(s.contains("done"));
    }

    #[test]
    fn tool_start_uses_command_for_bash() {
        let e = ev(json!({"t":"tool_start","name":"Bash","input":{"command":"ls -la"}}));
        let (s, _) = format_event(&e).unwrap();
        assert!(s.contains("ls -la"));
    }

    #[test]
    fn long_text_is_truncated_by_chars() {
        let text = "汉".repeat(MAX_BLOCK_CHARS + 10);
        let out = truncate_chars(&text, MAX_BLOCK_CHARS);
        assert!(out.contains("截断"));
        assert_eq!(out.chars().filter(|c| *c == '汉').count(), MAX_BLOCK_CHARS);
    }

    #[test]
    fn turn_end_footer() {
        let e = ev(json!({"t":"turn_end","tools":3,"tokens":12345}));
        let (s, _) = format_event(&e).unwrap();
        assert!(s.contains("3 个工具"));
        assert!(s.contains("12.3k"));
    }

    #[tokio::test]
    async fn tailer_reads_new_lines_and_skips_history() {
        let dir = tempdir().unwrap();
        let state = dir.path().to_path_buf();
        std::fs::create_dir_all(state.join("im-events")).unwrap();
        let sess = "sess_test";
        let file = state.join("im-events").join(format!("{sess}.ndjson"));
        std::fs::write(&file, "{\"t\":\"text\",\"text\":\"old\"}\n").unwrap();

        let mut chats = ChatsFile::default();
        chats.chats.insert(
            "wechat:dm:u1".into(),
            ChatBinding {
                sess_id: sess.into(),
                cwd: "/tmp".into(),
                bind_mode: "tui_sync".into(),
                bound_at: 0,
                title: None,
            },
        );
        let chats = Arc::new(RwLock::new(chats));
        let (tx, mut rx) = mpsc::channel(16);
        let mut tailer = EventTailer::new(&state, chats, tx);
        // 首拍只做初始化，不重放历史
        assert_eq!(tailer.poll_round().await, 0);

        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&file)
            .unwrap();
        writeln!(
            f,
            "{}",
            json!({"v":1,"seq":1,"ts":1,"sid":sess,"t":"text","text":"new"})
        )
        .unwrap();
        let n = tailer.poll_round().await;
        assert_eq!(n, 1);
        let msg = rx.try_recv().unwrap();
        assert!(msg.text.contains("new"));
        assert_eq!(msg.chat_key, "wechat:dm:u1");
    }

    #[tokio::test]
    async fn tailer_handles_partial_line() {
        let dir = tempdir().unwrap();
        let state = dir.path().to_path_buf();
        std::fs::create_dir_all(state.join("im-events")).unwrap();
        let sess = "sess_partial";
        let file = state.join("im-events").join(format!("{sess}.ndjson"));
        std::fs::write(&file, "").unwrap();
        let mut chats = ChatsFile::default();
        chats.chats.insert(
            "wechat:dm:u1".into(),
            ChatBinding {
                sess_id: sess.into(),
                cwd: "/tmp".into(),
                bind_mode: "tui_sync".into(),
                bound_at: 0,
                title: None,
            },
        );
        let chats = Arc::new(RwLock::new(chats));
        let (tx, mut rx) = mpsc::channel(16);
        let mut tailer = EventTailer::new(&state, chats, tx);
        tailer.poll_round().await;

        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&file)
            .unwrap();
        write!(f, "{{\"t\":\"text\",\"text\":\"half").unwrap();
        assert_eq!(tailer.poll_round().await, 0);
        writeln!(f, "\"}}").unwrap();
        assert_eq!(tailer.poll_round().await, 1);
        assert!(rx.try_recv().unwrap().text.contains("half"));
    }

    fn pmsg(priority: Priority, text: &str) -> ProcMsg {
        ProcMsg {
            chat_key: "wechat:dm:u1".into(),
            peer: "u1".into(),
            platform: Platform::Wechat,
            text: text.into(),
            priority,
        }
    }

    #[tokio::test]
    async fn dispatcher_queue_is_bounded_and_drops_low_first() {
        let (_tx, rx) = mpsc::channel(1);
        let (out_tx, _out_rx) = mpsc::channel(1);
        let mut d = ProcDispatcher::new(rx, out_tx);
        for i in 0..PROC_MAX_QUEUE {
            d.enqueue(pmsg(Priority::Low, &format!("low-{i}")));
        }
        let q = &d.queues["wechat:dm:u1"];
        assert_eq!(q.len(), PROC_MAX_QUEUE);
        assert_eq!(q.front().unwrap().text, "low-0");

        // 队列满时再来 Low：直接丢，不挤占已有消息
        d.enqueue(pmsg(Priority::Low, "low-dropped"));
        assert_eq!(d.queues["wechat:dm:u1"].len(), PROC_MAX_QUEUE);
        assert!(!d.queues["wechat:dm:u1"]
            .iter()
            .any(|m| m.text == "low-dropped"));

        // High 进来：挤掉最老的 Low
        d.enqueue(pmsg(Priority::High, "high-kept"));
        let q = &d.queues["wechat:dm:u1"];
        assert_eq!(q.len(), PROC_MAX_QUEUE);
        assert!(q.iter().any(|m| m.text == "high-kept"));
        assert!(!q.iter().any(|m| m.text == "low-0"));
    }

    #[tokio::test]
    async fn dispatcher_paces_one_message_per_gap() {
        let (_tx, rx) = mpsc::channel(1);
        let (out_tx, mut out_rx) = mpsc::channel(4);
        let mut d = ProcDispatcher::new(rx, out_tx);
        d.enqueue(pmsg(Priority::High, "first"));
        d.enqueue(pmsg(Priority::High, "second"));
        // 刚发过：不到 1.2s，drain 不发
        d.last_sent.insert("wechat:dm:u1".into(), Instant::now());
        d.drain().await;
        assert!(out_rx.try_recv().is_err());

        // 超过间隔：只发一条，第二条等下一拍
        d.last_sent.insert(
            "wechat:dm:u1".into(),
            Instant::now() - PROC_MIN_GAP - Duration::from_millis(10),
        );
        d.drain().await;
        assert_eq!(out_rx.try_recv().unwrap().text, "first");
        assert!(out_rx.try_recv().is_err());
    }
}
