//! IM 收件箱 + TUI 心跳。
//!
//! 目标：TUI 在线时，IM 消息由 **TUI 进程自己**跑回合（消息出现在界面上、回复两边都有）；
//! TUI 不在时，桥自己 headless 跑。两个执行者绝不能把同一条消息各跑一遍。
//!
//! 跨进程协议全靠 `rename(2)` 的原子性（同一文件系统内 rename 是原子的）：
//!
//! | 文件名后缀 | 含义 |
//! |---|---|
//! | `.json` | 待认领。TUI 轮询与桥的兜底扫描都从这里抢，谁先 rename 谁执行 |
//! | `.tui-running` | TUI 已认领，正在执行 |
//! | `.headless-running` | 桥已认领，正在执行 |
//! | `.failed` | TUI 连续 3 次提交失败，交还桥 |
//! | `.orphaned` | 认领者崩溃留下的孤儿（只归档不重跑，避免重复执行） |
//!
//! 心跳 `state/tui-heartbeat.json` 由 TUI 补丁每 2s 覆盖写一次：
//! `{pid, session_id, busy, ts}`。桥据此判断「TUI 是否在线」「TUI 在看哪个会话」。

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// TUI 心跳新鲜窗口：补丁每 2s 写一次，6s 没更新即视为已退出。
pub const TUI_FRESH_MS: i64 = 6_000;
/// 心跳新鲜但 TUI 一直忙（长回合）时，消息滞留的硬上限；超过就由桥兜底。
/// 兜底不会与 TUI 抢同一会话：worker 入队后先等会话空闲（见 `wait_session_idle`）。
pub const TUI_BUSY_HARD_MS: i64 = 300_000;
/// 认领文件超过这个时长仍未清理 → 视为认领者崩溃。
pub const CLAIM_ORPHAN_MS: i64 = 900_000;

/// 收件箱里的一条 IM 消息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboxMsg {
    pub id: String,
    pub platform: String,
    pub peer: String,
    pub chat_key: String,
    pub text: String,
    /// 桥绑定的会话 id；TUI 只在「自己正看着这个会话」时才认领。
    pub target_session: String,
    /// 写入时间（ms），也用于兜底超时判定。
    pub ts: i64,
}

/// TUI 心跳文件内容。
#[derive(Debug, Clone, Deserialize)]
pub struct Heartbeat {
    #[serde(default)]
    pub pid: u32,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub busy: bool,
    /// 新版 TUI 钩子置 true：它会写过程事件文件，桥据此跳过 DB 文本镜像（防双发）。
    #[serde(default)]
    pub events: bool,
    /// TUI 当前工作目录（用于把占位绑定补绑到正确的会话）。
    #[serde(default)]
    pub cwd: Option<String>,
    pub ts: i64,
}

impl Heartbeat {
    pub fn age_ms(&self, now_ms: i64) -> i64 {
        now_ms - self.ts
    }
}

/// 收件箱目录 + 心跳文件的轻量封装（无内部状态，可随时克隆）。
#[derive(Debug, Clone)]
pub struct ImInbox {
    dir: PathBuf,
    heartbeat_path: PathBuf,
}

impl ImInbox {
    pub fn new(state_dir: &Path) -> Self {
        Self {
            dir: state_dir.join("im-inbox"),
            heartbeat_path: state_dir.join("tui-heartbeat.json"),
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn heartbeat_path(&self) -> &Path {
        &self.heartbeat_path
    }

    /// 建目录（幂等）。
    pub fn ensure(&self) -> io::Result<()> {
        fs::create_dir_all(&self.dir)
    }

    /// 原子写一条消息：先写 `.tmp` 再 rename，避免 TUI 读到半截 JSON。
    pub fn write(&self, msg: &InboxMsg) -> io::Result<PathBuf> {
        self.ensure()?;
        let name = format!("{:013}-{}.json", msg.ts.max(0), msg.id);
        let path = self.dir.join(&name);
        let tmp = self.dir.join(format!(".{name}.tmp"));
        let body =
            serde_json::to_vec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        fs::write(&tmp, body)?;
        fs::rename(&tmp, &path)?;
        Ok(path)
    }

    /// 读心跳；文件缺失/损坏/时间戳非法都返回 None（视为 TUI 不在线）。
    pub fn heartbeat(&self) -> Option<Heartbeat> {
        let raw = fs::read(&self.heartbeat_path).ok()?;
        let hb: Heartbeat = serde_json::from_slice(&raw).ok()?;
        if hb.ts <= 0 {
            return None;
        }
        Some(hb)
    }

    /// 心跳新鲜才返回（TUI 在线）。
    pub fn fresh(&self, now_ms: i64) -> Option<Heartbeat> {
        self.heartbeat()
            .filter(|hb| hb.age_ms(now_ms) <= TUI_FRESH_MS)
    }

    fn list_with_suffix(&self, suffix: &str) -> Vec<PathBuf> {
        let mut out: Vec<PathBuf> = match fs::read_dir(&self.dir) {
            Ok(rd) => rd
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| {
                    p.extension()
                        .and_then(|s| s.to_str())
                        .map(|s| s == suffix)
                        .unwrap_or(false)
                })
                .collect(),
            Err(_) => Vec::new(),
        };
        // 文件名前缀是 13 位毫秒时间戳，字典序即时间序
        out.sort();
        out
    }

    /// 待认领的消息（`.json`），按时间升序。
    pub fn pending(&self) -> Vec<PathBuf> {
        self.list_with_suffix("json")
    }

    /// TUI 三次提交失败后交还的消息（`.failed`）。
    pub fn failed(&self) -> Vec<PathBuf> {
        self.list_with_suffix("failed")
    }

    /// 读一条消息内容。
    pub fn read(&self, path: &Path) -> io::Result<InboxMsg> {
        let raw = fs::read(path)?;
        serde_json::from_slice(&raw).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// 原子认领：`.json` → `.<tag>`。返回 Some(新路径) 表示抢到；None 表示被别人抢先。
    pub fn claim(&self, path: &Path, tag: &str) -> Option<PathBuf> {
        let claimed = path.with_extension(tag);
        match fs::rename(path, &claimed) {
            Ok(()) => Some(claimed),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(_) => None,
        }
    }

    /// 执行完毕，删除认领文件。
    pub fn complete(&self, claimed: &Path) {
        let _ = fs::remove_file(claimed);
    }

    /// 归档认领者崩溃留下的孤儿（`.tui-running` / `.headless-running` 超龄）。
    /// **只归档不重跑**：重跑可能和已经跑完的回合重复。
    pub fn recover_orphans(&self, now_ms: i64) -> usize {
        let mut n = 0;
        for suffix in ["tui-running", "headless-running"] {
            for path in self.list_with_suffix(suffix) {
                let Some(ts) = path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.split('-').next())
                    .and_then(|s| s.parse::<i64>().ok())
                else {
                    continue;
                };
                if now_ms.saturating_sub(ts) < CLAIM_ORPHAN_MS {
                    continue;
                }
                let orphaned = path.with_extension("orphaned");
                if fs::rename(&path, &orphaned).is_ok() {
                    n += 1;
                }
            }
        }
        n
    }
}

/// 当前毫秒时间戳（与 TUI 补丁的 `Date.now()` 同一口径）。
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn msg(ts: i64, id: &str, text: &str) -> InboxMsg {
        InboxMsg {
            id: id.into(),
            platform: "wechat".into(),
            peer: "peer1".into(),
            chat_key: "wechat:dm:peer1".into(),
            text: text.into(),
            target_session: "sess_x".into(),
            ts,
        }
    }

    #[test]
    fn write_then_pending_roundtrip() {
        let dir = tempdir().unwrap();
        let inbox = ImInbox::new(dir.path());
        let p = inbox.write(&msg(1000, "a", "你好")).unwrap();
        assert!(p.exists());
        assert_eq!(inbox.pending().len(), 1);
        let back = inbox.read(&p).unwrap();
        assert_eq!(back.text, "你好");
        assert_eq!(back.target_session, "sess_x");
        // 没有残留 .tmp
        assert_eq!(fs::read_dir(inbox.dir()).unwrap().count(), 1);
    }

    #[test]
    fn pending_is_time_ordered() {
        let dir = tempdir().unwrap();
        let inbox = ImInbox::new(dir.path());
        inbox.write(&msg(300, "c", "3")).unwrap();
        inbox.write(&msg(100, "a", "1")).unwrap();
        inbox.write(&msg(200, "b", "2")).unwrap();
        let names: Vec<String> = inbox
            .pending()
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert!(names[0].contains("-a.json"), "{names:?}");
        assert!(names[2].contains("-c.json"), "{names:?}");
    }

    #[test]
    fn claim_is_exclusive() {
        let dir = tempdir().unwrap();
        let inbox = ImInbox::new(dir.path());
        let p = inbox.write(&msg(1000, "a", "hi")).unwrap();
        let first = inbox.claim(&p, "tui-running").expect("first claim wins");
        assert!(first.exists());
        assert!(
            inbox.claim(&p, "headless-running").is_none(),
            "second claim must lose"
        );
        assert!(inbox.pending().is_empty());
        inbox.complete(&first);
        assert!(!first.exists());
    }

    #[test]
    fn failed_files_are_listed_separately() {
        let dir = tempdir().unwrap();
        let inbox = ImInbox::new(dir.path());
        let p = inbox.write(&msg(1000, "a", "hi")).unwrap();
        let claimed = inbox.claim(&p, "tui-running").unwrap();
        fs::rename(&claimed, claimed.with_extension("failed")).unwrap();
        assert!(inbox.pending().is_empty());
        assert_eq!(inbox.failed().len(), 1);
    }

    #[test]
    fn heartbeat_freshness() {
        let dir = tempdir().unwrap();
        let inbox = ImInbox::new(dir.path());
        assert!(inbox.heartbeat().is_none(), "missing file = offline");
        fs::write(
            inbox.heartbeat_path(),
            br#"{"pid":1,"session_id":"sess_x","busy":false,"ts":100000}"#,
        )
        .unwrap();
        let hb = inbox.fresh(100_000 + TUI_FRESH_MS - 1).expect("fresh");
        assert_eq!(hb.session_id.as_deref(), Some("sess_x"));
        assert!(inbox.fresh(100_000 + TUI_FRESH_MS + 1).is_none(), "stale");
        fs::write(inbox.heartbeat_path(), b"not json").unwrap();
        assert!(inbox.heartbeat().is_none(), "corrupt = offline");
    }

    #[test]
    fn recover_orphans_moves_only_old_claims() {
        let dir = tempdir().unwrap();
        let inbox = ImInbox::new(dir.path());
        let old = inbox.write(&msg(1_000, "old", "x")).unwrap();
        let new = inbox.write(&msg(1_000_000, "new", "y")).unwrap();
        let old_claimed = inbox.claim(&old, "tui-running").unwrap();
        let new_claimed = inbox.claim(&new, "tui-running").unwrap();

        let now = 1_000_000 + CLAIM_ORPHAN_MS / 2;
        assert_eq!(inbox.recover_orphans(now), 1);
        assert!(old_claimed.with_extension("orphaned").exists());
        assert!(new_claimed.exists(), "fresh claim must stay");
        // 孤儿不回到待认领队列（避免重复执行）
        assert!(inbox.pending().is_empty());
    }
}
