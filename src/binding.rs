//! zcode 会话绑定：IM chat ↔ zcode session 映射（`state/chats.json`）。
//!
//! 绑定目标解析优先级：
//! 1. 环境变量 `ZCODE_SESSION_ID`（zcode 自定义命令 shell 展开时注入，精确=当前 TUI 会话）
//! 2. `~/.zcode/cli/db/db.sqlite` 里本目录最近活跃会话（`--bind-latest` 兜底）

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{BridgeError, Result};

pub const BIND_MODE_TUI_SYNC: &str = "tui_sync";

/// 单个 IM chat 的绑定。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatBinding {
    pub sess_id: String,
    pub cwd: String,
    /// "tui_sync" = 与 zcode TUI 同会话双向同步
    pub bind_mode: String,
    /// unix 秒
    pub bound_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

/// `state/chats.json` 全量。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatsFile {
    pub version: u32,
    #[serde(default)]
    pub chats: BTreeMap<String, ChatBinding>,
}

impl Default for ChatsFile {
    fn default() -> Self {
        Self {
            version: 1,
            chats: BTreeMap::new(),
        }
    }
}

impl ChatsFile {
    /// 读（缺失/损坏 → 空表，不阻塞登录）。
    pub fn load(path: &Path) -> Self {
        match std::fs::read_to_string(path) {
            Ok(raw) => serde_json::from_str(&raw).unwrap_or_else(|e| {
                tracing::warn!("chats.json 解析失败（按空表处理）: {e}");
                Self::default()
            }),
            Err(_) => Self::default(),
        }
    }

    /// 原子写 0600。
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(self)?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, json.as_bytes())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
        }
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    pub fn bind(&mut self, chat_key: &str, target: &SessTarget) {
        self.chats.insert(
            chat_key.to_string(),
            ChatBinding {
                sess_id: target.sess_id.clone(),
                cwd: target.cwd.clone(),
                bind_mode: BIND_MODE_TUI_SYNC.into(),
                bound_at: now_unix(),
                title: target.title.clone(),
            },
        );
    }
}

/// 解析出的绑定目标（zcode 会话）。
#[derive(Debug, Clone)]
pub struct SessTarget {
    pub sess_id: String,
    pub cwd: String,
    pub title: Option<String>,
}

impl SessTarget {
    /// 优先 `ZCODE_SESSION_ID`（TUI 命令场景 100% 命中），否则 DB 本目录最近。
    ///
    /// 特殊：`ZCODE_TUI=1`（TUI 钩子拉起登录）但没有 `ZCODE_SESSION_ID` 时，
    /// 说明用户还没在这个 TUI 里发过消息、会话尚未创建。此时写一个**空会话占位绑定**，
    /// 等 TUI 心跳报出真实 session_id 后由桥自动补绑；绝不能拿 DB 里同目录的旧会话
    /// 冒充当前 TUI（否则微信消息会跑到别的会话里）。
    pub fn resolve_latest(db_path: &Path) -> Result<Self> {
        if let Ok(sess_id) = std::env::var("ZCODE_SESSION_ID") {
            let sess_id = sess_id.trim().to_string();
            if !sess_id.is_empty() {
                let cwd = std::env::var("ZCODE_PROJECT_DIR")
                    .ok()
                    .filter(|s| !s.is_empty())
                    .or_else(|| {
                        std::env::current_dir()
                            .ok()
                            .map(|p| p.display().to_string())
                    })
                    .unwrap_or_default();
                tracing::info!("绑定目标取自 ZCODE_SESSION_ID={sess_id}");
                return Ok(Self {
                    sess_id,
                    cwd,
                    title: None,
                });
            }
        }
        let cwd = std::env::var("ZCODE_PROJECT_DIR")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| {
                std::env::current_dir()
                    .ok()
                    .map(|p| p.display().to_string())
            })
            .unwrap_or_default();
        if std::env::var("ZCODE_TUI").ok().as_deref() == Some("1") {
            tracing::info!("TUI 尚未建立会话，写占位绑定（等心跳补绑），cwd={cwd}");
            return Ok(Self {
                sess_id: String::new(),
                cwd,
                title: None,
            });
        }
        match latest_session(db_path, Some(&cwd))? {
            Some(t) => Ok(t),
            None => Err(BridgeError::other(format!(
                "在 {db_path:?} 未找到目录 {cwd} 下的活跃 zcode 会话（也可设 ZCODE_SESSION_ID 后重试）"
            ))),
        }
    }
}

/// 只读查会话库：最近活跃会话（排除 subagent 会话）。
pub fn latest_session(db_path: &Path, cwd: Option<&str>) -> Result<Option<SessTarget>> {
    if !db_path.exists() {
        return Err(BridgeError::other(format!(
            "zcode 会话库不存在: {}",
            db_path.display()
        )));
    }
    let conn = rusqlite::Connection::open_with_flags(
        db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| BridgeError::other(format!("打开会话库失败: {e}")))?;

    let row = match cwd {
        Some(dir) => conn.query_row(
            "SELECT id, directory, title FROM session \
             WHERE id NOT LIKE 'sess_subagent_%' AND directory = ?1 \
             ORDER BY time_updated DESC LIMIT 1",
            [dir],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<String>>(2)?,
                ))
            },
        ),
        None => conn.query_row(
            "SELECT id, directory, title FROM session \
             WHERE id NOT LIKE 'sess_subagent_%' \
             ORDER BY time_updated DESC LIMIT 1",
            [],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<String>>(2)?,
                ))
            },
        ),
    };

    match row {
        Ok((sess_id, dir, title)) => Ok(Some(SessTarget {
            sess_id,
            cwd: dir,
            title,
        })),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(BridgeError::other(format!("查询会话库失败: {e}"))),
    }
}

/// 默认 zcode 会话库路径。
pub fn default_db_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_default()
        .join(".zcode/cli/db/db.sqlite")
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn chats_roundtrip_atomic() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("chats.json");
        let mut f = ChatsFile::default();
        f.bind(
            "wechat:dm:u1",
            &SessTarget {
                sess_id: "sess_x".into(),
                cwd: "/tmp".into(),
                title: Some("t".into()),
            },
        );
        f.save(&p).unwrap();
        let back = ChatsFile::load(&p);
        let b = back.chats.get("wechat:dm:u1").unwrap();
        assert_eq!(b.sess_id, "sess_x");
        assert_eq!(b.bind_mode, BIND_MODE_TUI_SYNC);
    }

    #[test]
    fn load_missing_is_empty() {
        let dir = tempdir().unwrap();
        let f = ChatsFile::load(&dir.path().join("nope.json"));
        assert!(f.chats.is_empty());
    }

    #[test]
    fn latest_session_on_real_db_or_skip() {
        let db = default_db_path();
        if !db.exists() {
            return;
        }
        // 只读查询不抛异常即可（库结构可能随 zcode 版本变化）
        let _ = latest_session(&db, None);
    }
}
