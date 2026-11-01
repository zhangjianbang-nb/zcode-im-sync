//! 健康状态模型（供 /healthz /status 输出）。

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelState {
    /// 未配置（缺凭证），不算故障
    Disabled,
    /// 正在连接/登录
    Starting,
    /// 在线
    Up,
    /// 故障（拉 /healthz 返回 503）
    Down,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChannelHealth {
    pub state: ChannelState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl ChannelHealth {
    pub fn disabled() -> Self {
        Self {
            state: ChannelState::Disabled,
            detail: None,
        }
    }
    pub fn starting() -> Self {
        Self {
            state: ChannelState::Starting,
            detail: None,
        }
    }
    pub fn up() -> Self {
        Self {
            state: ChannelState::Up,
            detail: None,
        }
    }
    pub fn down(detail: impl Into<String>) -> Self {
        Self {
            state: ChannelState::Down,
            detail: Some(detail.into()),
        }
    }
}

/// 每个 IM 会话（chat 键）的摘要。
#[derive(Debug, Clone, Serialize)]
pub struct ChatSummary {
    pub chat: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sess_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// 最后活跃 unix 秒
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_active: Option<u64>,
}

#[derive(Debug)]
pub struct HealthState {
    started: Instant,
    started_unix: u64,
    pub wechat: ChannelHealth,
    pub feishu: ChannelHealth,
    pub zcode: ChannelHealth,
    pub chats: Vec<ChatSummary>,
    pub last_error: Option<String>,
}

impl HealthState {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            started_unix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            wechat: ChannelHealth::disabled(),
            feishu: ChannelHealth::disabled(),
            zcode: ChannelHealth::disabled(),
            chats: Vec::new(),
            last_error: None,
        }
    }

    pub fn uptime_s(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    pub fn started_unix(&self) -> u64 {
        self.started_unix
    }

    /// 任一通道 Down 即整体不健康（Disabled 不算故障）。
    pub fn ok(&self) -> bool {
        self.wechat.state != ChannelState::Down
            && self.feishu.state != ChannelState::Down
            && self.zcode.state != ChannelState::Down
    }
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_channels_are_ok() {
        let st = HealthState::new();
        assert!(st.ok());
        assert_eq!(st.wechat.state, ChannelState::Disabled);
    }

    #[test]
    fn any_down_makes_not_ok() {
        let mut st = HealthState::new();
        st.wechat = ChannelHealth::down("token 过期");
        assert!(!st.ok());
    }
}
