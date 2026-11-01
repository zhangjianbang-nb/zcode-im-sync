//! 统一错误类型。

use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, BridgeError>;

#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON 解析错误: {0}")]
    Json(#[from] serde_json::Error),

    #[error("TOML 解析错误: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("配置错误: {0}")]
    Config(String),

    #[error("凭证缺失或损坏: {0}")]
    Credential(PathBuf),

    #[error("单实例锁冲突: {0}")]
    Lock(String),

    #[error("{0}")]
    Other(String),
}

impl BridgeError {
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }
}
