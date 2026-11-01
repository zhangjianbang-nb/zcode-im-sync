//! 飞书开放平台接入（协议按 Hermes-CN-Core（MIT）事实 Rust 重写）。
//!
//! 扫码注册走 `accounts.feishu.cn/oauth/v1/app/registration` device-code 流程——
//! 飞书自动建好带收发权限的 bot 应用，免手动在开放平台建应用。

pub mod channel;
pub mod onboard;

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{BridgeError, Result};
use crate::wechat_ilink::write_secret_file;

pub const ONBOARD_ACCOUNTS_URLS: &[(&str, &str)] = &[
    ("feishu", "https://accounts.feishu.cn"),
    ("lark", "https://accounts.larksuite.com"),
];
pub const ONBOARD_OPEN_URLS: &[(&str, &str)] = &[
    ("feishu", "https://open.feishu.cn"),
    ("lark", "https://open.larksuite.com"),
];
pub const REGISTRATION_PATH: &str = "/oauth/v1/app/registration";
pub const ONBOARD_REQUEST_TIMEOUT_S: u64 = 10;

pub fn accounts_base_url(domain: &str) -> &str {
    ONBOARD_ACCOUNTS_URLS
        .iter()
        .find(|(d, _)| *d == domain)
        .map(|(_, u)| *u)
        .unwrap_or(ONBOARD_ACCOUNTS_URLS[0].1)
}

pub fn open_base_url(domain: &str) -> &str {
    ONBOARD_OPEN_URLS
        .iter()
        .find(|(d, _)| *d == domain)
        .map(|(_, u)| *u)
        .unwrap_or(ONBOARD_OPEN_URLS[0].1)
}

/// 飞书应用凭据（`credentials/feishu.toml`，0600）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeishuCreds {
    pub app_id: String,
    pub app_secret: String,
    /// "feishu" | "lark"
    pub domain: String,
    /// 扫码用户（=用户本人）的 open_id；自动并入白名单
    pub open_id: String,
    #[serde(default)]
    pub bot_name: String,
    #[serde(default)]
    pub bot_open_id: String,
}

impl FeishuCreds {
    pub fn save(&self, path: &Path) -> Result<()> {
        let text = toml::to_string_pretty(self)
            .map_err(|e| BridgeError::other(format!("TOML 序列化: {e}")))?;
        write_secret_file(path, text.as_bytes())
    }

    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .map_err(|_| BridgeError::Credential(path.to_path_buf()))?;
        toml::from_str(&raw).map_err(|_| BridgeError::Credential(path.to_path_buf()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn creds_roundtrip_toml_0600() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("feishu.toml");
        let c = FeishuCreds {
            app_id: "cli_a".into(),
            app_secret: "s".into(),
            domain: "feishu".into(),
            open_id: "ou_1".into(),
            bot_name: "bot".into(),
            bot_open_id: "ou_bot".into(),
        };
        c.save(&p).unwrap();
        let back = FeishuCreds::load(&p).unwrap();
        assert_eq!(back.app_id, "cli_a");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&p).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
    }

    #[test]
    fn domain_urls_default_to_feishu() {
        assert_eq!(accounts_base_url("feishu"), "https://accounts.feishu.cn");
        assert_eq!(accounts_base_url("lark"), "https://accounts.larksuite.com");
        assert_eq!(accounts_base_url("unknown"), "https://accounts.feishu.cn");
    }
}
