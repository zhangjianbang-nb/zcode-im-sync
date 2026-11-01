//! 微信 iLink Bot API（协议按 Hermes-CN-Core（MIT）事实 Rust 重写，不复制其代码）。
//!
//! 腾讯官方 bot 接口 `https://ilinkai.weixin.qq.com`：长轮询收消息、sendmessage 回消息、
//! 媒体走 AES-128-ECB CDN；QR 登录 `get_bot_qrcode` → `get_qrcode_status` 状态机。

pub mod auth;
pub mod client;
pub mod media;
pub mod poll;
pub mod send;
pub mod tokens;

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{BridgeError, Result};

pub const ILINK_BASE_URL: &str = "https://ilinkai.weixin.qq.com";
pub const WEIXIN_CDN_BASE_URL: &str = "https://novac2c.cdn.weixin.qq.com/c2c";
pub const ILINK_APP_ID: &str = "bot";
pub const CHANNEL_VERSION: &str = "2.2.0";
/// `(2<<16)|(2<<8)|0` = 131584
pub const ILINK_APP_CLIENT_VERSION: u32 = (2 << 16) | (2 << 8);

pub const EP_GET_UPDATES: &str = "ilink/bot/getupdates";
pub const EP_SEND_MESSAGE: &str = "ilink/bot/sendmessage";
pub const EP_SEND_TYPING: &str = "ilink/bot/sendtyping";
pub const EP_GET_CONFIG: &str = "ilink/bot/getconfig";
pub const EP_GET_UPLOAD_URL: &str = "ilink/bot/getuploadurl";
pub const EP_GET_BOT_QR: &str = "ilink/bot/get_bot_qrcode";
pub const EP_GET_QR_STATUS: &str = "ilink/bot/get_qrcode_status";

pub const LONG_POLL_TIMEOUT_MS: u64 = 35_000;
pub const API_TIMEOUT_MS: u64 = 15_000;
pub const CONFIG_TIMEOUT_MS: u64 = 10_000;
pub const QR_TIMEOUT_MS: u64 = 35_000;

/// errcode 语义：-14 = context_token/会话过期；-2 = 限频或 stale（errmsg=="unknown error"）。
pub const SESSION_EXPIRED_ERRCODE: i64 = -14;
pub const RATE_LIMIT_ERRCODE: i64 = -2;

/// 微信登录凭据（`credentials/weixin.json`，0600）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeixinCreds {
    pub account_id: String,
    pub token: String,
    pub base_url: String,
    /// 扫码用户（=用户本人）的 ilink_user_id；自动并入白名单
    pub user_id: String,
    #[serde(default)]
    pub saved_at: String,
}

impl WeixinCreds {
    /// 原子写 0600。
    pub fn save(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        write_secret_file(path, json.as_bytes())
    }

    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .map_err(|_| BridgeError::Credential(path.to_path_buf()))?;
        serde_json::from_str(&raw).map_err(|_| BridgeError::Credential(path.to_path_buf()))
    }
}

/// 凭证类文件统一写法：原子替换 + 0600。
pub(crate) fn write_secret_file(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp-secret");
    std::fs::write(&tmp, bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// 生成 X-WECHAT-UIN：base64(str(rand u32))（每次请求随机；sendmessage 等带 token 接口用）。
pub fn random_uin_b64() -> String {
    use base64::Engine;
    let n: u32 = rand_u32();
    base64::engine::general_purpose::STANDARD.encode(n.to_string())
}

fn rand_u32() -> u32 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut h = RandomState::new().build_hasher();
    h.write_u64(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0),
    );
    h.finish() as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn creds_roundtrip_0600() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("weixin.json");
        let c = WeixinCreds {
            account_id: "acc".into(),
            token: "tok".into(),
            base_url: ILINK_BASE_URL.into(),
            user_id: "u1".into(),
            saved_at: "2026-10-31T00:00:00Z".into(),
        };
        c.save(&p).unwrap();
        let back = WeixinCreds::load(&p).unwrap();
        assert_eq!(back.token, "tok");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&p).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
    }

    #[test]
    fn uin_is_base64_digits() {
        use base64::Engine;
        let v = random_uin_b64();
        let dec = base64::engine::general_purpose::STANDARD
            .decode(&v)
            .unwrap();
        let s = String::from_utf8(dec).unwrap();
        assert!(s.parse::<u32>().is_ok());
    }
}
