//! `config.toml` 结构与加载（0600；凭证另存 `credentials/`）。

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{BridgeError, Result};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub general: General,
    pub whitelist: WhitelistCfg,
    pub zcode: ZcodeCfg,
    pub wechat: WechatCfg,
    pub feishu: FeishuCfg,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct General {
    /// zcode 默认工作目录（`/cwd` 命令可按会话覆盖）
    pub workdir_default: String,
    pub log_level: String,
    /// HTTP 状态服务监听地址（仅本机）
    pub http_bind: String,
}

impl Default for General {
    fn default() -> Self {
        let home = dirs::home_dir().unwrap_or_default();
        let workdir = home.join("workspace/ai_workspace");
        Self {
            workdir_default: if workdir.is_dir() {
                workdir.display().to_string()
            } else {
                home.display().to_string()
            },
            log_level: "info".into(),
            http_bind: "127.0.0.1:8106".into(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct WhitelistCfg {
    /// 本人微信 `ilink_user_id`（QR 登录得到的 user_id 会自动并入，可留空）
    pub wechat_user_ids: Vec<String>,
    /// 本人飞书 `open_id`（先发条消息从 doctor 日志抄录）
    pub feishu_open_ids: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ZcodeCfg {
    pub bin: String,
    /// 单轮 turn 超时（秒），超时 session/stop + 错误回执
    pub turn_timeout_s: u64,
}

impl Default for ZcodeCfg {
    fn default() -> Self {
        Self {
            bin: "zcode".into(),
            turn_timeout_s: 600,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct WechatCfg {
    /// iLink Bot API 基址
    pub base_url: String,
    /// 媒体 CDN 基址
    pub cdn_base_url: String,
}

impl Default for WechatCfg {
    fn default() -> Self {
        Self {
            base_url: "https://ilinkai.weixin.qq.com".into(),
            cdn_base_url: "https://novac2c.cdn.weixin.qq.com/c2c".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct FeishuCfg {
    /// 飞书开放平台域名：`feishu`（国内）或 `lark`（国际）
    pub domain: String,
}

impl Default for FeishuCfg {
    fn default() -> Self {
        Self {
            domain: "feishu".into(),
        }
    }
}

impl Config {
    /// 加载配置。文件不存在时返回默认值并由调用方告警（首次运行体验）。
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            tracing::warn!("配置文件 {} 不存在，使用内置默认值", path.display());
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(path)?;
        let cfg: Config = toml::from_str(&raw)
            .map_err(|e| BridgeError::config(format!("{} 解析失败: {e}", path.display())))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if self
            .general
            .http_bind
            .parse::<std::net::SocketAddr>()
            .is_err()
        {
            return Err(BridgeError::config(format!(
                "general.http_bind 不是合法 SocketAddr: {}",
                self.general.http_bind
            )));
        }
        if self.zcode.bin.is_empty() {
            return Err(BridgeError::config("zcode.bin 不能为空"));
        }
        if self.zcode.turn_timeout_s == 0 {
            return Err(BridgeError::config("zcode.turn_timeout_s 必须 > 0"));
        }
        match self.feishu.domain.as_str() {
            "feishu" | "lark" => {}
            other => {
                return Err(BridgeError::config(format!(
                    "feishu.domain 只能是 feishu/lark，收到: {other}"
                )))
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_valid() {
        Config::default().validate().unwrap();
    }

    #[test]
    fn parse_partial_toml() {
        let cfg: Config = toml::from_str(
            r#"
            [general]
            http_bind = "127.0.0.1:9000"

            [whitelist]
            wechat_user_ids = ["u_abc"]
            "#,
        )
        .unwrap();
        assert_eq!(cfg.general.http_bind, "127.0.0.1:9000");
        assert_eq!(cfg.whitelist.wechat_user_ids, vec!["u_abc".to_string()]);
        // 未填字段回默认
        assert_eq!(cfg.zcode.turn_timeout_s, 600);
    }

    #[test]
    fn reject_bad_bind() {
        let cfg = Config {
            general: General {
                http_bind: "not-an-addr".into(),
                ..General::default()
            },
            ..Config::default()
        };
        assert!(cfg.validate().is_err());
    }
}
