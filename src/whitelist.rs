//! 白名单：只有本人账号可驱动 zcode；未命中静默丢弃（调用方只记日志不回执）。

use serde::{Deserialize, Serialize};

use crate::config::WhitelistCfg;

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Whitelist {
    pub wechat_user_ids: Vec<String>,
    pub feishu_open_ids: Vec<String>,
    /// QR 登录得到的微信 user_id 运行时并入（等价本人账号，免改配置文件）
    #[serde(skip)]
    pub runtime_wechat: Vec<String>,
}

impl Whitelist {
    pub fn from_config(cfg: &WhitelistCfg) -> Self {
        Self {
            wechat_user_ids: cfg.wechat_user_ids.clone(),
            feishu_open_ids: cfg.feishu_open_ids.clone(),
            runtime_wechat: Vec::new(),
        }
    }

    /// 并入运行时微信 user_id（幂等，忽略空串）。
    pub fn add_runtime_wechat(&mut self, user_id: &str) {
        if !user_id.is_empty() && !self.runtime_wechat.iter().any(|x| x == user_id) {
            self.runtime_wechat.push(user_id.to_string());
        }
    }

    pub fn allows_wechat(&self, user_id: &str) -> bool {
        !user_id.is_empty()
            && (self.wechat_user_ids.iter().any(|x| x == user_id)
                || self.runtime_wechat.iter().any(|x| x == user_id))
    }

    pub fn allows_feishu(&self, open_id: &str) -> bool {
        !open_id.is_empty() && self.feishu_open_ids.iter().any(|x| x == open_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_id_never_matches() {
        let wl = Whitelist::default();
        assert!(!wl.allows_wechat(""));
        assert!(!wl.allows_feishu(""));
    }

    #[test]
    fn config_and_runtime_paths() {
        let cfg = WhitelistCfg {
            wechat_user_ids: vec!["u_cfg".into()],
            feishu_open_ids: vec!["ou_cfg".into()],
        };
        let mut wl = Whitelist::from_config(&cfg);
        assert!(wl.allows_wechat("u_cfg"));
        assert!(!wl.allows_wechat("u_qr"));
        assert!(wl.allows_feishu("ou_cfg"));
        assert!(!wl.allows_feishu("ou_other"));

        wl.add_runtime_wechat("u_qr");
        wl.add_runtime_wechat("u_qr"); // 幂等
        assert_eq!(wl.runtime_wechat.len(), 1);
        assert!(wl.allows_wechat("u_qr"));
    }
}
