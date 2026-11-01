//! 工程目录布局解析（自包含于服务目录）。

use std::fs;
use std::path::PathBuf;

use crate::error::{BridgeError, Result};

#[derive(Debug, Clone)]
pub struct AppPaths {
    pub root: PathBuf,
    pub credentials: PathBuf,
    pub state: PathBuf,
    pub media: PathBuf,
    pub logs: PathBuf,
    pub locks: PathBuf,
}

impl AppPaths {
    /// 工作根目录：`IM_BRIDGE_HOME` 环境变量 > 进程当前目录。
    pub fn resolve() -> Result<Self> {
        let root = std::env::var_os("IM_BRIDGE_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::current_dir().ok())
            .ok_or_else(|| {
                BridgeError::config("无法确定工作目录（IM_BRIDGE_HOME 未设且 getcwd 失败）")
            })?;
        Ok(Self::from_root(root))
    }

    pub fn from_root(root: PathBuf) -> Self {
        Self {
            credentials: root.join("credentials"),
            state: root.join("state"),
            media: root.join("media"),
            logs: root.join("logs"),
            locks: root.join("state").join("locks"),
            root,
        }
    }

    /// 确保运行期目录齐全；`credentials/` 收紧到 0700。
    pub fn ensure(&self) -> Result<()> {
        for dir in [&self.state, &self.media, &self.logs, &self.locks] {
            fs::create_dir_all(dir)?;
        }
        fs::create_dir_all(&self.credentials)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&self.credentials, fs::Permissions::from_mode(0o700));
        }
        Ok(())
    }

    pub fn config_file(&self) -> PathBuf {
        self.root.join("config.toml")
    }

    pub fn chats_file(&self) -> PathBuf {
        self.state.join("chats.json")
    }

    pub fn lock_file(&self) -> PathBuf {
        self.locks.join("im-bridge.lock")
    }

    pub fn wechat_credentials(&self) -> PathBuf {
        self.credentials.join("weixin.json")
    }

    pub fn feishu_credentials(&self) -> PathBuf {
        self.credentials.join("feishu.toml")
    }
}
