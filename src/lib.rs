//! im-bridge —— 微信/飞书 ↔ zcode 消息桥（100% Rust，零 Python）。
//!
//! - 微信：腾讯 iLink Bot API（协议事实参照 Hermes-CN-Core〔MIT〕，代码全部自写）
//! - 飞书：larksuite-oapi-sdk-rs WebSocket 长连接
//! - 执行层：zcode app-server（stdio JSON-RPC 2.0，全能力）
//!
//! 部署：`~/workspace/servers/im_bridge/`，systemd --user 常驻，HTTP 127.0.0.1:8106。

pub mod binding;
pub mod config;
pub mod error;
pub mod feishu;
pub mod health;
pub mod http_status;
pub mod im_inbox;
pub mod lockfile;
pub mod logging;
pub mod media;
pub mod paths;
pub mod process_mirror;
pub mod qr;
pub mod session_router;
pub mod wechat_ilink;
pub mod whitelist;
pub mod zcode_rpc;
