//! tracing 日志初始化。

use tracing_subscriber::EnvFilter;

/// `RUST_LOG` 优先，否则用配置里的 `log_level`。
pub fn init(default_level: &str) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}
