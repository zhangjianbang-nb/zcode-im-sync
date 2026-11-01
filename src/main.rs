//! im-bridge CLI 入口：run | login | doctor | test-send。

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{mpsc, RwLock};

use im_bridge::session_router::{OutboundMsg, Platform};
use im_bridge::wechat_ilink::client::IlinkClient;
use im_bridge::wechat_ilink::poll::{InboundMessage, WeixinPoller};
use im_bridge::wechat_ilink::send::WeixinSender;
use im_bridge::wechat_ilink::tokens::TokenStore;
use im_bridge::wechat_ilink::WeixinCreds;
use im_bridge::zcode_rpc::{ZEvent, ZcodeHandle};
use im_bridge::{
    binding, config, feishu, health, http_status, lockfile, logging, media, paths, process_mirror,
    session_router, wechat_ilink, whitelist,
};

#[derive(Parser)]
#[command(
    name = "im-bridge",
    version,
    about = "微信/飞书 ↔ zcode 消息桥（Rust 实现，零 Python）"
)]
struct Cli {
    /// 工作目录（默认 $IM_BRIDGE_HOME 或当前目录）
    #[arg(long, global = true)]
    home: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// 常驻运行（systemd 入口）
    Run {
        /// 配置文件路径（默认 <home>/config.toml）
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// 登录引导：wechat/feishu 扫码登录，并可绑定 zcode 会话（/wechat /feishu 命令用）
    Login {
        /// 平台：wechat | feishu
        platform: String,
        /// 绑定 zcode 会话：优先 $ZCODE_SESSION_ID（TUI 命令注入），否则本目录最近活跃会话
        #[arg(long)]
        bind_latest: bool,
        /// 生成二维码 PNG 并用桌面查看器弹出（DISPLAY 下）
        #[arg(long)]
        qr_open: bool,
        /// 终端内打印字符画二维码
        #[arg(long)]
        qr_inline: bool,
    },
    /// 自检：配置/凭证/通道/zcode 可用性
    Doctor,
    /// 给 zcode 发一条测试消息并等待回复（M1 zcode 驱动接通后可用）
    TestSend {
        /// 测试消息
        #[arg(long)]
        message: String,
        /// 目标 chat 键（默认造一个临时 chat）
        #[arg(long)]
        chat: Option<String>,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match &cli.cmd {
        Cmd::Run { config } => {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(run(cli.home.clone(), config.clone()))
        }
        Cmd::Login {
            platform,
            bind_latest,
            qr_open,
            qr_inline,
        } => {
            let app_paths = resolve_paths(cli.home.clone())?;
            login(platform, *bind_latest, *qr_open, *qr_inline, &app_paths)
        }
        Cmd::Doctor => {
            let app_paths = resolve_paths(cli.home.clone())?;
            doctor(&app_paths)
        }
        Cmd::TestSend { message, chat } => {
            let app_paths = resolve_paths(cli.home.clone())?;
            test_send(&app_paths, message, chat.as_deref())
        }
    }
}

fn resolve_paths(home: Option<PathBuf>) -> anyhow::Result<paths::AppPaths> {
    let app_paths = match home {
        Some(root) => paths::AppPaths::from_root(root),
        None => paths::AppPaths::resolve()?,
    };
    app_paths.ensure()?;
    Ok(app_paths)
}

/// QR 登录：wechat = iLink get_bot_qrcode 状态机；feishu = device-code 扫码注册。
/// 成功写 `credentials/`（0600），`--bind-latest` 再把 IM chat 绑到 zcode 会话。
fn login(
    platform: &str,
    bind_latest: bool,
    qr_open: bool,
    qr_inline: bool,
    app_paths: &paths::AppPaths,
) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(login_async(
        platform,
        bind_latest,
        qr_open,
        qr_inline,
        app_paths,
    ))
}

async fn login_async(
    platform: &str,
    bind_latest: bool,
    qr_open: bool,
    qr_inline: bool,
    app_paths: &paths::AppPaths,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(40))
        .build()?;
    let display = im_bridge::qr::QrDisplay {
        inline: qr_inline,
        open: qr_open,
        png_path: app_paths.logs.join(format!("{platform}-login-qrcode.png")),
    };

    match platform {
        "wechat" => {
            let creds = im_bridge::wechat_ilink::auth::qr_login(
                &client,
                im_bridge::wechat_ilink::ILINK_BASE_URL,
                &display,
            )
            .await?;
            creds.save(&app_paths.wechat_credentials())?;
            println!(
                "登录成功：微信 account_id={} user_id={}（凭据 {}）",
                creds.account_id,
                creds.user_id,
                app_paths.wechat_credentials().display()
            );
            if bind_latest {
                if creds.user_id.is_empty() {
                    anyhow::bail!("登录成功但未返回 ilink_user_id，无法绑定（请重试 /wechat）");
                }
                bind_chat(&format!("wechat:dm:{}", creds.user_id), app_paths)?;
            }
        }
        "feishu" => {
            let creds =
                im_bridge::feishu::onboard::qr_register(&client, "feishu", &display).await?;
            creds.save(&app_paths.feishu_credentials())?;
            println!(
                "登录成功：飞书 app_id={} open_id={}（凭据 {}）",
                creds.app_id,
                creds.open_id,
                app_paths.feishu_credentials().display()
            );
            if bind_latest {
                if creds.open_id.is_empty() {
                    anyhow::bail!("登录成功但未返回扫码者 open_id，无法绑定（请重试 /feishu）");
                }
                bind_chat(&format!("feishu:dm:{}", creds.open_id), app_paths)?;
            }
        }
        other => anyhow::bail!("不支持的平台: {other}（仅 wechat | feishu）"),
    }
    // 凭证/绑定刚落盘，必须让常驻服务重载——run() 只在启动时读 credentials/，
    // 不重载的话通道永久 disabled（实测踩坑：微信发消息无回复）。
    // try-restart：服务在跑就重启，没装/没跑则无害退出 0。
    match std::process::Command::new("systemctl")
        .args(["--user", "try-restart", "im-bridge"])
        .status()
    {
        Ok(s) if s.success() => {
            println!("已通知常驻服务重载凭证（systemctl --user try-restart im-bridge）")
        }
        Ok(s) => println!("常驻服务重载返回 {s}（未安装 systemd unit 可忽略）"),
        Err(e) => println!("常驻服务重载跳过：{e}（无 systemd 环境可忽略）"),
    }
    Ok(())
}

/// 把 IM chat 绑到 zcode 会话（$ZCODE_SESSION_ID 优先），写入 state/chats.json。
fn bind_chat(chat_key: &str, app_paths: &paths::AppPaths) -> anyhow::Result<()> {
    let target =
        im_bridge::binding::SessTarget::resolve_latest(&im_bridge::binding::default_db_path())
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut chats = im_bridge::binding::ChatsFile::load(&app_paths.chats_file());
    chats.bind(chat_key, &target);
    chats
        .save(&app_paths.chats_file())
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    println!(
        "绑定会话 {}（目录 {}）→ chat {}，双向同步已启用{}",
        if target.sess_id.is_empty() {
            "<待 TUI 建立>"
        } else {
            target.sess_id.as_str()
        },
        target.cwd,
        chat_key,
        if target.sess_id.is_empty() {
            "（发出第一条消息后自动补绑）"
        } else {
            ""
        }
    );
    Ok(())
}

/// 自检：只读检查，不改任何状态。
fn doctor(app_paths: &paths::AppPaths) -> anyhow::Result<()> {
    println!("im-bridge doctor（自检，只读）");
    println!("  工作目录:   {}", app_paths.root.display());

    let cfg_path = app_paths.config_file();
    match config::Config::load(&cfg_path) {
        Ok(cfg) => {
            println!("  配置文件:   {} ✓", cfg_path.display());
            println!("  HTTP 监听:  {}", cfg.general.http_bind);
            println!("  zcode bin:  {} {}", cfg.zcode.bin, {
                if std::path::Path::new(&cfg.zcode.bin).exists() {
                    "✓"
                } else {
                    "✗ 不存在"
                }
            });
            let wl = whitelist::Whitelist::from_config(&cfg.whitelist);
            println!(
                "  白名单:     微信 {} 个 / 飞书 {} 个（+ QR 登录自动并入）",
                wl.wechat_user_ids.len(),
                wl.feishu_open_ids.len()
            );
        }
        Err(e) => println!("  配置文件:   ✗ {e}"),
    }

    for (label, file) in [
        ("微信凭证", app_paths.wechat_credentials()),
        ("飞书凭证", app_paths.feishu_credentials()),
    ] {
        if file.exists() {
            let mode = file_mode(&file);
            println!("  {label}:   {} ✓ (mode {mode})", file.display());
        } else {
            println!("  {label}:   {} 未配置", file.display());
        }
    }

    let lock = app_paths.lock_file();
    let held = lockfile::LockFile::acquire(&lock);
    match held {
        Ok(_) => println!("  实例锁:     空闲（服务未在跑或已退出）"),
        Err(_) => println!("  实例锁:     已被占用（服务运行中，正常）"),
    }

    // 会话绑定与媒体用量（只读）
    let chats = binding::ChatsFile::load(&app_paths.chats_file());
    let bound: Vec<&String> = chats
        .chats
        .iter()
        .filter(|(_, b)| !b.sess_id.is_empty())
        .map(|(k, _)| k)
        .collect();
    if bound.is_empty() {
        println!("  会话绑定:   无（用 /wechat 或 /feishu 绑定 TUI 会话）");
    } else {
        for k in &bound {
            let b = &chats.chats[*k];
            println!("  会话绑定:   {k} → {} ({})", b.sess_id, b.cwd);
        }
    }
    let used = media::check_quota(&app_paths.media);
    println!(
        "  媒体用量:   {}（目录 {}）",
        media::human_size(used),
        app_paths.media.display()
    );
    Ok(())
}

fn file_mode(path: &std::path::Path) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path)
            .map(|m| format!("{:04o}", m.permissions().mode() & 0o7777))
            .unwrap_or_else(|_| "????".into())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        "n/a".into()
    }
}

fn test_send(app_paths: &paths::AppPaths, message: &str, chat: Option<&str>) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(test_send_async(app_paths, message, chat))
}

/// 直驱 zcode app-server 跑一轮（不经 IM 通道）：M1 驱动链路冒烟。
/// `--chat` 给 chat 键则 resume 其绑定会话，否则新建；回复打到 stdout。
async fn test_send_async(
    app_paths: &paths::AppPaths,
    message: &str,
    chat: Option<&str>,
) -> anyhow::Result<()> {
    logging::init("info"); // RUST_LOG 优先，便于诊断
    let cfg = config::Config::load(&app_paths.config_file())?;
    let cwd = PathBuf::from(&cfg.general.workdir_default);
    if !cwd.is_dir() {
        anyhow::bail!("工作目录不存在: {}", cwd.display());
    }

    // chat 指定且已绑定 → resume 对应会话；否则新开
    let resume_sid = match chat {
        Some(key) => {
            let chats = binding::ChatsFile::load(&app_paths.chats_file());
            match chats.chats.get(key) {
                Some(b) if !b.sess_id.is_empty() => Some(b.sess_id.clone()),
                Some(_) => {
                    anyhow::bail!("chat {key} 未绑定 zcode 会话（先 /wechat 或 /feishu 绑定）")
                }
                None => anyhow::bail!("chat {key} 不在 {} 中", app_paths.chats_file().display()),
            }
        }
        None => None,
    };

    let mut zh = ZcodeHandle::spawn(&cfg.zcode.bin, &cwd)
        .await
        .map_err(|e| anyhow::anyhow!("启动 zcode app-server 失败: {e}"))?;
    let mut ev = zh.subscribe_events();
    let sid = zh
        .open_session(resume_sid.as_deref(), &cwd)
        .await
        .map_err(|e| anyhow::anyhow!("打开会话失败: {e}"))?;
    println!(
        "zcode 会话 {sid} 已就绪（{}）",
        if resume_sid.is_some() {
            "resume"
        } else {
            "create"
        }
    );
    zh.send(message)
        .await
        .map_err(|e| anyhow::anyhow!("发送失败: {e}"))?;
    println!(">>> {message}");
    println!("等待回复（空闲超时 {}s）…", cfg.zcode.turn_timeout_s);

    let idle_timeout = Duration::from_secs(cfg.zcode.turn_timeout_s);
    loop {
        match tokio::time::timeout(idle_timeout, ev.recv()).await {
            Err(_) => {
                let _ = zh.stop_turn().await;
                zh.close().await;
                anyhow::bail!("等待回复超时（{idle_timeout:?}）");
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                eprintln!("（事件滞后 {n} 条）");
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                zh.close().await;
                anyhow::bail!("事件流已关闭（app-server 退出）");
            }
            Ok(Ok(ev)) => match ev {
                ZEvent::TurnDone {
                    response,
                    token_count,
                    tool_call_count,
                } => {
                    let text = response.unwrap_or_default();
                    println!(
                        "{}",
                        session_router::finalize_reply(&text, tool_call_count, token_count)
                    );
                    zh.close().await;
                    return Ok(());
                }
                ZEvent::TurnError { message: err } => {
                    zh.close().await;
                    anyhow::bail!("zcode turn 出错: {err}");
                }
                _ => {}
            },
        }
    }
}

/// 常驻：单实例锁 + HTTP 状态服务 + 信号退出。
async fn run(home: Option<PathBuf>, config_path: Option<PathBuf>) -> anyhow::Result<()> {
    let app_paths = resolve_paths(home)?;
    let _lock = lockfile::LockFile::acquire(&app_paths.lock_file())?;

    let cfg_path = config_path.unwrap_or_else(|| app_paths.config_file());
    let cfg = config::Config::load(&cfg_path)?;
    logging::init(&cfg.general.log_level);
    tracing::info!(
        "im-bridge {} 启动中，配置 {}",
        env!("CARGO_PKG_VERSION"),
        cfg_path.display()
    );
    // 媒体目录用量巡检（超 5GB 软限额只告警，不自动删）
    media::check_quota(&app_paths.media);

    let state = Arc::new(RwLock::new(health::HealthState::new()));

    let bind: SocketAddr =
        cfg.general.http_bind.parse().map_err(|e| {
            anyhow::anyhow!("general.http_bind 非法 {}: {e}", cfg.general.http_bind)
        })?;
    let http_state = state.clone();
    let http = tokio::spawn(async move { http_status::serve(bind, http_state).await });

    // —— 通道拉起：chat 映射 / 白名单 / 路由 / 镜像 / 平台通道 ——
    let chats = Arc::new(RwLock::new(binding::ChatsFile::load(
        &app_paths.chats_file(),
    )));
    let mut wl = whitelist::Whitelist::from_config(&cfg.whitelist);

    // 微信凭证（QR 登录 user_id 等价本人，自动并入白名单）
    let wechat_path = app_paths.wechat_credentials();
    let wechat_creds = if wechat_path.exists() {
        match WeixinCreds::load(&wechat_path) {
            Ok(c) => {
                wl.add_runtime_wechat(&c.user_id);
                Some(c)
            }
            Err(e) => {
                tracing::warn!("微信凭证损坏（{}）: {e}", wechat_path.display());
                None
            }
        }
    } else {
        tracing::info!(
            "微信凭证未配置（{}），微信通道不启用",
            wechat_path.display()
        );
        None
    };

    // 飞书凭证（扫码 open_id = 本人，自动并入白名单）
    let feishu_path = app_paths.feishu_credentials();
    let feishu_creds = if feishu_path.exists() {
        match feishu::FeishuCreds::load(&feishu_path) {
            Ok(c) => {
                if !c.open_id.is_empty() && !wl.feishu_open_ids.contains(&c.open_id) {
                    wl.feishu_open_ids.push(c.open_id.clone());
                }
                Some(c)
            }
            Err(e) => {
                tracing::warn!("飞书凭证损坏（{}）: {e}", feishu_path.display());
                None
            }
        }
    } else {
        tracing::info!(
            "飞书凭证未配置（{}），飞书通道不启用",
            feishu_path.display()
        );
        None
    };

    let whitelist = Arc::new(RwLock::new(wl));
    let (out_tx, out_rx) = session_router::outbound_channel();
    let (wx_out_tx, wx_out_rx) = mpsc::channel(64);
    let (fs_out_tx, fs_out_rx) = mpsc::channel(64);
    tokio::spawn(dispatch_outbound(out_rx, wx_out_tx, fs_out_tx));

    let router_cfg = session_router::router_config_from(
        &cfg.zcode.bin,
        cfg.zcode.turn_timeout_s,
        &cfg.general.workdir_default,
        binding::default_db_path(),
        app_paths.chats_file(),
        app_paths.state.clone(),
    );
    let router = session_router::SessionRouter::new(
        router_cfg.clone(),
        chats.clone(),
        whitelist.clone(),
        out_tx.clone(),
    );
    // TUI 收件箱兜底：TUI 掉线/长忙/提交失败时，桥自己 headless 跑，消息不丢
    router.spawn_inbox_sweeper();

    // 过程镜像：TUI 钩子写的块级事件 → 限速过程消息 → 平台通道。
    // 独立通道 + 独立限速器，不占 router/MirrorPoller 的出站队列。
    let (proc_tx, proc_rx) = mpsc::channel(256);
    tokio::spawn(process_mirror::ProcDispatcher::new(proc_rx, out_tx.clone()).run());
    let tailer = process_mirror::EventTailer::new(&app_paths.state, chats.clone(), proc_tx);
    tokio::spawn(tailer.run());

    // 镜像回程：zcode DB → 出站（与 router 共用 out_dedup 防双发）
    let mirror = session_router::MirrorPoller::new(router_cfg, chats, out_tx, router.out_dedup());
    tokio::spawn(mirror.run());

    // zcode 驱动就绪（worker 按需拉起 app-server）
    state.write().await.zcode = health::ChannelHealth::up();

    // —— 微信通道：getupdates 长轮询 + 发送 ——
    match wechat_creds {
        Some(creds) => {
            let http = reqwest::Client::builder()
                .timeout(Duration::from_secs(120))
                .build()?;
            let tokens = Arc::new(TokenStore::load(
                app_paths.state.join("weixin.context-tokens.json"),
            ));
            let poller = WeixinPoller::new(
                http.clone(),
                creds.clone(),
                tokens.clone(),
                app_paths.state.join("weixin.sync.json"),
            );
            let (wx_in_tx, wx_in_rx) = mpsc::channel(32);
            tokio::spawn(poller.run_loop(wx_in_tx));
            // 发闩/typing/熔断是实例级状态 → 入站/出站共享同一 sender
            let sender = Arc::new(WeixinSender::new(
                IlinkClient::new(http.clone(), creds),
                tokens,
            ));
            tokio::spawn(wechat_inbound_loop(
                wx_in_rx,
                router.clone(),
                sender.clone(),
                http,
                cfg.wechat.cdn_base_url.clone(),
                app_paths.media.clone(),
            ));
            tokio::spawn(wechat_outbound_loop(sender, wx_out_rx));
            state.write().await.wechat = health::ChannelHealth::up();
            tracing::info!("微信通道已启动（getupdates 长轮询）");
        }
        None => {
            drop(wx_out_rx); // 通道未接入 → 分发侧发不进即丢弃
            state.write().await.wechat = if wechat_path.exists() {
                health::ChannelHealth::down("凭证损坏，见日志")
            } else {
                health::ChannelHealth::disabled()
            };
        }
    }

    // —— 飞书通道：WS 长连接（阻塞任务，退出即 down） ——
    match feishu_creds {
        Some(creds) => {
            let st = state.clone();
            tokio::spawn(async move {
                tracing::info!("飞书通道启动中（WS 长连接）");
                match feishu::channel::run(creds, router, fs_out_rx, app_paths.media.clone()).await
                {
                    Ok(()) => {
                        tracing::warn!("飞书长连接退出");
                        st.write().await.feishu =
                            health::ChannelHealth::down("长连接退出，请重启服务");
                    }
                    Err(e) => {
                        tracing::error!("飞书通道异常退出: {e:#}");
                        st.write().await.feishu = health::ChannelHealth::down(format!("{e:#}"));
                    }
                }
            });
            state.write().await.feishu = health::ChannelHealth::starting();
        }
        None => {
            drop(fs_out_rx); // 通道未接入 → 分发侧发不进即丢弃
            state.write().await.feishu = if feishu_path.exists() {
                health::ChannelHealth::down("凭证损坏，见日志")
            } else {
                health::ChannelHealth::disabled()
            };
        }
    }

    let mut sigterm = signal(SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => tracing::info!("收到 SIGINT，退出"),
        _ = sigterm.recv() => tracing::info!("收到 SIGTERM，退出"),
        res = http => {
            match res {
                Ok(Err(e)) => tracing::error!("HTTP 状态服务异常退出: {e}"),
                Ok(Ok(())) => tracing::warn!("HTTP 状态服务停止"),
                Err(e) => tracing::error!("HTTP 状态服务任务 panic: {e}"),
            }
        }
    }
    // 直接退出，不等 tokio 运行时回收：在途的阻塞型 HTTP/DNS（微信长轮询/发送重试）
    // 会让 runtime drop 卡住，实测 systemd 停服只能靠 SIGKILL 兜底（90s）。
    // 桥的所有状态都是逐事件落盘的（chats/sync cursor/context-token），直接退出安全。
    tracing::info!("im-bridge 已退出");
    std::process::exit(0);
}

/// 出站分发：router/MirrorPoller → 平台通道；未接入的通道丢弃（只记日志）。
async fn dispatch_outbound(
    mut rx: mpsc::Receiver<OutboundMsg>,
    wx: mpsc::Sender<OutboundMsg>,
    fs: mpsc::Sender<OutboundMsg>,
) {
    while let Some(msg) = rx.recv().await {
        let tx = match msg.platform {
            Platform::Wechat => &wx,
            Platform::Feishu => &fs,
        };
        if tx.send(msg).await.is_err() {
            tracing::debug!("出站分发：目标通道未接入，消息丢弃");
        }
    }
}

/// 微信入站：轮询消息 → 媒体落盘/注入 → router（串行保序；typing 提示仅对白名单）。
async fn wechat_inbound_loop(
    mut rx: mpsc::Receiver<InboundMessage>,
    router: Arc<session_router::SessionRouter>,
    sender: Arc<WeixinSender>,
    http: reqwest::Client,
    cdn_base_url: String,
    media_root: PathBuf,
) {
    while let Some(msg) = rx.recv().await {
        // 媒体消息：下载 → 落盘 media/<chat>/<date>/ → 附件绝对路径注入 prompt
        let attachments = if msg.has_media {
            wechat_ilink::media::collect_inbound(
                &http,
                &cdn_base_url,
                &media_root,
                &Platform::Wechat.chat_key(&msg.from_user_id),
                msg.raw.get("item_list"),
            )
            .await
        } else {
            Vec::new()
        };
        let text = media::compose_prompt(&msg.text, &attachments);
        if text.trim().is_empty() {
            continue;
        }
        // 「机器人正在输入」提示（尽力而为，失败静默）——仅对白名单发送者，
        // 非白名单必须完全静默（防暴露机器人），判定与 handle_inbound 同一事实源
        if router.is_allowed(Platform::Wechat, &msg.from_user_id).await {
            sender.send_typing(&msg.from_user_id).await;
            // 入站秒回：不等模型 turn（TTFT 不可控），保证 10s 内必有回执
            if let Err(e) = sender
                .send_text(&msg.from_user_id, session_router::ACK_TEXT)
                .await
            {
                tracing::warn!("微信入站秒回失败 peer={}: {e}", msg.from_user_id);
            }
        }
        // 入队失败等确定性回执由 handle_inbound 统一给出（单一事实源），此处不重复
        let _ = router
            .handle_inbound(Platform::Wechat, &msg.from_user_id, &text)
            .await;
    }
}

/// 微信出站：OutboundMsg → send_text（内部分段 + context_token 状态机）。
async fn wechat_outbound_loop(sender: Arc<WeixinSender>, mut rx: mpsc::Receiver<OutboundMsg>) {
    while let Some(msg) = rx.recv().await {
        if msg.platform != Platform::Wechat || msg.text.trim().is_empty() {
            continue;
        }
        if let Err(e) = sender.send_text(&msg.peer, &msg.text).await {
            tracing::warn!("微信出站失败 peer={}: {e}", msg.peer);
        }
    }
}
