//! zcode_rpc ↔ mock app-server 集成测试：反向请求 gate / -32010 重试 /
//! 事件回放 → TurnDone / TurnError / resume 形状 / close 回收。
//!
//! mock 路径取 `CARGO_BIN_EXE_mock_app_server`（cargo 自动构建 bin target）。

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use im_bridge::zcode_rpc::{ZEvent, ZcodeHandle};

const MOCK: &str = env!("CARGO_BIN_EXE_mock_app_server");

fn tmpdir() -> PathBuf {
    std::env::temp_dir().join(format!("im-bridge-mock-{}", std::process::id()))
}

#[tokio::test]
async fn create_send_roundtrip_turn_done() {
    let cwd = tmpdir();
    std::fs::create_dir_all(&cwd).unwrap();
    let mut zh = ZcodeHandle::spawn(MOCK, &cwd).await.expect("spawn mock");
    let mut ev = zh.subscribe_events();
    let sid = zh.open_session(None, &cwd).await.expect("create");
    assert!(sid.starts_with("sess_mock-"), "{sid}");
    zh.send("hello").await.expect("send");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        assert!(!left.is_zero(), "5s 内应有 TurnDone");
        match tokio::time::timeout(left, ev.recv()).await {
            Ok(Ok(ZEvent::TurnDone {
                response,
                token_count,
                tool_call_count,
            })) => {
                assert_eq!(response.as_deref(), Some("MOCK-REPLY:hello"));
                assert_eq!(token_count, Some(42));
                assert_eq!(tool_call_count, Some(1));
                break;
            }
            Ok(Ok(_)) => continue, // text_delta / Title 等
            Ok(Err(_)) => panic!("事件流断开"),
            Err(_) => panic!("超时未见 TurnDone"),
        }
    }
    zh.close().await;
}

#[tokio::test]
async fn busy_error_retries_and_recovers() {
    let cwd = tmpdir();
    std::fs::create_dir_all(&cwd).unwrap();
    let mut zh = ZcodeHandle::spawn(MOCK, &cwd).await.expect("spawn mock");
    let mut ev = zh.subscribe_events();
    zh.open_session(None, &cwd).await.expect("create");
    let t0 = Instant::now();
    zh.send("BUSY please")
        .await
        .expect("send 应经 -32010 重试后成功");
    // mock 前 2 次回 -32010 → 至少两轮 500ms 退避
    assert!(t0.elapsed() >= Duration::from_millis(900), "重试应有退避");
    let mut got = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Ok(Ok(ZEvent::TurnDone { response, .. })) = tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            ev.recv(),
        )
        .await
        {
            assert_eq!(response.as_deref(), Some("MOCK-REPLY:BUSY please"));
            got = true;
            break;
        }
    }
    assert!(got, "重试后应有 TurnDone");
    zh.close().await;
}

#[tokio::test]
async fn fail_turn_yields_turn_error() {
    let cwd = tmpdir();
    std::fs::create_dir_all(&cwd).unwrap();
    let mut zh = ZcodeHandle::spawn(MOCK, &cwd).await.expect("spawn mock");
    let mut ev = zh.subscribe_events();
    zh.open_session(None, &cwd).await.expect("create");
    zh.send("FAIL").await.expect("send");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        assert!(!left.is_zero(), "5s 内应有 TurnError");
        match tokio::time::timeout(left, ev.recv()).await {
            Ok(Ok(ZEvent::TurnError { message })) => {
                assert!(message.contains("mock turn failed"), "{message}");
                break;
            }
            Ok(Ok(_)) => continue,
            other => panic!("期望 TurnError，得到 {other:?}"),
        }
    }
    zh.close().await;
}

#[tokio::test]
async fn resume_returns_session_shape() {
    let cwd = tmpdir();
    std::fs::create_dir_all(&cwd).unwrap();
    let mut zh = ZcodeHandle::spawn(MOCK, &cwd).await.expect("spawn mock");
    let _ev = zh.subscribe_events();
    // mock 的 resume 走同一 gate（requestRuntimePreferences 反向请求）
    let sid = zh
        .open_session(Some("sess_mock-old"), &cwd)
        .await
        .expect("resume");
    assert!(sid.starts_with("sess_mock-"), "{sid}");
    zh.close().await;
}

#[tokio::test]
async fn close_terminates_process() {
    let cwd = tmpdir();
    std::fs::create_dir_all(&cwd).unwrap();
    let mut zh = ZcodeHandle::spawn(MOCK, &cwd).await.expect("spawn mock");
    let _ev = zh.subscribe_events();
    zh.open_session(None, &cwd).await.expect("create");
    assert!(zh.is_alive());
    // close 消耗句柄：回包 + killpg 整组回收；正常返回即回收路径打通（真机 ps 验证过无残留）
    zh.close().await;
}

/// gate 语义（手写直连 mock）：反向请求不被应答 → create 无回包；应答 → 立即放行。
/// 顺带断言反向请求 id 是字符串（真机形态，字符串 id 曾因引号不匹配致 90s 超时）。
#[test]
fn reverse_request_gate_string_id() {
    let mut child = Command::new(MOCK)
        .arg("app-server")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn mock");
    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);

    writeln!(
        stdin,
        r#"{{"id":"req-1","method":"session/create","params":{{}}}}"#
    )
    .unwrap();
    stdin.flush().unwrap();

    let mut line = String::new();
    // 读到反向请求（跳过 startup 噪音）
    let mut reverse: Option<serde_json::Value> = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        line.clear();
        let n = reader.read_line(&mut line).expect("read");
        assert!(n > 0, "stdout 不应关闭");
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line.trim()) else {
            continue;
        };
        if v.get("id").is_some() && v.get("method").is_some() {
            // 反向请求：id 必须是字符串（真机形态）
            assert!(v["id"].is_string(), "反向请求 id 应为字符串: {}", v["id"]);
            reverse = Some(v);
            break;
        }
    }
    let rev = reverse.expect("应收到反向请求");
    assert_eq!(rev["method"], "interaction/requestOfficialMcpAuthHeaders");

    // 不应答：读下一行应超时（gate 不放行）——用非阻塞近似：开个线程读，主线程限时等
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut l = String::new();
        if reader.read_line(&mut l).ok().filter(|n| *n > 0).is_some() {
            let _ = tx.send(l);
        }
    });
    assert!(
        rx.recv_timeout(Duration::from_millis(800)).is_err(),
        "不应答反向请求时 create 不应回包"
    );

    // 应答 → gate 放行，立即回 create result
    writeln!(
        stdin,
        r#"{{"id":{},"result":{{"nativeSearchEnhancementsEnabled":true}}}}"#,
        rev["id"]
    )
    .unwrap();
    stdin.flush().unwrap();
    let l = rx
        .recv_timeout(Duration::from_secs(3))
        .expect("应答后应立即放行 create");
    let v: serde_json::Value = serde_json::from_str(l.trim()).unwrap();
    assert_eq!(v["id"], "req-1");
    assert!(v["result"]["sessionId"]
        .as_str()
        .unwrap()
        .starts_with("sess_mock-"));

    // 收尾
    let _ = child.kill();
    let _ = child.wait();
}

/// 回归：ChatWorker::ensure_handle 曾漏存句柄（Ok 分支忘 `*handle = Some(h)`，
/// spawn 出的 handle 被 drop 连带 kill_on_drop 杀掉 app-server，调用方
/// `unwrap()` panic 打死 worker → 入站消息永不回复）。
/// 走 SessionRouter 全链路（mock app-server 当 zcode_bin）连发两条文本：
/// 两条都要收到 TurnSignal 收尾，证明 worker 存活且句柄被正确复用。
#[tokio::test]
async fn router_worker_two_turns_handle_stored_and_reused() {
    use std::sync::Arc;

    use tokio::sync::RwLock;

    use im_bridge::binding::{ChatsFile, SessTarget};
    use im_bridge::session_router::{
        outbound_channel, InboundOutcome, Platform, RouterConfig, SessionRouter,
    };
    use im_bridge::whitelist::Whitelist;

    let cwd = tmpdir();
    std::fs::create_dir_all(&cwd).unwrap();
    let cfg = RouterConfig {
        zcode_bin: MOCK.to_string(),
        turn_timeout_s: 30,
        workdir_default: cwd.display().to_string(),
        db_path: cwd.join("db.sqlite"),
        chats_path: cwd.join("chats.json"),
        state_dir: cwd.clone(),
    };
    let mut cf = ChatsFile::default();
    cf.bind(
        "wechat:dm:u_test",
        &SessTarget {
            sess_id: "sess_mock-1".into(),
            cwd: cwd.display().to_string(),
            title: None,
        },
    );
    let mut wl = Whitelist::default();
    wl.add_runtime_wechat("u_test");
    let (out_tx, _out_rx) = outbound_channel();
    let router = SessionRouter::new(
        cfg,
        Arc::new(RwLock::new(cf)),
        Arc::new(RwLock::new(wl)),
        out_tx,
    );
    let mut sig = router.turn_signals();

    for (i, text) in ["第一条", "第二条"].into_iter().enumerate() {
        let out = router
            .handle_inbound(Platform::Wechat, "u_test", text)
            .await;
        assert!(
            matches!(out, InboundOutcome::Turn),
            "第{}条应入队 turn",
            i + 1
        );
        let s = tokio::time::timeout(Duration::from_secs(15), sig.recv())
            .await
            .unwrap_or_else(|_| panic!("第{}条 turn 信号超时（worker 疑似 panic 死亡）", i + 1))
            .expect("turn 信号通道关闭");
        assert!(s.ok, "第{}条 turn 应成功", i + 1);
        assert_eq!(s.chat_key, "wechat:dm:u_test");
    }
}
