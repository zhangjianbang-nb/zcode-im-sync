//! 手写最小 HTTP 服务：`/healthz`、`/status`（仅 127.0.0.1，不引 HTTP 框架）。

use std::net::SocketAddr;
use std::sync::Arc;

use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use crate::error::Result;
use crate::health::{ChannelHealth, ChatSummary, HealthState};

#[derive(Serialize)]
struct HealthzResponse<'a> {
    ok: bool,
    wechat: &'a ChannelHealth,
    feishu: &'a ChannelHealth,
    zcode: &'a ChannelHealth,
    uptime_s: u64,
}

#[derive(Serialize)]
struct StatusResponse<'a> {
    ok: bool,
    version: &'static str,
    started_unix: u64,
    uptime_s: u64,
    wechat: &'a ChannelHealth,
    feishu: &'a ChannelHealth,
    zcode: &'a ChannelHealth,
    chats: &'a [ChatSummary],
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<&'a str>,
}

/// 绑定并常驻 HTTP 服务（阻塞当前 task，直到出错才返回）。
pub async fn serve(addr: SocketAddr, state: Arc<RwLock<HealthState>>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("HTTP 状态服务监听 http://{addr}");
    run(listener, state).await
}

/// 在已绑定的监听器上循环接受连接（测试可传入自己的 listener）。
pub async fn run(listener: TcpListener, state: Arc<RwLock<HealthState>>) -> Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(stream, state).await {
                tracing::debug!("HTTP 连接 {peer} 处理失败: {e}");
            }
        });
    }
}

async fn handle(stream: TcpStream, state: Arc<RwLock<HealthState>>) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    reader.read_line(&mut request_line).await?;
    let raw_path = request_line.split_whitespace().nth(1).unwrap_or("/");
    let path = raw_path.split('?').next().unwrap_or(raw_path);

    // 读掉请求头直到空行（上限 64 行防呆）
    for _ in 0..64 {
        let mut header = String::new();
        let n = reader.read_line(&mut header).await?;
        if n == 0 || header == "\r\n" || header == "\n" {
            break;
        }
    }

    let st = state.read().await;
    let (status, reason, body) = match path {
        "/healthz" => {
            let ok = st.ok();
            let resp = HealthzResponse {
                ok,
                wechat: &st.wechat,
                feishu: &st.feishu,
                zcode: &st.zcode,
                uptime_s: st.uptime_s(),
            };
            let body = serde_json::to_vec(&resp).unwrap_or_default();
            (
                if ok { 200 } else { 503 },
                if ok { "OK" } else { "Service Unavailable" },
                body,
            )
        }
        "/status" => {
            let ok = st.ok();
            let resp = StatusResponse {
                ok,
                version: env!("CARGO_PKG_VERSION"),
                started_unix: st.started_unix(),
                uptime_s: st.uptime_s(),
                wechat: &st.wechat,
                feishu: &st.feishu,
                zcode: &st.zcode,
                chats: &st.chats,
                last_error: st.last_error.as_deref(),
            };
            let body = serde_json::to_vec(&resp).unwrap_or_default();
            (
                if ok { 200 } else { 503 },
                if ok { "OK" } else { "Service Unavailable" },
                body,
            )
        }
        _ => (404, "Not Found", br#"{"error":"not found"}"#.to_vec()),
    };
    drop(st);

    let mut stream = reader.into_inner();
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

    #[tokio::test]
    async fn healthz_serves_json() {
        let state = Arc::new(RwLock::new(HealthState::new()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let srv_state = state.clone();
        let srv = tokio::spawn(async move { run(listener, srv_state).await });

        let mut conn = TcpStream::connect(addr).await.unwrap();
        conn.write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        // 服务端 Connection: close，读到 EOF 后整体切头/体
        let mut raw = String::new();
        BufReader::new(conn).read_to_string(&mut raw).await.unwrap();
        let (head, body) = raw.split_once("\r\n\r\n").expect("响应应含头体分隔");
        assert!(head.starts_with("HTTP/1.1 200"), "head: {head}");
        assert!(body.contains("\"ok\":true"), "body: {body}");
        assert!(body.contains("\"uptime_s\""));

        srv.abort();
    }
}
