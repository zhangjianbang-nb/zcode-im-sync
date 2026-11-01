//! 微信扫码登录状态机（`get_bot_qrcode` → 轮询 `get_qrcode_status` → confirmed 落凭据）。
//!
//! 状态语义与 Hermes-CN-Core 一致：wait / scaned / scaned_but_redirect（切 base_url）/
//! expired（刷新 ≤3 次）/ confirmed（ilink_bot_id+bot_token+baseurl+ilink_user_id）。

use std::time::{Duration, Instant};

use serde_json::Value;

use super::{WeixinCreds, EP_GET_BOT_QR, EP_GET_QR_STATUS, ILINK_APP_CLIENT_VERSION, ILINK_APP_ID};
use crate::error::{BridgeError, Result};
use crate::qr::QrDisplay;

/// 单次登录总时限（含扫码等待）。
pub const QR_LOGIN_TIMEOUT: Duration = Duration::from_secs(480);
/// 过期后最多刷新次数。
pub const MAX_QR_REFRESH: u32 = 3;

/// GET 轻量请求（二维码接口无 Authorization）。
async fn api_get(client: &reqwest::Client, url: &str) -> Result<Value> {
    let resp = client
        .get(url)
        .header("iLink-App-Id", ILINK_APP_ID)
        .header(
            "iLink-App-ClientVersion",
            ILINK_APP_CLIENT_VERSION.to_string(),
        )
        .send()
        .await
        .map_err(|e| BridgeError::other(format!("iLink 请求失败: {e}")))?;
    let status = resp.status();
    let raw = resp
        .text()
        .await
        .map_err(|e| BridgeError::other(format!("iLink 响应读取失败: {e}")))?;
    if !status.is_success() {
        return Err(BridgeError::other(format!(
            "iLink HTTP {status}: {}",
            raw.chars().take(200).collect::<String>()
        )));
    }
    serde_json::from_str(&raw).map_err(|e| BridgeError::other(format!("iLink 响应非 JSON: {e}")))
}

/// 取一张登录二维码 → (hex token, 扫码内容 URL)。
///
/// 扫码内容必须用 `qrcode_img_content`（小程序 URL），不能拿裸 token 画码。
async fn fetch_qr(client: &reqwest::Client, base_url: &str) -> Result<(String, String)> {
    let url = format!(
        "{}/{EP_GET_BOT_QR}?bot_type=3",
        base_url.trim_end_matches('/')
    );
    let v = api_get(client, &url).await?;
    let token = v
        .get("qrcode")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    let scan_url = v
        .get("qrcode_img_content")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    if token.is_empty() {
        return Err(BridgeError::other("QR 响应缺少 qrcode 字段"));
    }
    Ok((token, scan_url))
}

async fn poll_status(client: &reqwest::Client, base_url: &str, qrcode: &str) -> Result<Value> {
    let url = format!(
        "{}/{EP_GET_QR_STATUS}?qrcode={}",
        base_url.trim_end_matches('/'),
        qrcode
    );
    api_get(client, &url).await
}

/// 扫码登录：展示二维码 → 轮询状态机 → confirmed 写凭据。
pub async fn qr_login(
    client: &reqwest::Client,
    base_url: &str,
    display: &QrDisplay,
) -> Result<WeixinCreds> {
    let (mut qrcode_token, mut scan_url) = fetch_qr(client, base_url).await?;
    println!("\n请使用微信扫描以下二维码：");
    display
        .show(
            if scan_url.is_empty() {
                &qrcode_token
            } else {
                &scan_url
            },
            "",
        )
        .or_else(|e| {
            // 二维码渲染失败不阻塞：直接打印 URL 让用户另想办法
            println!("（二维码渲染失败: {e}）");
            println!(
                "{}",
                if scan_url.is_empty() {
                    &qrcode_token
                } else {
                    &scan_url
                }
            );
            Ok::<(), BridgeError>(())
        })?;

    let deadline = Instant::now() + QR_LOGIN_TIMEOUT;
    let mut current_base = base_url.trim_end_matches('/').to_string();
    let mut refresh_count = 0u32;

    while Instant::now() < deadline {
        let status_resp = match poll_status(client, &current_base, &qrcode_token).await {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!("QR 状态轮询失败（续询）: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let status = status_resp
            .get("status")
            .and_then(|x| x.as_str())
            .unwrap_or("wait");
        match status {
            "wait" => {
                print!(".");
                use std::io::Write;
                let _ = std::io::stdout().flush();
            }
            "scaned" => println!("\n已扫码，请在微信里确认..."),
            "scaned_but_redirect" => {
                if let Some(host) = status_resp.get("redirect_host").and_then(|x| x.as_str()) {
                    if !host.is_empty() {
                        current_base = format!("https://{host}");
                        tracing::info!("QR 跟随重定向到 {current_base}");
                    }
                }
            }
            "expired" => {
                refresh_count += 1;
                if refresh_count > MAX_QR_REFRESH {
                    return Err(BridgeError::other(format!(
                        "二维码多次过期（>{MAX_QR_REFRESH} 次），请重新执行登录"
                    )));
                }
                println!("\n二维码已过期，正在刷新... ({refresh_count}/{MAX_QR_REFRESH})");
                let (t, u) = fetch_qr(client, &current_base).await?;
                qrcode_token = t;
                scan_url = u;
                let data = if scan_url.is_empty() {
                    qrcode_token.as_str()
                } else {
                    scan_url.as_str()
                };
                if let Err(e) = display.show(data, "") {
                    println!("（二维码渲染失败: {e}）\n{data}");
                }
            }
            "confirmed" => {
                let creds = WeixinCreds {
                    account_id: str_field(&status_resp, "ilink_bot_id"),
                    token: str_field(&status_resp, "bot_token"),
                    base_url: status_resp
                        .get("baseurl")
                        .and_then(|x| x.as_str())
                        .filter(|s| !s.is_empty())
                        .unwrap_or(&current_base)
                        .to_string(),
                    user_id: str_field(&status_resp, "ilink_user_id"),
                    saved_at: chrono_like_now(),
                };
                if creds.account_id.is_empty() || creds.token.is_empty() {
                    return Err(BridgeError::other(
                        "已确认但凭据字段缺失（ilink_bot_id/bot_token）",
                    ));
                }
                return Ok(creds);
            }
            other => tracing::debug!("未知 QR 状态 {other}，续询"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(BridgeError::other(format!(
        "微信登录超时（{}s），请重新执行",
        QR_LOGIN_TIMEOUT.as_secs()
    )))
}

fn str_field(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string()
}

fn chrono_like_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("unix:{secs}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn str_field_reads_or_empty() {
        let v: Value = serde_json::json!({"ilink_bot_id": "b1", "bot_token": ""});
        assert_eq!(str_field(&v, "ilink_bot_id"), "b1");
        assert_eq!(str_field(&v, "bot_token"), "");
        assert_eq!(str_field(&v, "missing"), "");
    }
}
