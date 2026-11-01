//! 飞书扫码注册（device-code 流程）：init → begin → poll → probe bot。
//!
//! 用户飞书 App 扫 `verification_uri_complete` 二维码并确认，飞书服务端自动创建
//! 带 im 收发权限的 bot 应用，返回 client_id/client_secret + 扫码者 open_id。

use std::time::{Duration, Instant};

use serde_json::Value;

use super::{
    accounts_base_url, open_base_url, FeishuCreds, ONBOARD_REQUEST_TIMEOUT_S, REGISTRATION_PATH,
};
use crate::error::{BridgeError, Result};
use crate::qr::QrDisplay;

/// 注册流程总时限（含扫码等待）。
pub const REGISTRATION_TIMEOUT: Duration = Duration::from_secs(600);

/// POST 表单到注册端点；4xx 也回 JSON（poll 的 pending 就是 400）。
async fn post_form(
    client: &reqwest::Client,
    base_url: &str,
    body: &[(&str, &str)],
) -> Result<Value> {
    let url = format!("{}{}", base_url.trim_end_matches('/'), REGISTRATION_PATH);
    let resp = client
        .post(&url)
        .form(body)
        .timeout(Duration::from_secs(ONBOARD_REQUEST_TIMEOUT_S))
        .send()
        .await
        .map_err(|e| BridgeError::other(format!("飞书注册请求失败: {e}")))?;
    let raw = resp
        .text()
        .await
        .map_err(|e| BridgeError::other(format!("飞书注册响应读取失败: {e}")))?;
    serde_json::from_str(&raw).map_err(|e| BridgeError::other(format!("飞书注册响应非 JSON: {e}")))
}

/// 步骤 1：确认注册环境支持 client_secret 授权。
async fn init_registration(client: &reqwest::Client, domain: &str) -> Result<()> {
    let res = post_form(client, accounts_base_url(domain), &[("action", "init")]).await?;
    let methods = res
        .get("supported_auth_methods")
        .and_then(|x| x.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
        .unwrap_or_default();
    if !methods.contains(&"client_secret") {
        return Err(BridgeError::other(format!(
            "飞书注册环境不支持 client_secret 授权，可用: {methods:?}"
        )));
    }
    Ok(())
}

struct BeginInfo {
    device_code: String,
    qr_url: String,
    interval: u64,
    expire_in: u64,
}

/// 步骤 2：发起 device-code 流程，拿扫码 URL。
async fn begin_registration(client: &reqwest::Client, domain: &str) -> Result<BeginInfo> {
    let res = post_form(
        client,
        accounts_base_url(domain),
        &[
            ("action", "begin"),
            ("archetype", "PersonalAgent"),
            ("auth_method", "client_secret"),
            ("request_user_info", "open_id"),
        ],
    )
    .await?;
    let device_code = res
        .get("device_code")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    if device_code.is_empty() {
        return Err(BridgeError::other("飞书注册未返回 device_code"));
    }
    let qr_url = res
        .get("verification_uri_complete")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    Ok(BeginInfo {
        device_code,
        qr_url,
        interval: res.get("interval").and_then(|x| x.as_u64()).unwrap_or(5),
        expire_in: res.get("expire_in").and_then(|x| x.as_u64()).unwrap_or(600),
    })
}

struct PollOutcome {
    app_id: String,
    app_secret: String,
    domain: String,
    open_id: String,
}

/// 步骤 3：轮询直到扫码确认（pending 400 续询；lark 租户自动切域名）。
async fn poll_registration(
    client: &reqwest::Client,
    begin: &BeginInfo,
    initial_domain: &str,
    deadline: Instant,
) -> Result<PollOutcome> {
    let mut current_domain = initial_domain.to_string();
    let mut domain_switched = false;
    loop {
        if Instant::now() >= deadline {
            return Err(BridgeError::other("飞书注册轮询超时，请重新执行 /feishu"));
        }
        let res = match post_form(
            client,
            accounts_base_url(&current_domain),
            &[
                ("action", "poll"),
                ("device_code", &begin.device_code),
                ("tp", "ob_app"),
            ],
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!("飞书注册轮询失败（续询）: {e}");
                tokio::time::sleep(Duration::from_secs(begin.interval)).await;
                continue;
            }
        };

        // 域名自动检测：lark 租户 → 切 lark 域名（本响应可能同时带凭据，不提前返回）
        let user_info = res.get("user_info").cloned().unwrap_or(Value::Null);
        if user_info.get("tenant_brand").and_then(|x| x.as_str()) == Some("lark")
            && !domain_switched
        {
            current_domain = "lark".to_string();
            domain_switched = true;
        }

        let client_id = res.get("client_id").and_then(|x| x.as_str()).unwrap_or("");
        let client_secret = res
            .get("client_secret")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        if !client_id.is_empty() && !client_secret.is_empty() {
            return Ok(PollOutcome {
                app_id: client_id.to_string(),
                app_secret: client_secret.to_string(),
                domain: current_domain.clone(),
                open_id: user_info
                    .get("open_id")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string(),
            });
        }

        match res.get("error").and_then(|x| x.as_str()).unwrap_or("") {
            "" | "authorization_pending" => {}
            "access_denied" | "expired_token" => {
                return Err(BridgeError::other(format!(
                    "飞书注册被拒绝或过期（{}）",
                    res.get("error").and_then(|x| x.as_str()).unwrap_or("")
                )));
            }
            other => tracing::debug!("飞书注册未知状态 {other}，续询"),
        }
        tokio::time::sleep(Duration::from_secs(begin.interval)).await;
    }
}

/// 步骤 4（best-effort）：拿 bot 名称与 bot open_id。
async fn probe_bot(client: &reqwest::Client, creds: &PollOutcome) -> (String, String) {
    let base = open_base_url(&creds.domain);
    let token_resp = client
        .post(format!(
            "{}/open-apis/auth/v3/tenant_access_token/internal",
            base.trim_end_matches('/')
        ))
        .json(&serde_json::json!({
            "app_id": creds.app_id,
            "app_secret": creds.app_secret,
        }))
        .timeout(Duration::from_secs(ONBOARD_REQUEST_TIMEOUT_S))
        .send()
        .await;
    let token = match token_resp {
        Ok(r) => r.json::<Value>().await.ok().and_then(|v| {
            v.get("tenant_access_token")
                .and_then(|x| x.as_str())
                .map(String::from)
        }),
        Err(_) => None,
    };
    let Some(token) = token else {
        return (String::new(), String::new());
    };
    let bot_resp = client
        .get(format!(
            "{}/open-apis/bot/v3/info",
            base.trim_end_matches('/')
        ))
        .bearer_auth(token)
        .timeout(Duration::from_secs(ONBOARD_REQUEST_TIMEOUT_S))
        .send()
        .await;
    let info = match bot_resp {
        Ok(r) => r.json::<Value>().await.ok(),
        Err(_) => None,
    };
    if let Some(v) = info {
        if v.get("code").and_then(|x| x.as_i64()) == Some(0) {
            let bot = v.get("bot").cloned().unwrap_or(Value::Null);
            let name = bot
                .get("app_name")
                .or_else(|| bot.get("bot_name"))
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            let open_id = bot
                .get("open_id")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            return (name, open_id);
        }
    }
    (String::new(), String::new())
}

/// 完整扫码注册：展示二维码 → 轮询确认 → 落凭据。
pub async fn qr_register(
    client: &reqwest::Client,
    domain: &str,
    display: &QrDisplay,
) -> Result<FeishuCreds> {
    println!("正在连接飞书 / Lark...");
    init_registration(client, domain).await?;
    let begin = begin_registration(client, domain).await?;
    if begin.qr_url.is_empty() {
        return Err(BridgeError::other("飞书注册未返回扫码 URL"));
    }
    println!("\n请使用飞书 App 扫描以下二维码并确认授权：");
    display.show(&begin.qr_url, "").or_else(|e| {
        println!("（二维码渲染失败: {e}）");
        println!("请在飞书里打开此链接完成授权：\n{}", begin.qr_url);
        Ok::<(), BridgeError>(())
    })?;

    let deadline = Instant::now() + REGISTRATION_TIMEOUT.min(Duration::from_secs(begin.expire_in));
    let outcome = poll_registration(client, &begin, domain, deadline).await?;
    let (bot_name, bot_open_id) = probe_bot(client, &outcome).await;
    Ok(FeishuCreds {
        app_id: outcome.app_id,
        app_secret: outcome.app_secret,
        domain: outcome.domain,
        open_id: outcome.open_id,
        bot_name,
        bot_open_id,
    })
}
