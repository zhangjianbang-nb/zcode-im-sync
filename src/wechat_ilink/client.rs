//! iLink HTTP 客户端：带鉴权头的 GET/POST + `base_info` 注入 + ret/errcode 语义分类。
//!
//! 协议事实来自 Hermes-CN-Core（MIT）：POST body 顶层并
//! `base_info:{channel_version:"2.2.0"}`；头 `AuthorizationType: ilink_bot_token`、
//! `X-WECHAT-UIN: base64(str(rand u32))`、`Authorization: Bearer <bot_token>`。

use std::time::Duration;

use serde_json::{json, Value};

use super::{random_uin_b64, WeixinCreds, CHANNEL_VERSION, ILINK_APP_CLIENT_VERSION, ILINK_APP_ID};
use crate::error::{BridgeError, Result};

/// iLink API 应用层结果分类（ret/errcode 双通道，None 视为 0）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiOutcome {
    /// ret/errcode 均为 0 或缺失
    Ok,
    /// -14 会话过期，或 -2 + errmsg∈{unknown error, prepare failed}（stale 同处理）
    SessionExpired,
    /// 真限流（-2 且非 stale）
    RateLimited,
    /// 其他业务错误
    Failed { errmsg: String },
}

/// 响应分类：失败判定 = ret∉{0,None} 或 errcode∉{0,None}。
pub fn classify(resp: &Value) -> ApiOutcome {
    let ret = int_field(resp, "ret");
    let errcode = int_field(resp, "errcode");
    let failed = matches!(ret, Some(r) if r != 0) || matches!(errcode, Some(c) if c != 0);
    if !failed {
        return ApiOutcome::Ok;
    }
    let errmsg = resp
        .get("errmsg")
        .or_else(|| resp.get("msg"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if ret == Some(super::SESSION_EXPIRED_ERRCODE)
        || errcode == Some(super::SESSION_EXPIRED_ERRCODE)
    {
        return ApiOutcome::SessionExpired;
    }
    let is_rate =
        ret == Some(super::RATE_LIMIT_ERRCODE) || errcode == Some(super::RATE_LIMIT_ERRCODE);
    if is_rate {
        // stale 会话伪装成 -2 限流，按过期处理（evict + 无 token 重试）
        if errmsg == "unknown error" || errmsg == "prepare failed" {
            return ApiOutcome::SessionExpired;
        }
        return ApiOutcome::RateLimited;
    }
    ApiOutcome::Failed {
        errmsg: if errmsg.is_empty() {
            format!("ret={ret:?} errcode={errcode:?}")
        } else {
            errmsg
        },
    }
}

fn int_field(v: &Value, key: &str) -> Option<i64> {
    match v.get(key) {
        Some(Value::Number(n)) => n.as_i64(),
        Some(Value::String(s)) => s.trim().parse().ok(),
        _ => None,
    }
}

/// iLink HTTP 客户端（每个登录账号一个；poll 侧出错可整只重建）。
pub struct IlinkClient {
    http: reqwest::Client,
    creds: WeixinCreds,
    base_url: String,
}

impl IlinkClient {
    pub fn new(http: reqwest::Client, creds: WeixinCreds) -> Self {
        let base_url = creds.base_url.trim_end_matches('/').to_string();
        Self {
            http,
            creds,
            base_url,
        }
    }

    pub fn account_id(&self) -> &str {
        &self.creds.account_id
    }

    pub fn user_id(&self) -> &str {
        &self.creds.user_id
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// 轻量 GET（无 Authorization；二维码/状态接口用）。
    pub async fn get(&self, endpoint: &str, timeout: Duration) -> Result<Value> {
        let url = format!("{}/{}", self.base_url, endpoint.trim_start_matches('/'));
        let resp = self
            .http
            .get(&url)
            .header("iLink-App-Id", ILINK_APP_ID)
            .header(
                "iLink-App-ClientVersion",
                ILINK_APP_CLIENT_VERSION.to_string(),
            )
            .timeout(timeout)
            .send()
            .await
            .map_err(|e| net_err("GET", endpoint, e))?;
        read_json(resp, endpoint).await
    }

    /// 鉴权 POST：body 顶层并 `base_info`，带 token 鉴权头。
    pub async fn post(&self, endpoint: &str, payload: Value, timeout: Duration) -> Result<Value> {
        let mut body = payload;
        if let Some(obj) = body.as_object_mut() {
            obj.insert(
                "base_info".into(),
                json!({ "channel_version": CHANNEL_VERSION }),
            );
        }
        let text = serde_json::to_string(&body)?;
        let url = format!("{}/{}", self.base_url, endpoint.trim_start_matches('/'));
        let resp = self
            .http
            .post(&url)
            .header("Content-Type", "application/json")
            .header("AuthorizationType", "ilink_bot_token")
            .header("X-WECHAT-UIN", random_uin_b64())
            .header("iLink-App-Id", ILINK_APP_ID)
            .header(
                "iLink-App-ClientVersion",
                ILINK_APP_CLIENT_VERSION.to_string(),
            )
            .header("Authorization", format!("Bearer {}", self.creds.token))
            .timeout(timeout)
            .body(text)
            .send()
            .await
            .map_err(|e| net_err("POST", endpoint, e))?;
        read_json(resp, endpoint).await
    }
}

async fn read_json(resp: reqwest::Response, endpoint: &str) -> Result<Value> {
    let status = resp.status();
    let raw = resp
        .text()
        .await
        .map_err(|e| net_err("READ", endpoint, e))?;
    if !status.is_success() {
        return Err(BridgeError::other(format!(
            "iLink {endpoint} HTTP {status}: {}",
            raw.chars().take(200).collect::<String>()
        )));
    }
    serde_json::from_str(&raw)
        .map_err(|e| BridgeError::other(format!("iLink {endpoint} 响应非 JSON: {e}")))
}

/// 传输层错误映射：超时折叠成 `Io(TimedOut)`（长轮询空轮询需可判别），其余 Other。
fn net_err(verb: &str, endpoint: &str, e: reqwest::Error) -> BridgeError {
    if e.is_timeout() {
        BridgeError::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("iLink {verb} {endpoint} 超时: {e}"),
        ))
    } else {
        BridgeError::other(format!("iLink {verb} {endpoint} 失败: {e}"))
    }
}

/// 请求超时判定（长轮询 35s 超时 = 空结果，不是错误）。
pub fn is_timeout(e: &BridgeError) -> bool {
    matches!(e, BridgeError::Io(io) if io.kind() == std::io::ErrorKind::TimedOut)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_ok_when_missing_or_zero() {
        assert_eq!(classify(&json!({})), ApiOutcome::Ok);
        assert_eq!(classify(&json!({"ret": 0, "errcode": 0})), ApiOutcome::Ok);
        assert_eq!(classify(&json!({"ret": null})), ApiOutcome::Ok);
    }

    #[test]
    fn classify_expired_on_minus14_and_stale_minus2() {
        assert_eq!(classify(&json!({"ret": -14})), ApiOutcome::SessionExpired);
        assert_eq!(
            classify(&json!({"errcode": -14, "errmsg": "x"})),
            ApiOutcome::SessionExpired
        );
        assert_eq!(
            classify(&json!({"ret": -2, "errmsg": "unknown error"})),
            ApiOutcome::SessionExpired
        );
        assert_eq!(
            classify(&json!({"errcode": -2, "errmsg": "prepare failed"})),
            ApiOutcome::SessionExpired
        );
    }

    #[test]
    fn classify_real_rate_limit_on_minus2() {
        assert_eq!(
            classify(&json!({"ret": -2, "errmsg": "rate limited"})),
            ApiOutcome::RateLimited
        );
        assert_eq!(classify(&json!({"errcode": -2})), ApiOutcome::RateLimited);
    }

    #[test]
    fn classify_failed_keeps_errmsg() {
        match classify(&json!({"ret": 1, "errmsg": "boom"})) {
            ApiOutcome::Failed { errmsg } => assert_eq!(errmsg, "boom"),
            other => panic!("wrong: {other:?}"),
        }
        // 字符串型 ret 也认
        assert_eq!(
            classify(&json!({"ret": "-2", "errmsg": "unknown error"})),
            ApiOutcome::SessionExpired
        );
    }
}
