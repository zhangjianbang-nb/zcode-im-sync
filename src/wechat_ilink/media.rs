//! 微信 iLink CDN 媒体：入站下载（AES-128-ECB 解密 + SSRF 白名单）+ 出站上传
//! （getuploadurl → 加密 POST → `x-encrypted-param` → item 形状）。
//!
//! 协议事实来源：Hermes-CN-Core `gateway/platforms/weixin.py`（MIT），只借协议不借代码。

use std::path::Path;
use std::time::Duration;

use base64::Engine;
use md5::{Digest, Md5};
use serde_json::{json, Value};

use super::{API_TIMEOUT_MS, EP_GET_UPLOAD_URL};
use crate::error::{BridgeError, Result};
use crate::media::{self, aes_ecb, MediaKind, SavedMedia, MAX_MEDIA_BYTES};

/// getuploadurl `media_type`：image/video/file/voice = 1/2/3/4。
pub const MEDIA_TYPE_IMAGE: i64 = 1;
pub const MEDIA_TYPE_VIDEO: i64 = 2;
pub const MEDIA_TYPE_FILE: i64 = 3;
pub const MEDIA_TYPE_VOICE: i64 = 4;

/// item_list `type`：text/image/voice/file/video = 1/2/3/4/5。
pub const ITEM_TEXT: i64 = 1;
pub const ITEM_IMAGE: i64 = 2;
pub const ITEM_VOICE: i64 = 3;
pub const ITEM_FILE: i64 = 4;
pub const ITEM_VIDEO: i64 = 5;

/// 下载 URL host 白名单（防 SSRF；Hermes 同款集合）。
pub const WEIXIN_CDN_ALLOWLIST: &[&str] = &[
    "novac2c.cdn.weixin.qq.com",
    "ilinkai.weixin.qq.com",
    "wx.qlogo.cn",
    "thirdwx.qlogo.cn",
    "res.wx.qq.com",
    "mmbiz.qpic.cn",
    "mmbiz.qlogo.cn",
];

/// 入站下载超时（秒）：image 30 / video 120 / file 60 / voice 60。
fn download_timeout_secs(kind: MediaKind) -> u64 {
    match kind {
        MediaKind::Image => 30,
        MediaKind::Video => 120,
        MediaKind::File | MediaKind::Voice => 60,
    }
}

// ── URL 安全校验 ──

/// 严格白名单校验（消息体自带的 `full_url` 可被构造 → 只认名单内 host）。
pub fn assert_cdn_url(url: &str) -> Result<()> {
    assert_url(url, false)
}

/// 宽松一档（getuploadurl 响应给出的上传 URL 来自腾讯 API，允许 `*.cdn.weixin.qq.com`）。
pub fn assert_upload_url(url: &str) -> Result<()> {
    assert_url(url, true)
}

fn assert_url(url: &str, allow_sub: bool) -> Result<()> {
    let u =
        reqwest::Url::parse(url).map_err(|e| BridgeError::other(format!("媒体 URL 非法: {e}")))?;
    if !matches!(u.scheme(), "http" | "https") {
        return Err(BridgeError::other(format!(
            "媒体 URL scheme 非法: {}",
            u.scheme()
        )));
    }
    let host = u.host_str().unwrap_or("");
    let ok = WEIXIN_CDN_ALLOWLIST.contains(&host)
        || (allow_sub && (host.ends_with(".cdn.weixin.qq.com") || host == "cdn.weixin.qq.com"));
    if !ok {
        return Err(BridgeError::other(format!(
            "媒体 URL host 不在白名单: {host}"
        )));
    }
    Ok(())
}

/// query 参数百分号编码（unreserved 之外全编码，同 Python `quote(s, safe='')`）。
pub fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

// ── 入站：下载 + 落盘 ──

/// 一条待下载媒体（从 item_list 抽出）。
#[derive(Debug, Clone)]
pub struct InboundMediaSpec {
    pub kind: MediaKind,
    /// `payload.media`（`encrypt_query_param` / `aes_key` / `full_url`）。
    pub media: Value,
    /// image_item 旁的裸 hex `aeskey`（优先于 media.aes_key）。
    pub sidecar_aeskey_hex: Option<String>,
    pub file_name: Option<String>,
}

/// 从 `item_list` 抽出全部媒体条目（媒体在类型化子对象或 item 自身）。
pub fn extract_media_specs(item_list: Option<&Value>) -> Vec<InboundMediaSpec> {
    let Some(items) = item_list.and_then(|x| x.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for item in items {
        let kind = match item.get("type").and_then(|x| x.as_i64()) {
            Some(ITEM_IMAGE) => MediaKind::Image,
            Some(ITEM_VOICE) => MediaKind::Voice,
            Some(ITEM_FILE) => MediaKind::File,
            Some(ITEM_VIDEO) => MediaKind::Video,
            _ => continue,
        };
        let payload = ["image_item", "voice_item", "file_item", "video_item"]
            .iter()
            .find_map(|k| item.get(*k))
            .unwrap_or(item);
        let Some(med) = payload
            .get("media")
            .or_else(|| item.get("media"))
            .filter(|m| !m.is_null())
        else {
            continue;
        };
        let sidecar = payload
            .get("aeskey")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let file_name = payload
            .get("file_name")
            .or_else(|| item.get("file_name"))
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        out.push(InboundMediaSpec {
            kind,
            media: med.clone(),
            sidecar_aeskey_hex: sidecar,
            file_name,
        });
    }
    out
}

/// 下载并解密单个媒体。
///
/// URL 来源：`encrypt_query_param` → `{cdn}/download?encrypted_query_param=…`；
/// 否则 `full_url`（严格白名单）；都没有 → 报错。有 aes_key 则 AES 解密，否则原文。
pub async fn download_media(
    http: &reqwest::Client,
    cdn_base_url: &str,
    spec: &InboundMediaSpec,
) -> Result<Vec<u8>> {
    let base = cdn_base_url.trim_end_matches('/');
    let url = if let Some(param) = spec
        .media
        .get("encrypt_query_param")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty())
    {
        format!(
            "{base}/download?encrypted_query_param={}",
            percent_encode(param)
        )
    } else if let Some(full) = spec
        .media
        .get("full_url")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty())
    {
        assert_cdn_url(full)?;
        full.to_string()
    } else {
        return Err(BridgeError::other(
            "媒体缺少 encrypt_query_param / full_url".to_string(),
        ));
    };

    let aes_key = resolve_aes_key(spec)?;

    let mut resp = http
        .get(&url)
        .timeout(Duration::from_secs(download_timeout_secs(spec.kind)))
        .send()
        .await
        .map_err(|e| BridgeError::other(format!("媒体下载失败: {e}")))?;
    if !resp.status().is_success() {
        return Err(BridgeError::other(format!(
            "媒体下载 HTTP {}",
            resp.status()
        )));
    }
    let body = read_capped(&mut resp, MAX_MEDIA_BYTES).await?;
    Ok(match aes_key {
        Some(k) => aes_ecb::decrypt(&body, &k),
        None => body,
    })
}

/// 解析 aes_key：image_item 旁裸 hex 旁路优先，其次 media.aes_key（双格式）。
fn resolve_aes_key(spec: &InboundMediaSpec) -> Result<Option<[u8; 16]>> {
    if let Some(hex_str) = spec.sidecar_aeskey_hex.as_deref() {
        let raw = hex::decode(hex_str.trim())
            .map_err(|e| BridgeError::other(format!("image_item.aeskey hex 解码失败: {e}")))?;
        let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
        return Ok(Some(
            aes_ecb::parse_aes_key(&b64).map_err(BridgeError::other)?,
        ));
    }
    if let Some(k) = spec
        .media
        .get("aes_key")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty())
    {
        return Ok(Some(aes_ecb::parse_aes_key(k).map_err(BridgeError::other)?));
    }
    Ok(None)
}

/// 入站媒体批量处理：逐条下载解密 → `media/<chat>/<date>/` 落盘（失败只记日志不致命）。
pub async fn collect_inbound(
    http: &reqwest::Client,
    cdn_base_url: &str,
    media_root: &Path,
    chat_key: &str,
    item_list: Option<&Value>,
) -> Vec<SavedMedia> {
    let mut saved = Vec::new();
    for spec in extract_media_specs(item_list) {
        match download_media(http, cdn_base_url, &spec).await {
            Ok(data) => {
                let name = spec
                    .file_name
                    .clone()
                    .unwrap_or_else(|| spec.kind.default_file_name().to_string());
                match media::save_media(media_root, chat_key, spec.kind, &name, &data) {
                    Ok(s) => {
                        tracing::info!(
                            "入站媒体落盘 {}（{}）→ {}",
                            s.file_name,
                            media::human_size(s.size as u64),
                            s.path.display()
                        );
                        saved.push(s);
                    }
                    Err(e) => tracing::warn!("入站媒体落盘失败: {e}"),
                }
            }
            Err(e) => tracing::warn!("入站媒体下载失败 kind={}: {e}", spec.kind.label()),
        }
    }
    saved
}

/// 分块读取 + 硬限额（Content-Length 先拒；chunk 累计超限即断，防媒体炸弹）。
async fn read_capped(resp: &mut reqwest::Response, cap: usize) -> Result<Vec<u8>> {
    if let Some(len) = resp.content_length() {
        if len > cap as u64 {
            return Err(BridgeError::other(format!(
                "媒体超过 {}MB 限额（声明 {len} 字节）",
                cap / 1024 / 1024
            )));
        }
    }
    let mut buf = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| BridgeError::other(format!("媒体读取失败: {e}")))?
    {
        if buf.len() + chunk.len() > cap {
            return Err(BridgeError::other(format!(
                "媒体超过 {}MB 限额",
                cap / 1024 / 1024
            )));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

// ── 出站：上传 + item 形状（一期出站只发文本，能力备用 + 形状快照测试）──

/// 语音 item 属性（.silk：encode_type=6, sample_rate=24000, bits_per_sample=16）。
#[derive(Debug, Clone)]
pub struct VoiceAttrs {
    pub encode_type: i64,
    pub bits_per_sample: i64,
    pub sample_rate: i64,
    pub playtime: i64,
}

impl Default for VoiceAttrs {
    fn default() -> Self {
        Self {
            encode_type: 6,
            bits_per_sample: 16,
            sample_rate: 24000,
            playtime: 0,
        }
    }
}

/// 出站媒体元信息（item 形状装配用）。
#[derive(Debug, Clone)]
pub struct OutboundMeta<'a> {
    pub file_name: &'a str,
    pub plaintext_size: usize,
    pub ciphertext_size: usize,
    pub rawfilemd5: &'a str,
    pub play_length: i64,
    pub voice: Option<VoiceAttrs>,
}

/// item.media 块。**`aes_key` = base64(hex 串)**（不是 base64(裸 key)，错则图片灰块）。
pub fn media_block(encrypt_query_param: &str, aes_key: &[u8; 16]) -> Value {
    json!({
        "encrypt_query_param": encrypt_query_param,
        "aes_key": aes_ecb::aes_key_for_api(aes_key),
        "encrypt_type": 1,
    })
}

/// getuploadurl 请求体（Hermes 同款字段）。
pub fn getuploadurl_body(
    filekey: &str,
    media_type: i64,
    to_user_id: &str,
    plaintext: &[u8],
    rawfilemd5: &str,
    aeskey_hex: &str,
) -> Value {
    let rawsize = plaintext.len();
    let filesize = ciphertext_size(rawsize);
    json!({
        "filekey": filekey,
        "media_type": media_type,
        "to_user_id": to_user_id,
        "rawsize": rawsize,
        "rawfilemd5": rawfilemd5,
        "filesize": filesize,
        "no_need_thumb": true,
        "aeskey": aeskey_hex,
    })
}

/// 密文尺寸（PKCS#7 填充后）：`((rawsize + 16) / 16) * 16`。
pub fn ciphertext_size(rawsize: usize) -> usize {
    (rawsize + 16) / 16 * 16
}

/// 装配 item_list 条目（四种媒体形状，Hermes `_ITEM_SHAPES` 同款）。
pub fn build_media_item(item_type: i64, media: &Value, meta: &OutboundMeta<'_>) -> Result<Value> {
    let item = match item_type {
        ITEM_IMAGE => json!({"type": ITEM_IMAGE, "image_item": {
            "media": media,
            "mid_size": meta.ciphertext_size,
        }}),
        ITEM_VOICE => {
            let v = meta.voice.clone().unwrap_or_default();
            json!({"type": ITEM_VOICE, "voice_item": {
                "media": media,
                "encode_type": v.encode_type,
                "bits_per_sample": v.bits_per_sample,
                "sample_rate": v.sample_rate,
                "playtime": v.playtime,
            }})
        }
        ITEM_FILE => json!({"type": ITEM_FILE, "file_item": {
            "media": media,
            "file_name": meta.file_name,
            "len": meta.plaintext_size.to_string(),
        }}),
        ITEM_VIDEO => json!({"type": ITEM_VIDEO, "video_item": {
            "media": media,
            "video_size": meta.ciphertext_size,
            "play_length": meta.play_length,
            "video_md5": meta.rawfilemd5,
        }}),
        other => {
            return Err(BridgeError::other(format!(
                "不支持的媒体 item type: {other}"
            )))
        }
    };
    Ok(item)
}

/// kind → getuploadurl media_type。
pub fn media_type_of(kind: MediaKind) -> i64 {
    match kind {
        MediaKind::Image => MEDIA_TYPE_IMAGE,
        MediaKind::Video => MEDIA_TYPE_VIDEO,
        MediaKind::File => MEDIA_TYPE_FILE,
        MediaKind::Voice => MEDIA_TYPE_VOICE,
    }
}

/// kind → item_list type。
pub fn item_type_of(kind: MediaKind) -> i64 {
    match kind {
        MediaKind::Image => ITEM_IMAGE,
        MediaKind::Video => ITEM_VIDEO,
        MediaKind::File => ITEM_FILE,
        MediaKind::Voice => ITEM_VOICE,
    }
}

/// 出站上传请求（文件名/明文/语音时长打包，免参数爆炸）。
#[derive(Debug, Clone, Copy)]
pub struct UploadReq<'a> {
    pub kind: MediaKind,
    pub file_name: &'a str,
    pub plaintext: &'a [u8],
    /// 语音/视频时长（秒）；其余忽略。
    pub play_length: i64,
}

/// 上传全流程：AES 加密 → getuploadurl → POST 密文 → 取 `x-encrypted-param` → 返回 item。
pub async fn upload_media(
    client: &super::client::IlinkClient,
    http: &reqwest::Client,
    cdn_base_url: &str,
    to_user_id: &str,
    req: &UploadReq<'_>,
) -> Result<Value> {
    let UploadReq {
        kind,
        file_name,
        plaintext,
        play_length,
    } = *req;
    if plaintext.is_empty() {
        return Err(BridgeError::other("空文件不上载"));
    }
    if plaintext.len() > MAX_MEDIA_BYTES {
        return Err(BridgeError::other(format!(
            "媒体超过 {}MB 限额",
            MAX_MEDIA_BYTES / 1024 / 1024
        )));
    }

    let filekey = uuid::Uuid::new_v4().simple().to_string(); // 32 hex，同 token_hex(16)
    let mut aes_key = [0u8; 16];
    aes_key.copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    let rawfilemd5 = hex::encode(Md5::digest(plaintext));
    let ciphertext = aes_ecb::encrypt(plaintext, &aes_key);

    let body = getuploadurl_body(
        &filekey,
        media_type_of(kind),
        to_user_id,
        plaintext,
        &rawfilemd5,
        &hex::encode(aes_key),
    );
    let resp = client
        .post(
            EP_GET_UPLOAD_URL,
            body,
            Duration::from_millis(API_TIMEOUT_MS),
        )
        .await?;

    let base = cdn_base_url.trim_end_matches('/');
    let upload_url = if let Some(u) = resp
        .get("upload_full_url")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty())
    {
        assert_upload_url(u)?;
        u.to_string()
    } else if let Some(param) = resp
        .get("upload_param")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty())
    {
        format!(
            "{base}/upload?encrypted_query_param={}&filekey={}",
            percent_encode(param),
            percent_encode(&filekey)
        )
    } else {
        return Err(BridgeError::other(
            "getuploadurl 响应缺少 upload_full_url/upload_param".to_string(),
        ));
    };

    let up = http
        .post(&upload_url)
        .header("Content-Type", "application/octet-stream")
        .body(ciphertext.clone())
        .timeout(Duration::from_secs(120))
        .send()
        .await
        .map_err(|e| BridgeError::other(format!("媒体上传失败: {e}")))?;
    if !up.status().is_success() {
        return Err(BridgeError::other(format!("媒体上传 HTTP {}", up.status())));
    }
    let param = up
        .headers()
        .get("x-encrypted-param")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| BridgeError::other("上传响应缺少 x-encrypted-param".to_string()))?;

    let media = media_block(param, &aes_key);
    let meta = OutboundMeta {
        file_name,
        plaintext_size: plaintext.len(),
        ciphertext_size: ciphertext.len(),
        rawfilemd5: &rawfilemd5,
        play_length,
        voice: (kind == MediaKind::Voice).then(VoiceAttrs::default),
    };
    build_media_item(item_type_of(kind), &media, &meta)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_encode_quote_semantics() {
        assert_eq!(percent_encode("abc-_.~"), "abc-_.~");
        assert_eq!(percent_encode("a b/c?d=e"), "a%20b%2Fc%3Fd%3De");
        assert_eq!(percent_encode("中文"), "%E4%B8%AD%E6%96%87");
        assert_eq!(percent_encode(""), "");
    }

    #[test]
    fn cdn_url_allowlist() {
        assert!(assert_cdn_url("https://novac2c.cdn.weixin.qq.com/c2c/download?x=1").is_ok());
        assert!(assert_cdn_url("https://evil.example.com/c2c").is_err());
        assert!(assert_cdn_url("file:///etc/passwd").is_err());
        // 子域仅上传口放宽
        assert!(assert_cdn_url("https://foo.cdn.weixin.qq.com/u").is_err());
        assert!(assert_upload_url("https://foo.cdn.weixin.qq.com/u").is_ok());
    }

    #[test]
    fn extract_specs_typed_subobject_and_sidecar() {
        let items = json!([
            {"type": 2, "image_item": {
                "media": {"encrypt_query_param": "p", "aes_key": "a2V5a2V5a2V5a2V5a2V5a2V5a2V5a2V5", "encrypt_type": 1},
                "aeskey": "00112233445566778899aabbccddeeff"
            }},
            {"type": 3, "voice_item": {"media": {"full_url": "https://novac2c.cdn.weixin.qq.com/v.silk"}, "text": "asr"}},
            {"type": 4, "file_item": {"media": {"encrypt_query_param": "q"}, "file_name": "报告.pdf"}},
            {"type": 5, "video_item": {}},
            {"type": 1, "text_item": {"text": "hi"}}
        ]);
        let specs = extract_media_specs(Some(&items));
        assert_eq!(specs.len(), 3); // video 无 media、text 跳过
        assert_eq!(specs[0].kind, MediaKind::Image);
        assert_eq!(
            specs[0].sidecar_aeskey_hex.as_deref(),
            Some("00112233445566778899aabbccddeeff")
        );
        assert_eq!(specs[1].kind, MediaKind::Voice);
        assert_eq!(specs[2].kind, MediaKind::File);
        assert_eq!(specs[2].file_name.as_deref(), Some("报告.pdf"));
    }

    #[test]
    fn extract_specs_media_on_item_itself() {
        let items = json!([{"type": 2, "media": {"full_url": "https://wx.qlogo.cn/a.png"}}]);
        let specs = extract_media_specs(Some(&items));
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].kind, MediaKind::Image);
    }

    #[test]
    fn sidecar_hex_key_resolves() {
        let spec = InboundMediaSpec {
            kind: MediaKind::Image,
            media: json!({"aes_key": "ignored"}),
            sidecar_aeskey_hex: Some("000102030405060708090a0b0c0d0e0f".into()),
            file_name: None,
        };
        let k = resolve_aes_key(&spec).unwrap().unwrap();
        assert_eq!(k[0], 0x00);
        assert_eq!(k[15], 0x0f);
    }

    #[test]
    fn media_block_uses_hex_string_b64() {
        let key = *b"0123456789abcdef";
        let block = media_block("EP", &key);
        // aes_key 必须是 base64(hex 串)——回解同一 key
        let parsed = aes_ecb::parse_aes_key(block["aes_key"].as_str().unwrap()).unwrap();
        assert_eq!(parsed, key);
        assert_eq!(block["encrypt_type"], 1);
        assert_eq!(block["encrypt_query_param"], "EP");
    }

    #[test]
    fn getuploadurl_body_shape() {
        let body = getuploadurl_body("fk", MEDIA_TYPE_IMAGE, "u1", b"abc", "md5hex", "keyhex");
        assert_eq!(body["filekey"], "fk");
        assert_eq!(body["media_type"], MEDIA_TYPE_IMAGE);
        assert_eq!(body["to_user_id"], "u1");
        assert_eq!(body["rawsize"], 3);
        assert_eq!(body["rawfilemd5"], "md5hex");
        assert_eq!(body["filesize"], ciphertext_size(3));
        assert_eq!(body["no_need_thumb"], true);
        assert_eq!(body["aeskey"], "keyhex");
    }

    #[test]
    fn ciphertext_size_formula() {
        // PKCS#7 对齐块也补一整块：rawsize=16 → 32
        assert_eq!(ciphertext_size(0), 16);
        assert_eq!(ciphertext_size(16), 32);
        assert_eq!(ciphertext_size(17), 32);
        assert_eq!(ciphertext_size(100), 112);
    }

    #[test]
    fn media_item_shapes_snapshot() {
        let mb = media_block("EP", &[0u8; 16]);
        let meta = OutboundMeta {
            file_name: "报告.pdf",
            plaintext_size: 100,
            ciphertext_size: 112,
            rawfilemd5: "abc123",
            play_length: 12,
            voice: None,
        };
        // FILE：len 是**字符串**的明文尺寸
        let f = build_media_item(ITEM_FILE, &mb, &meta).unwrap();
        assert_eq!(f["type"], ITEM_FILE);
        assert_eq!(f["file_item"]["file_name"], "报告.pdf");
        assert_eq!(f["file_item"]["len"], "100");
        // IMAGE：mid_size = 密文尺寸
        let i = build_media_item(ITEM_IMAGE, &mb, &meta).unwrap();
        assert_eq!(i["image_item"]["mid_size"], 112);
        // VIDEO：video_size + video_md5 + play_length
        let v = build_media_item(ITEM_VIDEO, &mb, &meta).unwrap();
        assert_eq!(v["video_item"]["video_size"], 112);
        assert_eq!(v["video_item"]["video_md5"], "abc123");
        assert_eq!(v["video_item"]["play_length"], 12);
        // VOICE：默认 .silk 属性
        let mut m = meta.clone();
        m.voice = Some(VoiceAttrs {
            playtime: 5,
            ..Default::default()
        });
        let vo = build_media_item(ITEM_VOICE, &mb, &m).unwrap();
        assert_eq!(vo["voice_item"]["encode_type"], 6);
        assert_eq!(vo["voice_item"]["bits_per_sample"], 16);
        assert_eq!(vo["voice_item"]["sample_rate"], 24000);
        assert_eq!(vo["voice_item"]["playtime"], 5);
        // 非媒体 type 拒绝
        assert!(build_media_item(ITEM_TEXT, &mb, &meta).is_err());
    }

    #[test]
    fn kind_maps_to_consts() {
        assert_eq!(media_type_of(MediaKind::Image), MEDIA_TYPE_IMAGE);
        assert_eq!(media_type_of(MediaKind::Voice), MEDIA_TYPE_VOICE);
        assert_eq!(item_type_of(MediaKind::File), ITEM_FILE);
        assert_eq!(item_type_of(MediaKind::Video), ITEM_VIDEO);
    }
}
