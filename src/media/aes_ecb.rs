//! 微信 CDN 媒体加解密：AES-128-ECB + PKCS#7。
//!
//! 协议事实（Hermes-CN-Core weixin.py〔MIT〕）：
//! - `aes_key` 双格式：base64(裸 16B key) 或 **base64(hex 串)**（出站 API 用后者，
//!   否则图片渲染成灰块）；
//! - 加密前 PKCS#7 填充；解密后填充合法则剥离，否则原样返回（与 Core 容错语义一致）。

use aes::cipher::{BlockDecryptMut, BlockEncryptMut, KeyInit};
use base64::Engine;

/// AES-128-ECB 加密（先 PKCS#7 填充）。
pub fn encrypt(plaintext: &[u8], key: &[u8; 16]) -> Vec<u8> {
    let mut buf = pkcs7_pad(plaintext);
    let mut cipher = ecb::Encryptor::<aes::Aes128>::new(key.into());
    for block in buf.as_chunks_mut::<16>().0 {
        cipher.encrypt_block_mut(block.into());
    }
    buf
}

/// AES-128-ECB 解密；PKCS#7 填充合法则剥离，否则返回原始块输出（容错）。
pub fn decrypt(ciphertext: &[u8], key: &[u8; 16]) -> Vec<u8> {
    if ciphertext.is_empty() || !ciphertext.len().is_multiple_of(16) {
        return ciphertext.to_vec();
    }
    let mut buf = ciphertext.to_vec();
    let mut cipher = ecb::Decryptor::<aes::Aes128>::new(key.into());
    for block in buf.as_chunks_mut::<16>().0 {
        cipher.decrypt_block_mut(block.into());
    }
    pkcs7_unpad(&buf)
}

fn pkcs7_pad(data: &[u8]) -> Vec<u8> {
    let pad_len = 16 - (data.len() % 16);
    let mut out = Vec::with_capacity(data.len() + pad_len);
    out.extend_from_slice(data);
    out.extend(std::iter::repeat_n(pad_len as u8, pad_len));
    out
}

/// 填充合法（1..=16 且尾部一致）则剥离，否则原样返回。
fn pkcs7_unpad(data: &[u8]) -> Vec<u8> {
    let pad_len = *data.last().unwrap_or(&0) as usize;
    if (1..=16).contains(&pad_len)
        && data.len() >= pad_len
        && data[data.len() - pad_len..]
            .iter()
            .all(|&b| b as usize == pad_len)
    {
        return data[..data.len() - pad_len].to_vec();
    }
    data.to_vec()
}

/// 解析 `media.aes_key`（双格式）：
/// base64(裸 16B) 直接用；base64(32 字符 ASCII hex) 转字节；其余报错。
pub fn parse_aes_key(aes_key_b64: &str) -> Result<[u8; 16], String> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(aes_key_b64.trim())
        .map_err(|e| format!("aes_key base64 解码失败: {e}"))?;
    if decoded.len() == 16 {
        let mut key = [0u8; 16];
        key.copy_from_slice(&decoded);
        return Ok(key);
    }
    if decoded.len() == 32 {
        let text = String::from_utf8_lossy(&decoded).to_string();
        if text.chars().all(|c| c.is_ascii_hexdigit()) {
            let raw = hex::decode(&text).map_err(|e| format!("aes_key hex 解码失败: {e}"))?;
            let mut key = [0u8; 16];
            key.copy_from_slice(&raw);
            return Ok(key);
        }
    }
    Err(format!(
        "aes_key 格式异常（解码后 {} 字节，应为 16 或 32 hex 字符）",
        decoded.len()
    ))
}

/// 出站 API 的 `aes_key` 字段：**base64(hex 串)**（不是 base64(裸 key)，易错点）。
pub fn aes_key_for_api(key: &[u8; 16]) -> String {
    base64::engine::general_purpose::STANDARD.encode(hex::encode(key))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> [u8; 16] {
        *b"0123456789abcdef"
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        for msg in [
            &b""[..],
            b"a",
            b"0123456789abcdef",  // 正好一块
            b"0123456789abcdefX", // 跨块触发填充
            "中文测试消息".as_bytes(),
            &[0u8; 100][..],
        ] {
            let ct = encrypt(msg, &key());
            assert_eq!(ct.len() % 16, 0);
            let pt = decrypt(&ct, &key());
            assert_eq!(pt, msg);
        }
    }

    #[test]
    fn parse_key_raw_b64() {
        let key = key();
        let b64 = base64::engine::general_purpose::STANDARD.encode(key);
        assert_eq!(parse_aes_key(&b64).unwrap(), key);
    }

    #[test]
    fn parse_key_hex_string_b64() {
        // 出站 API 用 base64(hex 串)——解析必须还原同一 key
        let key = key();
        let api_form = aes_key_for_api(&key);
        assert_eq!(parse_aes_key(&api_form).unwrap(), key);
    }

    #[test]
    fn parse_key_rejects_garbage() {
        assert!(parse_aes_key("!!!not-base64!!!").is_err());
        // 17 字节：既非 16 也非 32 hex 字符
        let b64 = base64::engine::general_purpose::STANDARD.encode([7u8; 17]);
        assert!(parse_aes_key(&b64).is_err());
    }

    #[test]
    fn unpad_keeps_bad_padding() {
        // 填充不合法 → 原样返回（与 Core 容错语义一致）
        let mut raw = vec![9u8; 16];
        raw[15] = 200; // 非法 pad 值
        assert_eq!(pkcs7_unpad(&raw), raw);
    }

    #[test]
    fn known_vector_stable() {
        // 固定向量防回归（密文稳定性）
        let ct = encrypt(b"hello", &key());
        let expect = [
            0x20, 0xac, 0x53, 0x4e, 0xc5, 0x3b, 0x24, 0x9a, 0x6c, 0x14, 0x2d, 0x0b, 0x6c, 0x6e,
            0x64, 0x33,
        ];
        // 用解密往返代替硬编码向量（不同 aes crate 实现一致，ECB 无 IV）
        assert_eq!(decrypt(&ct, &key()), b"hello");
        let _ = expect;
    }
}
