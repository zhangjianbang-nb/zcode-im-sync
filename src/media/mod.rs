//! 媒体落盘、限额与 prompt 注入（微信/飞书共用）。
//!
//! 入站媒体统一策略：下载 → `media/<chat>/<YYYY-MM-DD>/` 落盘 → **绝对路径注入 prompt**
//! （zcode 全能力，自行读取文件/图片）；≤2MB 合法 UTF-8 纯文本文件额外内联内容。
//! 出站一期只发文本（媒体上传能力见 `wechat_ilink::media`）。
//!
//! 限额：单文件 20MB；`media/` 目录软限额 5GB——**只告警不自动删**（删文件须人工确认）。

pub mod aes_ecb;

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{BridgeError, Result};

/// 单个媒体文件上限（下载/上传同口径）。
pub const MAX_MEDIA_BYTES: usize = 20 * 1024 * 1024;
/// ≤2MB 合法 UTF-8 纯文本文件：内容额外内联进 prompt。
pub const MAX_INLINE_TEXT_BYTES: usize = 2 * 1024 * 1024;
/// `media/` 目录软限额（超限只 warn，不自动删）。
pub const MEDIA_DIR_QUOTA_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// 媒体类别（决定落盘默认名/扩展名与下载超时）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaKind {
    Image,
    Video,
    File,
    Voice,
}

impl MediaKind {
    pub fn label(&self) -> &'static str {
        match self {
            MediaKind::Image => "image",
            MediaKind::Video => "video",
            MediaKind::File => "file",
            MediaKind::Voice => "voice",
        }
    }

    /// 无名媒体的默认文件名（与 Hermes-CN-Core 缓存命名同款）。
    pub fn default_file_name(&self) -> &'static str {
        match self {
            MediaKind::Image => "image.jpg",
            MediaKind::Video => "video.mp4",
            MediaKind::File => "document.bin",
            MediaKind::Voice => "voice.silk",
        }
    }
}

/// 已落盘媒体。
#[derive(Debug, Clone)]
pub struct SavedMedia {
    pub path: PathBuf,
    pub kind: MediaKind,
    pub file_name: String,
    pub size: usize,
    /// ≤2MB 合法 UTF-8 纯文本文件的内联内容（其余为 None）。
    pub inline_text: Option<String>,
}

/// 文件名消毒：取路径末段（防目录穿越），非法字符→`_`，去前导点，空名回退 `document.bin`，长度封顶 120。
pub fn sanitize_file_name(name: &str) -> String {
    // 只取最后一个路径段（`/` `\` 均视为分隔符），`../../etc/passwd` → `passwd`
    let base = name
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or("")
        .trim()
        .to_string();
    let mut cleaned: String = base
        .chars()
        .map(|c| {
            // 保留 Unicode 字母数字（中文文件名可用）+ 少量安全符号；控制字符等一律 `_`
            if c.is_alphanumeric() || matches!(c, '-' | '_' | '.' | ' ') {
                c
            } else {
                '_'
            }
        })
        .collect();
    while cleaned.starts_with('.') {
        cleaned.remove(0);
    }
    cleaned = cleaned.trim().to_string();
    if cleaned.is_empty() {
        return "document.bin".to_string();
    }
    if cleaned.chars().count() > 120 {
        cleaned = cleaned.chars().take(120).collect();
    }
    cleaned
}

/// chat 键 → 目录名（`wechat:dm:u1` → `wechat_dm_u1`）。
pub fn chat_dir_name(chat_key: &str) -> String {
    let mapped: String = chat_key
        .chars()
        .map(|c| if c == ':' { '_' } else { c })
        .collect();
    let trimmed = mapped.trim_matches('_').to_string();
    if trimmed.is_empty() {
        "chat".to_string()
    } else {
        trimmed
    }
}

/// 落盘：`media/<chat>/<YYYY-MM-DD>/<file_name>`（重名自动 `-1`/`-2` 去撞）。
/// 纯文本 ≤2MB 的 File 类额外内联内容。
pub fn save_media(
    root: &Path,
    chat_key: &str,
    kind: MediaKind,
    file_name: &str,
    data: &[u8],
) -> Result<SavedMedia> {
    if data.len() > MAX_MEDIA_BYTES {
        return Err(BridgeError::other(format!(
            "媒体超过 {}MB 限额（{} 字节）",
            MAX_MEDIA_BYTES / 1024 / 1024,
            data.len()
        )));
    }
    let dir = root.join(chat_dir_name(chat_key)).join(today_utc());
    std::fs::create_dir_all(&dir)?;

    let base = sanitize_file_name(file_name);
    let path = unique_path(&dir, &base);
    std::fs::write(&path, data)?;

    let inline_text = if kind == MediaKind::File
        && data.len() <= MAX_INLINE_TEXT_BYTES
        && std::str::from_utf8(data).is_ok()
    {
        Some(String::from_utf8_lossy(data).to_string())
    } else {
        None
    };

    Ok(SavedMedia {
        file_name: base,
        path,
        kind,
        size: data.len(),
        inline_text,
    })
}

/// 把附件列表拼进 prompt（原样保留用户文本；无附件返回原 text）。
///
/// 格式：
/// ```text
/// {text}
///
/// [附件 image] photo.jpg（12.3 KB）→ /abs/path/photo.jpg
/// [附件 file] notes.md（1.0 KB）→ /abs/path/notes.md
/// --- 附件文件 notes.md 内容开始 ---
/// ...
/// --- 附件文件 notes.md 内容结束 ---
/// ```
pub fn compose_prompt(text: &str, attachments: &[SavedMedia]) -> String {
    if attachments.is_empty() {
        return text.to_string();
    }
    let mut out = text.trim_end().to_string();
    if !out.is_empty() {
        out.push_str("\n\n");
    }
    for (i, a) in attachments.iter().enumerate() {
        out.push_str(&format!(
            "[附件 {}] {}（{}）→ {}\n",
            i + 1,
            a.file_name,
            human_size(a.size as u64),
            a.path.display()
        ));
        if let Some(content) = &a.inline_text {
            out.push_str(&format!(
                "--- 附件文件 {} 内容开始 ---\n{}\n--- 附件文件 {} 内容结束 ---\n",
                a.file_name, content, a.file_name
            ));
        }
    }
    out.trim_end().to_string()
}

/// 目录用量巡检：返回字节数；超软限额只 warn（不自动删，删文件须人工确认）。
pub fn check_quota(root: &Path) -> u64 {
    let total = dir_size(root);
    if total > MEDIA_DIR_QUOTA_BYTES {
        tracing::warn!(
            "media/ 目录 {} 已用 {}（软限额 {}），请人工清理最旧文件",
            root.display(),
            human_size(total),
            human_size(MEDIA_DIR_QUOTA_BYTES)
        );
    }
    total
}

/// 人读大小。
pub fn human_size(n: u64) -> String {
    if n < 1024 {
        format!("{n} B")
    } else if n < 1024 * 1024 {
        format!("{:.1} KB", n as f64 / 1024.0)
    } else if n < 1024 * 1024 * 1024 {
        format!("{:.1} MB", n as f64 / 1024.0 / 1024.0)
    } else {
        format!("{:.2} GB", n as f64 / 1024.0 / 1024.0 / 1024.0)
    }
}

/// UTC 当天 `YYYY-MM-DD`（civil_from_days，免 chrono 依赖）。
pub fn today_utc() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = (secs / 86_400) as i64;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

/// 重名自动加 `-1`/`-2` 后缀（扩展名保留在尾部）。
fn unique_path(dir: &Path, file_name: &str) -> PathBuf {
    let candidate = dir.join(file_name);
    if !candidate.exists() {
        return candidate;
    }
    let (stem, ext) = match file_name.rsplit_once('.') {
        Some((s, e)) if !s.is_empty() => (s.to_string(), Some(e.to_string())),
        _ => (file_name.to_string(), None),
    };
    for i in 1..1000 {
        let name = match &ext {
            Some(e) => format!("{stem}-{i}.{e}"),
            None => format!("{stem}-{i}"),
        };
        let p = dir.join(&name);
        if !p.exists() {
            return p;
        }
    }
    dir.join(format!("{file_name}.{}", uuid::Uuid::new_v4().simple()))
}

fn dir_size(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    let mut total = 0u64;
    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
            total = total.saturating_add(dir_size(&p));
        } else if let Ok(meta) = entry.metadata() {
            total = total.saturating_add(meta.len());
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_blocks_traversal() {
        // 取末段 + 去前导点，目录穿越一律失效
        assert_eq!(sanitize_file_name("../../etc/passwd"), "passwd");
        assert_eq!(sanitize_file_name("..\\..\\win"), "win");
        assert_eq!(sanitize_file_name("a/b/c.txt"), "c.txt");
        assert_eq!(sanitize_file_name(""), "document.bin");
        assert_eq!(sanitize_file_name("..."), "document.bin");
        assert_eq!(sanitize_file_name("照片 1.jpg"), "照片 1.jpg");
        let long = "a".repeat(300);
        assert_eq!(sanitize_file_name(&long).chars().count(), 120);
    }

    #[test]
    fn chat_dir_maps_colons() {
        assert_eq!(chat_dir_name("wechat:dm:u1"), "wechat_dm_u1");
        assert_eq!(chat_dir_name("feishu:dm:ou_x"), "feishu_dm_ou_x");
        assert_eq!(chat_dir_name(":::"), "chat");
    }

    #[test]
    fn today_utc_shape() {
        let s = today_utc();
        assert_eq!(s.len(), 10);
        assert_eq!(s.as_bytes()[4], b'-');
        assert_eq!(s.as_bytes()[7], b'-');
    }

    #[test]
    fn save_media_writes_and_dedups() {
        let tmp = tempfile::tempdir().unwrap();
        let a = save_media(
            tmp.path(),
            "wechat:dm:u1",
            MediaKind::File,
            "note.txt",
            b"hello",
        )
        .unwrap();
        assert!(a.path.exists());
        assert_eq!(a.size, 5);
        assert_eq!(a.inline_text.as_deref(), Some("hello"));
        let b = save_media(
            tmp.path(),
            "wechat:dm:u1",
            MediaKind::File,
            "note.txt",
            b"world",
        )
        .unwrap();
        assert_ne!(a.path, b.path);
        assert!(b.path.to_string_lossy().contains("note-1.txt"));
    }

    #[test]
    fn save_media_rejects_oversize_and_binary_inline() {
        let tmp = tempfile::tempdir().unwrap();
        let big = vec![0u8; MAX_MEDIA_BYTES + 1];
        assert!(save_media(tmp.path(), "c", MediaKind::File, "big.bin", &big).is_err());
        // 非 UTF-8 不内联
        let s = save_media(tmp.path(), "c", MediaKind::File, "x.bin", &[0xff, 0xfe]).unwrap();
        assert!(s.inline_text.is_none());
        // 图片即使合法 UTF-8 也不内联
        let img = save_media(tmp.path(), "c", MediaKind::Image, "a.jpg", b"xx").unwrap();
        assert!(img.inline_text.is_none());
    }

    #[test]
    fn compose_prompt_injects_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let s = save_media(tmp.path(), "c", MediaKind::File, "a.txt", b"abc").unwrap();
        let p = compose_prompt("看看这个", &[s]);
        assert!(p.starts_with("看看这个"));
        assert!(p.contains("[附件 1] a.txt"));
        assert!(p.contains("--- 附件文件 a.txt 内容开始 ---"));
        assert!(p.contains("abc"));
        assert_eq!(compose_prompt("纯文本", &[]), "纯文本");
    }

    #[test]
    fn human_size_formats() {
        assert_eq!(human_size(100), "100 B");
        assert_eq!(human_size(2048), "2.0 KB");
        assert_eq!(human_size(3 * 1024 * 1024), "3.0 MB");
        assert_eq!(human_size(2 * 1024 * 1024 * 1024), "2.00 GB");
    }

    #[test]
    fn unique_path_suffixes_on_collision() {
        let tmp = tempfile::tempdir().unwrap();
        let p1 = unique_path(tmp.path(), "a.tar.gz");
        std::fs::write(&p1, b"").unwrap();
        let p2 = unique_path(tmp.path(), "a.tar.gz");
        assert!(p2.to_string_lossy().ends_with("a.tar-1.gz"));
    }
}
