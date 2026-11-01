//! 二维码渲染：终端字符画 + PNG 落盘 + 桌面弹窗（扫码登录用）。

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use qrcode::QrCode;

use crate::error::{BridgeError, Result};

/// 终端字符画二维码（半格字符，UTF-8 终端通用）。
pub fn render_terminal(data: &str) -> Result<String> {
    let code = QrCode::new(data.as_bytes())
        .map_err(|e| BridgeError::other(format!("二维码内容非法: {e}")))?;
    Ok(code
        .render::<qrcode::render::unicode::Dense1x2>()
        .quiet_zone(true)
        .build())
}

/// 生成灰度 PNG 二维码；`px` 为最小边长（实际按模块对齐取整）。
pub fn render_png(data: &str, out: &Path, px: u32) -> Result<()> {
    let code = QrCode::new(data.as_bytes())
        .map_err(|e| BridgeError::other(format!("二维码内容非法: {e}")))?;
    let colors = code.to_colors();
    let dim = code.width();
    let quiet = 4usize;
    let scale = ((px as usize) / (dim + 2 * quiet)).max(3);
    let total = (dim + 2 * quiet) * scale;
    let mut buf = vec![255u8; total * total];
    for y in 0..dim {
        for x in 0..dim {
            if matches!(colors[y * dim + x], qrcode::Color::Dark) {
                for dy in 0..scale {
                    for dx in 0..scale {
                        let bx = (x + quiet) * scale + dx;
                        let by = (y + quiet) * scale + dy;
                        buf[by * total + bx] = 0;
                    }
                }
            }
        }
    }
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(out)?;
    let mut encoder = png::Encoder::new(file, total as u32, total as u32);
    encoder.set_color(png::ColorType::Grayscale);
    encoder.set_depth(png::BitDepth::Eight);
    let mut writer = encoder
        .write_header()
        .map_err(|e| BridgeError::other(format!("PNG 编码失败: {e}")))?;
    writer
        .write_image_data(&buf)
        .map_err(|e| BridgeError::other(format!("PNG 写入失败: {e}")))?;
    Ok(())
}

/// 用桌面图片查看器打开二维码 PNG（DISPLAY 下）；打不开只告警，不阻塞登录。
pub fn open_image(path: &Path) {
    if std::env::var_os("DISPLAY").is_none() {
        tracing::debug!("DISPLAY 未设置，跳过二维码弹窗");
        return;
    }
    for viewer in ["xdg-open", "eog", "gio"] {
        let mut cmd = Command::new(viewer);
        if viewer == "gio" {
            cmd.arg("open");
        }
        match cmd
            .arg(path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(_) => {
                tracing::info!("已用 {viewer} 打开二维码 {}", path.display());
                return;
            }
            Err(e) => tracing::debug!("{viewer} 启动失败: {e}"),
        }
    }
    tracing::warn!("无可用图片查看器，请手动打开 {}", path.display());
}

/// 登录二维码的统一呈现：可选终端字符画 + 可选 PNG 落盘/弹窗。
pub struct QrDisplay {
    pub inline: bool,
    pub open: bool,
    pub png_path: PathBuf,
}

impl QrDisplay {
    pub fn show(&self, data: &str, hint: &str) -> Result<()> {
        if self.open {
            render_png(data, &self.png_path, 480)?;
            open_image(&self.png_path);
        }
        if self.inline {
            println!("{}", render_terminal(data)?);
        }
        if self.open {
            println!("（二维码图片: {}）", self.png_path.display());
        }
        if !self.open && !self.inline {
            println!("{data}");
        }
        println!("{hint}");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn png_roundtrip_writes_file() {
        let dir = tempdir().unwrap();
        let out = dir.path().join("qr.png");
        render_png("https://example.com/qr-test", &out, 240).unwrap();
        let bytes = std::fs::read(&out).unwrap();
        assert_eq!(&bytes[..8], b"\x89PNG\r\n\x1a\n");
    }

    #[test]
    fn terminal_render_nonempty() {
        let s = render_terminal("hello-qr").unwrap();
        assert!(s.lines().count() >= 3);
    }
}
