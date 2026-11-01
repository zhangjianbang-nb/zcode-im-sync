#!/usr/bin/env bash
# zcode-im-sync 安装器：桥常驻服务 + TUI 可拆卸钩子 + NODE_OPTIONS 接入。
#
# 用法：
#   ./install.sh                     # 默认装到 ~/.local/share/zcode-im-sync
#   ./install.sh --home /opt/im-sync # 自定义数据目录
#   ./install.sh --no-service        # 只装钩子和二进制，不起 systemd
#   ./install.sh --no-shell          # 不改 shell rc（自行设置 NODE_OPTIONS）
#   ./install.sh --skip-build        # 用已有 target/release/im-bridge
#
# 卸载：./uninstall.sh [--purge]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IM_SYNC_HOME="${IM_SYNC_HOME:-$HOME/.local/share/zcode-im-sync}"
BIN_DIR="${IM_SYNC_BIN_DIR:-$HOME/.local/bin}"
HOOK_SRC="$ROOT/tui/zcode-tui-im-sync.cjs"
HOOK_DST="$IM_SYNC_HOME/tui/zcode-tui-im-sync.cjs"
UNIT_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
UNIT="$UNIT_DIR/im-bridge.service"
WITH_SERVICE=1
WITH_SHELL=1
DO_BUILD=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --home) IM_SYNC_HOME="$2"; shift 2 ;;
    --bin-dir) BIN_DIR="$2"; shift 2 ;;
    --no-service) WITH_SERVICE=0; shift ;;
    --no-shell) WITH_SHELL=0; shift ;;
    --skip-build) DO_BUILD=0; shift ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "未知参数: $1" >&2; exit 2 ;;
  esac
done

HOOK_DST="$IM_SYNC_HOME/tui/zcode-tui-im-sync.cjs"
say() { printf '\033[1;36m[im-sync]\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m[im-sync] %s\033[0m\n' "$*" >&2; exit 1; }

command -v node >/dev/null || die "未找到 node（zcode TUI 需要 Node 22.15+）"
command -v cargo >/dev/null || die "未找到 cargo（构建 im-bridge 需要 Rust 1.95+）"

# 1) 构建
if [[ "$DO_BUILD" == 1 ]]; then
  say "构建 im-bridge（release）…"
  (cd "$ROOT" && cargo build --release)
fi
[[ -x "$ROOT/target/release/im-bridge" ]] || die "缺少 $ROOT/target/release/im-bridge"

# 2) 目录与二进制
say "安装到 $IM_SYNC_HOME"
mkdir -p "$IM_SYNC_HOME"/{state/im-inbox,state/im-events,logs,credentials,media,tui} "$BIN_DIR"
chmod 700 "$IM_SYNC_HOME/credentials"
install -m 755 "$ROOT/target/release/im-bridge" "$BIN_DIR/im-bridge"
install -m 644 "$HOOK_SRC" "$HOOK_DST"

# 3) 配置（不存在才生成，绝不覆盖用户已有配置）
CONFIG="$IM_SYNC_HOME/config.toml"
if [[ ! -f "$CONFIG" ]]; then
  ZCODE_BIN="$(command -v zcode || true)"
  [[ -n "$ZCODE_BIN" ]] || ZCODE_BIN="$HOME/.npm-global/bin/zcode"
  WORKDIR="$HOME/workspace/ai_workspace"
  [[ -d "$WORKDIR" ]] || WORKDIR="$HOME"
  cat > "$CONFIG" <<EOF
# zcode-im-sync 配置（由 install.sh 生成；凭证在 credentials/，勿提交到 git）
[general]
workdir_default = "$WORKDIR"
log_level = "info"
http_bind = "127.0.0.1:8106"

[whitelist]
# 扫码登录会自动把本人账号并入；也可手动填
wechat_user_ids = []
feishu_open_ids = []

[zcode]
bin = "$ZCODE_BIN"
turn_timeout_s = 600

[wechat]
base_url = "https://ilinkai.weixin.qq.com"
cdn_base_url = "https://novac2c.cdn.weixin.qq.com/c2c"

[feishu]
domain = "feishu"
EOF
  chmod 600 "$CONFIG"
  say "已生成 $CONFIG"
else
  say "保留已有配置 $CONFIG"
fi

# 4) systemd user service
if [[ "$WITH_SERVICE" == 1 ]]; then
  mkdir -p "$UNIT_DIR"
  cat > "$UNIT" <<EOF
[Unit]
Description=zcode-im-sync: 微信/飞书 ↔ zcode TUI 消息同步
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$IM_SYNC_HOME
ExecStart=$BIN_DIR/im-bridge --home $IM_SYNC_HOME run
Restart=always
RestartSec=5
Environment=PATH=$BIN_DIR:$HOME/.npm-global/bin:$HOME/.cargo/bin:/usr/local/bin:/usr/bin:/bin
Environment=RUST_LOG=info
Environment=IM_BRIDGE_HOME=$IM_SYNC_HOME
StandardOutput=append:$IM_SYNC_HOME/logs/service.log
StandardError=append:$IM_SYNC_HOME/logs/service.err
OOMScoreAdjust=-800

[Install]
WantedBy=default.target
EOF
  systemctl --user daemon-reload
  systemctl --user enable --now im-bridge
  say "im-bridge.service 已启用"
else
  say "跳过 systemd（--no-service）"
fi

# 5) shell rc 接入（幂等：先删旧块再写新块）
if [[ "$WITH_SHELL" == 1 ]]; then
  BEGIN="# >>> zcode-im-sync >>>"
  END="# <<< zcode-im-sync <<<"
  BLOCK=$(cat <<EOF
$BEGIN
# zcode TUI 消息同步钩子（只对 zcode TUI 生效；卸载运行 uninstall.sh）
if [ -f "$HOOK_DST" ]; then
  case "\${NODE_OPTIONS:-}" in
    *--require*"$HOOK_DST"*) ;;
    *) export NODE_OPTIONS="\${NODE_OPTIONS:+\$NODE_OPTIONS }--require $HOOK_DST" ;;
  esac
fi
$END
EOF
)
  for rc in "$HOME/.bashrc" "$HOME/.profile"; do
    [[ -f "$rc" ]] || continue
    python3 - "$rc" "$BEGIN" "$END" "$BLOCK" <<'PY'
import sys
path, begin, end, block = sys.argv[1:5]
try:
    src = open(path, encoding="utf-8").read()
except FileNotFoundError:
    src = ""
out, skip = [], False
for line in src.splitlines():
    if line.strip() == begin:
        skip = True
        continue
    if skip and line.strip() == end:
        skip = False
        continue
    if not skip:
        out.append(line)
text = "\n".join(out).rstrip() + "\n\n" + block + "\n"
open(path, "w", encoding="utf-8").write(text)
PY
    say "已更新 $rc（NODE_OPTIONS 钩子）"
  done
else
  say "跳过 shell rc（--no-shell）；请自行 export NODE_OPTIONS=--require $HOOK_DST"
fi

# 6) 自检提示
say "安装完成。新开一个终端（或 source ~/.bashrc）后启动 zcode TUI，敲 /wechat 扫码。"
say "状态：$BIN_DIR/im-bridge --home $IM_SYNC_HOME doctor"
say "健康：curl -s 127.0.0.1:8106/healthz"
say "卸载：$ROOT/uninstall.sh（加 --purge 连数据/凭证一起删）"
