#!/usr/bin/env bash
# zcode-im-sync 卸载器：停服务、摘 NODE_OPTIONS 钩子、删二进制。
# 默认保留 config.toml / credentials/ / state/ / logs/ / media/（可重新安装续用）。
#   ./uninstall.sh           # 保留用户数据
#   ./uninstall.sh --purge   # 连数据目录和凭证一起删除
set -euo pipefail

IM_SYNC_HOME="${IM_SYNC_HOME:-$HOME/.local/share/zcode-im-sync}"
BIN_DIR="${IM_SYNC_BIN_DIR:-$HOME/.local/bin}"
UNIT_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
UNIT="$UNIT_DIR/im-bridge.service"
PURGE=0
WITH_SHELL=1
WITH_SERVICE=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --home) IM_SYNC_HOME="$2"; shift 2 ;;
    --bin-dir) BIN_DIR="$2"; shift 2 ;;
    --purge) PURGE=1; shift ;;
    --no-shell) WITH_SHELL=0; shift ;;
    --no-service) WITH_SERVICE=0; shift ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) echo "未知参数: $1" >&2; exit 2 ;;
  esac
done

say() { printf '\033[1;36m[im-sync]\033[0m %s\n' "$*"; }

# 1) 服务
if [[ "$WITH_SERVICE" == 1 ]]; then
  if systemctl --user list-unit-files im-bridge.service >/dev/null 2>&1; then
    systemctl --user disable --now im-bridge 2>/dev/null || true
    say "已停止并禁用 im-bridge.service"
  fi
  if [[ -f "$UNIT" ]]; then
    rm -f "$UNIT"
    systemctl --user daemon-reload 2>/dev/null || true
    say "已删除 $UNIT"
  fi
else
  say "跳过 systemd（--no-service）"
fi

# 2) shell rc 块
if [[ "$WITH_SHELL" == 1 ]]; then
  BEGIN="# >>> zcode-im-sync >>>"
  END="# <<< zcode-im-sync <<<"
  for rc in "$HOME/.bashrc" "$HOME/.profile"; do
    [[ -f "$rc" ]] || continue
    python3 - "$rc" "$BEGIN" "$END" <<'PY'
import sys
path, begin, end = sys.argv[1:4]
try:
    src = open(path, encoding="utf-8").read()
except FileNotFoundError:
    sys.exit(0)
out, skip, changed = [], False, False
for line in src.splitlines():
    if line.strip() == begin:
        skip = changed = True
        continue
    if skip and line.strip() == end:
        skip = False
        continue
    if not skip:
        out.append(line)
if changed:
    open(path, "w", encoding="utf-8").write("\n".join(out).rstrip() + "\n")
PY
    say "已从 $rc 移除钩子"
  done
fi

# 3) 二进制与钩子
if [[ -f "$BIN_DIR/im-bridge" ]]; then
  rm -f "$BIN_DIR/im-bridge"
  say "已删除 $BIN_DIR/im-bridge"
fi
if [[ -f "$IM_SYNC_HOME/tui/zcode-tui-im-sync.cjs" ]]; then
  rm -f "$IM_SYNC_HOME/tui/zcode-tui-im-sync.cjs"
  say "已删除 TUI 钩子"
fi

# 4) 数据
if [[ "$PURGE" == 1 ]]; then
  if [[ -d "$IM_SYNC_HOME" ]]; then
    rm -rf "$IM_SYNC_HOME"
    say "已删除数据目录 $IM_SYNC_HOME（含凭证）"
  fi
else
  say "保留用户数据：$IM_SYNC_HOME（config/credentials/state/logs/media）"
fi
say "卸载完成。重启 zcode TUI 后钩子不再生效。"
