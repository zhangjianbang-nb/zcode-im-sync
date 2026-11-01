#!/usr/bin/env bash
# zcode-im-sync 自检：安装完整性 + 桥健康 + 钩子锚点 + 事件目录。
#   ./scripts/selftest.sh [--home DIR]
# 退出码 0 = 全 PASS；1 = 有 FAIL。
set -uo pipefail

IM_SYNC_HOME="${IM_SYNC_HOME:-$HOME/.local/share/zcode-im-sync}"
BIN_DIR="${IM_SYNC_BIN_DIR:-$HOME/.local/bin}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --home) IM_SYNC_HOME="$2"; shift 2 ;;
    --bin-dir) BIN_DIR="$2"; shift 2 ;;
    *) echo "未知参数: $1" >&2; exit 2 ;;
  esac
done

PASS=0
FAIL=0
ok() { printf '  \033[32mPASS\033[0m %s\n' "$*"; PASS=$((PASS+1)); }
bad() { printf '  \033[31mFAIL\033[0m %s\n' "$*"; FAIL=$((FAIL+1)); }

echo "== zcode-im-sync 自检 =="
echo "home: $IM_SYNC_HOME"

# 1) 二进制
if [[ -x "$BIN_DIR/im-bridge" ]]; then ok "im-bridge 可执行 ($BIN_DIR/im-bridge)"; else bad "缺少 $BIN_DIR/im-bridge"; fi

# 2) 配置与凭证目录
if [[ -f "$IM_SYNC_HOME/config.toml" ]]; then ok "config.toml 存在"; else bad "缺少 config.toml"; fi
if [[ -d "$IM_SYNC_HOME/credentials" ]]; then
  mode=$(stat -c '%a' "$IM_SYNC_HOME/credentials")
  [[ "$mode" == "700" ]] && ok "credentials 权限 700" || bad "credentials 权限 $mode（应为 700）"
else bad "缺少 credentials/"; fi

# 3) 钩子
HOOK="$IM_SYNC_HOME/tui/zcode-tui-im-sync.cjs"
if [[ -f "$HOOK" ]]; then
  ok "TUI 钩子存在"
  if node --check "$HOOK" 2>/dev/null; then ok "钩子语法通过"; else bad "钩子语法错误"; fi
  if node "$HOOK" --check >/tmp/im-sync-hook-check.txt 2>&1; then
    ok "钩子锚点命中（$(grep 'anchor' /tmp/im-sync-hook-check.txt | tr -s ' ')）"
  else
    bad "钩子锚点未命中：$(tail -1 /tmp/im-sync-hook-check.txt)"
  fi
else bad "缺少 TUI 钩子 $HOOK"; fi

# 4) shell 接入
if grep -q '# >>> zcode-im-sync >>>' "$HOME/.bashrc" "$HOME/.profile" 2>/dev/null; then
  ok "shell rc 含 NODE_OPTIONS 块"
else bad "shell rc 未接入（新终端不会加载钩子）"; fi

# 5) systemd
if systemctl --user is-active --quiet im-bridge 2>/dev/null; then
  ok "im-bridge.service active"
else bad "im-bridge.service 未运行"; fi

# 6) HTTP 健康
if command -v curl >/dev/null && curl -sf --max-time 3 127.0.0.1:8106/healthz >/tmp/im-sync-health.json 2>/dev/null; then
  ok "healthz 200（$(cat /tmp/im-sync-health.json)）"
else bad "healthz 不可达"; fi

# 7) doctor
if [[ -x "$BIN_DIR/im-bridge" ]]; then
  if "$BIN_DIR/im-bridge" --home "$IM_SYNC_HOME" doctor >/tmp/im-sync-doctor.txt 2>&1; then
    ok "im-bridge doctor 通过"
  else
    bad "im-bridge doctor 失败：$(tail -3 /tmp/im-sync-doctor.txt | tr '\n' ' ')"
  fi
fi

# 8) 事件目录
[[ -d "$IM_SYNC_HOME/state/im-events" ]] && ok "事件目录存在" || bad "缺少 state/im-events"

echo "== 结果：$PASS PASS / $FAIL FAIL =="
[[ "$FAIL" == 0 ]]
