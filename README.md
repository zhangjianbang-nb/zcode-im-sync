# zcode-im-sync

把 **zcode TUI** 变成可远程遥控、全过程可见的 agent：在微信/飞书里发一句话，
本机 zcode 就像你坐在电脑前一样执行——**用户消息、思考块、工具入参、工具输出、
回答、回合统计**都同步到 IM；你在 IM 里看到的不是"另一个聊天机器人"，而是
**当前 TUI 会话的镜像**。

```
微信 App ──iLink Bot API──┐
                          ├─ im-bridge（Rust 常驻）──stdio JSON-RPC── zcode app-server
飞书 App ──WS 长连接──────┘        │
                                   │ 读 state/im-events/<sess>.ndjson
zcode TUI ──NODE_OPTIONS 钩子──────┘
   │  onEvent 块级采集 → 事件文件
   └  IM 消息 → tui.submit()（与键盘回车同一条路径）
```

## 为什么是"同步器"而不是"聊天桥"

普通桥只把 IM 文本转发给 agent、再把最终回答发回。zcode-im-sync 额外做了三件事：

1. **同一会话**：钩子把 IM 消息注入 `tui.submit(text)`，和你在 TUI 里敲回车完全同路径，
   消息会出现在界面上、上下文连续。
2. **全过程**：钩子包住 `tui.onEvent`，按**块**采集（不是逐 token），写
   `state/im-events/<session>.ndjson`；桥增量 tail 后格式化成 IM 消息：
   `💭 思考`、`🔧 工具调用`（含 Bash 命令/Read 路径等入参）、`✅ 工具输出`、
   `🤖 回答`、`✅ 本轮完成（N tools · tokens）`。
3. **不双发**：TUI 钩子在线时心跳带 `events:true`，桥的 DB 文本镜像自动让位；
   钩子掉线 6 秒后桥恢复 DB 兜底，不会重复回答。

## 特性

- `/wechat`、`/feishu`：TUI 内直接拉起扫码登录，二维码 PNG 弹窗（无 DISPLAY 时字符码写日志）。
- **可拆卸**：独立 Rust 守护进程 + 一个 `NODE_OPTIONS=--require` 钩子 + systemd user unit；
  `uninstall.sh` 一次摘干净（默认保留凭证/配置，`--purge` 全删）。
- **对 zcode 零侵入**：不改 zcode bundle、不写它的数据库、不抢它的回合；
  钩子只在 `stdin/stdout 是 TTY` 的 TUI 进程里激活，app-server/脚本进程直接跳过。
- **有界内存**：块级事件、队列上限 512 行、异步 append、事件文件 8MB 轮转、7 天清理；
  桥侧 tail 每 300ms 最多读 256KB，出站 1.2s/条限速，低优先级消息队列满时丢弃。
- **单实例**：`state/locks/im-bridge.lock` 文件锁；TUI 收件箱用 `rename(2)` 原子认领，
  TUI 与桥兜底不会把同一条消息各跑一遍。
- **白名单**：只服务本人；扫码登录自动并入本人 ID，其他账号静默丢弃（只记日志）。
- **凭证 0600**：微信 `credentials/weixin.json`、飞书 `credentials/feishu.toml`，
  目录 0700；仓库不含任何凭证。

## 安装

前置：Linux、Node ≥22.15（zcode TUI 自带）、Rust ≥1.95、`protoc`（飞书 WS 构建依赖）。

```bash
git clone https://github.com/zhangjianbang-nb/zcode-im-sync.git
cd zcode-im-sync
./install.sh
```

安装器做五件事（全部幂等）：

1. `cargo build --release`，装到 `~/.local/bin/im-bridge`；
2. 数据目录 `~/.local/share/zcode-im-sync/`（state/logs/credentials/media/config.toml）；
3. 写 `~/.config/systemd/user/im-bridge.service` 并 `enable --now`；
4. 把钩子复制到数据目录，并在 `~/.bashrc`、`~/.profile` 注入带标记的
   `NODE_OPTIONS=--require .../zcode-tui-im-sync.cjs` 块；
5. 打印自检命令。

选项：`--home DIR`、`--bin-dir DIR`、`--no-service`、`--no-shell`、`--skip-build`。

> 钩子只在 **zcode TUI** 生效；普通 node 进程会立刻跳过，不产生日志、不加载 TUI 模块。

## 使用

1. 新开终端，启动 `zcode`（确保加载了新的 `NODE_OPTIONS`）。
2. 在 TUI 里敲 `/wechat`（或 `/feishu`），手机扫码。
   - 如果 TUI 还没发过第一条消息，登录会写一个"待绑定"占位；你发出第一条消息后，
     桥读取心跳里的真实 `session_id` 自动补绑。不会绑到同目录的旧会话。
3. 在微信里给机器人发消息：
   - TUI 立即出现你的消息并开始跑；
   - 微信里依次收到：`📥 收到，开始处理…` → `💭 思考中…` / `💭 思考：…` →
     `🔧 工具名：入参摘要` → `✅ 工具名（耗时）：输出` → `✍️ 正在输出回答…` →
     `🤖 回答：…` → `✅ 本轮完成（N 个工具 · tokens）`。
4. 你在 TUI 里直接敲的消息/回合，也会镜像到微信（`src=tui` 的用户消息 + 全过程）；
   但微信 Bot API 的 `context_token` 只在收到你消息后的一段时间内有效，纯本机输入若距
   上一条微信消息太久，过程消息会因 token 失效被丢弃（只记 warn，不补发、不重放）。

IM 内控制命令：`/new`（新会话）、`/stop`、`/cwd <path>`、`/status`、`/help`。
其他 `/xxx` 当普通 prompt 转发。

## 验证

```bash
./scripts/selftest.sh          # 11 项：二进制/配置/权限/钩子锚点/服务/healthz/doctor
cargo test                     # 128 Rust 测试（121 单元 + 7 集成）
node --test tui/test/tui-hook.test.cjs   # 9 钩子测试
node tui/zcode-tui-im-sync.cjs --check   # 锚点自检（自动定位 TUI bundle）
```

手工端到端：

```bash
# 1) 真实 TUI（tmux 里也行）
bash -lc 'cd ~/workspace/ai_workspace && zcode'
# 2) 敲 /wechat 扫码
# 3) 微信发 "用 Bash 运行 echo hello，然后一句话说明结果"
# 4) 观察微信是否按 思考→工具→输出→回答→结束 的顺序收到
```

## 兼容性

- 实测 zcode **3.17.1-42**（TUI bundle `@zcode/tui/dist/index.js`）。
- 钩子靠唯一锚点 `editor.onSubmit = (text) => void this.submit(text);` 注入；
  锚点命中数 ≠ 1 时**自动放弃注入**（fail-safe，不会改坏 TUI）。
- 升级 zcode 后先跑 `node tui/zcode-tui-im-sync.cjs --check`；若 `anchor: 0 hit(s)`，
  说明 TUI 内部改了，需要更新锚点/事件映射后再装。
- 事件协议 `v=1`；桥与钩子字段全部 `serde(default)`，未知字段忽略，向后兼容。

## 卸载

```bash
./uninstall.sh            # 停服务、摘 NODE_OPTIONS、删二进制/钩子；保留 config/credentials/state
./uninstall.sh --purge    # 连数据目录和凭证一起删
./uninstall.sh --no-service --no-shell   # 只删二进制/钩子（调试用）
```

## 目录

```
src/
  main.rs               CLI（run/login/doctor/test-send）+ 登录/绑定
  session_router.rs     IM 入站 → TUI 收件箱/headless 兜底；MirrorPoller DB 兜底
  process_mirror.rs     事件 tailer + 格式化 + 1.2s 限速 + 有界队列
  im_inbox.rs           收件箱协议 + TUI 心跳（rename 原子认领）
  binding.rs            chat ↔ session 绑定（占位绑定/心跳补绑）
  wechat_ilink/         腾讯 iLink Bot API（QR/长轮询/发送/媒体）
  feishu/               飞书 device-code 扫码 + WS 长连接
tui/
  zcode-tui-im-sync.cjs 可拆卸 TUI 钩子（ESM registerHooks + CJS 兜底）
  test/                 钩子单测
install.sh / uninstall.sh / scripts/selftest.sh
deploy/                 示例 unit / logrotate
```

## 安全与边界

- 仅本人白名单；未命中静默丢弃，不回执，防暴露机器人。
- zcode 以 `yolo` 权限运行（与终端同权）：能读写文件、执行命令。**只绑自己的账号**。
- IM 出站目前只发文本；入站支持图片/语音/文件/视频落盘后把绝对路径注入 prompt。
- 事件文件含完整思考/工具输出，落在本机 `state/im-events/`（0600，7 天清理）；
  如需更短保留期，改 `process_mirror.rs` 的 `EVENT_FILE_TTL`。
- 不包含任何第三方 IM 协议的逆向代码；微信走官方 iLink Bot API，飞书走官方 SDK。

## License

MIT。协议事实参考 Hermes-CN-Core（MIT），未复制其代码。
