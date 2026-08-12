# 下一阶段与接手状态

## 职责

TCP Gateway 负责连接、鉴权、能力协商、限流和到 Realtime 的路由；不成为数据库、关系或媒体内容的业务权威。

## 下一阶段顺序

详细执行清单唯一维护在 `docs/roadmap-todo.md`；本文件只定义跨仓顺序和接手边界。

1. **`TCP-P0-2`：先收口正确性。** 修复 Windows 并行测试下 DirectSocket/Persistent 握手 abort 的生命周期竞态，并复核请求关联、Sync 硬预算、Resume fencing 与关闭后的资源归零。
2. **`REL-READ-3`：完成关系只读链路。** Shared 的 list/sync DTO 与错误/预算语义已经收口；Realtime 完成投影 list/catch-up 同源读取后，Gateway 只做显式映射和默认关闭的 capability。Server HTTP 仍是权威，关系 mutation 继续走 HTTP。
3. **`BIN-SCHEMA-2`：接入真实 payload schema。** Shared 覆盖全部握手后 payload；Gateway 不复制 schema，也不恢复旧二进制实现。
4. **`TCP-PERF-2`：短测驱动 CPU/内存优化。** 基于既有内存画像只选择一个可归因热点，先做微基准与聚焦测试，再做短时同构 A/B。
5. **`BIN-INTEGRATION-3`：接入双 codec。** JSON 仍为默认；握手完成后格式按 session 固化，全部 payload 覆盖且短测通过前不开放协商。
6. **`VOICE-SIGNAL-1`：增加 1:1 语音通话信令。** TCP 只承载可靠的 offer/answer/ICE/结束信令；媒体使用 WebRTC/SRTP 与 TURN，不进入 Gateway 数据面。

Realtime 的 `REL-READ-3`、Shared 的 `BIN-SCHEMA-2` 可在 `TCP-P0-2` 期间并行推进；Gateway 的关系、二进制和语音入口必须在 `TCP-P0-2` 收口后再接入。不得把多个 capability 合并成一次改动。

## 当前接手事实

- JSON 与固定 10-byte 帧头仍是唯一启用的生产路径。
- TCP relation list/sync/mutation 当前继续 fail-closed；Server HTTP 是唯一在线关系权威。
- `TCP-MEM-1` 已完成并有可用的 gcdump/PSS/socket 证据；下一阶段读取现有证据，不重复整批画像。
- Shared 已完成 `chatapp-bin-v1` 公共底座，但真实业务 DTO、Gateway codec 与格式协商尚未接入。
- 语音消息应复用附件生命周期；通话信令和媒体传输尚未实现。

其余已完成能力、基线数字和历史证据见 `docs/roadmap-current-state.md` 与 `docs/roadmap-changelog.md`，不要复制回待办。

## 接手约束

接手时写明批次号，先理解现有命令语义、上下游契约和资源所有权。一次只推进一个 Gateway capability；缺少前置契约或正确性证据时，只补测试和诊断，不提前改变默认行为。

## 验证顺序

聚焦单测/契约测试 → Release 构建 → 3–5 分钟 smoke 或同构 A/B。只有已经胜出的性能改动需要补充归因时，才增加一次 10–15 分钟 Linux 样本；本阶段不安排更长测试。
