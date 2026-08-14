# 下一阶段与接手状态

## 职责与优先级

TCP Gateway 负责连接、鉴权、能力协商、限流和到 Realtime 的路由；不成为数据库、关系或媒体内容的业务权威。

当前以功能链路完整为主：关系读取 → 语音消息 → 1:1 通话。性能和二进制是支撑轨，只有在不打断功能主线且有明确收益证据时推进。详细执行清单唯一维护在 [`roadmap-todo.md`](roadmap-todo.md)。

## 下一阶段顺序

1. **`REL-E2E-4`：关系读取端到端闭环。** 接入 Realtime 已完成的投影 list/catch-up，使用 Shared wire 做显式映射；覆盖分页、reset、gap、partial、权限变化和 Client 水位恢复。mutation 永久走 Server HTTP。
2. **`VOICE-MSG-2`：语音附件消息。** 复用现有附件与聊天命令，只传有界元数据和对象引用；补齐历史、同步、错误映射与跨 Gateway 投递，不让音频正文进入 TCP frame。
3. **`CALL-E2E-2`：1:1 通话控制面。** 接入 Server call grant 与 Realtime 临时信令状态机，完成 invite/accept/reject/end/reconnect 的鉴权、限流、幂等、TTL 和跨 Gateway 路由；媒体走 WebRTC/TURN。
4. **`BIN-INTEGRATION-3`：二进制开发接入。** 待功能 payload 与命令目录稳定后再接双 codec；JSON 继续默认，格式按 session 固化，混合连接按格式共享编码。
5. **`PERF-SUPPORT-1`：有证据的性能收口。** 优先修复新功能引入的 CPU、allocation、PSS 或 p99 回退；一次只优化一个已归因热点，不重跑已完成的整批画像。

关系、语音消息和通话必须分批接入、分开关闭。二进制或性能改动不得与新业务 capability 合并成同一变更。

## 当前接手事实

- DirectSocket/Persistent 并行握手 abort 已确认是测试隔离问题并完成定向串行化；连接生命周期、预算和资源清理已有确定性测试。
- Server HTTP 是唯一关系权威；Realtime 投影 list/catch-up 已具备，Gateway 当前任务是把真实 backend 与 Shared wire 接通。
- 语音附件元数据与 Realtime 通话状态机已有下游基础，但 Gateway/Shared/Client 的外部链路尚未闭环。
- JSON 与固定 10-byte 帧头仍是当前运行路径。Shared binary 底座已存在，但 Gateway codec、协商和完整命令覆盖尚未接入。
- `TCP-MEM-1` 已有 gcdump/PSS/socket 证据；后续直接读取证据选择热点，不重复全套长连接画像。

已完成能力和历史证据见 [`roadmap-current-state.md`](roadmap-current-state.md) 与 [`roadmap-changelog.md`](roadmap-changelog.md)，不要复制回待办。

## 接手约束

接手时写明批次号，先理解现有命令语义、上下游契约和资源所有权。缺少真实契约或正确性证据时，先补 mapper、测试和诊断，不提前改变默认行为；不引入无界队列、连接级线程或第二套业务 DTO。

## 验证顺序

聚焦单测/契约测试 → Release 构建 → 3–5 分钟 smoke；只有需要确认资源趋势时才补一次 10–15 分钟样本。当前阶段到功能联调验收为止。
