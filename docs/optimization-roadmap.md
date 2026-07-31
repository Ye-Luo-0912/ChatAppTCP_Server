# 优化与功能路线图（已归档）

本文件已拆分为三份独立文档，避免历史状态与当前状态/待办混合导致 Agent 依据过期
数字（旧测试数、旧 `_fired`、旧默认模式）做错误判断。

> **本文件保留为历史归档，不再更新。** 新内容请参阅：

| 文档 | 用途 |
|------|------|
| [roadmap-current-state.md](./roadmap-current-state.md) | 系统当前真实状态（默认值、文件规模、已完成能力） |
| [roadmap-changelog.md](./roadmap-changelog.md) | 历史变更记录（按时间倒序，已完成工作） |
| [roadmap-todo.md](./roadmap-todo.md) | 待办路线图（四大主线 + 性能长测 + CI + 可观测性收尾） |

## 拆分原因

原文件混合了：

1. **过期测试数字**：225 / 227 / 259 / 287 / 301 / 312 等（当前 366/366）。
2. **已移除的实现**：`DeadlineWheel._fired` 集合（已改用分桶 + 计数器，
   发送超时迁移到独立 `SendTimeoutTracker`）。
3. **过期的默认模式**：`OutboundSendMode` 现有三种（新增 `PerSessionDrain`），
   原文件仅提两种。
4. **过期的文件规模**：`TcpGatewayService` 原 669 行（现 869）、
   `GatewayMetrics` 原 492 行（现 871）。
5. **已完成但仍列在待办**的条目。

新文档结构确保 Agent 只需读 `roadmap-current-state.md` 即可获得准确当前状态，
读 `roadmap-todo.md` 获得准确待办，不会被历史信息误导。

---

原文件内容已按主题迁移至 `roadmap-changelog.md`（历史变更）与
`roadmap-current-state.md`（当前状态），不再在此重复保留。

