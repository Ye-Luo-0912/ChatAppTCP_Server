# 群通话 / SFU 立项设计（草案 v1）

> 状态：立项设计（待评审）。前置条件"1:1 通话稳定"已满足：CALL-E2E-2 真机联调 27/27、
> ICE restart 跨端复测 PASS 9/0、三条降级路径自动化覆盖、生产 coturn（4.6.3，3478/5349，
> lt-cred-mech）在 relgate 运行。
> 边界延续：媒体始终留在 WebRTC/STUN/TURN/SFU 独立媒体面，**不经 Server 数据库、Outbox
> 或 TCP Gateway 转发音频**；Server 不接收或持久化 SDP/ICE/media。

## 1. 背景与目标

1:1 通话（CALL-E2E-2）已交付：Server 签发短期 HMAC call grant → Gateway 校验后驱动
信令状态机 → 双端 SIPSorcery WebRTC P2P（coturn TURN 兜底）。群通话目标：支持
N 人（初始目标：视频 ≤9 路 / 音频 ≤30 路）多方通话，复用既有信令安全模型与 TURN
基础设施，媒体面按规模分阶段演进（Mesh → SFU），不预建空壳。

## 2. 现状盘点（设计输入）

| 组件 | 现状 | 群通话复用度 |
|---|---|---|
| 信令 wire | `CallCommandRequest/Response` + `CallSignal` push（TCP wire，P2 批已二进制化） | 高——信令类型需扩展（见 §4） |
| 凭据 | Server `CallsController` 签发短期 HMAC `TcpCallGrant`（issuer/参与者/设备/nonce/过期/撤销校验，Gateway `SignedCallGrantVerifier` 驱动状态机） | 高——grant 需多人化（见 §4.2） |
| 客户端状态机 | `CallSessionManager`（Ringing/Active/Ended + 超时/角色/媒体工厂注入） | 中——单对端假设需改为多方会话 |
| 媒体面 | `SipsorceryCallMediaSession`（PcmAudioPlayer 采集回放、Opus 编解码、coturn ICE） | 中——Mesh 直接复用；SFU 需替换为 SFU 客户端栈 |
| 媒体基础设施 | coturn 4.6.3（3478/5349，lt-cred-mech）已生产运行 | 高——TURN 直接复用 |
| 1:1 → N 的核心鸿沟 | 媒体拓扑（1:1 直连 vs N 方转发）与成员变更语义 | —— |

## 3. 拓扑演进决策：Mesh（阶段一）→ SFU（阶段二）

### 阶段一：Mesh 全连（≤4 人，不引入新基础设施）
- 每参与者与其他 n-1 人建立独立 P2P（复用既有 grant/signal/ICE 全链路，仅多人化）。
- 上行带宽 = (n-1) × 单路码率：Opus 音频 ~32kbps → 4 人 = ~96kbps 上行，家庭宽带可承受；
- **优点**：零新服务、零运维、交付最快；**上限硬约束**：n≥5 时上行与编解码数超限（4 路
  上行编码 ≈ 移动端功耗红线），且任意成员网络抖动影响所有链路。
- 触发进入阶段二：产品需要 >4 人，或 Mesh 体验指标（§7）不达标。

### 阶段二：SFU 选择性转发（>4 人至目标上限）
- 引入独立 **SFU 媒体服务**：每参与者 1 路上行，SFU 转发 n-1 路下行；
  SFU 出口带宽 ≈ n×(n-2)×码率（音频优先阶段可接受）。
- 选型候选（按本项目约束"自托管、可运维、协议开放"排序）：
  1. **LiveKit（自托管）**：Go 单二进制、内建 TURN/信令 token、客户端 SDK 成熟（但引入
     其信令协议，与本项目 TCP wire 信令需桥接）、社区活跃。
  2. **mediasoup（Node/C++ lib）**：更底层、可完全自定义信令（复用本项目 wire 信令）、
     运维与开发成本更高。
  3. **Jitsi Videobridge**：成熟但栈重。
  - **建议**：阶段二启动时做 2 周技术验证（LiveKit 与 mediasoup 各跑通 1 房间 12 路
    音频压测），按验证结果定选型；本设计不预设结论。
- 硬边界（沿用既定原则）：SFU 只转发 RTP/RTCP 与房间级媒体事件；**信令与权限仍走
  既有 TCP wire + Realtime + Server grant 链**；SFU 不落库、不进 Outbox。

## 4. 控制面扩展设计（两阶段共用）

### 4.1 wire 信令扩展（Shared，版本化）
- 新增 `CallSignal` kind：`participant-joined` / `participant-left` / `call-ended`
  / `speaker-changed`（阶段二）；既有 invite/accept/reject/cancel/end 语义保留为
  "邀请→建会"阶段。
- 会话模型：`CallId` 升级为"通话会话"（多人），成员集合 = grant 签发名单；
  **成员变更 = 新 grant 批次 + revision 递增**（沿用 grant 防重放的 nonce/revision 惯例）。
- 尺寸上限：参与者名单 ≤30、单信令 ≤既有 payload 预算；unknown kind 容忍跳过
  （前向兼容）。

### 4.2 grant 多人化（Server）
- `CallGrantRequest` 增加参与者列表（初始 ≤上限）与 `CallKind`（direct/group）；
- HMAC 签名覆盖全部参与者（防替换攻击）；逐参与者校验关系/拉黑/设备数；
- 撤销：通话中成员变更 → Server 重签新 revision grant → Realtime 扇出
  `participant-joined/left` → 客户端按 revision 丢弃旧批次。

### 4.3 客户端状态机（Chat_App）
- `CallSessionManager` 从单对端改为"会话 + 成员字典"模型；Mesh 阶段每成员一条
  PeerConnection；SFU 阶段一条上行 + n 条下行（阶段二）；
- 免打扰/来电聚合：多方来电振铃去重（同一 CallId 聚合）。

## 5. 媒体面设计（阶段二 SFU）

- 房间模型：room = CallId；上行订阅校验 = SFU media token（Server 签发的 grant 派生
  短时 token，含 CallId/UserId/过期——与 call grant 同密钥族或独立密钥，评审定）；
- 媒体：音频 Opus 32-48kbps 优先；视频（阶段二后期）VP8/H264 + simulcast 2 层评估；
- TURN：直接复用 relgate coturn（SFU 部署后 SFU 自身即"公网端点"，ICE 仍需 TURN
  覆盖客户端 NAT 对称场景）；
- 不做（明确出范围）：录制、混音（MCU）、直播扇出、屏幕共享（后续单列）。

## 6. 容量模型（公式 + 验证计划）

音频优先假设：单路 Opus 32kbps + RTCP 开销 ~10% ≈ 35kbps。

| 拓扑 | 单参与者上行 | SFU 出口（每房间） | 参与者上限依据 |
|---|---|---|---|
| Mesh n 人 | (n-1)×35kbps | ——（P2P） | 客户端上行/编码数：n≤4 |
| SFU n 人（纯音频） | 35kbps | n×(n-1)×35kbps | n=30 → 30×29×35k ≈ 30Mbps/房间 |
| SFU n 人（+视频 500kbps） | 535kbps | 视 simulcast 层数 | 阶段二压测后定 |

容量验收计划（阶段二 MVP 前）：单 SFU 实例 30 房间 × 10 人纯音频 30 分钟，
出/入口带宽、CPU（转发核数）、丢包重传率、端到端延迟 p95；指标随选型压测细化，
立项阶段只锁定**公式与测试形态**，不预设机器规格。

## 7. 阶段验收标准

- **阶段一（Mesh ≤4 人）**：创建/邀请/加入/离开/挂断/超时全序列真机通过；3 人真机
  30 分钟音频稳定性（丢包、重连、成员变更各≥1 次）；信令 revision 变更演练（中途
  加/减人 grant 重签）；既有 1:1 测试零回归。
- **阶段二（SFU MVP）**：选型压测达标（§6 表）；30 人音频房间功能全序列；SFU 故障
  时房间终止语义明确（fail-closed，不静音续聊）；网关/Realtime 零改动（信令面）。

## 8. 未决问题（评审输入）

1. SFU 选型（§3 建议阶段二做技术验证后定）；
2. media token 密钥族（复用 grant 密钥 vs 独立）；
3. 视频是否进入阶段二 MVP（音频优先建议）；
4. 免打扰/来电聚合的产品交互细则（控制面已预留 IsMention/聚合位）。

## 9. 与既有文档的边界

- Server：仅 grant 多人化扩展；不接收/持久化 SDP/ICE/media（原则不变）；
- Gateway：信令 wire 扩展的校验与转发（CommandCatalog/CallCommandHandler 延伸）；
- Realtime：跨实例信令扇出沿用（participant 事件）；
- Client：状态机多人化 + 媒体栈分阶段替换；
- 本设计不覆盖：录制、直播、会议纪要等衍生能力。
