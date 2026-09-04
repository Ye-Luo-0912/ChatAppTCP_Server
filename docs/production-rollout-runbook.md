# 生产 Rollout 运维手册（Push 启用 / 二进制格式启用）

> 面向运维与值班。两项 rollout 均为**配置开关操作，无代码变更**；各自含灰度/验证/回滚步骤。
> 环境：ChatAppTCP_Server 部署形态（Gateway + PushWorker + Realtime + Server + Postgres/Redis/NATS/coturn）。

## 1. Push 通知启用

### 1.1 前置凭据获取（一次性，平台控制台操作）

| 平台 | 需获取 | 配置键（PushWorker appsettings） |
|---|---|---|
| FCM（Android） | Firebase 项目 ProjectId + 服务账号 JSON（含 private_key） | `Push:Providers:Fcm:ProjectId` / `ServiceAccountKeyPath`（或 `ServiceAccountJson` 内联） |
| APNs（iOS） | Apple Developer TeamId、Token Auth KeyId + .p8 私钥、BundleId | `Push:Providers:Apns:TeamId/KeyId/PrivateKeyPem/BundleId`；生产 ApiEndpoint 默认 `api.push.apple.com`（沙箱另配） |
| WebPush（浏览器） | VAPID 密钥对（subject 邮箱 + 公私钥） | `Push:Providers:WebPush:VapidSubject/VapidPublicKey/VapidPrivateKeyPem` |

凭据**不得入库**：经部署管道注入环境变量或挂载密钥文件（ServiceAccountKeyPath / PrivateKeyPem 支持路径）。

### 1.2 配置变更

**Gateway**（触发点所在；appsettings 或环境变量）：
```json
{ "Push": { "Enabled": true } }
```
**PushWorker**（投递所在；分阶段）：
```json
{ "Push": { "Enabled": true, "ProviderMode": "TestNoop" },
  "Push:Providers:Fcm": { "...": "..." }, "Push:Providers:Apns": { "...": "..." }, "Push:Providers:WebPush": { "...": "..." } }
```
- `ProviderMode=TestNoop`：全链路（JetStream 消费、token 解密、DLQ、幂等）真实运行，仅"投递"为空操作——**灰度首选**。
- `ProviderMode=Production`：真实平台投递；启动时 `PushProviderStartupValidator` 会 fail-fast 校验三平台凭据完整性（缺任一启用平台凭据则拒绝启动）。

前置确认：`TokenEncryptionKeys` 至少一把主密钥已配置（token AES-GCM 加密所需；轮换密钥按旧密钥保留解密）。

### 1.3 启用顺序与验证

1. **Redis 前置**：确认 Gateway 与 PushWorker 指向同一 Redis（token 存储 `{userId}` hash tag、幂等 L2、presence ZSET 共用）。
2. **TestNoop 灰度**：Gateway `Push.Enabled=true` + PushWorker `ProviderMode=TestNoop` → 滚动重启 →
   验证清单：
   - 客户端注册：`RegisterPushToken` 成功；Redis token hash 出现该 userId 记录；
   - 离线判定：接收方全下线后发消息 → JetStream `push_delivery.publish` 有消息 → PushWorker 消费（日志）→ Noop 投递计数；
   - 免打扰：会话静音成员不再出现在推送目标（EventId 3005 截断/过滤日志）；
   - 幂等：同消息重投不重复计数（幂等 L2 命中）。
3. **Production 切换**：PushWorker `ProviderMode=Production` → 单实例先行 → 真实设备收推送 → 多实例滚动。
4. **生产验证**：真实设备收推送（前台/后台/杀进程三态）；无效 token（卸载 App）触发 `InvalidTokenUnregister` 自动注销；DLQ 无堆积（`IPushDlqStore`）。

### 1.4 回滚

- `PushWorker ProviderMode=TestNoop`（或 Gateway `Push.Enabled=false`）→ 滚动重启即回滚；
- token 数据与审计事件保留，重新启用即恢复；
- 密钥轮换：新增新主密钥 → `PushTokenReencryptionWorker` 自动重加密 → 下线旧密钥。

## 2. 二进制 wire 格式启用（chatapp-bin-v1）

现状：relgate 已启用并经真机三套 e2e 验证；本节为生产环境启用程序。

1. **前置**：客户端版本 ≥ 支持二进制协商的版本（旧客户端自动回退 JSON，无需停机窗口）。
2. **配置**：Gateway appsettings `TcpGateway:EnableBinaryPayloadFormat = true` → 滚动重启（重启期间连接闪断，客户端自动重连）。
3. **灰度语义**：开关按实例生效——混合实例池中新老实例并存时，同用户不同连接可能协商出不同格式，均为合法（连接级格式不可变）。
4. **验证**：
   - 新连接 ServerHello `PayloadFormat=chatapp-bin-v1` 且功能全序列可用（对照 `BinaryPayloadNegotiationTests` 场景）；
   - 旧客户端连接仍协商 json（fallback）；
   - 指标：协议错误率无抬升（BinaryPayloadDecodeException 计数）。
5. **回滚**：开关置 false → 滚动重启 → 新连接全部回退 JSON（已建立二进制连接随断开自然回收）。

## 3. 已知边界

- Push：S3 直传存储模式无分块续传（整包 PUT 预签名 URL）；群聊单条消息离线推送上限 200（提及优先）；
- 二进制：`ResumeRequest` 永不入寄存器（非 wire 命令）；Resume 路径恒 JSON；
- 推送真实投递率依赖平台凭据有效性：APNs 证书过期 / FCM 服务账号失效会以 Warning + 无效 token 注销队列体现，值班需关注。
