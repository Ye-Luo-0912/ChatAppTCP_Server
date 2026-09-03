namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// 离线推送配置（P0-1：避免 NoopPushProvider 静默吞掉真实推送）。
/// <para>
/// 默认 <see cref="Enabled"/>=false：未显式启用时不注册 PushDeliveryConsumerService，
/// 离线推送命令留在 JetStream 中等待 Push Worker 消费，不会被 Noop ACK 吞掉。
/// </para>
/// <para>
/// <see cref="ProviderMode"/> 控制运行时校验：
/// <list type="bullet">
/// <item><c>Disabled</c>：不注册 Consumer，不注册 Provider（默认）。</item>
/// <item><c>TestNoop</c>：仅用于开发/测试环境，NoopPushProvider 返回 <c>provider_unavailable</c>
///   （不返回 Ok），推送命令会被 NAK 重投，不会静默成功。</item>
/// <item><c>Production</c>：启动时校验三个平台均非 Noop，发现 Noop 立即启动失败。</item>
/// </list>
/// </para>
/// <para>
/// 主线一9：本类位于 Infrastructure 层，使 <see cref="PushDispatcher"/> 可直接消费配置，
/// 避免 Infrastructure → Gateway 反向依赖（违反架构边界）。
/// </para>
/// </summary>
public sealed class PushOptions
{
    public const string SectionName = "Push";

    public bool Enabled { get; set; }

    public PushProviderMode ProviderMode { get; set; } = PushProviderMode.Disabled;

    /// <summary>
    /// 主线一7：每个 Provider 的最大并发投递数。0 = 不限制（每 token 一个 Task）。
    /// 默认 10：FCM/APNs/WebPush 各 10 并发，避免突发流量打爆 Provider API。
    /// </summary>
    public int MaxConcurrentSendsPerProvider { get; set; } = 10;

    /// <summary>
    /// 主线一4/6：Token 粒度内部重试次数（仅对 rate_limited / provider_unavailable 重试）。
    /// 0 = 不重试（由 JetStream NAK 重投整条命令）。默认 2：Provider 恢复后重试 2 次。
    /// 重试使用指数退避：RetryAfter 或 base * 2^attempt。
    /// </summary>
    public int TokenRetryCount { get; set; } = 2;

    /// <summary>
    /// 主线一6：Token 粒度重试基础延迟。实际延迟 = max(RetryAfter, base * 2^attempt)。
    /// 默认 500ms：第一次重试 500ms，第二次 1000ms。
    /// </summary>
    public TimeSpan TokenRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// 主线一8：无效 Token 注销失败时的重试次数。默认 3。
    /// 注销失败不阻塞投递返回，但会在后台重试，避免无效 Token 残留。
    /// </summary>
    public int InvalidTokenUnregisterRetryCount { get; set; } = 3;

    /// <summary>
    /// 门禁4：无效 Token 清理工作队列容量。默认 1024。
    /// 队列满时丢弃最旧项（DropOldest），不阻塞投递热路径。
    /// </summary>
    public int InvalidTokenCleanupQueueCapacity { get; set; } = 1024;

    /// <summary>
    /// 单条群聊消息触发的离线推送数量上限（按提及优先排序后截断），
    /// 防止超大群的病理性 fanout。超出部分丢弃并记日志。
    /// </summary>
    public int MaxGroupOfflinePushesPerMessage { get; set; } = 200;

    /// <summary>
    /// 门禁3：Push Token 加密密钥环（支持旧 Key 读取 + 当前 Key 写入）。
    /// <para>
    /// 每项为 <see cref="TokenEncryptionKeyConfig"/>：<c>KeyId</c>（默认 "1"）+ <c>Key</c>
    /// （Base64 编码的 32 字节 AES-256 密钥）。写入使用 KeyId 最大的密钥（当前 Key），
    /// 读取按密文头部 key_id 从密钥环查找对应密钥，支持旧 Key 解密。
    /// </para>
    /// <para>
    /// 未配置密钥环时：若设置了 <see cref="TokenEncryptionKey"/>，则当作单密钥（KeyId="1"）；
    /// 都未配置则使用 <c>NullPushTokenProtector</c>（明文存储，向后兼容）。
    /// </para>
    /// </summary>
    public List<TokenEncryptionKeyConfig> TokenEncryptionKeys { get; set; } = [];

    /// <summary>
    /// 门禁3：后台渐进式重加密调度间隔。默认 1 小时。
    /// 密钥轮换后，旧 Key 加密的历史令牌由 <c>PushTokenReencryptionWorker</c> 逐步重加密。
    /// </summary>
    public TimeSpan TokenReencryptionInterval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// 主线一10：Push Token 加密密钥（Base64 编码的 32 字节 AES-256 密钥）。
    /// <para>
    /// 配置后，Redis 中存储的 PushTokenRecord JSON 将被 AES-GCM 加密，
    /// 防止 Redis 数据泄露时暴露用户推送令牌。
    /// </para>
    /// <para>
    /// 未配置（null/空）时使用 <c>NullPushTokenProtector</c>（明文存储，向后兼容）。
    /// 生产环境应通过环境变量或密钥管理服务注入此密钥。
    /// </para>
    /// <para>
    /// 生成密钥示例（PowerShell）：
    /// <code>[Convert]::ToBase64String([System.Security.Cryptography.RandomNumberGenerator]::GetBytes(32))</code>
    /// </para>
    /// </summary>
    public string? TokenEncryptionKey { get; set; }

    public bool IsValid() => Enum.IsDefined(ProviderMode);
}

/// <summary>
/// 门禁3：单条 Push Token 加密密钥配置。
/// </summary>
public sealed class TokenEncryptionKeyConfig
{
    /// <summary>密钥 Id（默认 "1"）。写入用当前 Key，读取按此 Id 定位。</summary>
    public string KeyId { get; set; } = "1";

    /// <summary>Base64 编码的 32 字节 AES-256 密钥。</summary>
    public string Key { get; set; } = "";
}

public enum PushProviderMode
{
    Disabled = 0,
    TestNoop = 1,
    Production = 2
}
