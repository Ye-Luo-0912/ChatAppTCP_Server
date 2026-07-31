namespace ChatApp.TcpGateway.Gateway.Configuration;

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
/// </summary>
public sealed class PushOptions
{
    public const string SectionName = "Push";

    public bool Enabled { get; set; }

    public PushProviderMode ProviderMode { get; set; } = PushProviderMode.Disabled;

    public bool IsValid() => Enum.IsDefined(ProviderMode);
}

public enum PushProviderMode
{
    Disabled = 0,
    TestNoop = 1,
    Production = 2
}
