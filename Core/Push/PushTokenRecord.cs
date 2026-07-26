using ChatApp.TcpGateway.Core.Messaging.Push;

namespace ChatApp.TcpGateway.Core.Push;

/// <summary>持久化的推送令牌记录（Push Service 拉取用）。</summary>
public sealed class PushTokenRecord
{
    public required string Token { get; init; }

    public PushPlatform Platform { get; init; }

    /// <summary>设备哈希（用于多设备去重与按设备注销）。</summary>
    public ulong DeviceIdHash { get; init; }

    public string? AppDeviceLabel { get; init; }

    /// <summary>令牌最后更新时间（Unix ms）。</summary>
    public long UpdatedAtMs { get; init; }
}
