namespace ChatApp.TcpGateway.Core.Messaging;

/// <summary>
/// 服务端 → 客户端连接排空通知（PacketCommand.GoAway = 5）。
/// 服务端在滚动升级或优雅停机时发送，提示客户端重连其他实例。
/// 客户端收到后应主动断开并重连，不应发送新请求。
/// </summary>
public sealed class GoAway
{
    /// <summary>
    /// 重试建议间隔（毫秒）。客户端应等待此时间后重连，避免雪崩。
    /// </summary>
    public int RetryAfterMs { get; set; }

    /// <summary>
    /// 排空原因：
    /// "shutdown" = 优雅停机；
    /// "upgrade" = 滚动升级；
    /// "overloaded" = 服务过载主动排空。
    /// </summary>
    public string? Reason { get; set; }

    /// <summary>
    /// 服务端提示的替代连接地址（host:port）。客户端可优先重连此地址。
    /// null 表示无建议，客户端按原配置重连。
    /// </summary>
    public string? ServerHint { get; set; }
}
