namespace ChatApp.TcpGateway.Core.Messaging;

/// <summary>取消在线状态订阅（客户端 → 网关）。</summary>
public sealed class PresenceUnwatchRequest
{
    public IReadOnlyList<long>? UserIds { get; set; }
}
