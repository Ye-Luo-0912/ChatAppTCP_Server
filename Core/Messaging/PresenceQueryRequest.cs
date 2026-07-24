namespace ChatApp.TcpGateway.Core.Messaging;

/// <summary>在线状态查询（客户端 → 网关）。</summary>
public sealed class PresenceQueryRequest
{
    public string? RequestId { get; set; }
    public IReadOnlyList<long>? UserIds { get; set; }
}
