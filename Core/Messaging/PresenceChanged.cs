namespace ChatApp.TcpGateway.Core.Messaging;

/// <summary>在线状态变更推送（网关 → 已订阅的观察者）。</summary>
public sealed class PresenceChanged
{
    public long UserId { get; set; }
    public bool IsOnline { get; set; }
}
