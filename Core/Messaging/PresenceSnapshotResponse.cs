namespace ChatApp.TcpGateway.Core.Messaging;

public sealed class PresenceSnapshotItem
{
    public long UserId { get; set; }
    public bool IsOnline { get; set; }
}

/// <summary>在线状态快照（网关 → 客户端）。</summary>
public sealed class PresenceSnapshotResponse
{
    public string RequestId { get; set; } = string.Empty;
    public IReadOnlyList<PresenceSnapshotItem> Items { get; set; } = [];
}
