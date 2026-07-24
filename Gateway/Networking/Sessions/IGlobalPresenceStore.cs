namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>跨 Gateway 全局在线状态（Redis/Garnet）。</summary>
internal interface IGlobalPresenceStore
{
    Task SetOnlineAsync(long userId, string instanceId, CancellationToken ct = default);
    Task SetOfflineAsync(long userId, string instanceId, CancellationToken ct = default);
    Task RefreshOnlineAsync(long userId, string instanceId, CancellationToken ct = default);
    Task<bool> IsOnlineAsync(long userId, CancellationToken ct = default);
    Task<IReadOnlyDictionary<long, bool>> GetOnlineManyAsync(
        IReadOnlyList<long> userIds,
        CancellationToken ct = default);
}
