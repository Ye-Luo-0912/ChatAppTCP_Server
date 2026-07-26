namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 全局在线状态转换结果。
/// <para>
/// 跨网关 Presence 改 ZSET 多实例模型后，只在 0&lt;-&gt;1 转换时发布事件。
/// 调用方据此决定是否广播与发布跨网关事件，避免每实例本地连接/断开都发布。
/// </para>
/// </summary>
public enum PresenceTransition
{
    /// <summary>无转换：状态未改变（仍在线或仍离线）。</summary>
    None = 0,

    /// <summary>全局上线转换：0 -&gt; 1（之前无任何在线实例，本次操作后存在在线实例）。</summary>
    WentOnline = 1,

    /// <summary>全局下线转换：1 -&gt; 0（之前有在线实例，本次操作后无任何在线实例）。</summary>
    WentOffline = 2
}

/// <summary>跨 Gateway 全局在线状态（Redis/Garnet）。</summary>
internal interface IGlobalPresenceStore
{
    /// <summary>
    /// 标记用户在指定实例上线（ZADD 成员 + 刷新 score）。
    /// <para>
    /// 原子清理过期成员后 ZADD 当前实例，返回全局状态转换。
    /// 仅当返回 <see cref="PresenceTransition.WentOnline"/> 时调用方应发布上线事件。
    /// </para>
    /// </summary>
    Task<PresenceTransition> SetOnlineAsync(long userId, string instanceId, CancellationToken ct = default);

    /// <summary>
    /// 标记用户在指定实例下线（ZREM 成员）。
    /// <para>
    /// 原子清理过期成员后 ZREM 当前实例，返回全局状态转换。
    /// 仅当返回 <see cref="PresenceTransition.WentOffline"/> 时调用方应发布下线事件。
    /// 即使本实例非最后一个，也安全移除自身成员，不影响其他实例。
    /// </para>
    /// </summary>
    Task<PresenceTransition> SetOfflineAsync(long userId, string instanceId, CancellationToken ct = default);

    /// <summary>
    /// 刷新当前实例的到期 score（若仍为成员）。
    /// </summary>
    Task RefreshOnlineAsync(long userId, string instanceId, CancellationToken ct = default);

    Task<bool> IsOnlineAsync(long userId, CancellationToken ct = default);
    Task<IReadOnlyDictionary<long, bool>> GetOnlineManyAsync(
        IReadOnlyList<long> userIds,
        CancellationToken ct = default);
}
