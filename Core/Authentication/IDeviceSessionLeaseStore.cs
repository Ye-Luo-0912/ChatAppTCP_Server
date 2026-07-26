namespace ChatApp.TcpGateway.Core.Authentication;

/// <summary>
/// 跨 Gateway 同设备 TCP 会话租约：发现旧 SessionId 以便广播 SessionRevoked。
/// </summary>
/// <remarks>
/// 拆分 DeviceId/SessionId/ConnectionLeaseId。
/// <para>
/// 租约 key = (userId, deviceIdHash)；租约 value = (connectionLeaseId, sessionId)。
/// <list type="bullet">
/// <item><c>sessionId</c>：用户可见会话标识（用于 SessionRevoked 路由）。</item>
/// <item><c>connectionLeaseId</c>：每次 TCP 连接生成的唯一所有权令牌（GUID），用于原子 compare-and-delete/refresh，避免 SessionId 复用导致误删。</item>
/// </item>
/// </para>
/// </remarks>
public interface IDeviceSessionLeaseStore
{
    /// <summary>
    /// 夺取设备租约。若已有不同 ConnectionLeaseId，返回旧 SessionId；否则返回 null。
    /// </summary>
    ValueTask<string?> TakeOverAsync(
        long userId,
        ulong deviceIdHash,
        string sessionId,
        string connectionLeaseId,
        TimeSpan ttl,
        CancellationToken cancellationToken);

    /// <summary>
    /// 仅当租约仍属于本 ConnectionLeaseId 时释放（避免擦掉更新的登录）。
    /// </summary>
    ValueTask ReleaseIfOwnerAsync(
        long userId,
        ulong deviceIdHash,
        string connectionLeaseId,
        CancellationToken cancellationToken);

    /// <summary>
    /// 仅当租约仍属于本 ConnectionLeaseId 时刷新 TTL。
    /// <para>
    /// 在心跳扫描中对活跃连接调用，防止长连接租约过期被误判为离线。
    /// </para>
    /// </summary>
    /// <returns> true 表示刷新成功（仍持有租约）；false 表示不再持有（已被接管）。</returns>
    ValueTask<bool> RefreshIfOwnerAsync(
        long userId,
        ulong deviceIdHash,
        string connectionLeaseId,
        TimeSpan ttl,
        CancellationToken cancellationToken);
}
