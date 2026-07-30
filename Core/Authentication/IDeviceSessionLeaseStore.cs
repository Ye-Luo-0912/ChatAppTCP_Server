namespace ChatApp.TcpGateway.Core.Authentication;

/// <summary>
/// 跨 Gateway 同设备 TCP 会话租约：发现旧 SessionId 以便广播 SessionRevoked。
/// </summary>
/// <remarks>
/// P1-A2：拆分 TransportId（公开路由标识）与 LeaseOwnerToken（私有所有权凭证）。
/// <para>
/// 租约 key = (userId, deviceIdHash)；
/// 租约 value = (leaseOwnerToken, transportId, sessionId)。
/// <list type="bullet">
/// <item><c>sessionId</c>：用户可见会话标识（用于 SessionRevoked 路由）。</item>
/// <item><c>transportId</c>：每次 TCP 连接生成的公开路由标识（= ConnectionLeaseId），
/// 写入 SessionRevokedPayload 供跨 Gateway 吊销匹配，可出现在日志/事件中。</item>
/// <item><c>leaseOwnerToken</c>：每次 TCP 连接生成的私有所有权凭证（独立 Guid），
/// 仅用于 Redis CAS（compare-and-delete/refresh），不写入广播事件，遵循最小权限原则。</item>
/// </list>
/// </para>
/// </remarks>
public interface IDeviceSessionLeaseStore
{
    /// <summary>
    /// 夺取设备租约。返回 <see cref="TakeOverResult"/> 三态：
    /// <list type="bullet">
    /// <item><see cref="TakeOverStatus.Success"/>：接管成功，存在旧租约，
    ///   <see cref="TakeOverResult.PreviousTransportId"/> 携带旧 TransportId 供吊销路由。</item>
    /// <item><see cref="TakeOverStatus.NoPreviousLease"/>：接管成功，无旧租约或旧租约与本连接相同。</item>
    /// <item><see cref="TakeOverStatus.DependencyUnavailable"/>：Redis 异常或熔断器开路。
    ///   调用方应 fail-closed（拒绝 Resume、回滚本地状态、要求完整认证）。</item>
    /// </list>
    /// <para>
    /// P1-A2：<paramref name="transportId"/> 与 <paramref name="leaseOwnerToken"/> 分离。
    /// transportId 写入 Redis 值供未来 TakeOver 读取（作为 PreviousTransportId 返回）；
    /// leaseOwnerToken 写入 Redis 值供未来 Release/Refresh CAS 校验。
    /// </para>
    /// </summary>
    ValueTask<TakeOverResult> TakeOverAsync(
        long userId,
        ulong deviceIdHash,
        string sessionId,
        string transportId,
        string leaseOwnerToken,
        TimeSpan ttl,
        CancellationToken cancellationToken);

    /// <summary>
    /// 仅当租约仍属于本 <paramref name="leaseOwnerToken"/> 时释放（避免擦掉更新的登录）。
    /// <para>
    /// P1-A2：使用私有 LeaseOwnerToken 而非公开 TransportId 做 CAS，遵循最小权限原则。
    /// </para>
    /// </summary>
    ValueTask ReleaseIfOwnerAsync(
        long userId,
        ulong deviceIdHash,
        string leaseOwnerToken,
        CancellationToken cancellationToken);

    /// <summary>
    /// 仅当租约仍属于本 <paramref name="leaseOwnerToken"/> 时刷新 TTL。
    /// <para>
    /// 在心跳扫描中对活跃连接调用，防止长连接租约过期被误判为离线。
    /// </para>
    /// <para>
    /// P1-A2：使用私有 LeaseOwnerToken 而非公开 TransportId 做 CAS。
    /// </para>
    /// </summary>
    /// <returns> true 表示刷新成功（仍持有租约）；false 表示不再持有（已被接管）。</returns>
    ValueTask<bool> RefreshIfOwnerAsync(
        long userId,
        ulong deviceIdHash,
        string leaseOwnerToken,
        TimeSpan ttl,
        CancellationToken cancellationToken);

    /// <summary>
    /// 查询当前持有设备租约的 SessionId（只读，不修改租约）。
    /// <para>
    /// 用于 Resume 恢复时校验待恢复会话是否仍为当前有效代次：
    /// 若返回值非空且与待恢复 SessionId 不同，说明已被更新登录接管，应拒绝恢复。
    /// </para>
    /// </summary>
    /// <returns>当前租约持有者的 SessionId；租约不存在或过期返回 null。</returns>
    ValueTask<string?> GetCurrentSessionIdAsync(
        long userId,
        ulong deviceIdHash,
        CancellationToken cancellationToken);
}
