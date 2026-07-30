using System.Globalization;
using StackExchange.Redis;

namespace ChatApp.ResumeVerification.Runtime;

/// <summary>
/// 直接查询 Redis 设备租约，用于场景验证 TakeOver 后 owner 已更新。
/// <para>
/// Key 格式：<c>tcp:devlease:{userId}:{deviceIdHash}</c>
/// Value 格式（P1-A2 三字段）：<c>leaseOwnerToken\ntransportId\nsessionId</c>
/// 兼容旧值（两字段）：<c>connectionLeaseId\nsessionId</c>
/// </para>
/// </summary>
internal sealed class RedisLeaseProbe : IAsyncDisposable
{
    private const string KeyPrefix = "tcp:devlease:";
    private readonly IConnectionMultiplexer _redis;

    private RedisLeaseProbe(IConnectionMultiplexer redis)
    {
        _redis = redis;
    }

    /// <summary>连接到 Redis 并返回探测实例。</summary>
    public static async Task<RedisLeaseProbe> ConnectAsync(
        string connectionString,
        CancellationToken cancellationToken)
    {
        var redis = await ConnectionMultiplexer.ConnectAsync(connectionString)
            .ConfigureAwait(false);
        return new RedisLeaseProbe(redis);
    }

    /// <summary>
    /// 读取指定用户+设备的租约值。返回 null 表示 key 不存在。
    /// </summary>
    public async Task<DeviceLeaseSnapshot?> ReadLeaseAsync(
        long userId,
        ulong deviceIdHash,
        CancellationToken cancellationToken)
    {
        var key = CreateKey(userId, deviceIdHash);
        var db = _redis.GetDatabase();
        var value = await db.StringGetAsync(key).ConfigureAwait(false);
        if (!value.HasValue)
        {
            return null;
        }

        return ParseLeaseValue(value.ToString());
    }

    /// <summary>
    /// 删除指定用户+设备的租约（测试清理用）。
    /// </summary>
    public async Task DeleteLeaseAsync(
        long userId,
        ulong deviceIdHash,
        CancellationToken cancellationToken)
    {
        var key = CreateKey(userId, deviceIdHash);
        var db = _redis.GetDatabase();
        await db.KeyDeleteAsync(key).ConfigureAwait(false);
    }

    private static string CreateKey(long userId, ulong deviceIdHash) =>
        string.Concat(
            KeyPrefix,
            userId.ToString(CultureInfo.InvariantCulture),
            ":",
            deviceIdHash.ToString(CultureInfo.InvariantCulture));

    /// <summary>
    /// 解析租约值。支持三字段（新）与两字段（旧）格式。
    /// </summary>
    private static DeviceLeaseSnapshot ParseLeaseValue(string value)
    {
        var parts = value.Split('\n');
        if (parts.Length >= 3)
        {
            // 新格式：leaseOwnerToken\ntransportId\nsessionId
            return new DeviceLeaseSnapshot(
                LeaseOwnerToken: parts[0],
                TransportId: parts[1],
                SessionId: parts[2]);
        }

        if (parts.Length == 2)
        {
            // 旧格式：connectionLeaseId\nsessionId（connectionLeaseId 同时承担两个角色）
            return new DeviceLeaseSnapshot(
                LeaseOwnerToken: parts[0],
                TransportId: parts[0],
                SessionId: parts[1]);
        }

        // 极旧格式：单字段 connectionLeaseId（无 sessionId）
        return new DeviceLeaseSnapshot(
            LeaseOwnerToken: parts[0],
            TransportId: parts[0],
            SessionId: string.Empty);
    }

    public async ValueTask DisposeAsync()
    {
        await _redis.CloseAsync().ConfigureAwait(false);
        _redis.Dispose();
    }
}

/// <summary>
/// 设备租约快照，用于场景验证。
/// </summary>
/// <param name="LeaseOwnerToken">私有所有权凭证（仅用于 Redis CAS）。</param>
/// <param name="TransportId">公开路由标识（用于跨 Gateway 吊销匹配）。</param>
/// <param name="SessionId">用户可见会话标识。</param>
internal sealed record DeviceLeaseSnapshot(
    string LeaseOwnerToken,
    string TransportId,
    string SessionId);
