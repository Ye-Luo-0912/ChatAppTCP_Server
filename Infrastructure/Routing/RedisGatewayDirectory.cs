using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.Routing;

/// <summary>
/// Redis 实现：读取与 <c>RedisGlobalPresenceStore</c> 相同的 ZSET
/// （<c>presence:{userId}:instances</c>，member = GatewayInstanceId，score = ExpiresAtUnixMs），
/// 过滤过期成员后返回有效的 Gateway 实例 ID 集合。
/// <para>
/// 查询失败时返回空集合（不抛异常），调用方据此回退到广播模式。
/// 不执行清理操作（ZREMRANGEBYSCORE），清理由 PresenceStore 的写路径负责。
/// </para>
/// </summary>
internal sealed class RedisGatewayDirectory(
    RedisConnectionProvider connectionProvider,
    TimeProvider timeProvider,
    ILogger<RedisGatewayDirectory> logger) : IGatewayDirectory
{
    private static RedisKey Key(long userId) => $"presence:{userId}:instances";

    public async Task<IReadOnlyList<string>> GetOnlineGatewaysAsync(
        long userId,
        CancellationToken cancellationToken = default)
    {
        if (userId <= 0)
            return Array.Empty<string>();

        try
        {
            var nowMs = timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
            var db = connectionProvider.Database;

            // 读取 score > nowMs 的成员（未过期）。
            // exclude: Start 表示 (nowMs, +inf]，排除已过期成员。
            var members = await db.SortedSetRangeByScoreAsync(
                Key(userId),
                nowMs,
                double.PositiveInfinity,
                Exclude.Start)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            if (members.Length == 0)
                return Array.Empty<string>();

            var result = new string[members.Length];
            for (var i = 0; i < members.Length; i++)
                result[i] = members[i].ToString();

            return result;
        }
        catch (Exception ex)
        {
            // 路由目录查询失败时返回空集合，调用方回退到广播模式。
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.GatewayDirectoryQuery,
                ex);
            return Array.Empty<string>();
        }
    }

    public async Task<IReadOnlyDictionary<long, IReadOnlyList<string>>> GetOnlineGatewaysManyAsync(
        IReadOnlyList<long> userIds,
        CancellationToken cancellationToken = default)
    {
        var result = new Dictionary<long, IReadOnlyList<string>>(userIds.Count);
        if (userIds.Count == 0)
            return result;

        try
        {
            var nowMs = timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
            var db = connectionProvider.Database;
            var batch = db.CreateBatch();

            var tasks = new Task<RedisValue[]>[userIds.Count];
            for (var i = 0; i < userIds.Count; i++)
            {
                var userId = userIds[i];
                if (userId <= 0)
                {
                    result[userId] = Array.Empty<string>();
                    tasks[i] = Task.FromResult(Array.Empty<RedisValue>());
                    continue;
                }

                tasks[i] = batch.SortedSetRangeByScoreAsync(
                    Key(userId),
                    nowMs,
                    double.PositiveInfinity,
                    Exclude.Start);
            }

            batch.Execute();
            var results = await Task.WhenAll(tasks).WaitAsync(cancellationToken).ConfigureAwait(false);

            for (var i = 0; i < userIds.Count; i++)
            {
                var userId = userIds[i];
                if (userId <= 0)
                    continue;

                var members = results[i];
                if (members.Length == 0)
                {
                    result[userId] = Array.Empty<string>();
                    continue;
                }

                var gateways = new string[members.Length];
                for (var j = 0; j < members.Length; j++)
                    gateways[j] = members[j].ToString();

                result[userId] = gateways;
            }
        }
        catch (Exception ex)
        {
            // 批量查询失败：所有用户回退到空集合（广播模式）。
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.GatewayDirectoryQuery,
                ex);

            foreach (var userId in userIds)
                result.TryAdd(userId, Array.Empty<string>());
        }

        return result;
    }
}
