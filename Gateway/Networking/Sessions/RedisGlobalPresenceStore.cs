using ChatApp.TcpGateway.Infrastructure.Caching;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// Redis 全局在线：key = gw:presence:online:{userId} → instanceId。
/// TTL 防止实例崩溃后永久脏数据；本机心跳会刷新。
/// </summary>
internal sealed partial class RedisGlobalPresenceStore(
    RedisConnectionProvider connectionProvider,
    ILogger<RedisGlobalPresenceStore> logger) : IGlobalPresenceStore
{
    public static readonly TimeSpan OnlineTtl = TimeSpan.FromMinutes(5);

    private static RedisKey Key(long userId) => $"gw:presence:online:{userId}";

    public async Task SetOnlineAsync(long userId, string instanceId, CancellationToken ct = default)
    {
        if (userId <= 0 || string.IsNullOrWhiteSpace(instanceId))
            return;

        try
        {
            await connectionProvider.Database
                .StringSetAsync(Key(userId), instanceId, OnlineTtl)
                .WaitAsync(ct)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogSetOnlineFailed(logger, userId, ex);
        }
    }

    public async Task SetOfflineAsync(long userId, string instanceId, CancellationToken ct = default)
    {
        if (userId <= 0)
            return;

        try
        {
            var db = connectionProvider.Database;
            var key = Key(userId);
            var current = await db.StringGetAsync(key).WaitAsync(ct).ConfigureAwait(false);
            if (current.HasValue
                && !string.IsNullOrWhiteSpace(instanceId)
                && !string.Equals(current.ToString(), instanceId, StringComparison.Ordinal))
            {
                return;
            }

            await db.KeyDeleteAsync(key).WaitAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogSetOfflineFailed(logger, userId, ex);
        }
    }

    public async Task RefreshOnlineAsync(long userId, string instanceId, CancellationToken ct = default)
    {
        if (userId <= 0 || string.IsNullOrWhiteSpace(instanceId))
            return;

        try
        {
            var db = connectionProvider.Database;
            var key = Key(userId);
            var current = await db.StringGetAsync(key).WaitAsync(ct).ConfigureAwait(false);
            if (!current.HasValue
                || string.Equals(current.ToString(), instanceId, StringComparison.Ordinal))
            {
                await db.StringSetAsync(key, instanceId, OnlineTtl).WaitAsync(ct).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            LogRefreshFailed(logger, userId, ex);
        }
    }

    public async Task<bool> IsOnlineAsync(long userId, CancellationToken ct = default)
    {
        if (userId <= 0)
            return false;

        try
        {
            return await connectionProvider.Database
                .KeyExistsAsync(Key(userId))
                .WaitAsync(ct)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogQueryFailed(logger, userId, ex);
            return false;
        }
    }

    public async Task<IReadOnlyDictionary<long, bool>> GetOnlineManyAsync(
        IReadOnlyList<long> userIds,
        CancellationToken ct = default)
    {
        var result = new Dictionary<long, bool>(userIds.Count);
        if (userIds.Count == 0)
            return result;

        try
        {
            var db = connectionProvider.Database;
            var keys = userIds.Select(static id => Key(id)).ToArray();
            var values = await db.StringGetAsync(keys).WaitAsync(ct).ConfigureAwait(false);
            for (var i = 0; i < userIds.Count; i++)
                result[userIds[i]] = values[i].HasValue;
        }
        catch (Exception ex)
        {
            LogBatchQueryFailed(logger, userIds.Count, ex);
            foreach (var id in userIds)
                result[id] = false;
        }

        return result;
    }

    [LoggerMessage(EventId = 60, Level = LogLevel.Warning, Message = "设置全局在线失败 UserId={UserId}")]
    private static partial void LogSetOnlineFailed(ILogger logger, long userId, Exception exception);

    [LoggerMessage(EventId = 61, Level = LogLevel.Warning, Message = "清除全局在线失败 UserId={UserId}")]
    private static partial void LogSetOfflineFailed(ILogger logger, long userId, Exception exception);

    [LoggerMessage(EventId = 62, Level = LogLevel.Debug, Message = "刷新全局在线失败 UserId={UserId}")]
    private static partial void LogRefreshFailed(ILogger logger, long userId, Exception exception);

    [LoggerMessage(EventId = 63, Level = LogLevel.Debug, Message = "查询全局在线失败 UserId={UserId}")]
    private static partial void LogQueryFailed(ILogger logger, long userId, Exception exception);

    [LoggerMessage(EventId = 64, Level = LogLevel.Warning, Message = "批量查询全局在线失败 Count={Count}")]
    private static partial void LogBatchQueryFailed(ILogger logger, int count, Exception exception);
}
