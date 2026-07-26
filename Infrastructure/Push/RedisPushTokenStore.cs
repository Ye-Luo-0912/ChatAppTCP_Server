using System.Text.Json;
using ChatApp.TcpGateway.Core.Messaging.Push;
using ChatApp.TcpGateway.Core.Push;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// Redis 推送令牌存储。
/// <para>
/// Key = push:tokens:{userId}（Hash）；field = deviceIdHash（十进制字符串）；
/// value = PushTokenRecord JSON。每次写入刷新 90 天 TTL。
/// </para>
/// <para>
/// 多设备场景：同一用户可有多个 deviceIdHash 各持一个令牌。
/// 超过 <see cref="PushTokenLimits.MaxTokensPerUser"/> 时按 UpdatedAtMs 最旧淘汰。
/// </para>
/// </summary>
internal sealed class RedisPushTokenStore(
    RedisConnectionProvider connectionProvider,
    TimeProvider timeProvider,
    ILogger<RedisPushTokenStore> logger)
    : IPushTokenStore
{
    private const string KeyPrefix = "push:tokens:";
    private const long DefaultTtlMs = 90L * 24 * 60 * 60 * 1000; // 90 天

    public async ValueTask<int> RegisterAsync(
        long userId,
        ulong deviceIdHash,
        PushPlatform platform,
        string token,
        string? appDeviceLabel,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        ArgumentException.ThrowIfNullOrWhiteSpace(token);
        ArgumentOutOfRangeException.ThrowIfZero(deviceIdHash);

        var key = CreateKey(userId);
        var field = deviceIdHash.ToString("D20", System.Globalization.CultureInfo.InvariantCulture);
        var nowMs = timeProvider.GetUtcNow().ToUnixTimeMilliseconds();

        var record = new PushTokenRecord
        {
            Token = token,
            Platform = platform,
            DeviceIdHash = deviceIdHash,
            AppDeviceLabel = appDeviceLabel,
            UpdatedAtMs = nowMs
        };
        var valueJson = JsonSerializer.Serialize(
            record,
            GatewayJsonSerializerContext.Default.PushTokenRecord);

        try
        {
            var db = connectionProvider.Database;
            await db.HashSetAsync(key, field, valueJson)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            await db.KeyExpireAsync(key, TimeSpan.FromMilliseconds(DefaultTtlMs))
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            // 超额淘汰：拉取全部字段，按 UpdatedAtMs 排序，删除最旧的直到 ≤ MaxTokensPerUser。
            var count = await EvictExcessAsync(db, key, cancellationToken)
                .ConfigureAwait(false);
            return count;
        }
        catch (RedisException exception)
        {
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.PushTokenRegister,
                exception);
            throw;
        }
    }

    public async ValueTask<int> UnregisterByDeviceAsync(
        long userId,
        ulong deviceIdHash,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        ArgumentOutOfRangeException.ThrowIfZero(deviceIdHash);

        var key = CreateKey(userId);
        var field = deviceIdHash.ToString("D20", System.Globalization.CultureInfo.InvariantCulture);

        try
        {
            var db = connectionProvider.Database;
            await db.HashDeleteAsync(key, field)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            return await EnsureKeyRemovedIfEmptyAsync(db, key, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (RedisException exception)
        {
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.PushTokenUnregister,
                exception);
            throw;
        }
    }

    public async ValueTask<int> UnregisterByTokenAsync(
        long userId,
        string token,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        ArgumentException.ThrowIfNullOrWhiteSpace(token);

        var key = CreateKey(userId);

        try
        {
            var db = connectionProvider.Database;
            var entries = await db.HashGetAllAsync(key)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            if (entries.Length == 0)
                return 0;

            // 扫描所有字段，删除匹配 token 的（同一 token 在多设备注册时全部删除）。
            var fieldsToDelete = new List<RedisValue>();
            foreach (var entry in entries)
            {
                PushTokenRecord? record;
                try
                {
                    record = JsonSerializer.Deserialize(
                        (byte[]?)entry.Value,
                        GatewayJsonSerializerContext.Default.PushTokenRecord);
                }
                catch (JsonException)
                {
                    // 损坏数据：跳过，不参与本次删除。
                    continue;
                }

                if (record is not null
                    && string.Equals(record.Token, token, StringComparison.Ordinal))
                {
                    fieldsToDelete.Add(entry.Name);
                }
            }

            if (fieldsToDelete.Count == 0)
                return entries.Length;

            await db.HashDeleteAsync(key, fieldsToDelete.ToArray())
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            return await EnsureKeyRemovedIfEmptyAsync(db, key, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (RedisException exception)
        {
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.PushTokenUnregister,
                exception);
            throw;
        }
    }

    public async ValueTask<IReadOnlyList<PushTokenRecord>> ListAsync(
        long userId,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);

        var key = CreateKey(userId);

        try
        {
            var db = connectionProvider.Database;
            var entries = await db.HashGetAllAsync(key)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            if (entries.Length == 0)
                return [];

            var result = new List<PushTokenRecord>(entries.Length);
            foreach (var entry in entries)
            {
                try
                {
                    var record = JsonSerializer.Deserialize(
                        (byte[]?)entry.Value,
                        GatewayJsonSerializerContext.Default.PushTokenRecord);
                    if (record is not null)
                        result.Add(record);
                }
                catch (JsonException ex)
                {
                    logger.DependencyDataInvalid(
                        GatewayDependency.Redis,
                        GatewayDependencyOperation.PushTokenList,
                        ex);
                }
            }

            return result;
        }
        catch (RedisException exception)
        {
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.PushTokenList,
                exception);
            throw;
        }
    }

    private static async ValueTask<int> EvictExcessAsync(
        IDatabase db,
        RedisKey key,
        CancellationToken cancellationToken)
    {
        var entries = await db.HashGetAllAsync(key)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        if (entries.Length <= PushTokenLimits.MaxTokensPerUser)
            return entries.Length;

        // 解析所有记录并按 UpdatedAtMs 升序排序，删除最旧的。
        var parsed = new List<(RedisValue Field, long UpdatedAtMs)>(entries.Length);
        foreach (var entry in entries)
        {
            try
            {
                var record = JsonSerializer.Deserialize(
                    (byte[]?)entry.Value,
                    GatewayJsonSerializerContext.Default.PushTokenRecord);
                if (record is not null)
                    parsed.Add((entry.Name, record.UpdatedAtMs));
            }
            catch (JsonException)
            {
                // 损坏数据优先淘汰。
                parsed.Add((entry.Name, 0));
            }
        }

        parsed.Sort(static (a, b) => a.UpdatedAtMs.CompareTo(b.UpdatedAtMs));
        var toEvict = parsed.Count - PushTokenLimits.MaxTokensPerUser;
        if (toEvict <= 0)
            return parsed.Count;

        var fieldsToDelete = new RedisValue[toEvict];
        for (var i = 0; i < toEvict; i++)
            fieldsToDelete[i] = parsed[i].Field;

        await db.HashDeleteAsync(key, fieldsToDelete)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        return parsed.Count - toEvict;
    }

    private static async ValueTask<int> EnsureKeyRemovedIfEmptyAsync(
        IDatabase db,
        RedisKey key,
        CancellationToken cancellationToken)
    {
        var len = await db.HashLengthAsync(key)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        if (len == 0)
        {
            await db.KeyDeleteAsync(key)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            return 0;
        }
        return (int)len;
    }

    private static RedisKey CreateKey(long userId) =>
        KeyPrefix + userId.ToString("D", System.Globalization.CultureInfo.InvariantCulture);
}
