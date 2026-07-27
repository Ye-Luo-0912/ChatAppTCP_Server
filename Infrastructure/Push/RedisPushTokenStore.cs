using System.Globalization;
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
/// Redis 推送令牌存储（原子 Lua 实现）。
/// <para>
/// 双 key 模型，使用 hash tag <c>{userId}</c> 保证同用户 Hash 与 ZSET 落在 Redis Cluster
/// 同一 slot，从而可在 Lua 中原子操作：
/// <list type="bullet">
/// <item><c>push:tokens:{userId}:h</c> — Hash，field = deviceIdHash（20 位十进制字符串），value = PushTokenRecord JSON。</item>
/// <item><c>push:tokens:{userId}:z</c> — ZSET，member = deviceIdHash，score = UpdatedAtMs，用于按时间淘汰。</item>
/// </list>
/// </para>
/// <para>
/// 旧版 key <c>push:tokens:userId</c>（无 ZSET）已废弃，不主动迁移：旧 key 仍持 90 天 TTL，
/// 自然过期；新注册一律走新 key。这避免一次性 SCAN 全库的迁移成本，且旧 key 与新 key 互不干扰。
/// </para>
/// <para>
/// 多设备场景：同一用户可有多个 deviceIdHash 各持一个令牌。
/// 超过 <see cref="PushTokenLimits.MaxTokensPerUser"/> 时按 UpdatedAtMs 最旧淘汰，淘汰操作在
/// Lua 内完成（HDEL + ZREM 同步），保证 Hash 与 ZSET 不会出现 membership 漂移。
/// </para>
/// </summary>
internal sealed class RedisPushTokenStore(
    RedisConnectionProvider connectionProvider,
    TimeProvider timeProvider,
    ILogger<RedisPushTokenStore> logger)
    : IPushTokenStore
{
    private const string KeyPrefix = "push:tokens:";
    private const string HashKeySuffix = ":h";
    private const string ZsetKeySuffix = ":z";
    private const long DefaultTtlMs = 90L * 24 * 60 * 60 * 1000; // 90 天

    /// <summary>
    /// 原子注册脚本：HSET upsert + ZADD upsert + 超额淘汰 + PEXPIRE 双 key。
    /// <para>
    /// ARGV 顺序：field, value, now, ttl, max。
    /// 返回淘汰后的剩余令牌数（HLEN）。
    /// </para>
    /// </summary>
    private const string RegisterScript = @"
local hashKey = KEYS[1]
local zsetKey = KEYS[2]
local field = ARGV[1]
local value = ARGV[2]
local now = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local max = tonumber(ARGV[5])

redis.call('HSET', hashKey, field, value)
redis.call('ZADD', zsetKey, now, field)

local count = redis.call('ZCARD', zsetKey)
while count > max do
    local oldest = redis.call('ZRANGE', zsetKey, 0, 0)
    if #oldest == 0 then break end
    local oldestField = oldest[1]
    redis.call('HDEL', hashKey, oldestField)
    redis.call('ZREM', zsetKey, oldestField)
    count = count - 1
end

redis.call('PEXPIRE', hashKey, ttl)
redis.call('PEXPIRE', zsetKey, ttl)

return redis.call('HLEN', hashKey)
";

    /// <summary>
    /// 原子按设备注销脚本：HDEL + ZREM + 空则 DEL 双 key。
    /// <para>
    /// ARGV 顺序：field。
    /// 返回剩余令牌数（HLEN）。
    /// </para>
    /// </summary>
    private const string UnregisterByDeviceScript = @"
local hashKey = KEYS[1]
local zsetKey = KEYS[2]
local field = ARGV[1]

redis.call('HDEL', hashKey, field)
redis.call('ZREM', zsetKey, field)

local remaining = redis.call('HLEN', hashKey)
if remaining == 0 then
    redis.call('DEL', hashKey)
    redis.call('DEL', zsetKey)
end
return remaining
";

    /// <summary>
    /// 原子按 token 注销脚本：批量 HDEL + ZREM + 空则 DEL 双 key。
    /// <para>
    /// ARGV 顺序：field1, field2, ...（待删除字段）。
    /// 返回剩余令牌数（HLEN）。
    /// </para>
    /// </summary>
    private const string UnregisterByTokenScript = @"
local hashKey = KEYS[1]
local zsetKey = KEYS[2]

for i = 1, #ARGV do
    redis.call('HDEL', hashKey, ARGV[i])
    redis.call('ZREM', zsetKey, ARGV[i])
end

local remaining = redis.call('HLEN', hashKey)
if remaining == 0 then
    redis.call('DEL', hashKey)
    redis.call('DEL', zsetKey)
end
return remaining
";

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

        var (hashKey, zsetKey) = CreateKeys(userId);
        var field = FormatField(deviceIdHash);
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
            var result = await connectionProvider.Database
                .ScriptEvaluateAsync(
                    RegisterScript,
                    new RedisKey[] { hashKey, zsetKey },
                    new RedisValue[]
                    {
                        field,
                        valueJson,
                        nowMs,
                        DefaultTtlMs,
                        PushTokenLimits.MaxTokensPerUser
                    })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            return (int)(long)result;
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

        var (hashKey, zsetKey) = CreateKeys(userId);
        var field = FormatField(deviceIdHash);

        try
        {
            var result = await connectionProvider.Database
                .ScriptEvaluateAsync(
                    UnregisterByDeviceScript,
                    new RedisKey[] { hashKey, zsetKey },
                    new RedisValue[] { field })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            return (int)(long)result;
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

        var (hashKey, zsetKey) = CreateKeys(userId);

        try
        {
            var db = connectionProvider.Database;
            var entries = await db.HashGetAllAsync(hashKey)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            if (entries.Length == 0)
                return 0;

            // 客户端解析 JSON 收集匹配 field，再通过 Lua 原子删除 Hash + ZSET。
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

            var args = new RedisValue[fieldsToDelete.Count];
            for (var i = 0; i < fieldsToDelete.Count; i++)
                args[i] = fieldsToDelete[i];

            var result = await db
                .ScriptEvaluateAsync(
                    UnregisterByTokenScript,
                    new RedisKey[] { hashKey, zsetKey },
                    args)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            return (int)(long)result;
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

        var (hashKey, _) = CreateKeys(userId);

        try
        {
            var db = connectionProvider.Database;
            var entries = await db.HashGetAllAsync(hashKey)
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

    private static string FormatField(ulong deviceIdHash) =>
        deviceIdHash.ToString("D20", CultureInfo.InvariantCulture);

    private static (RedisKey HashKey, RedisKey ZsetKey) CreateKeys(long userId)
    {
        // hash tag {userId} 保证 Hash 与 ZSET 落在 Redis Cluster 同一 slot。
        var userSegment = userId.ToString("D", CultureInfo.InvariantCulture);
        var hashKey = new RedisKey(KeyPrefix + "{" + userSegment + "}" + HashKeySuffix);
        var zsetKey = new RedisKey(KeyPrefix + "{" + userSegment + "}" + ZsetKeySuffix);
        return (hashKey, zsetKey);
    }
}
