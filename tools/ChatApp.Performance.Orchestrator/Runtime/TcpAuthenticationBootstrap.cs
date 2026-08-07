using System.Security.Cryptography;
using System.Text.Json;
using ChatApp.Auth.Contracts;
using StackExchange.Redis;

namespace ChatApp.Performance.Orchestrator.Runtime;

internal sealed class TcpAuthenticationBootstrap : IAsyncDisposable
{
    private const int RedisBatchSize = 1_024;

    private readonly ConnectionMultiplexer _connection;
    private readonly RedisKey[] _cacheKeys;

    private TcpAuthenticationBootstrap(
        ConnectionMultiplexer connection,
        RedisKey[] cacheKeys,
        IReadOnlyList<TcpBootstrapIdentity> identities,
        TimeSpan tokenLifetime)
    {
        _connection = connection;
        _cacheKeys = cacheKeys;
        Identities = identities;
        TokenLifetime = tokenLifetime;
    }

    public IReadOnlyList<TcpBootstrapIdentity> Identities { get; }
    public TimeSpan TokenLifetime { get; }

    public static async Task<TcpAuthenticationBootstrap> CreateAsync(
        string connectionString,
        long firstUserId,
        int userCount,
        TimeSpan tokenLifetime,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(firstUserId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userCount);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(tokenLifetime, TimeSpan.Zero);
        _ = checked(firstUserId + userCount - 1L);

        var connection = await ConnectionMultiplexer
            .ConnectAsync(connectionString)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        var identities = new TcpBootstrapIdentity[userCount];
        var cacheKeys = new RedisKey[userCount];
        var expiresAt = DateTimeOffset.UtcNow.Add(tokenLifetime);
        var database = connection.GetDatabase();

        try
        {
            await VerifyRedisCapabilitiesAsync(database, cancellationToken).ConfigureAwait(false);
            for (var offset = 0; offset < userCount; offset += RedisBatchSize)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var count = Math.Min(RedisBatchSize, userCount - offset);
                var writes = new Task<bool>[count];
                var batchKeys = new RedisKey[count];
                for (var batchIndex = 0; batchIndex < count; batchIndex++)
                {
                    var index = offset + batchIndex;
                    var userId = checked(firstUserId + index);
                    var token = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
                    var cacheKey = AccessTokenCacheKey.Create(token);
                    var record = JsonSerializer.SerializeToUtf8Bytes(
                        new AccessTokenCacheRecord
                        {
                            UserId = userId,
                            UserName = $"performance-benchmark-{userId}",
                            Roles = [],
                            ExpiresAtMs = expiresAt.ToUnixTimeMilliseconds(),
                            SessionId = $"performance-benchmark-{userId}",
                        },
                        AuthContractsJsonSerializerContext.Default.AccessTokenCacheRecord);

                    identities[index] = new TcpBootstrapIdentity(userId, token);
                    batchKeys[batchIndex] = cacheKey;
                    writes[batchIndex] = database.StringSetAsync(
                        cacheKey,
                        record,
                        tokenLifetime,
                        When.NotExists);
                }

                var results = await Task.WhenAll(writes).ConfigureAwait(false);
                for (var batchIndex = 0; batchIndex < count; batchIndex++)
                {
                    if (results[batchIndex])
                        cacheKeys[offset + batchIndex] = batchKeys[batchIndex];
                }
                if (results.Any(static written => !written))
                    throw new InvalidOperationException("A generated benchmark access-token key already existed.");
            }

            return new TcpAuthenticationBootstrap(
                connection,
                cacheKeys,
                identities,
                tokenLifetime);
        }
        catch
        {
            await DeleteKeysBestEffortAsync(database, cacheKeys).ConfigureAwait(false);
            await connection.CloseAsync().ConfigureAwait(false);
            connection.Dispose();
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await DeleteKeysBestEffortAsync(_connection.GetDatabase(), _cacheKeys)
                .ConfigureAwait(false);
        }
        finally
        {
            await _connection.CloseAsync().ConfigureAwait(false);
            _connection.Dispose();
        }
    }

    private static async Task DeleteKeysBestEffortAsync(
        IDatabase database,
        RedisKey[] cacheKeys)
    {
        for (var offset = 0; offset < cacheKeys.Length; offset += RedisBatchSize)
        {
            var count = Math.Min(RedisBatchSize, cacheKeys.Length - offset);
            var batch = new RedisKey[count];
            for (var index = 0; index < count; index++)
            {
                var key = cacheKeys[offset + index];
                if (!key.Equals(default(RedisKey)))
                    batch[index] = key;
            }

            var populated = batch
                .Where(static key => !key.Equals(default(RedisKey)))
                .ToArray();
            if (populated.Length != 0)
                await database.KeyDeleteAsync(populated).ConfigureAwait(false);
        }
    }

    private static async Task VerifyRedisCapabilitiesAsync(
        IDatabase database,
        CancellationToken cancellationToken)
    {
        var scriptResult = await database.ScriptEvaluateAsync("return 1")
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        if ((long)scriptResult != 1L)
            throw new InvalidOperationException("Redis Lua capability probe returned an unexpected value.");

        var probeKey = (RedisKey)$"chatapp:perf:capability:{Guid.NewGuid():N}";
        var probeValue = (RedisValue)Guid.NewGuid().ToString("N");
        try
        {
            if (!await database.StringSetAsync(probeKey, probeValue, TimeSpan.FromMinutes(1), When.NotExists)
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false))
            {
                throw new InvalidOperationException("Redis capability probe could not create a temporary key.");
            }

            var read = await database.StringGetAsync(probeKey)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            if (read != probeValue)
                throw new InvalidOperationException("Redis capability probe read did not match its write.");
        }
        finally
        {
            await database.KeyDeleteAsync(probeKey).ConfigureAwait(false);
        }
    }
}

internal sealed record TcpBootstrapIdentity(long UserId, string Token);
