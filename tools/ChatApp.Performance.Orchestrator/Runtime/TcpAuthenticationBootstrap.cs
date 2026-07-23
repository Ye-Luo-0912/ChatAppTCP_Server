using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using StackExchange.Redis;

namespace ChatApp.Performance.Orchestrator.Runtime;

internal sealed class TcpAuthenticationBootstrap : IAsyncDisposable
{
    private const string CacheKeyPrefix = "cache:AT:";
    private const string ValueField = "value";

    private readonly ConnectionMultiplexer _connection;
    private readonly RedisKey _cacheKey;

    private TcpAuthenticationBootstrap(
        ConnectionMultiplexer connection,
        RedisKey cacheKey,
        string token)
    {
        _connection = connection;
        _cacheKey = cacheKey;
        Token = token;
    }

    public string Token { get; }

    public static async Task<TcpAuthenticationBootstrap> CreateAsync(
        string connectionString,
        long userId,
        CancellationToken cancellationToken)
    {
        var connection = await ConnectionMultiplexer
            .ConnectAsync(connectionString)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        try
        {
            var token = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
            var tokenHash = SHA256.HashData(Encoding.UTF8.GetBytes(token));
            var cacheKey = CacheKeyPrefix + Convert.ToHexString(tokenHash);
            var record = JsonSerializer.SerializeToUtf8Bytes(
                new BootstrapAccessTokenRecord
                {
                    UserId = userId,
                    UserName = "performance-benchmark",
                    Roles = [],
                    ExpiresAtMs = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeMilliseconds(),
                    SessionId = "performance-benchmark"
                });

            await connection.GetDatabase()
                .HashSetAsync(cacheKey, ValueField, record)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            return new TcpAuthenticationBootstrap(connection, cacheKey, token);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await _connection.GetDatabase()
                .KeyDeleteAsync(_cacheKey)
                .ConfigureAwait(false);
        }
        finally
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    private sealed class BootstrapAccessTokenRecord
    {
        [JsonPropertyName("u")]
        public required long UserId { get; init; }

        [JsonPropertyName("n")]
        public required string UserName { get; init; }

        [JsonPropertyName("r")]
        public string[]? Roles { get; init; }

        [JsonPropertyName("e")]
        public required long ExpiresAtMs { get; init; }

        [JsonPropertyName("s")]
        public string? SessionId { get; init; }
    }
}