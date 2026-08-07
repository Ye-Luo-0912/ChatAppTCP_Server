using System.Security.Cryptography;
using System.Text.Json;
using ChatApp.Auth.Contracts;
using StackExchange.Redis;

namespace ChatApp.ResumeVerification.Runtime;

/// <summary>
/// 在 Redis 中引导写入 AccessToken，供网关认证使用。
/// 镜像 <c>TcpAuthenticationBootstrap</c> 的行为，但使用源生成 JSON 上下文，
/// 并允许设置 <see cref="AccessTokenCacheRecord.DeviceIdHash"/> 以便设备绑定场景验证。
/// </summary>
internal sealed class ResumeTokenBootstrap : IAsyncDisposable
{
    private readonly ConnectionMultiplexer _connection;
    private readonly RedisKey _cacheKey;

    private ResumeTokenBootstrap(
        ConnectionMultiplexer connection,
        RedisKey cacheKey,
        string token,
        ulong? deviceIdHash)
    {
        _connection = connection;
        _cacheKey = cacheKey;
        Token = token;
        DeviceIdHash = deviceIdHash;
    }

    /// <summary>引导写入的 AccessToken 明文（客户端发送给网关）。</summary>
    public string Token { get; }

    /// <summary>
    /// 写入 Redis 的 <see cref="AccessTokenCacheRecord.DeviceIdHash"/>。
    /// 调用方应将同一值通过 <c>AuthenticationRequest.DeviceIdHash</c> 发送给网关，
    /// 使 same-device fencing 路径被执行（AccessToken 与认证请求的设备指纹一致）。
    /// </summary>
    public ulong? DeviceIdHash { get; }

    /// <summary>
    /// 创建引导实例：连接 Redis，生成随机 Token，写入 AccessToken 记录。
    /// </summary>
    public static async Task<ResumeTokenBootstrap> CreateAsync(
        string connectionString,
        long userId,
        ulong? deviceIdHash,
        CancellationToken cancellationToken)
    {
        var connection = await ConnectionMultiplexer
            .ConnectAsync(connectionString)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        try
        {
            var token = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
            var cacheKey = AccessTokenCacheKey.Create(token);

            var record = new AccessTokenCacheRecord
            {
                UserId = userId,
                UserName = "resume-verification",
                Roles = [],
                ExpiresAtMs = DateTimeOffset.UtcNow.AddHours(1)
                    .ToUnixTimeMilliseconds(),
                SessionId = $"rv-{userId}",
                DeviceIdHash = deviceIdHash
            };

            var payload = JsonSerializer.SerializeToUtf8Bytes(
                record,
                AuthContractsJsonSerializerContext.Default.AccessTokenCacheRecord);

            await connection.GetDatabase()
                .StringSetAsync(cacheKey, payload)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            return new ResumeTokenBootstrap(connection, cacheKey, token, deviceIdHash);
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
}
