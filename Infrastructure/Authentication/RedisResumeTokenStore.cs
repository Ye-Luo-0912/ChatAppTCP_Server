using System.Text.Json;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.Authentication;

/// <summary>
/// Redis 实现的 ResumeToken 存储。
/// Key 格式：tcp:resume:{token}
/// Value 格式：JSON 序列化的 <see cref="ResumeContext"/>
/// TTL 由 Redis PEXPIRE 自动管理。
/// </summary>
internal sealed class RedisResumeTokenStore(
    RedisConnectionProvider connectionProvider) : IResumeTokenStore
{
    private const string KeyPrefix = "tcp:resume:";

    public async Task<string> IssueAsync(
        ResumeContext context,
        TimeSpan ttl,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(ttl, TimeSpan.Zero);

        var token = Guid.NewGuid().ToString("N");
        var key = new RedisKey(KeyPrefix + token);
        var value = JsonSerializer.SerializeToUtf8Bytes(
            context,
            GatewayJsonSerializerContext.Default.ResumeContext);

        await connectionProvider.Database.StringSetAsync(
            key,
            value,
            ttl,
            When.NotExists,
            CommandFlags.DemandMaster).ConfigureAwait(false);

        return token;
    }

    public async Task<ResumeContext?> TryValidateAsync(
        string resumeToken,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(resumeToken))
            return null;

        var key = new RedisKey(KeyPrefix + resumeToken.Trim());

        // GETDEL：消费式读取，Token 一次性使用。
        // 即使客户端误重放，第二次返回 null。
        var value = (byte[]?)await connectionProvider.Database
            .StringGetDeleteAsync(key)
            .ConfigureAwait(false);
        if (value is null || value.Length == 0)
            return null;

        try
        {
            return JsonSerializer.Deserialize(
                value,
                GatewayJsonSerializerContext.Default.ResumeContext);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    public async Task RevokeAsync(string resumeToken, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(resumeToken))
            return;

        var key = new RedisKey(KeyPrefix + resumeToken.Trim());
        await connectionProvider.Database.KeyDeleteAsync(key).ConfigureAwait(false);
    }
}
