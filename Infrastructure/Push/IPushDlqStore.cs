using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// 推送投递死信队列（DLQ）存储。
/// </summary>
public interface IPushDlqStore
{
    /// <summary>记录一条投递死信。</summary>
    Task RecordAsync(PushDlqEntry entry, CancellationToken cancellationToken = default);
}

/// <summary>
/// 测试用内存 DLQ 存储。
/// </summary>
internal sealed class InMemoryPushDlqStore : IPushDlqStore
{
    private readonly object _lock = new();
    public List<PushDlqEntry> Entries { get; } = [];

    public Task RecordAsync(PushDlqEntry entry, CancellationToken cancellationToken = default)
    {
        lock (_lock)
        {
            Entries.Add(entry);
        }
        return Task.CompletedTask;
    }
}

/// <summary>
/// Redis 推送 DLQ 存储。
/// <para>
/// 以 list <c>push:dlq</c> 追加（RPUSH）<see cref="PushDlqEntry"/> JSON，并控制长度上限与 TTL，
/// 防止无限增长。Redis 故障时记录到日志（best-effort），不阻断投递主流程。
/// </para>
/// </summary>
internal sealed class RedisPushDlqStore : IPushDlqStore
{
    private const string ListKey = "push:dlq";
    private const int MaxEntries = 100_000;
    private static readonly TimeSpan DefaultTtl = TimeSpan.FromDays(7);

    private readonly RedisConnectionProvider _connectionProvider;
    private readonly ILogger<RedisPushDlqStore> _logger;

    public RedisPushDlqStore(
        RedisConnectionProvider connectionProvider,
        ILogger<RedisPushDlqStore> logger)
    {
        _connectionProvider = connectionProvider;
        _logger = logger;
    }

    public async Task RecordAsync(
        PushDlqEntry entry,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var db = _connectionProvider.Database;
            var json = System.Text.Json.JsonSerializer.Serialize(
                entry,
                ChatApp.TcpGateway.Infrastructure.Serialization.Json.GatewayJsonSerializerContext.Default.PushDlqEntry);

            await db.ListRightPushAsync(ListKey, json).WaitAsync(cancellationToken).ConfigureAwait(false);
            // 控制长度：超出上限从左侧裁剪最旧条目。
            await db.ListTrimAsync(ListKey, -MaxEntries, -1).WaitAsync(cancellationToken).ConfigureAwait(false);
            // 刷新 TTL（每次写入重置 7 天保留期）。
            await db.KeyExpireAsync(ListKey, DefaultTtl).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.PushDlqRecord,
                ex);
        }
    }
}