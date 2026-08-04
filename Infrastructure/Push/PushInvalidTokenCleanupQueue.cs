using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// 无效 Token 清理工作项（门禁4）。
/// </summary>
internal sealed record PushInvalidTokenCleanupItem(long UserId, string Token);

/// <summary>
/// 无效 Token 可靠清理工作队列（门禁4）。
/// <para>
/// 推送投递主流程检测到 <c>invalid_token</c> 后，仅将清理任务入队（不阻塞请求生命周期），
/// 由 <see cref="PushInvalidTokenCleanupWorker"/> 后台可靠消费并带重试注销。
/// 有界队列使用 DropOldest：队列满时丢弃最旧项，绝不阻塞投递热路径。
/// </para>
/// </summary>
internal sealed partial class PushInvalidTokenCleanupQueue : IDisposable
{
    private readonly Channel<PushInvalidTokenCleanupItem> _channel;
    private readonly ILogger<PushInvalidTokenCleanupQueue> _logger;
    private bool _disposed;

    public PushInvalidTokenCleanupQueue(
        IOptions<PushOptions> options,
        ILogger<PushInvalidTokenCleanupQueue> logger)
    {
        _logger = logger;
        var capacity = Math.Max(1, options.Value.InvalidTokenCleanupQueueCapacity);
        _channel = Channel.CreateBounded<PushInvalidTokenCleanupItem>(
            new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
                SingleWriter = false
            });
    }

    /// <summary>后台 worker 读取端。</summary>
    public ChannelReader<PushInvalidTokenCleanupItem> Reader => _channel.Reader;

    /// <summary>入队一个无效 Token 清理任务。队列满时丢弃最旧（DropOldest），不阻塞投递路径。</summary>
    public void Enqueue(long userId, string token)
    {
        if (!_channel.Writer.TryWrite(new PushInvalidTokenCleanupItem(userId, token)))
            LogQueueFull(_logger, userId);
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
        _channel.Writer.TryComplete();
    }

    [LoggerMessage(
        LogLevel.Warning,
        "Invalid-token cleanup queue full; dropped entry for userId={UserId}.")]
    static partial void LogQueueFull(ILogger logger, long userId);
}