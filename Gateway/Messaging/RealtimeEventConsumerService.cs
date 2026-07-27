using System.Threading.Channels;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using ChatApp.TcpGateway.Observability.Tracing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.TcpGateway.Gateway.Messaging;

/// <summary>
/// Realtime 事件消费者：从 JetStream 拉取事件后分发到本机会话。
/// <para>
/// 支持分区并行消费：按 <c>TargetUserId % PartitionCount</c> 将事件路由到固定分区，
/// 每分区单消费者保证同一用户的局部顺序；跨分区并行提升吞吐。
/// 分区数为 1 时退化为原有串行消费语义。
/// </para>
/// <para>
/// ACK 在分区 worker 内完成（dispatch 成功后 ACK，失败则 NAK 延迟重投）。
/// JetStream <c>MaxAckPending</c> 提供全局背压：分区 channel 满时主循环阻塞，
/// ACK 停止，broker 停止推送新消息。
/// </para>
/// </summary>
internal sealed class RealtimeEventConsumerService : BackgroundService
{
    private static readonly TimeSpan DeliveryRetryDelay =
        TimeSpan.FromSeconds(1);

    private readonly IRealtimeMessageBus _messageBus;
    private readonly RealtimeEventDispatcher _dispatcher;
    private readonly GatewayMetrics _metrics;
    private readonly TcpGatewayOptions _options;
    private readonly ILogger<RealtimeEventConsumerService> _logger;

    public RealtimeEventConsumerService(
        IRealtimeMessageBus messageBus,
        RealtimeEventDispatcher dispatcher,
        GatewayMetrics metrics,
        IOptions<TcpGatewayOptions> options,
        ILogger<RealtimeEventConsumerService> logger)
    {
        _messageBus = messageBus;
        _dispatcher = dispatcher;
        _metrics = metrics;
        _options = options.Value;
        _logger = logger;
    }

    public override async Task StartAsync(
        CancellationToken cancellationToken)
    {
        var latency = await _messageBus
            .PingAsync(cancellationToken)
            .ConfigureAwait(false);
        _logger.RealtimeBusReady(latency.TotalMilliseconds);

        await base.StartAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var retryAttempt = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConsumeAsync(stoppingToken)
                    .ConfigureAwait(false);
                throw new InvalidOperationException(
                    "Realtime event subscription ended unexpectedly.");
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                retryAttempt++;
                var retryDelay = TimeSpan.FromSeconds(
                    Math.Min(30, 1 << Math.Min(retryAttempt - 1, 5)));
                _logger.RealtimeSubscriptionFailed(
                    RealtimeSubscriptionKind.DurableEvents,
                    retryDelay,
                    exception);
                await Task.Delay(retryDelay, stoppingToken)
                    .ConfigureAwait(false);
            }
        }
    }

    private async Task ConsumeAsync(CancellationToken stoppingToken)
    {
        var partitionCount = _options.RealtimeEventPartitionCount;

        // 分区数为 1 时保持原有串行消费语义，避免 Channel 开销。
        if (partitionCount <= 1)
        {
            await ConsumeSerialAsync(stoppingToken).ConfigureAwait(false);
            return;
        }

        await ConsumePartitionedAsync(partitionCount, stoppingToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// 串行消费（原语义）：单循环 deserialize → dispatch → ACK → next。
    /// </summary>
    private async Task ConsumeSerialAsync(CancellationToken stoppingToken)
    {
        await foreach (var delivery in _messageBus
                           .ConsumeEventsAsync(stoppingToken)
                           .ConfigureAwait(false))
        {
            using var activity = GatewayTelemetry.StartEventConsumer(
                delivery.ParentContext);
            activity?.SetTag(
                "chat.event.type",
                delivery.Event.Type.ToString());
            _metrics.RealtimeEventReceived();

            try
            {
                _dispatcher.Dispatch(delivery.Event);
                await delivery.AckAsync(stoppingToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                GatewayTelemetry.RecordException(activity, exception);
                _logger.RealtimeDeliveryFailed(
                    delivery.Event.EventId,
                    delivery.DeliveryCount,
                    exception);
                await TryNakAsync(delivery, stoppingToken)
                    .ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// 分区并行消费：主循环拉取 JetStream 消息后按 TargetUserId 路由到分区 channel，
    /// 每分区单 worker 消费保证局部顺序。
    /// </summary>
    private async Task ConsumePartitionedAsync(
        int partitionCount,
        CancellationToken stoppingToken)
    {
        // 每分区容量：从 MaxAckPending 派生，保证全局在途消息受 broker 背压约束。
        // 使用 ConfigureAwait(false) 避免 partition worker 的同步上下文捕获。
        var perPartitionCapacity = Math.Max(16, 512 / partitionCount);
        var partitions = new Channel<RealtimeEventDelivery>[partitionCount];
        for (var i = 0; i < partitionCount; i++)
        {
            partitions[i] = Channel.CreateBounded<RealtimeEventDelivery>(
                new BoundedChannelOptions(perPartitionCapacity)
                {
                    SingleReader = true,
                    SingleWriter = false,
                    FullMode = BoundedChannelFullMode.Wait
                });
        }

        // 启动分区 worker。
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var workers = new Task[partitionCount];
        for (var i = 0; i < partitionCount; i++)
        {
            var partitionIndex = i;
            workers[i] = ConsumePartitionWorkerAsync(
                partitions[partitionIndex].Reader,
                cts.Token);
        }

        // 主循环：从 JetStream 拉取消息，路由到分区 channel。
        // 分区 channel 满时 WriteAsync 阻塞 → 停止 ACK → broker 背压生效。
        Task? mainLoop = null;
        try
        {
            mainLoop = Task.Run(async () =>
            {
                await foreach (var delivery in _messageBus
                                   .ConsumeEventsAsync(cts.Token)
                                   .ConfigureAwait(false))
                {
                    var partitionIndex = GetPartitionIndex(
                        delivery.Event.TargetUserId,
                        partitionCount);
                    await partitions[partitionIndex].Writer
                        .WriteAsync(delivery, cts.Token)
                        .ConfigureAwait(false);
                }
            }, cts.Token);

            await mainLoop.ConfigureAwait(false);
        }
        finally
        {
            // 主循环结束（正常或异常）：完成所有分区 channel，等待 worker 排空。
            foreach (var ch in partitions)
            {
                ch.Writer.TryComplete();
            }

            try
            {
                await Task.WhenAll(workers).ConfigureAwait(false);
            }
            catch
            {
                // worker 异常已通过 Task 状态传播；主循环异常优先抛出。
            }

            // 如果 worker 先于主循环失败，取消主循环以加速终止。
            if (mainLoop is { IsFaulted: false, IsCanceled: false, IsCompletedSuccessfully: false })
            {
                cts.Cancel();
                try { await mainLoop.ConfigureAwait(false); }
                catch { /* 取消导致的异常已预期 */ }
            }
        }
    }

    /// <summary>
    /// 分区 worker：从 channel 读取 delivery，dispatch + ACK/NAK。
    /// 单 reader 保证同一分区（同一 TargetUserId）的事件顺序。
    /// </summary>
    private async Task ConsumePartitionWorkerAsync(
        ChannelReader<RealtimeEventDelivery> reader,
        CancellationToken ct)
    {
        await foreach (var delivery in reader.ReadAllAsync(ct)
                           .ConfigureAwait(false))
        {
            using var activity = GatewayTelemetry.StartEventConsumer(
                delivery.ParentContext);
            activity?.SetTag(
                "chat.event.type",
                delivery.Event.Type.ToString());
            _metrics.RealtimeEventReceived();

            try
            {
                _dispatcher.Dispatch(delivery.Event);
                await delivery.AckAsync(ct)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                GatewayTelemetry.RecordException(activity, exception);
                _logger.RealtimeDeliveryFailed(
                    delivery.Event.EventId,
                    delivery.DeliveryCount,
                    exception);
                await TryNakAsync(delivery, ct)
                    .ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// 按 TargetUserId 计算分区索引。
    /// <para>
    /// 使用模运算而非 GetHashCode（后者在 .NET 中对 long 是进程随机的，
    /// 不适合跨进程稳定分区）。TargetUserId 是单调递增的数字 ID，
    /// 模分布均匀。
    /// </para>
    /// </summary>
    private static int GetPartitionIndex(long targetUserId, int partitionCount)
    {
        // 处理负数（理论不应出现，但防御性处理）。
        var positiveId = targetUserId >= 0 ? targetUserId : -targetUserId;
        return (int)((ulong)positiveId % (ulong)partitionCount);
    }

    private async Task TryNakAsync(
        RealtimeEventDelivery delivery,
        CancellationToken cancellationToken)
    {
        try
        {
            await delivery.NakAsync(
                    DeliveryRetryDelay,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
            when (!cancellationToken.IsCancellationRequested)
        {
            _logger.RealtimeNakFailed(
                delivery.Event.EventId,
                exception);
        }
    }
}
