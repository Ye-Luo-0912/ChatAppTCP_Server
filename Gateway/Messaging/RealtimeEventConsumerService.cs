using System.Threading.Channels;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Routing;
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

    /// <summary>
    /// 七：Drain 超时——主循环结束后 worker 排空已入队 delivery 的独立预算。
    /// 不链接 stoppingToken（宿主已取消时 linked CTS 立即取消，Drain 退化为 0）；
    /// 宿主总 ShutdownTimeout（Program.cs 配置 20s）约束整体停机时长。
    /// </summary>
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(30);

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
        // 七：Worker token 独立于 stoppingToken——宿主停机时主循环停止拉取，
        // 但 worker 继续排空已入队 delivery（在 DrainTimeout 内），减少 broker 重投。
        // 宿主总 ShutdownTimeout（Program.cs 配置 20s）约束整体停机时长。
        using var workerStopCts = new CancellationTokenSource();
        // 主循环 token 链接 stoppingToken：宿主请求停机时立即停止从 broker 拉取。
        using var mainLoopCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var workers = new Task[partitionCount];
        for (var i = 0; i < partitionCount; i++)
        {
            var partitionIndex = i;
            workers[i] = ConsumePartitionWorkerAsync(
                partitions[partitionIndex].Reader,
                workerStopCts.Token);
        }

        // 主循环：从 JetStream 拉取消息，路由到分区 channel。
        // 分区 channel 满时 WriteAsync 阻塞 → 停止 ACK → broker 背压生效。
        Task? mainLoop = null;
        try
        {
            mainLoop = Task.Run(async () =>
            {
                await foreach (var delivery in _messageBus
                                   .ConsumeEventsAsync(mainLoopCts.Token)
                                   .ConfigureAwait(false))
                {
                    var partitionIndex = GetPartitionIndex(
                        delivery.Event,
                        partitionCount);
                    await partitions[partitionIndex].Writer
                        .WriteAsync(delivery, mainLoopCts.Token)
                        .ConfigureAwait(false);
                }
            }, mainLoopCts.Token);

            // 使用 Task.WhenAny 竞争 mainLoop 与 workers：
            // 若 worker 先 Fault → 取消 mainLoop（避免 channel 满后永久阻塞 WriteAsync）；
            // 若 mainLoop 先结束（正常停机或异常）→ 完成 channel 并等待 workers 排空。
            var workersTask = Task.WhenAll(workers);
            var completed = await Task.WhenAny(mainLoop, workersTask)
                .ConfigureAwait(false);

            if (completed == workersTask)
            {
                // 七：Worker 先 Fault——必须先捕获 worker 根因，再取消 mainLoop。
                // 旧实现先 cts.Cancel() 再 await mainLoop，OCE 会掩盖真正的 worker 异常，
                // 导致 `await workersTask` 永远到不了。改为：先观测 workersTask 拿到根因，
                // 再取消 mainLoop（解除 WriteAsync 阻塞），观测 mainLoop（吞掉预期 OCE），
                // 最后抛出原始 worker 异常。
                Exception? workerException = null;
                try { await workersTask.ConfigureAwait(false); }
                catch (Exception ex) { workerException = ex; }

                mainLoopCts.Cancel();
                try { await mainLoop.ConfigureAwait(false); }
                catch (OperationCanceledException) { /* 预期：mainLoop 被取消 */ }
                catch { /* 次要故障——丢弃，传播原始 workerException */ }

                if (workerException is not null)
                    throw workerException;
                return;
            }

            // Main loop completed (normally or faulted).
            // Complete all partition writers so workers can drain remaining deliveries.
            foreach (var ch in partitions)
            {
                ch.Writer.TryComplete();
            }

            Exception? mainLoopException = null;
            try
            {
                await mainLoop.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Normal shutdown via stoppingToken — not an error.
            }
            catch (Exception ex)
            {
                mainLoopException = ex;
            }

            if (mainLoopException is not null)
            {
                // Main loop faulted: cancel workers, they can't drain meaningfully.
                workerStopCts.Cancel();
                try { await workersTask.ConfigureAwait(false); }
                catch { /* propagate mainLoopException below */ }
                throw mainLoopException;
            }

            // 七：主循环正常结束——允许 worker 在独立 DrainTimeout 内排空剩余 delivery。
            // 旧实现 drainCts = CreateLinkedTokenSource(stoppingToken)：宿主 stoppingToken 已取消时
            // linked CTS 立即取消，Drain 窗口退化为 0。改为对 workerStopCts 直接 CancelAfter，
            // 不链接 stoppingToken——宿主总 ShutdownTimeout 约束整体时长。
            workerStopCts.CancelAfter(DrainTimeout);
            try
            {
                await workersTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (workerStopCts.IsCancellationRequested)
            {
                // Drain 超时：剩余 delivery 由 broker 重投。
            }
            // Worker faulted during drain — propagate.
        }
        finally
        {
            // 主循环结束（正常或异常）：完成所有分区 channel，等待 worker 排空。
            foreach (var ch in partitions)
            {
                ch.Writer.TryComplete();
            }

            // 防御性：确保 worker 已退出。CancelAfter 可能尚未触发，此处显式取消。
            workerStopCts.Cancel();
            try
            {
                await Task.WhenAll(workers).ConfigureAwait(false);
            }
            catch
            {
                // 异常已在上文传播。
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
    private static int GetPartitionIndex(RealtimeEvent evt, int partitionCount)
    {
        // P0-8：会话级聚合事件按 ConversationId 分区，保证同一会话的
        // Message/Edit/Reaction/MemberChange 进入同一分区、保持顺序。
        // 用户级事件按 TargetUserId 分区，保证同一用户的事件顺序。
        if (evt.AudienceKind == AudienceKind.Conversation
            && !string.IsNullOrEmpty(evt.ConversationId))
        {
            return PartitionByString(evt.ConversationId!, partitionCount);
        }

        var positiveId = evt.TargetUserId >= 0 ? evt.TargetUserId : -evt.TargetUserId;
        return (int)((ulong)positiveId % (ulong)partitionCount);
    }

    /// <summary>
    /// 确定性字符串哈希分区（FNV-1a 64-bit，跨进程稳定，不使用进程随机种子）。
    /// </summary>
    private static int PartitionByString(string key, int partitionCount)
    {
        unchecked
        {
            ulong hash = 14695981039346656037UL; // FNV offset basis
            foreach (var c in key)
            {
                hash ^= (uint)c;
                hash *= 1099511628211UL; // FNV prime
            }
            return (int)(hash % (ulong)partitionCount);
        }
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
