using System.Collections.Concurrent;
using System.Threading.Channels;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Ephemeral;

/// <summary>
/// Typing 扇出宿主：封装 Typing 时间轮 pump 与 emission 消费三件套
///（pump、consumer、fanout），从 <see cref="Networking.TcpGatewayService"/> 抽取。
/// <para>
/// 职责边界：
/// <list type="bullet">
/// <item>驱动 <see cref="TypingFanoutCoordinator.PumpExpired"/> 推进过期扫描；</item>
/// <item>从 <see cref="TypingFanoutCoordinator.ReadEmissionsAsync"/> 拉取合并后的最新状态；</item>
/// <item>本机 fanout（按 targetUserId 投递到 <see cref="TcpClientSession"/>）；</item>
/// <item>跨网关 ephemeral 发布（有界：keyed pending + 单槽唤醒 + 固定 worker + 故障丢弃）。</item>
/// </list>
/// 协议级编解码与 session 注册表由外部注入；本类型不持有连接状态。
/// </para>
/// <para>
/// 跨网关发布的有界语义：
/// <list type="bullet">
/// <item><b>keyed pending</b>：以 (SenderUserId, ConversationId) 为键，新事件覆盖旧状态（latest wins）；</item>
/// <item><b>单槽唤醒</b>：容量 1 的 DropWrite 信号 channel，worker 醒来后一次性排空全部 pending；</item>
/// <item><b>固定 worker</b>：单个发布 worker，避免 NATS 慢时积累任意数量的并发发布 Task；</item>
/// <item><b>故障有界丢弃</b>：NATS 发布失败时记 metric + 日志后丢弃，不重试不堆积；</item>
/// <item><b>本地 fanout 不被远端阻塞</b>：本机 fanout 同步执行，远端发布异步入 pending。</item>
/// </list>
/// </para>
/// <para>
/// 由 <see cref="Networking.TcpGatewayService"/> 在 <c>ExecuteAsync</c> 中通过
/// <see cref="RunAsync"/> 驱动，停机时取消 token 即可让 pump/consumer/publisher 退出。
/// </para>
/// </summary>
internal sealed class TypingFanoutHost
{
    private readonly TcpGatewayOptions _options;
    private readonly TypingFanoutCoordinator _typingFanout;
    private readonly UserSessionRegistry _userSessions;
    private readonly IRealtimeMessageBus _messageBus;
    private readonly RealtimeIntegrationOptions _integrationOptions;
    private readonly GatewayMetrics _metrics;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;
    private readonly IPayloadCodec<TypingUpdate> _typingUpdateCodec;

    // 跨网关 ephemeral 发布的有界 pending store。
    // key = (SenderUserId, ConversationId)：同一发送者在同一会话的 typing 状态可覆盖，
    // 不同发送者/不同会话互不覆盖。新事件覆盖旧状态（latest wins）。
    private readonly ConcurrentDictionary<PendingKey, EphemeralTypingEvent> _pendingPublishes = new();

    // 单槽唤醒信号：容量 1 + DropWrite。worker 醒来后一次性排空 _pendingPublishes。
    // 信号丢弃是安全的：已有 worker 正在排空，新事件已在 _pendingPublishes 中等待下一轮。
    private readonly Channel<int> _publishSignal = Channel.CreateBounded<int>(
        new BoundedChannelOptions(1)
        {
            SingleReader = false,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
            FullMode = BoundedChannelFullMode.DropWrite
        });

    public TypingFanoutHost(
        TcpGatewayOptions options,
        TypingFanoutCoordinator typingFanout,
        UserSessionRegistry userSessions,
        IRealtimeMessageBus messageBus,
        RealtimeIntegrationOptions integrationOptions,
        GatewayMetrics metrics,
        TimeProvider timeProvider,
        ILogger logger,
        IPayloadCodec<TypingUpdate> typingUpdateCodec)
    {
        _options = options;
        _typingFanout = typingFanout;
        _userSessions = userSessions;
        _messageBus = messageBus;
        _integrationOptions = integrationOptions;
        _metrics = metrics;
        _timeProvider = timeProvider;
        _logger = logger;
        _typingUpdateCodec = typingUpdateCodec;
    }

    /// <summary>
    /// 启动 pump + consumer + publisher 三任务，等待全部退出。
    /// 调用方应在 host stopping 时取消 <paramref name="cancellationToken"/> 让循环优雅退出。
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        if (!_options.EnableEphemeralPresenceAndTyping)
            return;

        var pumpTask = RunPumpAsync(cancellationToken);
        var consumeTask = RunEmissionConsumerAsync(cancellationToken);
        var publishTask = RunPublisherWorkerAsync(cancellationToken);
        await Task.WhenAll(pumpTask, consumeTask, publishTask).ConfigureAwait(false);
    }

    private async Task RunPumpAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(
            TypingFanoutCoordinator.DefaultTickInterval,
            _timeProvider);

        try
        {
            while (await timer
                       .WaitForNextTickAsync(cancellationToken)
                       .ConfigureAwait(false))
            {
                _typingFanout.PumpExpired();
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            // Normal host shutdown.
        }
    }

    private async Task RunEmissionConsumerAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var emission in _typingFanout
                               .ReadEmissionsAsync(cancellationToken)
                               .ConfigureAwait(false))
            {
                try
                {
                    // 本机 fanout 同步执行，不受远端发布影响。
                    FanoutTypingUpdate(
                        emission.SenderUserId,
                        emission.TargetUserId,
                        emission.ConversationId,
                        emission.IsTyping);
                    // 远端发布入 keyed pending，覆盖旧状态后唤醒 worker。
                    EnqueuePublish(
                        emission.SenderUserId,
                        emission.TargetUserId,
                        emission.ConversationId,
                        emission.IsTyping);
                }
                catch (Exception ex)
                {
                    _metrics.EphemeralEventDropped("typing_fanout_failed");
                    _logger.DependencyOperationFailed(
                        GatewayDependency.RealtimeService,
                        GatewayDependencyOperation.EphemeralTypingPublish,
                        ex);
                }
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            // Normal host shutdown.
        }
    }

    /// <summary>
    /// 发布 worker：等待信号，排空 pending 字典并逐条发布。
    /// 单 worker 保证 NATS 慢时不积累并发发布 Task；失败丢弃保证有界。
    /// </summary>
    private async Task RunPublisherWorkerAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var _ in _publishSignal.Reader
                               .ReadAllAsync(cancellationToken)
                               .ConfigureAwait(false))
            {
                await DrainPendingPublishesAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            // Normal host shutdown.
        }
    }

    /// <summary>
    /// 排空 pending 字典。循环直到 _pendingPublishes 为空，期间新到达的事件会被下一轮捕获。
    /// </summary>
    private async Task DrainPendingPublishesAsync(CancellationToken cancellationToken)
    {
        while (!_pendingPublishes.IsEmpty)
        {
            foreach (var key in _pendingPublishes.Keys)
            {
                if (!_pendingPublishes.TryRemove(key, out var evt))
                    continue;

                try
                {
                    await _messageBus
                        .PublishEphemeralTypingAsync(evt, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                    when (cancellationToken.IsCancellationRequested)
                {
                    // 停机：将事件放回 pending（可选，此处直接丢弃避免停机期间重试）。
                    _metrics.EphemeralEventDropped("typing_publish_shutdown");
                    throw;
                }
                catch (Exception ex)
                {
                    // NATS 故障：记 metric + 日志后丢弃，不重试不堆积（有界丢弃）。
                    _metrics.EphemeralEventDropped("typing_publish_failed");
                    _logger.DependencyOperationFailed(
                        GatewayDependency.RealtimeService,
                        GatewayDependencyOperation.EphemeralTypingPublish,
                        ex);
                }
            }
        }
    }

    /// <summary>
    /// 将 typing 事件入 keyed pending（覆盖旧状态）并唤醒发布 worker。
    /// 信号 DropWrite 是安全的：worker 醒来后会一次性排空全部 pending。
    /// </summary>
    private void EnqueuePublish(
        long senderUserId,
        long targetUserId,
        string conversationId,
        bool isTyping)
    {
        var key = new PendingKey(senderUserId, conversationId);
        var evt = new EphemeralTypingEvent
        {
            OriginInstanceId = _integrationOptions.InstanceId,
            SenderUserId = senderUserId,
            TargetUserId = targetUserId,
            ConversationId = conversationId,
            IsTyping = isTyping
        };
        // 覆盖旧状态（latest wins）：同一发送者+会话只保留最新 IsTyping。
        _pendingPublishes[key] = evt;
        // 单槽唤醒：DropWrite 保证信号不堆积。worker 正忙时信号被丢弃是安全的。
        _publishSignal.Writer.TryWrite(0);
    }

    private void FanoutTypingUpdate(
        long senderUserId,
        long targetUserId,
        string conversationId,
        bool isTyping)
    {
        var targets = _userSessions.GetSnapshot(targetUserId);
        if (targets.Length == 0)
            return;

        var update = new TypingUpdate
        {
            SenderUserId = senderUserId,
            ConversationId = conversationId,
            IsTyping = isTyping
        };

        using var frame = OutboundFrameFactory.Create(
            PacketCommand.TypingUpdate,
            _typingUpdateCodec,
            update);
        // Key = (SenderUserId, ConversationIdHash)：同一发送者在同一会话的 typing 状态可覆盖，
        // 不同发送者互不覆盖。
        var key = EphemeralKey.Typing(
            senderUserId,
            EphemeralKey.HashConversationId(conversationId));
        foreach (var target in targets)
            target.TryQueueEphemeral(frame, key);
    }

    /// <summary>
    /// Pending 发布键：(SenderUserId, ConversationId)。
    /// 同一发送者在同一会话的 typing 状态只保留最新（latest wins）。
    /// </summary>
    private readonly record struct PendingKey(long SenderUserId, string? ConversationId);
}
