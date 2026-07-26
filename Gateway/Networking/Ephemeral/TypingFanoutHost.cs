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
/// <item>本机 fanout（按 targetUserId 投递到 <see cref="TcpClientSession"/>）+ 跨网关 ephemeral 发布。</item>
/// </list>
/// 协议级编解码与 session 注册表由外部注入；本类型不持有连接状态。
/// </para>
/// <para>
/// 由 <see cref="Networking.TcpGatewayService"/> 在 <c>ExecuteAsync</c> 中通过
/// <see cref="RunAsync"/> 驱动，停机时取消 token 即可让 pump/consumer 退出。
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
    /// 启动 pump + consumer 双任务，等待两者全部退出。
    /// 调用方应在 host stopping 时取消 <paramref name="cancellationToken"/> 让循环优雅退出。
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        if (!_options.EnableEphemeralPresenceAndTyping)
            return;

        var pumpTask = RunPumpAsync(cancellationToken);
        var consumeTask = RunEmissionConsumerAsync(cancellationToken);
        await Task.WhenAll(pumpTask, consumeTask).ConfigureAwait(false);
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
                    FanoutTypingUpdate(
                        emission.SenderUserId,
                        emission.TargetUserId,
                        emission.ConversationId,
                        emission.IsTyping);
                    PublishEphemeralTypingFireAndForget(
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

    private void PublishEphemeralTypingFireAndForget(
        long senderUserId,
        long targetUserId,
        string conversationId,
        bool isTyping)
    {
        var evt = new EphemeralTypingEvent
        {
            OriginInstanceId = _integrationOptions.InstanceId,
            SenderUserId = senderUserId,
            TargetUserId = targetUserId,
            ConversationId = conversationId,
            IsTyping = isTyping
        };
        _ = PublishEphemeralTypingSafeAsync(evt);
    }

    private async Task PublishEphemeralTypingSafeAsync(EphemeralTypingEvent evt)
    {
        try
        {
            await _messageBus.PublishEphemeralTypingAsync(evt).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _metrics.DependencyOperationFailed(
                GatewayDependency.RealtimeService,
                GatewayDependencyOperation.EphemeralTypingPublish);
            _logger.DependencyOperationFailed(
                GatewayDependency.RealtimeService,
                GatewayDependencyOperation.EphemeralTypingPublish,
                ex);
        }
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
        foreach (var target in targets)
            target.TryQueueEphemeral(frame);
    }
}
