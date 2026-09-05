using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.TcpGateway.Gateway.Messaging;

/// <summary>
/// 订阅 NATS Core ephemeral Typing/Presence，扇出到本机会话。
/// 无 queue group：每个 Gateway 实例都收到全量事件。
/// </summary>
internal sealed class EphemeralPresenceTypingConsumerService : BackgroundService
{
    private readonly IRealtimeMessageBus _messageBus;
    private readonly RealtimeIntegrationOptions _integrationOptions;
    private readonly TcpGatewayOptions _gatewayOptions;
    private readonly UserSessionRegistry _userSessions;
    private readonly PresenceWatcherRegistry _presenceWatchers;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger<EphemeralPresenceTypingConsumerService> _logger;
    private readonly JsonPayloadCodec<TypingUpdate> _typingUpdateCodec = new(
        GatewayJsonSerializerContext.Default.TypingUpdate);
    private readonly JsonPayloadCodec<PresenceChanged> _presenceChangedCodec = new(
        GatewayJsonSerializerContext.Default.PresenceChanged);

    public EphemeralPresenceTypingConsumerService(
        IRealtimeMessageBus messageBus,
        RealtimeIntegrationOptions integrationOptions,
        IOptions<TcpGatewayOptions> gatewayOptions,
        UserSessionRegistry userSessions,
        PresenceWatcherRegistry presenceWatchers,
        GatewayMetrics metrics,
        ILogger<EphemeralPresenceTypingConsumerService> logger)
    {
        _messageBus = messageBus;
        _integrationOptions = integrationOptions;
        _gatewayOptions = gatewayOptions.Value;
        _userSessions = userSessions;
        _presenceWatchers = presenceWatchers;
        _metrics = metrics;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_gatewayOptions.EnableEphemeralPresenceAndTyping)
        {
            _logger.EphemeralDisabled();
            return;
        }

        var typingTask = ConsumeTypingLoopAsync(stoppingToken);
        var presenceTask = ConsumePresenceLoopAsync(stoppingToken);
        await Task.WhenAll(typingTask, presenceTask).ConfigureAwait(false);
    }

    private async Task ConsumeTypingLoopAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await foreach (var evt in _messageBus
                                   .ConsumeEphemeralTypingAsync(stoppingToken)
                                   .ConfigureAwait(false))
                {
                    if (string.Equals(
                            evt.OriginInstanceId,
                            _integrationOptions.InstanceId,
                            StringComparison.Ordinal))
                    {
                        continue;
                    }

                    FanoutTyping(evt);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.RealtimeSubscriptionFailed(
                    RealtimeSubscriptionKind.EphemeralTyping,
                    TimeSpan.FromSeconds(2),
                    ex);
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken).ConfigureAwait(false);
            }
        }
    }

    private async Task ConsumePresenceLoopAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await foreach (var evt in _messageBus
                                   .ConsumeEphemeralPresenceAsync(stoppingToken)
                                   .ConfigureAwait(false))
                {
                    if (string.Equals(
                            evt.OriginInstanceId,
                            _integrationOptions.InstanceId,
                            StringComparison.Ordinal))
                    {
                        continue;
                    }

                    FanoutPresence(evt);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.RealtimeSubscriptionFailed(
                    RealtimeSubscriptionKind.EphemeralPresence,
                    TimeSpan.FromSeconds(2),
                    ex);
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken).ConfigureAwait(false);
            }
        }
    }

    private void FanoutTyping(EphemeralTypingEvent evt)
    {
        var targets = _userSessions.GetSnapshot(evt.TargetUserId);
        if (targets.Length == 0)
            return;

        var update = new TypingUpdate
        {
            SenderUserId = evt.SenderUserId,
            ConversationId = evt.ConversationId,
            IsTyping = evt.IsTyping
        };

        using var frames = new FormatGroupedFrame<TypingUpdate>(
            PacketCommand.TypingUpdate,
            _typingUpdateCodec,
            update);
        // Key = (SenderUserId, ConversationIdHash)：同一发送者在同一会话的 typing 状态可覆盖，
        // 不同发送者互不覆盖。
        var key = EphemeralKey.Typing(
            evt.SenderUserId,
            EphemeralKey.HashConversationId(evt.ConversationId));
        foreach (var target in targets)
            target.TryQueueEphemeral(frames.GetFrame(target), key);
    }

    private void FanoutPresence(EphemeralPresenceEvent evt)
    {
        var watchers = _presenceWatchers.GetWatchers(evt.UserId);
        if (watchers.Length == 0)
        {
            _metrics.PresenceFanoutSkipped();
            return;
        }

        var update = new PresenceChanged
        {
            UserId = evt.UserId,
            IsOnline = evt.IsOnline
        };

        using var frames = new FormatGroupedFrame<PresenceChanged>(
            PacketCommand.PresenceChanged,
            _presenceChangedCodec,
            update);
        // Key = UserId：同一用户的在线状态可覆盖。
        var key = EphemeralKey.Presence(evt.UserId);
        var recipientCount = 0;
        foreach (var watcherId in watchers)
        {
            foreach (var session in _userSessions.GetSnapshot(watcherId))
            {
                session.TryQueueEphemeral(frames.GetFrame(session), key);
                recipientCount++;
            }
        }
        _metrics.PresenceFanoutDelivered(recipientCount);
    }
}
