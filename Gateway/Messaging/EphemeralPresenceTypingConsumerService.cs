using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.TcpGateway.Gateway.Messaging;

/// <summary>
/// 订阅 NATS Core ephemeral Typing/Presence，扇出到本机会话。
/// 无 queue group：每个 Gateway 实例都收到全量事件。
/// </summary>
internal sealed partial class EphemeralPresenceTypingConsumerService : BackgroundService
{
    private readonly IRealtimeMessageBus _messageBus;
    private readonly RealtimeIntegrationOptions _integrationOptions;
    private readonly TcpGatewayOptions _gatewayOptions;
    private readonly UserSessionRegistry _userSessions;
    private readonly PresenceWatcherRegistry _presenceWatchers;
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
        ILogger<EphemeralPresenceTypingConsumerService> logger)
    {
        _messageBus = messageBus;
        _integrationOptions = integrationOptions;
        _gatewayOptions = gatewayOptions.Value;
        _userSessions = userSessions;
        _presenceWatchers = presenceWatchers;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_gatewayOptions.EnableEphemeralPresenceAndTyping)
        {
            LogDisabled(_logger);
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
                LogTypingSubscribeFailed(_logger, ex);
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
                LogPresenceSubscribeFailed(_logger, ex);
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

        using var frame = OutboundFrameFactory.Create(
            PacketCommand.TypingUpdate,
            _typingUpdateCodec,
            update);
        foreach (var target in targets)
            target.TryQueue(frame);
    }

    private void FanoutPresence(EphemeralPresenceEvent evt)
    {
        var watchers = _presenceWatchers.GetWatchers(evt.UserId);
        if (watchers.Length == 0)
            return;

        var update = new PresenceChanged
        {
            UserId = evt.UserId,
            IsOnline = evt.IsOnline
        };

        using var frame = OutboundFrameFactory.Create(
            PacketCommand.PresenceChanged,
            _presenceChangedCodec,
            update);
        foreach (var watcherId in watchers)
        {
            foreach (var session in _userSessions.GetSnapshot(watcherId))
                session.TryQueue(frame);
        }
    }

    [LoggerMessage(
        EventId = 70,
        Level = LogLevel.Information,
        Message = "Ephemeral Presence/Typing 已关闭，跳过 NATS Core 订阅")]
    private static partial void LogDisabled(ILogger logger);

    [LoggerMessage(
        EventId = 71,
        Level = LogLevel.Warning,
        Message = "Ephemeral Typing 订阅异常，将重试")]
    private static partial void LogTypingSubscribeFailed(ILogger logger, Exception exception);

    [LoggerMessage(
        EventId = 72,
        Level = LogLevel.Warning,
        Message = "Ephemeral Presence 订阅异常，将重试")]
    private static partial void LogPresenceSubscribeFailed(ILogger logger, Exception exception);
}
