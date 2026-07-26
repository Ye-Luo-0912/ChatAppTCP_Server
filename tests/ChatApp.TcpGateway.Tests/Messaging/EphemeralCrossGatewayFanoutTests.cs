using System.Diagnostics.Metrics;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Networking.Sessions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;
using EphemeralPresenceTypingConsumerService = ChatApp.TcpGateway.Gateway.Messaging.EphemeralPresenceTypingConsumerService;

namespace ChatApp.TcpGateway.Tests.Messaging;

/// <summary>
/// 模拟跨 Gateway：远端 OriginInstanceId 的 Typing 事件应扇出到本机目标会话。
/// </summary>
[Collection("MeterListenerSerial")]
public sealed class EphemeralCrossGatewayFanoutTests
{
    [Fact]
    public async Task RemoteTypingEvent_QueuesTypingUpdateOnLocalTarget()
    {
        using var metrics = new GatewayMetrics();
        await using var target = CreateSession(1, metrics);
        target.Authenticate(99, "target-session", deviceIdHash: 1);
        var registry = new UserSessionRegistry();
        Assert.True(registry.Add(target));

        using var enqueueCounter = new OutboundEnqueueCounter();
        var baseline = enqueueCounter.PositiveEnqueues;
        var bus = new ScriptedEphemeralBus(
            typing: new EphemeralTypingEvent
            {
                OriginInstanceId = "gateway-b",
                SenderUserId = 42,
                TargetUserId = 99,
                ConversationId = ConversationId.CreateDirect(42, 99),
                IsTyping = true
            });

        using var consumer = new EphemeralPresenceTypingConsumerService(
            bus,
            new RealtimeIntegrationOptions { InstanceId = "gateway-a" },
            Options.Create(new TcpGatewayOptions { EnableEphemeralPresenceAndTyping = true }),
            registry,
            new PresenceWatcherRegistry(),
            NullLogger<EphemeralPresenceTypingConsumerService>.Instance);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        await consumer.StartAsync(cts.Token);

        var saw = false;
        for (var i = 0; i < 50; i++)
        {
            if (enqueueCounter.PositiveEnqueues > baseline)
            {
                saw = true;
                break;
            }

            await Task.Delay(20, cts.Token);
        }

        Assert.True(saw);
        await consumer.StopAsync(CancellationToken.None);
    }

    private static TcpClientSession CreateSession(uint connectionId, GatewayMetrics metrics) =>
        new(
            new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp),
            connectionId,
            outboundQueueCapacity: 8,
            maxOutboundQueuedBytes: 128 * 1024,
            sendTimeout: TimeSpan.FromSeconds(1),
            TimeProvider.System,
            metrics,
            NullLogger<TcpClientSession>.Instance);

    private sealed class OutboundEnqueueCounter : IDisposable
    {
        private readonly MeterListener _listener = new();
        private long _positive;

        public long PositiveEnqueues => Volatile.Read(ref _positive);

        public OutboundEnqueueCounter()
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == GatewayMetrics.MeterName
                    && instrument.Name == "gateway.outbound.queued.frames")
                {
                    listener.EnableMeasurementEvents(instrument, this);
                }
            };
            _listener.SetMeasurementEventCallback<long>(static (_, measurement, _, state) =>
            {
                if (measurement > 0 && state is OutboundEnqueueCounter counter)
                    Interlocked.Add(ref counter._positive, measurement);
            });
            _listener.Start();
        }

        public void Dispose() => _listener.Dispose();
    }
}

file sealed class ScriptedEphemeralBus(EphemeralTypingEvent? typing = null) : IRealtimeMessageBus
{
    public Task PublishIncomingMessageAsync(IncomingMessageCommand command, CancellationToken ct = default) =>
        Task.CompletedTask;

    public Task PublishMessageReceiptAsync(MessageReceiptCommand command, CancellationToken ct = default) =>
        Task.CompletedTask;

    public Task<MessageHistoryPage> QueryMessageHistoryAsync(MessageHistoryQuery query, CancellationToken ct = default) =>
        Task.FromResult(MessageHistoryPage.Failed(query.RequestId, "x", "x"));

    public Task<ConversationListPage> QueryConversationListAsync(ConversationListQuery query, CancellationToken ct = default) =>
        Task.FromResult(ConversationListPage.Failed(query.RequestId, "x", "x"));

    public Task<ConversationMarkReadResult> MarkConversationReadAsync(ConversationMarkReadCommand command, CancellationToken ct = default) =>
        Task.FromResult(ConversationMarkReadResult.Failed(command.RequestId, "x", "x"));

    public Task<ConversationSetPrefsResult> SetConversationPrefsAsync(ConversationSetPrefsCommand command, CancellationToken ct = default) =>
        Task.FromResult(ConversationSetPrefsResult.Failed(command.RequestId, "x", "x"));

    public Task<GroupConversationResult> MutateGroupConversationAsync(GroupConversationCommand command, CancellationToken ct = default) =>
        Task.FromResult(GroupConversationResult.Failed(command.RequestId, "x", "x"));

    public Task<MessageRecallResult> RecallMessageAsync(MessageRecallCommand command, CancellationToken ct = default) =>
        Task.FromResult(MessageRecallResult.Failed(command.RequestId, "x", "x"));

    public Task<MessageEditResult> EditMessageAsync(MessageEditCommand command, CancellationToken ct = default) =>
        Task.FromResult(MessageEditResult.Failed(command.RequestId, "x", "x"));

    public Task<MessageReactionResult> ReactToMessageAsync(MessageReactionCommand command, CancellationToken ct = default) =>
        Task.FromResult(MessageReactionResult.Failed(command.RequestId, "x", "x"));

    public Task<SyncBootstrapPage> QuerySyncBootstrapAsync(SyncBootstrapQuery query, CancellationToken ct = default) =>
        Task.FromResult(SyncBootstrapPage.Failed(query.RequestId, "x", "x"));

    public Task<RealtimeHistoryMessage?> TryGetMessageByIdAsync(long userId, string messageId, CancellationToken ct = default) =>
        Task.FromResult<RealtimeHistoryMessage?>(null);

    public Task PublishEventAsync(RealtimeEvent evt, CancellationToken ct = default) => Task.CompletedTask;

    public async IAsyncEnumerable<RealtimeEventDelivery> ConsumeEventsAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        await Task.CompletedTask;
        yield break;
    }

    public async IAsyncEnumerable<RealtimeEventDelivery> ConsumeAccountCleanupEventsAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        await Task.CompletedTask;
        yield break;
    }

    public Task PublishEphemeralTypingAsync(EphemeralTypingEvent evt, CancellationToken ct = default) =>
        Task.CompletedTask;

    public Task PublishEphemeralPresenceAsync(EphemeralPresenceEvent evt, CancellationToken ct = default) =>
        Task.CompletedTask;

    public async IAsyncEnumerable<EphemeralTypingEvent> ConsumeEphemeralTypingAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (typing is not null)
            yield return typing;
        await Task.Delay(Timeout.InfiniteTimeSpan, ct);
    }

    public async IAsyncEnumerable<EphemeralPresenceEvent> ConsumeEphemeralPresenceAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        await Task.CompletedTask;
        yield break;
    }

    public Task<PresenceAuthorizeResponse> AuthorizePresenceAsync(
        PresenceAuthorizeQuery query,
        CancellationToken ct = default) =>
        Task.FromResult(new PresenceAuthorizeResponse { AllowedUserIds = query.TargetUserIds });

    public Task ServePresenceAuthorizeAsync(
        Func<PresenceAuthorizeQuery, CancellationToken, ValueTask<PresenceAuthorizeResponse>> handler,
        CancellationToken ct = default) =>
        Task.CompletedTask;

    public Task<TimeSpan> PingAsync(CancellationToken ct = default) =>
        Task.FromResult(TimeSpan.Zero);
}
