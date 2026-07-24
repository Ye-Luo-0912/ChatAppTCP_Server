using System.Diagnostics.Metrics;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
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
using EphemeralPresenceTypingConsumerService = ChatApp.TcpGateway.Gateway.Messaging.EphemeralPresenceTypingConsumerService;

namespace ChatApp.TcpGateway.Tests.Messaging;

/// <summary>
/// 双 Gateway 进程内联调：共享内存 ephemeral 总线，验证跨实例 Typing/Presence 扇出与同实例自跳过。
/// </summary>
public sealed class EphemeralDualGatewayInProcessTests
{
    [Fact]
    public async Task Typing_PublishedOnGatewayA_FansOutOnGatewayB()
    {
        var bus = new InMemoryEphemeralBus();
        using var metrics = new GatewayMetrics();
        await using var target = CreateSession(1, metrics);
        target.Authenticate(99, "target-session", deviceIdHash: 1);

        var registryB = new UserSessionRegistry();
        Assert.True(registryB.Add(target));

        using var consumerA = CreateConsumer(bus, "gateway-a", new UserSessionRegistry(), new PresenceWatcherRegistry());
        using var consumerB = CreateConsumer(bus, "gateway-b", registryB, new PresenceWatcherRegistry());

        using var enqueueCounter = new OutboundEnqueueCounter();
        var baseline = enqueueCounter.PositiveEnqueues;

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await consumerA.StartAsync(cts.Token);
        await consumerB.StartAsync(cts.Token);
        await Task.Delay(50, cts.Token);

        await bus.PublishEphemeralTypingAsync(
            new EphemeralTypingEvent
            {
                OriginInstanceId = "gateway-a",
                SenderUserId = 42,
                TargetUserId = 99,
                ConversationId = ConversationId.CreateDirect(42, 99),
                IsTyping = true
            },
            cts.Token);

        Assert.True(await WaitForEnqueueAsync(enqueueCounter, baseline, cts.Token));

        await consumerA.StopAsync(CancellationToken.None);
        await consumerB.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Typing_SameOriginInstance_DoesNotFanOutLocally()
    {
        var bus = new InMemoryEphemeralBus();
        using var metrics = new GatewayMetrics();
        await using var target = CreateSession(2, metrics);
        target.Authenticate(99, "target-session", deviceIdHash: 2);

        var registry = new UserSessionRegistry();
        Assert.True(registry.Add(target));

        using var consumer = CreateConsumer(bus, "gateway-a", registry, new PresenceWatcherRegistry());
        using var enqueueCounter = new OutboundEnqueueCounter();
        var baseline = enqueueCounter.PositiveEnqueues;

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        await consumer.StartAsync(cts.Token);
        await Task.Delay(50, cts.Token);

        await bus.PublishEphemeralTypingAsync(
            new EphemeralTypingEvent
            {
                OriginInstanceId = "gateway-a",
                SenderUserId = 42,
                TargetUserId = 99,
                ConversationId = ConversationId.CreateDirect(42, 99),
                IsTyping = true
            },
            cts.Token);

        await Task.Delay(150, cts.Token);
        Assert.Equal(baseline, enqueueCounter.PositiveEnqueues);

        await consumer.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Presence_PublishedOnGatewayA_FansOutToWatcherOnGatewayB()
    {
        var bus = new InMemoryEphemeralBus();
        using var metrics = new GatewayMetrics();
        await using var watcherSession = CreateSession(3, metrics);
        watcherSession.Authenticate(7, "watcher-session", deviceIdHash: 3);

        var registryB = new UserSessionRegistry();
        Assert.True(registryB.Add(watcherSession));

        var watchersB = new PresenceWatcherRegistry();
        watchersB.Watch(watchedUserId: 42, watcherUserId: 7);

        using var consumerA = CreateConsumer(bus, "gateway-a", new UserSessionRegistry(), new PresenceWatcherRegistry());
        using var consumerB = CreateConsumer(bus, "gateway-b", registryB, watchersB);

        using var enqueueCounter = new OutboundEnqueueCounter();
        var baseline = enqueueCounter.PositiveEnqueues;

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await consumerA.StartAsync(cts.Token);
        await consumerB.StartAsync(cts.Token);
        await Task.Delay(50, cts.Token);

        await bus.PublishEphemeralPresenceAsync(
            new EphemeralPresenceEvent
            {
                OriginInstanceId = "gateway-a",
                UserId = 42,
                IsOnline = true
            },
            cts.Token);

        Assert.True(await WaitForEnqueueAsync(enqueueCounter, baseline, cts.Token));

        await consumerA.StopAsync(CancellationToken.None);
        await consumerB.StopAsync(CancellationToken.None);
    }

    private static EphemeralPresenceTypingConsumerService CreateConsumer(
        IRealtimeMessageBus bus,
        string instanceId,
        UserSessionRegistry sessions,
        PresenceWatcherRegistry watchers) =>
        new(
            bus,
            new RealtimeIntegrationOptions { InstanceId = instanceId },
            Options.Create(new TcpGatewayOptions { EnableEphemeralPresenceAndTyping = true }),
            sessions,
            watchers,
            NullLogger<EphemeralPresenceTypingConsumerService>.Instance);

    private static async Task<bool> WaitForEnqueueAsync(
        OutboundEnqueueCounter counter,
        long baseline,
        CancellationToken ct)
    {
        for (var i = 0; i < 50; i++)
        {
            if (counter.PositiveEnqueues > baseline)
                return true;
            await Task.Delay(20, ct);
        }

        return false;
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

/// <summary>进程内多订阅者 ephemeral 总线（模拟 NATS Core 无 queue-group 全量投递）。</summary>
file sealed class InMemoryEphemeralBus : IRealtimeMessageBus
{
    private readonly object _gate = new();
    private readonly List<Channel<EphemeralTypingEvent>> _typingSubscribers = [];
    private readonly List<Channel<EphemeralPresenceEvent>> _presenceSubscribers = [];

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

    public Task PublishEphemeralTypingAsync(EphemeralTypingEvent evt, CancellationToken ct = default)
    {
        Channel<EphemeralTypingEvent>[] snapshot;
        lock (_gate)
            snapshot = _typingSubscribers.ToArray();
        foreach (var ch in snapshot)
            ch.Writer.TryWrite(evt);
        return Task.CompletedTask;
    }

    public Task PublishEphemeralPresenceAsync(EphemeralPresenceEvent evt, CancellationToken ct = default)
    {
        Channel<EphemeralPresenceEvent>[] snapshot;
        lock (_gate)
            snapshot = _presenceSubscribers.ToArray();
        foreach (var ch in snapshot)
            ch.Writer.TryWrite(evt);
        return Task.CompletedTask;
    }

    public async IAsyncEnumerable<EphemeralTypingEvent> ConsumeEphemeralTypingAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var ch = Channel.CreateUnbounded<EphemeralTypingEvent>();
        lock (_gate)
            _typingSubscribers.Add(ch);
        try
        {
            await foreach (var evt in ch.Reader.ReadAllAsync(ct).ConfigureAwait(false))
                yield return evt;
        }
        finally
        {
            lock (_gate)
                _typingSubscribers.Remove(ch);
            ch.Writer.TryComplete();
        }
    }

    public async IAsyncEnumerable<EphemeralPresenceEvent> ConsumeEphemeralPresenceAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var ch = Channel.CreateUnbounded<EphemeralPresenceEvent>();
        lock (_gate)
            _presenceSubscribers.Add(ch);
        try
        {
            await foreach (var evt in ch.Reader.ReadAllAsync(ct).ConfigureAwait(false))
                yield return evt;
        }
        finally
        {
            lock (_gate)
                _presenceSubscribers.Remove(ch);
            ch.Writer.TryComplete();
        }
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
