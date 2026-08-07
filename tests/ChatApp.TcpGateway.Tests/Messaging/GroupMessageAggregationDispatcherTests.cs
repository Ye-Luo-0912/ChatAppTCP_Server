using System.Diagnostics.Metrics;
using System.Net.Sockets;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Gateway.Messaging;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using RealtimeEventDispatcher = ChatApp.TcpGateway.Gateway.Messaging.RealtimeEventDispatcher;

namespace ChatApp.TcpGateway.Tests.Messaging;

/// <summary>
/// 群聊聚合事件分发验证：当 <see cref="RealtimeEvent.TargetUserIds"/> 非空时，
/// Dispatcher 应遍历多目标列表投递本机会话，并跳过来源 SessionId。
/// </summary>
[Collection("MeterListenerSerial")]
public sealed class GroupMessageAggregationDispatcherTests
{
    private const string GroupConversationId = "grp:0123456789abcdef0123456789abcdef";

    [Fact]
    public async Task AggregatedGroupEvent_DispatchesToAllLocalRecipients()
    {
        using var metrics = new GatewayMetrics();
        var registry = new UserSessionRegistry();
        // sender=7（origin session）, recipient1=42, recipient2=43
        await using var senderSession = CreateSession(1, metrics);
        await using var recipient1 = CreateSession(2, metrics);
        await using var recipient2 = CreateSession(3, metrics);
        senderSession.Authenticate(7, "sender-session", deviceIdHash: 1);
        recipient1.Authenticate(42, "recipient-1-session", deviceIdHash: 2);
        recipient2.Authenticate(43, "recipient-2-session", deviceIdHash: 3);
        registry.Add(senderSession);
        registry.Add(recipient1);
        registry.Add(recipient2);

        var dispatcher = CreateDispatcher(registry, metrics);

        using var enqueueCounter = new OutboundEnqueueCounter();
        var baseline = enqueueCounter.PositiveEnqueues;

        await dispatcher.DispatchAsync(BuildAggregatedGroupEvent(
            senderUserId: 7,
            senderSessionId: "sender-session",
            targetUserIds: [7, 42, 43]), TestContext.Current.CancellationToken);

        // 三个目标中，senderSession 会被 SessionId 跳过；recipient1 + recipient2 各入队一帧。
        var enqueued = enqueueCounter.PositiveEnqueues - baseline;
        Assert.True(
            enqueued >= 2,
            $"聚合事件应至少为两个非来源目标会话各入队一帧，实际入队={enqueued}。");
    }

    [Fact]
    public async Task AggregatedGroupEvent_SkipsOriginSession()
    {
        using var metrics = new GatewayMetrics();
        var registry = new UserSessionRegistry();
        // 同一用户 7 的两个会话：origin 与非 origin
        await using var originSession = CreateSession(1, metrics);
        await using var otherDeviceSession = CreateSession(2, metrics);
        originSession.Authenticate(7, "origin-session", deviceIdHash: 1);
        otherDeviceSession.Authenticate(7, "other-device-session", deviceIdHash: 2);
        registry.Add(originSession);
        registry.Add(otherDeviceSession);

        var dispatcher = CreateDispatcher(registry, metrics);

        using var enqueueCounter = new OutboundEnqueueCounter();
        var baseline = enqueueCounter.PositiveEnqueues;

        await dispatcher.DispatchAsync(BuildAggregatedGroupEvent(
            senderUserId: 7,
            senderSessionId: "origin-session",
            targetUserIds: [7]),
            TestContext.Current.CancellationToken);

        // origin session 应被跳过；同用户的另一设备会话应收到一帧（多设备回声）。
        var enqueued = enqueueCounter.PositiveEnqueues - baseline;
        Assert.True(
            enqueued >= 1,
            $"同用户非来源会话应收到回声帧，实际入队={enqueued}。");
    }

    [Fact]
    public async Task AggregatedGroupEvent_NoLocalTargets_ProducesNoEnqueues()
    {
        using var metrics = new GatewayMetrics();
        var registry = new UserSessionRegistry();
        // 本机只有用户 42；聚合事件目标全是其他用户。
        await using var localUser = CreateSession(1, metrics);
        localUser.Authenticate(42, "local-session", deviceIdHash: 1);
        registry.Add(localUser);

        var dispatcher = CreateDispatcher(registry, metrics);

        using var enqueueCounter = new OutboundEnqueueCounter();
        var baseline = enqueueCounter.PositiveEnqueues;

        await dispatcher.DispatchAsync(BuildAggregatedGroupEvent(
            senderUserId: 7,
            senderSessionId: "sender-session",
            targetUserIds: [7, 99, 100]),
            TestContext.Current.CancellationToken);

        Assert.Equal(baseline, enqueueCounter.PositiveEnqueues);
    }

    [Fact]
    public async Task SingleTargetGroupEvent_WithoutTargetUserIds_DispatchesToTargetUser()
    {
        using var metrics = new GatewayMetrics();
        var registry = new UserSessionRegistry();
        await using var recipient = CreateSession(1, metrics);
        recipient.Authenticate(42, "recipient-session", deviceIdHash: 1);
        registry.Add(recipient);

        var dispatcher = CreateDispatcher(registry, metrics);

        using var enqueueCounter = new OutboundEnqueueCounter();
        var baseline = enqueueCounter.PositiveEnqueues;

        // TargetUserIds 为空：走单目标路径，按 TargetUserId=42 投递。
        await dispatcher.DispatchAsync(BuildSingleTargetGroupEvent(
            senderUserId: 7,
            senderSessionId: "sender-session",
            targetUserId: 42),
            TestContext.Current.CancellationToken);

        var enqueued = enqueueCounter.PositiveEnqueues - baseline;
        Assert.True(
            enqueued >= 1,
            $"单目标路径应入队一帧，实际入队={enqueued}。");
    }

    [Fact]
    public async Task MessageEvent_WithBlankClientMessageId_IsRejected()
    {
        using var metrics = new GatewayMetrics();
        var registry = new UserSessionRegistry();
        await using var recipient = CreateSession(1, metrics);
        recipient.Authenticate(42, "recipient-session", deviceIdHash: 1);
        registry.Add(recipient);
        var dispatcher = CreateDispatcher(registry, metrics);

        using var enqueueCounter = new OutboundEnqueueCounter();
        var baseline = enqueueCounter.PositiveEnqueues;

        await dispatcher.DispatchAsync(
            BuildSingleTargetGroupEvent(
                senderUserId: 7,
                senderSessionId: "sender-session",
                targetUserId: 42,
                clientMessageId: " "),
            TestContext.Current.CancellationToken);

        Assert.Equal(baseline, enqueueCounter.PositiveEnqueues);
    }

    [Fact]
    public async Task AggregatedGroupEvent_RecordsAggregatedDispatchMetrics()
    {
        using var metrics = new GatewayMetrics();
        var registry = new UserSessionRegistry();
        await using var senderSession = CreateSession(1, metrics);
        await using var recipient1 = CreateSession(2, metrics);
        await using var recipient2 = CreateSession(3, metrics);
        senderSession.Authenticate(7, "sender-session", deviceIdHash: 1);
        recipient1.Authenticate(42, "recipient-1-session", deviceIdHash: 2);
        recipient2.Authenticate(43, "recipient-2-session", deviceIdHash: 3);
        registry.Add(senderSession);
        registry.Add(recipient1);
        registry.Add(recipient2);

        var dispatcher = CreateDispatcher(registry, metrics);

        using var aggregatedCounter = new AggregatedDispatchCounter();
        var baselineEvents = aggregatedCounter.Events;
        var baselineLocal = aggregatedCounter.LocalRecipientsSum;
        var baselineTargets = aggregatedCounter.TotalTargetsSum;

        await dispatcher.DispatchAsync(BuildAggregatedGroupEvent(
            senderUserId: 7,
            senderSessionId: "sender-session",
            targetUserIds: [7, 42, 43]), TestContext.Current.CancellationToken);

        // 计数器应增加 1；local_recipients_sum 应增加 >=2（recipient1 + recipient2）；
        // total_targets_sum 应增加 3（TargetUserIds 长度）。
        Assert.Equal(baselineEvents + 1, aggregatedCounter.Events);
        Assert.True(
            aggregatedCounter.LocalRecipientsSum - baselineLocal >= 2,
            $"本机命中接收者数应至少为 2，实际={aggregatedCounter.LocalRecipientsSum - baselineLocal}。");
        Assert.Equal(
            baselineTargets + 3,
            aggregatedCounter.TotalTargetsSum);
    }

    private static RealtimeEvent BuildAggregatedGroupEvent(
        long senderUserId,
        string senderSessionId,
        long[] targetUserIds) =>
        new()
        {
            EventId = "grp-agg-event-1",
            Type = RealtimeEventType.MessageReceived,
            // 聚合事件 TargetUserId 设为发送者，通过 isSenderEcho 校验
            TargetUserId = senderUserId,
            ActorUserId = senderUserId,
            MessageId = "grp-msg-1",
            SessionId = senderSessionId,
            PayloadJson = BuildGroupChatPayloadJson(senderUserId, senderSessionId),
            OccurredAtMs = 1_700_000_000_000L,
            TargetUserIds = targetUserIds
        };

    private static RealtimeEvent BuildSingleTargetGroupEvent(
        long senderUserId,
        string senderSessionId,
        long targetUserId,
        string clientMessageId = "grp-client-1") =>
        new()
        {
            EventId = "grp-single-event-1",
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = targetUserId,
            ActorUserId = senderUserId,
            MessageId = "grp-msg-1",
            SessionId = senderSessionId,
            PayloadJson = BuildGroupChatPayloadJson(
                senderUserId,
                senderSessionId,
                clientMessageId),
            OccurredAtMs = 1_700_000_000_000L,
            TargetUserIds = null
        };

    private static string BuildGroupChatPayloadJson(
        long senderUserId,
        string senderSessionId,
        string clientMessageId = "grp-client-1") =>
        $$"""
        {
          "messageId": "grp-msg-1",
          "clientMessageId": "{{clientMessageId}}",
          "senderUserId": {{senderUserId}},
          "senderSessionId": "{{senderSessionId}}",
          "receiverUserId": 0,
          "conversationId": "{{GroupConversationId}}",
          "content": "hello group",
          "receivedAtMs": 1700000000000
        }
        """;

    private static RealtimeEventDispatcher CreateDispatcher(
        UserSessionRegistry registry,
        GatewayMetrics metrics) =>
        new(
            registry,
            new JsonPayloadCodec<ChatMessage>(
                GatewayJsonSerializerContext.Default.ChatMessage),
            new JsonPayloadCodec<MessageReceiptUpdate>(
                GatewayJsonSerializerContext.Default.MessageReceiptUpdate),
            new JsonPayloadCodec<ConversationChanged>(
                GatewayJsonSerializerContext.Default.ConversationChanged),
            new JsonPayloadCodec<UnreadCountChanged>(
                GatewayJsonSerializerContext.Default.UnreadCountChanged),
            new JsonPayloadCodec<ConversationReadUpdate>(
                GatewayJsonSerializerContext.Default.ConversationReadUpdate),
            new JsonPayloadCodec<MessageRecalledUpdate>(
                GatewayJsonSerializerContext.Default.MessageRecalledUpdate),
            new JsonPayloadCodec<MessageEditedUpdate>(
                GatewayJsonSerializerContext.Default.MessageEditedUpdate),
            new JsonPayloadCodec<ReactionAddedUpdate>(
                GatewayJsonSerializerContext.Default.ReactionAddedUpdate),
            new JsonPayloadCodec<ReactionRemovedUpdate>(
                GatewayJsonSerializerContext.Default.ReactionRemovedUpdate),
            new JsonPayloadCodec<MemberJoinedUpdate>(
                GatewayJsonSerializerContext.Default.MemberJoinedUpdate),
            new JsonPayloadCodec<MemberLeftUpdate>(
                GatewayJsonSerializerContext.Default.MemberLeftUpdate),
            new JsonPayloadCodec<MemberRemovedUpdate>(
                GatewayJsonSerializerContext.Default.MemberRemovedUpdate),
            new JsonPayloadCodec<RoleChangedUpdate>(
                GatewayJsonSerializerContext.Default.RoleChangedUpdate),
            metrics,
            TimeProvider.System,
            NullLogger<RealtimeEventDispatcher>.Instance);

    private static TcpClientSession CreateSession(
        uint connectionId,
        GatewayMetrics metrics) =>
        new(
            new Socket(
                AddressFamily.InterNetwork,
                SocketType.Stream,
                ProtocolType.Tcp),
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

    /// <summary>
    /// 监听 gateway.realtime.aggregated.* 指标，验证聚合事件分发的本机命中率与 fanout 记录。
    /// </summary>
    private sealed class AggregatedDispatchCounter : IDisposable
    {
        private readonly MeterListener _listener = new();
        private long _events;
        private long _localRecipientsSum;
        private long _totalTargetsSum;

        public long Events => Volatile.Read(ref _events);
        public long LocalRecipientsSum => Volatile.Read(ref _localRecipientsSum);
        public long TotalTargetsSum => Volatile.Read(ref _totalTargetsSum);

        public AggregatedDispatchCounter()
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name != GatewayMetrics.MeterName)
                    return;

                if (instrument.Name == "gateway.realtime.aggregated.events"
                    || instrument.Name == "gateway.realtime.aggregated.local_recipients"
                    || instrument.Name == "gateway.realtime.aggregated.total_targets")
                {
                    listener.EnableMeasurementEvents(instrument, this);
                }
            };
            _listener.SetMeasurementEventCallback<long>(static (instrument, measurement, _, state) =>
            {
                if (state is not AggregatedDispatchCounter counter)
                    return;

                switch (instrument.Name)
                {
                    case "gateway.realtime.aggregated.events":
                        Interlocked.Add(ref counter._events, measurement);
                        break;
                    case "gateway.realtime.aggregated.local_recipients":
                        Interlocked.Add(ref counter._localRecipientsSum, measurement);
                        break;
                    case "gateway.realtime.aggregated.total_targets":
                        Interlocked.Add(ref counter._totalTargetsSum, measurement);
                        break;
                }
            });
            _listener.Start();
        }

        public void Dispose() => _listener.Dispose();
    }
}
