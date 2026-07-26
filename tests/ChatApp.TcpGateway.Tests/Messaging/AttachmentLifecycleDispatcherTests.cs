using System.Diagnostics.Metrics;
using System.Net.Sockets;
using System.Text.Json;
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
/// 附件生命周期下发验证：Dispatcher 收到 AttachmentLifecycleChanged 事件后，
/// 应将 <see cref="AttachmentLifecycleUpdate"/> 扇出到目标用户（上传者本人）的活跃会话。
/// </summary>
[Collection("MeterListenerSerial")]
public sealed class AttachmentLifecycleDispatcherTests
{
    [Fact]
    public async Task AttachmentLifecycleChanged_FansOutToTargetUserSessions()
    {
        using var metrics = new GatewayMetrics();
        var registry = new UserSessionRegistry();
        await using var target = CreateSession(1, metrics);
        target.Authenticate(42, "target-session", deviceIdHash: 1);
        Assert.True(registry.Add(target));

        var dispatcher = CreateDispatcher(registry, metrics, withAttachmentCodec: true);

        var payload = new RealtimeAttachmentLifecyclePayload
        {
            AttachmentId = "attach-1",
            Status = (short)AttachmentWireStatus.Available,
            OccurredAtMs = 1_700_000_000_000L,
            ThumbnailApiHint = "/api/attachments/attach-1/thumbnail",
            DownloadToken = "token-1"
        };
        var payloadJson = JsonSerializer.Serialize(
            payload,
            GatewayJsonSerializerContext.Default.RealtimeAttachmentLifecyclePayload);

        using var enqueueCounter = new OutboundEnqueueCounter();
        var baseline = enqueueCounter.PositiveEnqueues;

        dispatcher.Dispatch(
            new RealtimeEvent
            {
                EventId = "attachment-lifecycle-event",
                Type = RealtimeEventType.AttachmentLifecycleChanged,
                TargetUserId = 42,
                PayloadJson = payloadJson,
                OccurredAtMs = 1_700_000_000_000L
            });

        Assert.True(
            enqueueCounter.PositiveEnqueues > baseline,
            "目标会话应至少入队一帧 AttachmentLifecycleChanged 下行。");
    }

    [Fact]
    public async Task AttachmentLifecycleChanged_WithoutCodec_SilentlySkips()
    {
        using var metrics = new GatewayMetrics();
        var registry = new UserSessionRegistry();
        await using var target = CreateSession(1, metrics);
        target.Authenticate(42, "target-session", deviceIdHash: 1);
        Assert.True(registry.Add(target));

        // 不注入 attachmentLifecycleCodec：应静默跳过，不入队、不抛。
        var dispatcher = CreateDispatcher(registry, metrics, withAttachmentCodec: false);

        using var enqueueCounter = new OutboundEnqueueCounter();
        var baseline = enqueueCounter.PositiveEnqueues;

        dispatcher.Dispatch(
            new RealtimeEvent
            {
                EventId = "attachment-lifecycle-event",
                Type = RealtimeEventType.AttachmentLifecycleChanged,
                TargetUserId = 42,
                PayloadJson = "{\"attachmentId\":\"attach-1\",\"status\":1,\"occurredAtMs\":1700000000000}",
                OccurredAtMs = 1_700_000_000_000L
            });

        Assert.Equal(baseline, enqueueCounter.PositiveEnqueues);
    }

    [Fact]
    public async Task AttachmentLifecycleChanged_InvalidPayload_IsRejected()
    {
        using var metrics = new GatewayMetrics();
        var registry = new UserSessionRegistry();
        await using var target = CreateSession(1, metrics);
        target.Authenticate(42, "target-session", deviceIdHash: 1);
        Assert.True(registry.Add(target));

        var dispatcher = CreateDispatcher(registry, metrics, withAttachmentCodec: true);

        using var enqueueCounter = new OutboundEnqueueCounter();
        var baseline = enqueueCounter.PositiveEnqueues;

        // 缺 AttachmentId：应被 RejectEvent 拦截，不入队。
        dispatcher.Dispatch(
            new RealtimeEvent
            {
                EventId = "attachment-lifecycle-event",
                Type = RealtimeEventType.AttachmentLifecycleChanged,
                TargetUserId = 42,
                PayloadJson = "{\"attachmentId\":\"\",\"status\":1,\"occurredAtMs\":1700000000000}",
                OccurredAtMs = 1_700_000_000_000L
            });

        Assert.Equal(baseline, enqueueCounter.PositiveEnqueues);
    }

    private static RealtimeEventDispatcher CreateDispatcher(
        UserSessionRegistry registry,
        GatewayMetrics metrics,
        bool withAttachmentCodec) =>
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
            NullLogger<RealtimeEventDispatcher>.Instance,
            relationshipListChangedCodec: null,
            attachmentLifecycleCodec: withAttachmentCodec
                ? new JsonPayloadCodec<AttachmentLifecycleUpdate>(
                    GatewayJsonSerializerContext.Default.AttachmentLifecycleUpdate)
                : null);

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
}
