using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;

namespace ChatApp.TcpGateway.Gateway.Messaging.Realtime.Handlers;

/// <summary>
/// 附件生命周期事件处理器（AttachmentLifecycleChanged）。
/// <para>
/// 从 <c>RealtimeEventDispatcher</c> 抽取。payload 为 <see cref="RealtimeAttachmentLifecyclePayload"/>，
/// 通过 <see cref="GatewayJsonSerializerContext"/> 直接反序列化。目标为上传者本人。
/// codec 未注入（测试场景）时静默跳过并记 0 入队指标。
/// </para>
/// </summary>
internal sealed class AttachmentLifecycleHandler : IRealtimeEventHandler
{
    private readonly IPayloadCodec<AttachmentLifecycleUpdate>? _attachmentLifecycleCodec;
    private readonly RealtimeEventDeliveryHelper _delivery;
    private readonly RealtimeEventRejectionSink _rejection;
    private readonly GatewayMetrics _metrics;

    public AttachmentLifecycleHandler(
        IPayloadCodec<AttachmentLifecycleUpdate>? attachmentLifecycleCodec,
        RealtimeEventDeliveryHelper delivery,
        RealtimeEventRejectionSink rejection,
        GatewayMetrics metrics)
    {
        _attachmentLifecycleCodec = attachmentLifecycleCodec;
        _delivery = delivery;
        _rejection = rejection;
        _metrics = metrics;
    }

    public void Handle(RealtimeEvent realtimeEvent)
    {
        if (_attachmentLifecycleCodec is null)
        {
            _metrics.RealtimeEventHandled(queuedDeliveries: 0);
            return;
        }

        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingPayload);
            return;
        }

        RealtimeAttachmentLifecyclePayload? payload;
        try
        {
            payload = JsonSerializer.Deserialize(
                realtimeEvent.PayloadJson,
                GatewayJsonSerializerContext.Default.RealtimeAttachmentLifecyclePayload);
        }
        catch (JsonException)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidJson);
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.AttachmentId)
            || realtimeEvent.TargetUserId <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        _delivery.Deliver(
            realtimeEvent,
            PacketCommand.AttachmentLifecycleChanged,
            _attachmentLifecycleCodec,
            new AttachmentLifecycleUpdate
            {
                AttachmentId = payload.AttachmentId,
                Status = payload.Status,
                OccurredAtMs = payload.OccurredAtMs,
                RejectReason = payload.RejectReason,
                ThumbnailApiHint = payload.ThumbnailApiHint,
                DownloadToken = payload.DownloadToken
            },
            skipOriginSession: false);
    }
}
