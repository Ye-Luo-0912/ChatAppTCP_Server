using ChatApp.Realtime.Abstractions.Events;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Metrics;

namespace ChatApp.TcpGateway.Gateway.Messaging.Realtime;

/// <summary>
/// Realtime 事件本机 fanout 通用工具：消除 7+ 处样板代码（snapshot / empty-check /
/// build-frame / foreach-queue / metric）。两种模式：
/// <list type="bullet">
///   <item><see cref="Deliver{TUpdate}"/>：单目标 fanout，可选跳过来源 SessionId。</item>
///   <item><see cref="DeliverAggregated{TUpdate}"/>：群聊聚合多目标 fanout，记录放大系数指标。</item>
/// </list>
/// </summary>
internal sealed class RealtimeEventDeliveryHelper
{
    private readonly UserSessionRegistry _userSessions;
    private readonly GatewayMetrics _metrics;

    public RealtimeEventDeliveryHelper(
        UserSessionRegistry userSessions,
        GatewayMetrics metrics)
    {
        _userSessions = userSessions;
        _metrics = metrics;
    }

    /// <summary>
    /// 单目标 fanout：取 <see cref="RealtimeEvent.TargetUserId"/> 的本机会话快照，
    /// 构造帧并入队。空快照直接记 0 入队指标返回。
    /// </summary>
    /// <param name="skipOriginSession">true 时跳过 SessionId 匹配来源的会话（多设备同步去重）。</param>
    /// <returns>实际入队接收者数。</returns>
    public int Deliver<TUpdate>(
        RealtimeEvent realtimeEvent,
        PacketCommand command,
        IPayloadCodec<TUpdate> codec,
        TUpdate update,
        bool skipOriginSession)
    {
        var targets = _userSessions.GetSnapshot(realtimeEvent.TargetUserId);
        if (targets.Length == 0)
        {
            _metrics.RealtimeEventHandled(queuedDeliveries: 0);
            return 0;
        }

        using var frame = OutboundFrameFactory.Create(command, codec, update);
        var queued = 0;
        foreach (var target in targets)
        {
            if (skipOriginSession && ShouldSkipOrigin(target, realtimeEvent))
                continue;

            if (target.TryQueue(frame))
                queued++;
        }

        _metrics.RealtimeEventHandled(queued);
        return queued;
    }

    /// <summary>
    /// 群聊聚合多目标 fanout：遍历 <see cref="RealtimeEvent.TargetUserIds"/>，
    /// 对每个 userId 取本机快照并入队。同时记录总目标数与实际入队数指标以监控放大系数。
    /// </summary>
    /// <returns>实际入队接收者数。</returns>
    public int DeliverAggregated<TUpdate>(
        RealtimeEvent realtimeEvent,
        PacketCommand command,
        IPayloadCodec<TUpdate> codec,
        TUpdate update,
        bool skipOriginSession)
    {
        var targetUserIds = realtimeEvent.TargetUserIds;
        if (targetUserIds is null || targetUserIds.Length == 0)
            return 0;

        using var frame = OutboundFrameFactory.Create(command, codec, update);
        var queued = 0;
        foreach (var userId in targetUserIds)
        {
            var userTargets = _userSessions.GetSnapshot(userId);
            for (var i = 0; i < userTargets.Length; i++)
            {
                var target = userTargets[i];
                if (skipOriginSession && ShouldSkipOrigin(target, realtimeEvent))
                    continue;

                if (target.TryQueue(frame))
                    queued++;
            }
        }

        _metrics.RealtimeEventHandled(queued);
        _metrics.RealtimeAggregatedDispatch(
            totalTargets: targetUserIds.Length,
            queuedRecipients: queued);
        return queued;
    }

    private static bool ShouldSkipOrigin(TcpClientSession target, RealtimeEvent evt) =>
        !string.IsNullOrWhiteSpace(evt.SessionId) &&
        string.Equals(target.SessionId, evt.SessionId, StringComparison.Ordinal);
}
