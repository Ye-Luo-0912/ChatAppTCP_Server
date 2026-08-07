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
    private readonly ConversationAudienceCache? _audienceCache;

    public RealtimeEventDeliveryHelper(
        UserSessionRegistry userSessions,
        GatewayMetrics metrics,
        ConversationAudienceCache? audienceCache = null)
    {
        _userSessions = userSessions;
        _metrics = metrics;
        _audienceCache = audienceCache;
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

            if (ShouldSkipByProtocolVersion(target, realtimeEvent))
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
        => DeliverAggregatedCore(
            realtimeEvent,
            command,
            codec,
            update,
            skipOriginSession,
            skipOriginForUserId: null);

    /// <summary>
    /// 多目标 fanout：仅当遍历到 <paramref name="skipOriginForUserId"/> 时跳过来源 SessionId。
    /// 用于单聊聚合事件，让接收方正常投递，同时只对发送方目标执行多设备回声去重。
    /// </summary>
    public int DeliverAggregated<TUpdate>(
        RealtimeEvent realtimeEvent,
        PacketCommand command,
        IPayloadCodec<TUpdate> codec,
        TUpdate update,
        long skipOriginForUserId)
        => DeliverAggregatedCore(
            realtimeEvent,
            command,
            codec,
            update,
            skipOriginSession: true,
            skipOriginForUserId);

    private int DeliverAggregatedCore<TUpdate>(
        RealtimeEvent realtimeEvent,
        PacketCommand command,
        IPayloadCodec<TUpdate> codec,
        TUpdate update,
        bool skipOriginSession,
        long? skipOriginForUserId)
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
                if (skipOriginSession
                    && (!skipOriginForUserId.HasValue || skipOriginForUserId.Value == userId)
                    && ShouldSkipOrigin(target, realtimeEvent))
                    continue;

                if (ShouldSkipByProtocolVersion(target, realtimeEvent))
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

    /// <summary>
    /// P1-2：会话级广播 fanout（AudienceKind=Conversation、TargetUserIds=null）。
    /// 经 <see cref="ConversationAudienceCache"/> 解析会话成员集合，投递到本机会话，
    /// 并跳过 <see cref="RealtimeEvent.ExcludeUserId"/>（如群 MarkRead 的读者本人）。
    /// <para>
    /// 受众解析失败（NATS 超时 / 熔断）时 fail-closed：不投递（返回 0），
    /// 由事件消费者 NAK 重投，绝不投递给错误的受众集合。
    /// </para>
    /// </summary>
    /// <returns>实际入队接收者数。</returns>
    public async ValueTask<int> DeliverToConversationAudienceAsync<TUpdate>(
        RealtimeEvent realtimeEvent,
        PacketCommand command,
        IPayloadCodec<TUpdate> codec,
        TUpdate update,
        bool skipOriginSession,
        CancellationToken ct)
    {
        if (_audienceCache is null
            || string.IsNullOrWhiteSpace(realtimeEvent.ConversationId))
        {
            _metrics.RealtimeEventHandled(queuedDeliveries: 0);
            return 0;
        }

        long[] memberUserIds;
        try
        {
            memberUserIds = await _audienceCache
                .GetOrResolveAsync(
                    realtimeEvent.ConversationId,
                    realtimeEvent.AudienceVersion,
                    ct)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception)
        {
            // fail-closed：解析失败不投递，交由消费者 NAK 重投。
            _metrics.RealtimeEventHandled(queuedDeliveries: 0);
            return 0;
        }

        if (memberUserIds.Length == 0)
        {
            _metrics.RealtimeEventHandled(queuedDeliveries: 0);
            return 0;
        }

        using var frame = OutboundFrameFactory.Create(command, codec, update);
        var queued = 0;
        var exclude = realtimeEvent.ExcludeUserId;
        foreach (var userId in memberUserIds)
        {
            if (exclude.HasValue && userId == exclude.Value)
                continue;

            var userTargets = _userSessions.GetSnapshot(userId);
            for (var i = 0; i < userTargets.Length; i++)
            {
                var target = userTargets[i];
                if (skipOriginSession && ShouldSkipOrigin(target, realtimeEvent))
                    continue;

                if (ShouldSkipByProtocolVersion(target, realtimeEvent))
                    continue;

                if (target.TryQueue(frame))
                    queued++;
            }
        }

        _metrics.RealtimeEventHandled(queued);
        _metrics.RealtimeAggregatedDispatch(
            totalTargets: memberUserIds.Length,
            queuedRecipients: queued);
        return queued;
    }

    /// <summary>
    /// P0-7：协议版本过滤——事件标注的 <see cref="RealtimeEvent.MinProtocolVersion"/>
    /// 高于目标会话协商版本时跳过投递，避免低版本客户端收到不兼容的事件。
    /// </summary>
    private static bool ShouldSkipByProtocolVersion(TcpClientSession target, RealtimeEvent evt)
    {
        if (!evt.MinProtocolVersion.HasValue)
            return false;
        return target.NegotiatedProtocolVersion < (ushort)evt.MinProtocolVersion.Value;
    }

    private static bool ShouldSkipOrigin(TcpClientSession target, RealtimeEvent evt) =>
        !string.IsNullOrWhiteSpace(evt.SessionId) &&
        string.Equals(target.SessionId, evt.SessionId, StringComparison.Ordinal);
}
