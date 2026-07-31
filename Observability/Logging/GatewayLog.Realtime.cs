using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Observability.Logging;

/// <summary>
/// Realtime bus subscription, delivery and dispatch-rejection log templates.
/// Typing and Presence ephemeral subscription failures share the
/// <see cref="RealtimeSubscriptionFailed"/> template, distinguished by
/// <see cref="RealtimeSubscriptionKind"/>.
/// </summary>
public static partial class GatewayLog
{
    [LoggerMessage(
        GatewayEventIds.RealtimeBusReady,
        LogLevel.Information,
        "Realtime message bus is ready; ping latency: {LatencyMilliseconds:F2} ms.",
        EventName = "TcpGateway.RealtimeBusReady")]
    public static partial void RealtimeBusReady(
        this ILogger logger,
        double latencyMilliseconds);

    [LoggerMessage(
        GatewayEventIds.RealtimeSubscriptionFailed,
        LogLevel.Warning,
        "Realtime subscription {Subscription} failed; retrying after {RetryDelay}.",
        EventName = "TcpGateway.RealtimeSubscriptionFailed")]
    public static partial void RealtimeSubscriptionFailed(
        this ILogger logger,
        RealtimeSubscriptionKind subscription,
        TimeSpan retryDelay,
        Exception exception);

    [LoggerMessage(
        GatewayEventIds.RealtimeDeliveryFailed,
        LogLevel.Error,
        "Realtime event {RealtimeEventId} failed at delivery {DeliveryCount}; redelivery requested.",
        EventName = "TcpGateway.RealtimeDeliveryFailed")]
    public static partial void RealtimeDeliveryFailed(
        this ILogger logger,
        string realtimeEventId,
        ulong? deliveryCount,
        Exception exception);

    [LoggerMessage(
        GatewayEventIds.RealtimeNakFailed,
        LogLevel.Error,
        "Realtime event {RealtimeEventId} NAK failed; JetStream AckWait will control redelivery.",
        EventName = "TcpGateway.RealtimeNakFailed")]
    public static partial void RealtimeNakFailed(
        this ILogger logger,
        string realtimeEventId,
        Exception exception);

    [LoggerMessage(
        GatewayEventIds.RealtimeEventRejected,
        LogLevel.Warning,
        "Realtime event {RealtimeEventId} of type {RealtimeEventType} was rejected: {Reason}.",
        EventName = "TcpGateway.RealtimeEventRejected")]
    public static partial void RealtimeEventRejected(
        this ILogger logger,
        string realtimeEventId,
        string realtimeEventType,
        RealtimeRejectReason reason);

    [LoggerMessage(
        GatewayEventIds.RealtimeEventUnsupported,
        LogLevel.Debug,
        "Realtime event {RealtimeEventId} of type {RealtimeEventType} has no TCP wire mapping and was acknowledged.",
        EventName = "TcpGateway.RealtimeEventUnsupported")]
    public static partial void RealtimeEventUnsupported(
        this ILogger logger,
        string realtimeEventId,
        string realtimeEventType);

    [LoggerMessage(
        GatewayEventIds.PushDeliveryDispatched,
        LogLevel.Information,
        "Push delivery for user {TargetUserId} dispatched: attempted={AttemptedCount}, succeeded={SucceededCount}.",
        EventName = "TcpGateway.PushDeliveryDispatched")]
    public static partial void PushDeliveryDispatched(
        this ILogger logger,
        long targetUserId,
        int attemptedCount,
        int succeededCount);

    [LoggerMessage(
        GatewayEventIds.PushDeliveryFailed,
        LogLevel.Error,
        "Push delivery for user {TargetUserId} failed; redelivery requested.",
        EventName = "TcpGateway.PushDeliveryFailed")]
    public static partial void PushDeliveryFailed(
        this ILogger logger,
        long targetUserId,
        Exception exception);
}
