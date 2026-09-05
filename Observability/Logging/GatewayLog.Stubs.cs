using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Observability.Logging;

/// <summary>
/// 主线四：附件/关系后端 stub 日志事件。
/// </summary>
public static partial class GatewayLog
{
    [LoggerMessage(
        GatewayEventIds.AttachmentBackendUnavailable,
        LogLevel.Warning,
        "Attachment finalize backend not configured; returning service_unavailable. " +
        "RequestId={RequestId}, AttachmentId={AttachmentId}, UserId={UserId}.",
        EventName = "Stub.AttachmentBackendUnavailable")]
    public static partial void AttachmentBackendUnavailable(
        this ILogger logger,
        string requestId,
        string attachmentId,
        long userId);

    [LoggerMessage(
        GatewayEventIds.RelationshipMutateBackendUnavailable,
        LogLevel.Warning,
        "Relationship mutate backend not configured; returning service_unavailable. " +
        "RequestId={RequestId}, Operation={Operation}, UserId={UserId}.",
        EventName = "Stub.RelationshipMutateBackendUnavailable")]
    public static partial void RelationshipMutateBackendUnavailable(
        this ILogger logger,
        string requestId,
        int operation,
        long userId);

    [LoggerMessage(
        GatewayEventIds.RelationshipListBackendUnavailable,
        LogLevel.Warning,
        "Relationship list query backend not configured; returning service_unavailable. " +
        "RequestId={RequestId}, ListType={ListType}, UserId={UserId}.",
        EventName = "Stub.RelationshipListBackendUnavailable")]
    public static partial void RelationshipListBackendUnavailable(
        this ILogger logger,
        string requestId,
        int listType,
        long userId);

    [LoggerMessage(
        GatewayEventIds.CallBackendUnavailable,
        LogLevel.Warning,
        "Call command backend not configured; returning service_unavailable. " +
        "RequestId={RequestId}, CallId={CallId}, UserId={UserId}.",
        EventName = "Stub.CallBackendUnavailable")]
    public static partial void CallBackendUnavailable(
        this ILogger logger,
        string requestId,
        string callId,
        long userId);
}
