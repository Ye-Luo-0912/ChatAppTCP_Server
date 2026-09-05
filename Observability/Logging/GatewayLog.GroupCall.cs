using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Observability.Logging;

/// <summary>
/// GROUP-CALL-1：群通话无状态信令中继日志事件。
/// </summary>
public static partial class GatewayLog
{
    [LoggerMessage(
        1604,
        LogLevel.Debug,
        "Group call signal relayed statelessly. " +
        "CallId={CallId}, ActorUserId={ActorUserId}, CommandType={CommandType}, SignalCount={SignalCount}.",
        EventName = "GroupCall.Relayed")]
    public static partial void GroupCallRelayed(
        this ILogger logger,
        string callId,
        long actorUserId,
        int commandType,
        int signalCount);
}
