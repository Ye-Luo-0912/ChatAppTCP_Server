using ChatApp.TcpGateway.Core.Protocol;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Observability.Logging;

/// <summary>
/// Source-generated logger extension methods for the TCP gateway.
/// Each partial file groups a cohesive set of events and owns its EventId allocation.
/// Methods are declared as <see cref="ILogger"/> extension methods so the original
/// <c>ILogger&lt;T&gt;</c> category of the caller is preserved.
/// </summary>
public static partial class GatewayLog
{
    [LoggerMessage(
        GatewayEventIds.CommandFailed,
        LogLevel.Warning,
        "Command {Command} failed on connection {ConnectionId}; correlation {CorrelationId}.",
        EventName = "TcpGateway.CommandFailed")]
    public static partial void CommandFailed(
        this ILogger logger,
        PacketCommand command,
        uint connectionId,
        string correlationId,
        Exception exception);

    [LoggerMessage(
        GatewayEventIds.SessionRevocationFailed,
        LogLevel.Warning,
        "Session revocation publish failed on connection {ConnectionId} for session {SessionId}.",
        EventName = "TcpGateway.SessionRevocationFailed")]
    public static partial void SessionRevocationFailed(
        this ILogger logger,
        uint connectionId,
        string sessionId,
        Exception exception);
}
