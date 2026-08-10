using System.Net;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Observability.Logging;

/// <summary>
/// Gateway lifecycle and transport log templates.
/// </summary>
public static partial class GatewayLog
{
    [LoggerMessage(
        GatewayEventIds.GatewayStarted,
        LogLevel.Information,
        "TCP gateway listening on {Endpoint}; maximum connections: {MaxConnections}.",
        EventName = "TcpGateway.GatewayStarted")]
    public static partial void GatewayStarted(
        this ILogger logger,
        IPEndPoint endpoint,
        int maxConnections);

    [LoggerMessage(
        GatewayEventIds.GatewayStopped,
        LogLevel.Information,
        "TCP gateway stopped.",
        EventName = "TcpGateway.GatewayStopped")]
    public static partial void GatewayStopped(this ILogger logger);

    [LoggerMessage(
        GatewayEventIds.SessionCloseSummary,
        LogLevel.Information,
        "TCP gateway session close summary: {Summary}.",
        EventName = "TcpGateway.SessionCloseSummary")]
    public static partial void SessionCloseSummary(
        this ILogger logger,
        string summary);

    [LoggerMessage(
        GatewayEventIds.GatewayFatal,
        LogLevel.Critical,
        "TCP gateway stopped due to a fatal error.",
        EventName = "TcpGateway.GatewayFatal")]
    public static partial void GatewayFatal(
        this ILogger logger,
        Exception exception);

    [LoggerMessage(
        GatewayEventIds.TransportFailed,
        LogLevel.Error,
        "Transport operation {Operation} failed on connection {ConnectionId}.",
        EventName = "TcpGateway.TransportFailed")]
    public static partial void TransportFailed(
        this ILogger logger,
        GatewayTransportOperation operation,
        uint connectionId,
        Exception exception);

    [LoggerMessage(
        GatewayEventIds.EphemeralDisabled,
        LogLevel.Information,
        "Ephemeral Presence/Typing is disabled; skipping NATS Core subscription.",
        EventName = "TcpGateway.EphemeralDisabled")]
    public static partial void EphemeralDisabled(this ILogger logger);

    [LoggerMessage(
        GatewayEventIds.LifecycleCleanupFailed,
        LogLevel.Warning,
        "External lifecycle cleanup failed on connection {ConnectionId}; local resource accounting proceeds.",
        EventName = "TcpGateway.LifecycleCleanupFailed")]
    public static partial void LifecycleCleanupFailed(
        this ILogger logger,
        uint connectionId,
        Exception exception);
}
