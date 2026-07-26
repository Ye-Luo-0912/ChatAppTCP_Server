using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Observability.Logging;

/// <summary>
/// Dependency-operation log templates. High-frequency transient failures
/// (e.g. per-Presence-refresh) are reported via metrics only and do not use
/// these templates; these templates cover state changes and operation failures
/// that benefit from a log record.
/// </summary>
public static partial class GatewayLog
{
    [LoggerMessage(
        GatewayEventIds.DependencyOperationFailed,
        LogLevel.Warning,
        "Dependency operation {Operation} failed on {Dependency}.",
        EventName = "TcpGateway.DependencyOperationFailed")]
    public static partial void DependencyOperationFailed(
        this ILogger logger,
        GatewayDependency dependency,
        GatewayDependencyOperation operation,
        Exception exception);

    [LoggerMessage(
        GatewayEventIds.DependencyUnavailable,
        LogLevel.Error,
        "Dependency {Dependency} is unavailable during {Operation}.",
        EventName = "TcpGateway.DependencyUnavailable")]
    public static partial void DependencyUnavailable(
        this ILogger logger,
        GatewayDependency dependency,
        GatewayDependencyOperation operation,
        Exception exception);

    [LoggerMessage(
        GatewayEventIds.DependencyDataInvalid,
        LogLevel.Warning,
        "Dependency {Dependency} returned invalid data during {Operation}.",
        EventName = "TcpGateway.DependencyDataInvalid")]
    public static partial void DependencyDataInvalid(
        this ILogger logger,
        GatewayDependency dependency,
        GatewayDependencyOperation operation,
        Exception exception);

    [LoggerMessage(
        GatewayEventIds.DependencyConnected,
        LogLevel.Information,
        "Dependency {Dependency} connection established.",
        EventName = "TcpGateway.DependencyConnected")]
    public static partial void DependencyConnected(
        this ILogger logger,
        GatewayDependency dependency);

    [LoggerMessage(
        GatewayEventIds.DependencyDisconnected,
        LogLevel.Error,
        "Dependency {Dependency} connection failed at {Endpoint}: {FailureType}.",
        EventName = "TcpGateway.DependencyDisconnected")]
    public static partial void DependencyDisconnected(
        this ILogger logger,
        GatewayDependency dependency,
        string? endpoint,
        string failureType,
        Exception? exception);

    [LoggerMessage(
        GatewayEventIds.DependencyRestored,
        LogLevel.Information,
        "Dependency {Dependency} connection restored at {Endpoint}.",
        EventName = "TcpGateway.DependencyRestored")]
    public static partial void DependencyRestored(
        this ILogger logger,
        GatewayDependency dependency,
        string? endpoint);
}
