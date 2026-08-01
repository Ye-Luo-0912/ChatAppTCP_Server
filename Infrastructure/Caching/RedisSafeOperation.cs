using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Infrastructure.Caching;

/// <summary>
/// Executes a Redis operation with uniform cancellation and failure handling.
/// <para>
/// <see cref="OperationCanceledException"/> raised under an active cancellation is propagated;
/// any other exception is reported via <see cref="GatewayLog.DependencyOperationFailed"/>
/// without rethrowing. The query overload returns a caller-supplied fallback on failure.
/// </para>
/// <para>
/// Used only on non-hot Redis paths (startup, teardown, account cleanup, periodic shard
/// enumeration). Per-message routing queries keep inline try/catch to avoid a per-call
/// closure allocation on the hot path; the closure here is negligible relative to the
/// Redis network round-trip every caller performs.
/// </para>
/// </summary>
internal static class RedisSafeOperation
{
    /// <summary>
    /// Fire-and-forget: executes <paramref name="operation"/>; logs failures without throwing.
    /// Active cancellation propagates.
    /// </summary>
    public static async Task ExecuteAsync(
        Func<CancellationToken, Task> operation,
        ILogger logger,
        GatewayDependencyOperation operationLabel,
        CancellationToken cancellationToken)
    {
        try
        {
            await operation(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.DependencyOperationFailed(GatewayDependency.Redis, operationLabel, ex);
        }
    }

    /// <summary>
    /// Query with fallback: executes <paramref name="operation"/>; on failure logs and returns
    /// <paramref name="fallback"/>. Active cancellation propagates.
    /// </summary>
    public static async Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        T fallback,
        ILogger logger,
        GatewayDependencyOperation operationLabel,
        CancellationToken cancellationToken)
    {
        try
        {
            return await operation(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.DependencyOperationFailed(GatewayDependency.Redis, operationLabel, ex);
            return fallback;
        }
    }
}
