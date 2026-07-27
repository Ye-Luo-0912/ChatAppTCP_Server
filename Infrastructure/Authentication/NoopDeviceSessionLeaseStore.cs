using ChatApp.TcpGateway.Core.Authentication;

namespace ChatApp.TcpGateway.Infrastructure.Authentication;

/// <summary>无 Redis 时的空实现（仅本机 TakeOverSameDevice）。</summary>
internal sealed class NoopDeviceSessionLeaseStore : IDeviceSessionLeaseStore
{
    public ValueTask<string?> TakeOverAsync(
        long userId,
        ulong deviceIdHash,
        string sessionId,
        string connectionLeaseId,
        TimeSpan ttl,
        CancellationToken cancellationToken) =>
        ValueTask.FromResult<string?>(null);

    public ValueTask ReleaseIfOwnerAsync(
        long userId,
        ulong deviceIdHash,
        string connectionLeaseId,
        CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;

    public ValueTask<bool> RefreshIfOwnerAsync(
        long userId,
        ulong deviceIdHash,
        string connectionLeaseId,
        TimeSpan ttl,
        CancellationToken cancellationToken) =>
        ValueTask.FromResult(true);

    public ValueTask<string?> GetCurrentSessionIdAsync(
        long userId,
        ulong deviceIdHash,
        CancellationToken cancellationToken) =>
        ValueTask.FromResult<string?>(null);
}
