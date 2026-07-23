using ChatApp.TcpGateway.Core.Authentication;

namespace ChatApp.TcpGateway.Infrastructure.Authentication;

/// <summary>无 Redis 时的空实现（仅本机 TakeOverSameDevice）。</summary>
internal sealed class NoopDeviceSessionLeaseStore : IDeviceSessionLeaseStore
{
    public ValueTask<string?> TakeOverAsync(
        long userId,
        ulong deviceIdHash,
        string sessionId,
        TimeSpan ttl,
        CancellationToken cancellationToken) =>
        ValueTask.FromResult<string?>(null);

    public ValueTask ReleaseIfOwnerAsync(
        long userId,
        ulong deviceIdHash,
        string sessionId,
        CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;
}
