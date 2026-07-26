using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Networking.Sessions;

namespace ChatApp.TcpGateway.Tests;

internal sealed class NoopGlobalPresenceStore : IGlobalPresenceStore
{
    public Task<PresenceTransition> SetOnlineAsync(long userId, string instanceId, CancellationToken ct = default) =>
        Task.FromResult(PresenceTransition.None);

    public Task<PresenceTransition> SetOfflineAsync(long userId, string instanceId, CancellationToken ct = default) =>
        Task.FromResult(PresenceTransition.None);

    public Task RefreshOnlineAsync(long userId, string instanceId, CancellationToken ct = default) =>
        Task.CompletedTask;

    public Task<bool> IsOnlineAsync(long userId, CancellationToken ct = default) =>
        Task.FromResult(false);

    public Task<IReadOnlyDictionary<long, bool>> GetOnlineManyAsync(
        IReadOnlyList<long> userIds,
        CancellationToken ct = default) =>
        Task.FromResult<IReadOnlyDictionary<long, bool>>(
            userIds.ToDictionary(static id => id, static _ => false));
}
