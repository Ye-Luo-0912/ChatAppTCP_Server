namespace ChatApp.TcpGateway.Core.Authentication;

/// <summary>
/// 跨 Gateway 同设备 TCP 会话租约：发现旧 SessionId 以便广播 SessionRevoked。
/// </summary>
public interface IDeviceSessionLeaseStore
{
    /// <summary>
    /// 夺取设备租约。若已有不同 SessionId，返回旧 SessionId；否则返回 null。
    /// </summary>
    ValueTask<string?> TakeOverAsync(
        long userId,
        ulong deviceIdHash,
        string sessionId,
        TimeSpan ttl,
        CancellationToken cancellationToken);

    /// <summary>
    /// 仅当租约仍属于本 SessionId 时释放（避免擦掉更新的登录）。
    /// </summary>
    ValueTask ReleaseIfOwnerAsync(
        long userId,
        ulong deviceIdHash,
        string sessionId,
        CancellationToken cancellationToken);
}
