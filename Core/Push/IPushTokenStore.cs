using ChatApp.TcpGateway.Core.Messaging.Push;

namespace ChatApp.TcpGateway.Core.Push;

/// <summary>
/// 设备推送令牌存储。按 (userId, deviceIdHash) 索引；
/// 支持多设备（同一用户多个 token），用于离线推送时拉取全部活跃令牌。
/// </summary>
public interface IPushTokenStore
{
    /// <summary>
    /// 注册或覆盖一个推送令牌。同 (userId, deviceIdHash) 已存在则覆盖。
    /// 超过 <see cref="PushTokenLimits.MaxTokensPerUser"/> 时按 updatedAtMs 最旧淘汰。
    /// </summary>
    /// <returns>注册后该用户的活跃令牌数。</returns>
    ValueTask<int> RegisterAsync(
        long userId,
        ulong deviceIdHash,
        PushPlatform platform,
        string token,
        string? appDeviceLabel,
        CancellationToken cancellationToken);

    /// <summary>
    /// 按当前连接的 deviceIdHash 注销该设备的全部令牌。
    /// </summary>
    /// <returns>注销后该用户的活跃令牌数。</returns>
    ValueTask<int> UnregisterByDeviceAsync(
        long userId,
        ulong deviceIdHash,
        CancellationToken cancellationToken);

    /// <summary>
    /// 按精确令牌字符串注销（用于令牌被平台失效的场景）。
    /// </summary>
    /// <returns>注销后该用户的活跃令牌数。</returns>
    ValueTask<int> UnregisterByTokenAsync(
        long userId,
        string token,
        CancellationToken cancellationToken);

    /// <summary>
    /// 列出该用户的全部活跃推送令牌（Push Service 拉取用）。
    /// </summary>
    ValueTask<IReadOnlyList<PushTokenRecord>> ListAsync(
        long userId,
        CancellationToken cancellationToken);
}
