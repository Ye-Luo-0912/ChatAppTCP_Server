namespace ChatApp.TcpGateway.Core.Messaging.Push;

/// <summary>
/// 注销推送令牌。登出或令牌失效时调用。
/// 不传 Token 时按当前连接的 deviceIdHash 注销。
/// </summary>
public sealed class UnregisterPushTokenRequest
{
    public string? RequestId { get; init; }

    /// <summary>
    /// 可选：精确指定要注销的令牌字符串。
    /// 不传则按当前连接的 deviceIdHash 注销该设备所有令牌。
    /// </summary>
    public string? Token { get; init; }
}
