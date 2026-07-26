namespace ChatApp.TcpGateway.Core.Messaging.Push;

/// <summary>
/// 客户端注册设备推送令牌。服务端按 (userId, deviceIdHash) 幂等覆盖。
/// </summary>
public sealed class RegisterPushTokenRequest
{
    public string? RequestId { get; init; }

    /// <summary>
    /// 推送平台。1=Fcm，2=Apns。
    /// </summary>
    public PushPlatform Platform { get; init; }

    /// <summary>
    /// 平台下发的推送令牌（FCM token 或 APNs device token）。
    /// 长度上限见 <see cref="PushTokenLimits.MaxTokenLength"/>。
    /// </summary>
    public required string Token { get; init; }

    /// <summary>
    /// 可选的应用级设备标识（用于多 App 共存的去重判断）。
    /// 不传则只按 <c>deviceIdHash</c> 去重。
    /// </summary>
    public string? AppDeviceLabel { get; init; }
}
