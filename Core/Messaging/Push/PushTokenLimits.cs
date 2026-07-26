namespace ChatApp.TcpGateway.Core.Messaging.Push;

/// <summary>推送令牌注册相关结构上限。</summary>
public static class PushTokenLimits
{
    /// <summary>令牌字符串最大长度（FCM ~150，APNs 64 hex；留余量）。</summary>
    public const int MaxTokenLength = 1024;

    /// <summary>RequestId 最大长度。</summary>
    public const int MaxRequestIdLength = 64;

    /// <summary>AppDeviceLabel 最大长度。</summary>
    public const int MaxAppDeviceLabelLength = 128;

    /// <summary>单用户最多保留的活跃推送令牌数（多设备场景）。</summary>
    public const int MaxTokensPerUser = 8;
}
