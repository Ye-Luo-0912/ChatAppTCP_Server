namespace ChatApp.PushWorker.Providers;

/// <summary>
/// 推送 Provider 配置（FCM / APNs / WebPush 凭据与端点）。
/// <para>
/// 仅在 <see cref="ChatApp.TcpGateway.Infrastructure.Push.PushProviderMode.Production"/> 模式下使用。
/// 对应 appsettings.json 的 "Push:Providers" 节。
/// </para>
/// </summary>
public sealed class PushProviderOptions
{
    public const string SectionName = "Push:Providers";

    public FcmOptions Fcm { get; set; } = new();
    public ApnsOptions Apns { get; set; } = new();
    public WebPushOptions WebPush { get; set; } = new();

    /// <summary>
    /// HTTP 请求超时（含 TLS 握手）。默认 10 秒。
    /// 推送 API 通常在 1-3 秒内响应；超时视为 provider_unavailable。
    /// </summary>
    public TimeSpan HttpTimeout { get; set; } = TimeSpan.FromSeconds(10);

    public bool IsValid()
    {
        if (HttpTimeout <= TimeSpan.Zero)
            return false;
        if (!Fcm.IsValid())
            return false;
        if (!Apns.IsValid())
            return false;
        if (!WebPush.IsValid())
            return false;
        return true;
    }
}

/// <summary>
/// Firebase Cloud Messaging (Android / 浏览器) 配置。
/// <para>
/// 使用 HTTP v1 API：<c>POST https://fcm.googleapis.com/v1/projects/{ProjectId}/messages:send</c>。
/// 认证：OAuth2 JWT Bearer Token（Service Account 私钥 RS256 签名），1 小时有效。
/// </para>
/// </summary>
public sealed class FcmOptions
{
    /// <summary>Firebase 项目 Id（用于 URL 路径）。</summary>
    public string? ProjectId { get; set; }

    /// <summary>
    /// Service Account JSON 文件路径（含 private_key 与 client_email）。
    /// 与 <see cref="ServiceAccountJson"/> 二选一。
    /// </summary>
    public string? ServiceAccountKeyPath { get; set; }

    /// <summary>
    /// Service Account JSON 内容（内联）。与 <see cref="ServiceAccountKeyPath"/> 二选一。
    /// </summary>
    public string? ServiceAccountJson { get; set; }

    /// <summary>FCM API 端点前缀（可覆盖用于测试）。默认 <c>https://fcm.googleapis.com</c>。</summary>
    public string ApiEndpoint { get; set; } = "https://fcm.googleapis.com";

    public bool IsValid()
    {
        if (string.IsNullOrWhiteSpace(ProjectId))
            return false;
        if (string.IsNullOrWhiteSpace(ServiceAccountKeyPath) && string.IsNullOrWhiteSpace(ServiceAccountJson))
            return false;
        return true;
    }
}

/// <summary>
/// Apple Push Notification service (iOS / macOS) 配置。
/// <para>
/// 使用 HTTP/2 API：<c>POST https://api.push.apple.com/3/device/{token}</c>。
/// 认证：Provider JWT（p8 私钥 ES256 签名），最长 1 小时有效。
/// </para>
/// </summary>
public sealed class ApnsOptions
{
    /// <summary>Apple Developer Team ID（10 字符，见 Membership 页面）。</summary>
    public string? TeamId { get; set; }

    /// <summary>Key ID（10 字符，创建 Auth Key 时分配）。</summary>
    public string? KeyId { get; set; }

    /// <summary>
    /// p8 私钥 PEM 内容（<c>-----BEGIN PRIVATE KEY-----</c> 格式）。
    /// </summary>
    public string? PrivateKeyPem { get; set; }

    /// <summary>App Bundle ID（作为 apns-topic header）。</summary>
    public string? BundleId { get; set; }

    /// <summary>
    /// APNs 端点。开发环境：<c>https://api.sandbox.push.apple.com</c>；
    /// 生产环境：<c>https://api.push.apple.com</c>（默认）。
    /// </summary>
    public string ApiEndpoint { get; set; } = "https://api.push.apple.com";

    public bool IsValid()
    {
        if (string.IsNullOrWhiteSpace(TeamId))
            return false;
        if (string.IsNullOrWhiteSpace(KeyId))
            return false;
        if (string.IsNullOrWhiteSpace(PrivateKeyPem))
            return false;
        if (string.IsNullOrWhiteSpace(BundleId))
            return false;
        return true;
    }
}

/// <summary>
/// Web Push API（浏览器 Service Worker）配置。
/// <para>
/// 使用 RFC 8291 加密 + VAPID JWT 认证。
/// 推送目标 URL 由客户端订阅 endpoint 提供（每 token 不同）。
/// </para>
/// </summary>
public sealed class WebPushOptions
{
    /// <summary>VAPID subject（mailto: 链系或 https URL，用于联系推送方）。</summary>
    public string? VapidSubject { get; set; }

    /// <summary>VAPID 私钥 PEM（ECDSA P-256，<c>-----BEGIN PRIVATE KEY-----</c> 格式）。</summary>
    public string? VapidPrivateKeyPem { get; set; }

    /// <summary>VAPID 公钥（Base64Url 编码，87 字符无 padding）。客户端用于订阅。</summary>
    public string? VapidPublicKey { get; set; }

    public bool IsValid()
    {
        if (string.IsNullOrWhiteSpace(VapidSubject))
            return false;
        if (string.IsNullOrWhiteSpace(VapidPrivateKeyPem))
            return false;
        if (string.IsNullOrWhiteSpace(VapidPublicKey))
            return false;
        return true;
    }
}
