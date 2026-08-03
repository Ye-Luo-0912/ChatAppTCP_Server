using ChatApp.Realtime.Abstractions.Push;

namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// 单平台推送 Provider 抽象（FCM/APNs/WebPush 各一实现）。
/// <para>
/// 实现方负责：
/// <list type="bullet">
/// <item>向平台 API 发送推送请求（HTTP/2 长连接复用）。</item>
/// <item>解析平台响应，返回 <see cref="PushProviderResult"/>。</item>
/// <item>实现自身限流：单 Provider QPS 上限，超限返回 <c>rate_limited</c>。</item>
/// <item>不负责令牌拉取、多平台分发、Retry/DLQ（由 <see cref="PushDispatcher"/> 统一编排）。</item>
/// </list>
/// </para>
/// </summary>
public interface IPushProvider
{
    /// <summary>该 Provider 支持的平台。</summary>
    PushPlatform Platform { get; }

    /// <summary>
    /// 向单个令牌投递推送。
    /// </summary>
    /// <param name="token">平台推送令牌（FCM token / APNs device token / WebPush endpoint subscription）。</param>
    /// <param name="title">推送标题（已本地化）。</param>
    /// <param name="body">推送正文（已本地化）。</param>
    /// <param name="collapseKey">折叠键（同 key 的推送在锁屏折叠）。可空。</param>
    /// <param name="customData">自定义数据（点击跳转 payload 等）。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>投递结果。</returns>
    Task<PushProviderResult> SendAsync(
        string token,
        string title,
        string body,
        string? collapseKey,
        IReadOnlyDictionary<string, string>? customData,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// 单次推送投递结果。
/// </summary>
public readonly record struct PushProviderResult
{
    /// <summary>是否投递成功（Provider API 返回成功）。</summary>
    public required bool Succeeded { get; init; }

    /// <summary>
    /// 失败原因代码（成功时为 null）：
    /// <list type="bullet">
    /// <item><c>invalid_token</c>：令牌无效，应注销。</item>
    /// <item><c>provider_unavailable</c>：Provider 暂时不可用（5xx/超时），可重试。</item>
    /// <item><c>rate_limited</c>：触发限流（429），可重试。</item>
    /// <item><c>payload_too_large</c>：负载超限，不可重试。</item>
    /// <item><c>unknown</c>：未知错误。</item>
    /// </list>
    /// </summary>
    public string? ErrorCode { get; init; }

    /// <summary>失败时的重试建议间隔（仅 rate_limited / provider_unavailable 有值）。</summary>
    public TimeSpan? RetryAfter { get; init; }

    public static PushProviderResult Ok() => new() { Succeeded = true };

    public static PushProviderResult Fail(string errorCode, TimeSpan? retryAfter = null) =>
        new() { Succeeded = false, ErrorCode = errorCode, RetryAfter = retryAfter };
}
