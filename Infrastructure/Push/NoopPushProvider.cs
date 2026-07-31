using ChatApp.TcpGateway.Core.Messaging.Push;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// 空实现 Push Provider：不实际发送，仅记录日志。
/// <para>
/// 用于开发/测试环境，或未配置真实 Provider 凭证时的占位。
/// 生产环境应注册 FcmPushProvider / ApnsPushProvider / WebPushProvider 替换。
/// </para>
/// </summary>
internal sealed partial class NoopPushProvider : IPushProvider
{
    private readonly ILogger<NoopPushProvider> _logger;

    public NoopPushProvider(PushPlatform platform, ILogger<NoopPushProvider> logger)
    {
        Platform = platform;
        _logger = logger;
    }

    public PushPlatform Platform { get; }

    public Task<PushProviderResult> SendAsync(
        string token,
        string title,
        string body,
        string? collapseKey,
        IReadOnlyDictionary<string, string>? customData,
        CancellationToken cancellationToken = default)
    {
        LogNoopSend(_logger, Platform, title, body, collapseKey);
        return Task.FromResult(PushProviderResult.Ok());
    }

    [LoggerMessage(
        LogLevel.Information,
        "NoopPushProvider {Platform}: would send push title={Title} body={Body} collapseKey={CollapseKey}")]
    static partial void LogNoopSend(
        ILogger logger,
        PushPlatform platform,
        string title,
        string body,
        string? collapseKey);
}
