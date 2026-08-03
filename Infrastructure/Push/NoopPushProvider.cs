using ChatApp.Realtime.Abstractions.Push;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// 空实现 Push Provider：不实际发送推送。
/// <para>
/// P0-1 修复：Noop 永远返回 <c>provider_unavailable</c>（可重试失败），
/// 而非 <c>Ok</c>。这样推送命令会被 Consumer NAK 重投，不会被静默吞掉。
/// </para>
/// <para>
/// 仅用于开发/测试环境（<see cref="PushProviderMode.TestNoop"/>）。
/// 生产环境必须注册真实 FcmPushProvider / ApnsPushProvider / WebPushProvider，
/// 并通过 <see cref="PushProviderMode.Production"/> 启动校验。
/// </para>
/// <para>
/// 日志级别为 Debug，且不记录推送正文（避免敏感信息泄露与 Information 级别噪音）。
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
        // P0-1：不返回 Ok，返回 provider_unavailable 让 Consumer NAK 重投。
        LogNoopSkipped(_logger, Platform);
        return Task.FromResult(PushProviderResult.Fail("provider_unavailable"));
    }

    [LoggerMessage(
        LogLevel.Debug,
        "NoopPushProvider {Platform}: skipped (returns provider_unavailable for retry).")]
    static partial void LogNoopSkipped(
        ILogger logger,
        PushPlatform platform);
}
