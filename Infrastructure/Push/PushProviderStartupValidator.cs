using ChatApp.TcpGateway.Core.Messaging.Push;
using ChatApp.TcpGateway.Infrastructure.Push;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// P0-1：Production 模式启动校验器。
/// <para>
/// 在 <see cref="PushProviderMode.Production"/> 模式下，启动时校验三个平台
/// （Fcm / Apns / WebPush）均注册了非 <c>Noop</c> 的 <see cref="IPushProvider"/>。
/// 发现任一平台仍为 Noop 时立即抛出 <see cref="InvalidOperationException"/>，
/// 阻止 Host 启动——避免 NoopPushProvider 静默吞掉真实推送。
/// </para>
/// <para>
/// 作为 <see cref="IHostedService"/> 注册：StartAsync 在 Host 启动阶段同步执行校验；
/// StopAsync 无副作用。校验失败时 Host 启动失败，进程退出码非零。
/// </para>
/// </summary>
internal sealed partial class PushProviderStartupValidator : IHostedService
{
    private readonly IEnumerable<IPushProvider> _providers;
    private readonly ILogger<PushProviderStartupValidator> _logger;

    public PushProviderStartupValidator(
        IEnumerable<IPushProvider> providers,
        ILogger<PushProviderStartupValidator> logger)
    {
        _providers = providers;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var requiredPlatforms = new[]
        {
            PushPlatform.Fcm,
            PushPlatform.Apns,
            PushPlatform.WebPush
        };

        var registeredByPlatform = _providers
            .GroupBy(p => p.Platform)
            .ToDictionary(g => g.Key, g => g.ToList());

        var missing = new List<PushPlatform>();
        var noop = new List<PushPlatform>();

        foreach (var platform in requiredPlatforms)
        {
            if (!registeredByPlatform.TryGetValue(platform, out var list) || list.Count == 0)
            {
                missing.Add(platform);
                continue;
            }

            // 任一平台只要存在非 Noop 注册即视为通过；全为 Noop 则失败。
            // NoopPushProvider 标记为 internal sealed，无法在此处直接类型判定，
            // 改用类型名匹配——NoopPushProvider 是约定的 Noop 标识。
            var hasReal = list.Any(p => !IsNoop(p));
            if (!hasReal)
                noop.Add(platform);
        }

        if (missing.Count > 0 || noop.Count > 0)
        {
            var missingStr = missing.Count > 0
                ? string.Join(", ", missing)
                : null;
            var noopStr = noop.Count > 0
                ? string.Join(", ", noop)
                : null;
            var detail = string.Join(
                "; ",
                new[] { missingStr, noopStr }.Where(s => s is not null));

            throw new InvalidOperationException(
                $"Push.ProviderMode=Production 但推送 Provider 配置不完整 ({detail})。" +
                "请在 Program.cs 注册真实 FcmPushProvider / ApnsPushProvider / WebPushProvider，" +
                "或改用 Push.ProviderMode=TestNoop 进行开发/测试。");
        }

        LogValidationPassed(_logger);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private static bool IsNoop(IPushProvider provider) =>
        provider.GetType().Name.Equals("NoopPushProvider", StringComparison.Ordinal);

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Push Provider startup validation passed: all three platforms registered with real providers.")]
    private static partial void LogValidationPassed(ILogger logger);
}
