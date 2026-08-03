using ChatApp.PushWorker.Providers.Apns;
using ChatApp.PushWorker.Providers.Fcm;
using ChatApp.PushWorker.Providers.WebPush;
using ChatApp.TcpGateway.Infrastructure.Push;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace ChatApp.PushWorker.Providers;

/// <summary>
/// 真实推送 Provider（FCM / APNs / WebPush）的 DI 注册扩展。
/// <para>
/// 仅在 Production 模式下调用（TestNoop 模式由 <c>AddPushServices</c> 自动注册 NoopPushProvider）。
/// 注册内容：
/// <list type="bullet">
/// <item><see cref="PushProviderOptions"/> 绑定（从 "Push:Providers" 节）。</item>
/// <item>三个 <c>HttpClient</c>（FCM / APNs / WebPush），HTTP/2，超时由配置控制。</item>
/// <item><see cref="FcmPushProvider"/> / <see cref="ApnsPushProvider"/> / <see cref="WebPushPushProvider"/>
///   各注册为 <see cref="IPushProvider"/>（PushDispatcher 按 Platform 分发）。</item>
/// </list>
/// </para>
/// </summary>
public static class PushProviderRegistrationExtensions
{
    /// <summary>
    /// 注册真实 FCM / APNs / WebPush Provider。须在 <c>AddPushServices</c> 之前调用，
    /// 以便 <c>PushProviderStartupValidator</c> 校验通过。
    /// </summary>
    public static IServiceCollection AddRealPushProviders(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var options = configuration
            .GetSection(PushProviderOptions.SectionName)
            .Get<PushProviderOptions>() ?? new PushProviderOptions();

        if (!options.IsValid())
        {
            throw new InvalidOperationException(
                "Push provider configuration is invalid. " +
                "Ensure Push:Providers:Fcm, Push:Providers:Apns, and Push:Providers:WebPush " +
                "sections are all correctly configured for Production mode.");
        }

        services.AddSingleton(options);

        // FCM HttpClient（OAuth2 + FCM API，HTTP/2）
        services.AddHttpClient<FcmPushProvider>((sp, client) =>
        {
            client.BaseAddress = new Uri(options.Fcm.ApiEndpoint);
            client.Timeout = options.HttpTimeout;
            client.DefaultRequestVersion = System.Net.HttpVersion.Version20;
            client.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact;
        });

        // APNs HttpClient（HTTP/2 必须）
        services.AddHttpClient<ApnsPushProvider>((sp, client) =>
        {
            client.BaseAddress = new Uri(options.Apns.ApiEndpoint);
            client.Timeout = options.HttpTimeout;
            client.DefaultRequestVersion = System.Net.HttpVersion.Version20;
            client.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact;
        });

        // WebPush HttpClient（各 push service endpoint 不同，不设 BaseAddress）
        services.AddHttpClient<WebPushPushProvider>((sp, client) =>
        {
            client.Timeout = options.HttpTimeout;
            client.DefaultRequestVersion = System.Net.HttpVersion.Version20;
            client.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact;
        });

        // 注册为 IPushProvider（PushDispatcher 按 Platform 分发）
        services.AddSingleton<IPushProvider>(sp =>
            sp.GetRequiredService<FcmPushProvider>());
        services.AddSingleton<IPushProvider>(sp =>
            sp.GetRequiredService<ApnsPushProvider>());
        services.AddSingleton<IPushProvider>(sp =>
            sp.GetRequiredService<WebPushPushProvider>());

        return services;
    }
}
