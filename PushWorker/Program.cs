using ChatApp.PushWorker.Providers;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.DependencyInjection;
using ChatApp.TcpGateway.Infrastructure;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Infrastructure.Push;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// 主线一9：独立 Push Worker 进程。
// 从 JetStream 拉取 PushDeliveryCommand 并执行实际推送（令牌拉取 + Provider 调用 + 无效令牌注销）。
// 与 TcpGateway 解耦：Gateway 仅负责在线连接，离线推送由本进程独立消费。
var builder = Host.CreateApplicationBuilder(args);
builder.Configuration
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
    .AddJsonFile(
        $"appsettings.{builder.Environment.EnvironmentName}.json",
        optional: true,
        reloadOnChange: false)
    .AddEnvironmentVariables()
    .AddCommandLine(args);

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(options =>
{
    options.SingleLine = true;
    options.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fff zzz ";
});

builder.Services
    .AddOptions<RedisOptions>()
    .Bind(builder.Configuration.GetSection(RedisOptions.SectionName))
    .Validate(
        static options =>
            !string.IsNullOrWhiteSpace(options.ConnectionString) &&
            options.StartupTimeout > TimeSpan.Zero,
        "Redis configuration is invalid.")
    .ValidateOnStart();

builder.Services
    .AddOptions<PushOptions>()
    .Bind(builder.Configuration.GetSection(PushOptions.SectionName))
    .Validate(static options => options.IsValid(), "Push configuration is invalid.")
    .ValidateOnStart();

builder.Services.Configure<HostOptions>(options =>
{
    options.ShutdownTimeout = TimeSpan.FromSeconds(20);
    options.BackgroundServiceExceptionBehavior =
        BackgroundServiceExceptionBehavior.StopHost;
});

var realtimeIntegrationOptions = builder.Configuration
    .GetSection("RealtimeIntegration")
    .Get<RealtimeIntegrationOptions>()
    ?? new RealtimeIntegrationOptions();

// PushWorker 不引入完整 Gateway 可观测性栈（OpenTelemetry exporter 等），
// 仅注册 GatewayMetrics——PushDeliveryConsumerService / RedisGroupIdempotencyStore 依赖。
builder.Services.AddSingleton<TimeProvider>(TimeProvider.System);
builder.Services.AddSingleton<GatewayMetrics>();
builder.Services.AddGatewayInfrastructure();
builder.Services.AddChatAppRealtimeIntegration(realtimeIntegrationOptions);

// Production 模式：注册真实 FCM/APNs/WebPush Provider（须在 AddPushServices 之前，
// 以便 PushProviderStartupValidator 校验通过）。
// TestNoop 模式：由 AddPushServices 自动注册 NoopPushProvider。
var pushOptions = builder.Configuration
    .GetSection(PushOptions.SectionName)
    .Get<PushOptions>() ?? new PushOptions();
if (pushOptions.Enabled
    && pushOptions.ProviderMode == PushProviderMode.Production)
{
    builder.Services.AddRealPushProviders(builder.Configuration);
}

builder.Services.AddPushServices(builder.Configuration);

await builder.Build().RunAsync();
