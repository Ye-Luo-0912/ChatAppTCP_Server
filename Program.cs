using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.DependencyInjection;
using ChatApp.TcpGateway.Configuration;
using ChatApp.TcpGateway.Diagnostics;
using ChatApp.TcpGateway.Infrastructure;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Messaging;
using ChatApp.TcpGateway.Networking;
using ChatApp.TcpGateway.Networking.Sessions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

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
    .AddOptions<TcpGatewayOptions>()
    .Bind(builder.Configuration.GetSection(TcpGatewayOptions.SectionName))
    .Validate(
        static options => options.IsValid(),
        "TcpGateway configuration is invalid.")
    .ValidateOnStart();

builder.Services
    .AddOptions<RedisOptions>()
    .Bind(builder.Configuration.GetSection(RedisOptions.SectionName))
    .Validate(
        static options =>
            !string.IsNullOrWhiteSpace(options.ConnectionString) &&
            options.StartupTimeout > TimeSpan.Zero,
        "Redis configuration is invalid.")
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

builder.Services.AddGatewayObservability(
    builder.Configuration,
    realtimeIntegrationOptions.InstanceId);
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<GatewayMetrics>();
builder.Services.AddSingleton<UserSessionRegistry>();
builder.Services.AddSingleton<RealtimeEventDispatcher>();
builder.Services.AddGatewayInfrastructure();
builder.Services.AddChatAppRealtimeIntegration(
    realtimeIntegrationOptions);
builder.Services.AddHostedService<RealtimeEventConsumerService>();
builder.Services.AddHostedService<TcpGatewayService>();

await builder.Build().RunAsync();
