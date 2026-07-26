using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.DependencyInjection;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure;
using ChatApp.TcpGateway.Infrastructure.Caching;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using EphemeralPresenceTypingConsumerService = ChatApp.TcpGateway.Gateway.Messaging.EphemeralPresenceTypingConsumerService;
using RealtimeEventConsumerService = ChatApp.TcpGateway.Gateway.Messaging.RealtimeEventConsumerService;
using RealtimeEventDispatcher = ChatApp.TcpGateway.Gateway.Messaging.RealtimeEventDispatcher;
using RedisGlobalPresenceStore = ChatApp.TcpGateway.Gateway.Networking.Sessions.RedisGlobalPresenceStore;
using TcpGatewayService = ChatApp.TcpGateway.Gateway.Networking.TcpGatewayService;

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
builder.Services.AddSingleton<UserSessionRegistry>();
builder.Services.AddSingleton<PresenceWatcherRegistry>();
builder.Services.AddSingleton<TypingFanoutCoordinator>();
builder.Services.AddSingleton<IGlobalPresenceStore, RedisGlobalPresenceStore>();
builder.Services.AddSingleton<RealtimeEventDispatcher>();
builder.Services.AddGatewayInfrastructure();
builder.Services.AddChatAppRealtimeIntegration(
    realtimeIntegrationOptions);
builder.Services.AddHostedService<RealtimeEventConsumerService>();
builder.Services.AddHostedService<EphemeralPresenceTypingConsumerService>();
builder.Services.AddHostedService<TcpGatewayService>();

await builder.Build().RunAsync();
