using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.DependencyInjection;
using ChatApp.TcpGateway.Gateway.Commands.Attachments;
using ChatApp.TcpGateway.Gateway.Commands.Conversations;
using ChatApp.TcpGateway.Gateway.Commands.Groups;
using ChatApp.TcpGateway.Gateway.Commands.Messaging;
using ChatApp.TcpGateway.Gateway.Commands.Presence;
using ChatApp.TcpGateway.Gateway.Commands.Push;
using ChatApp.TcpGateway.Gateway.Commands.Queries;
using ChatApp.TcpGateway.Gateway.Commands.Reactions;
using ChatApp.TcpGateway.Gateway.Commands.Relationships;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Ephemeral;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Infrastructure.GroupIdempotency;
using ChatApp.TcpGateway.Infrastructure.Push;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using EphemeralPresenceTypingConsumerService = ChatApp.TcpGateway.Gateway.Messaging.EphemeralPresenceTypingConsumerService;
using PushDispatcher = ChatApp.TcpGateway.Infrastructure.Push.PushDispatcher;
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

// P0-1：Push 配置门控。默认 Enabled=false / ProviderMode=Disabled，
// 未显式启用时不注册 PushDeliveryConsumerService，推送命令留在 JetStream 等待 Push Worker。
// Production 模式启动校验三个平台均非 Noop，发现 Noop 立即启动失败。
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

builder.Services.AddGatewayObservability(
    builder.Configuration,
    realtimeIntegrationOptions.InstanceId);
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<UserSessionRegistry>();
builder.Services.AddSingleton<PresenceWatcherRegistry>();
builder.Services.AddSingleton<TypingFanoutCoordinator>();
builder.Services.AddSingleton<IGlobalPresenceStore, RedisGlobalPresenceStore>();
// Typing 授权失效桥接器：RelationshipListHandler 注入此单例（作为 ITypingAuthorizationInvalidator），
// TcpGatewayService 创建 TypingActorPipeline 后调用 SetInstance 注册真实实现。
builder.Services.AddSingleton<TypingAuthorizationInvalidatorAccessor>();
builder.Services.AddSingleton<ITypingAuthorizationInvalidator>(sp =>
    sp.GetRequiredService<TypingAuthorizationInvalidatorAccessor>());
builder.Services.AddSingleton<RealtimeEventDispatcher>();
builder.Services.AddGatewayInfrastructure();
builder.Services.AddChatAppRealtimeIntegration(
    realtimeIntegrationOptions);
// 命令处理器与分发器
builder.Services.AddSingleton<PushTokenCommandHandler>();
builder.Services.AddSingleton<ReactionCommandHandler>();
builder.Services.AddSingleton<MessagingCommandHandler>();
builder.Services.AddSingleton<HistoryQueryCommandHandler>();
builder.Services.AddSingleton<ConversationPrefsCommandHandler>();
builder.Services.AddSingleton<GroupRequestIdempotencyCache>();
// 群组命令幂等：L1（内存）+ L2（Redis）Composite。L2 可选——Redis 不可用时退化为仅 L1。
builder.Services.AddSingleton<IGroupIdempotencyStore>(static provider =>
{
    var l1 = provider.GetRequiredService<GroupRequestIdempotencyCache>();
    var metrics = provider.GetRequiredService<GatewayMetrics>();
    var l2 = provider.GetService<RedisGroupIdempotencyStore>();
    return new CompositeGroupIdempotencyStore(l1, l2, metrics);
});
builder.Services.AddSingleton<GroupCommandHandler>();
builder.Services.AddSingleton<TypingCommandHandler>();
builder.Services.AddSingleton<PresenceCommandHandler>();
// 主线四：附件与关系后端端口（当前 stub，待 sibling 仓库 IRealtimeMessageBus 接入后替换）。
builder.Services.AddSingleton<IAttachmentBackend, StubAttachmentBackend>();
builder.Services.AddSingleton<IRelationshipBackend, StubRelationshipBackend>();
builder.Services.AddSingleton<AttachmentCommandHandler>();
builder.Services.AddSingleton<RelationshipCommandHandler>();
builder.Services.AddSingleton<CommandDispatcher>();
builder.Services.AddHostedService<RealtimeEventConsumerService>();

// 主线一9：Push 注册抽取到 AddPushServices 扩展方法，Gateway 与独立 PushWorker 共用。
builder.Services.AddPushServices(builder.Configuration);

builder.Services.AddHostedService<EphemeralPresenceTypingConsumerService>();
builder.Services.AddHostedService<TcpGatewayService>();
builder.Services.AddHostedService<PresenceMaintenanceService>();

await builder.Build().RunAsync();
