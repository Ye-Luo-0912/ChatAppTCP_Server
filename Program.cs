using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.DependencyInjection;
using ChatApp.TcpGateway.Gateway.Commands.Attachments;
using ChatApp.TcpGateway.Gateway.Commands.Calls;
using ChatApp.TcpGateway.Gateway.Commands.Conversations;
using ChatApp.TcpGateway.Gateway.Commands.Groups;
using Microsoft.Extensions.Options;
using ChatApp.Realtime.Integration;
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
using RealtimeConversationAudienceCache = ChatApp.TcpGateway.Gateway.Messaging.Realtime.ConversationAudienceCache;
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
// P1-2：会话受众缓存。经 IRealtimeMessageBus 查询成员 + audience_version，
// 供会话级广播事件（AudienceKind=Conversation）解析成员并校验 AudienceVersion。
builder.Services.AddSingleton<RealtimeConversationAudienceCache>();
builder.Services.AddGatewayInfrastructure();
builder.Services.AddChatAppRealtimeIntegration(
    realtimeIntegrationOptions);
// 命令处理器与分发器
builder.Services.AddSingleton<PushTokenCommandHandler>();
builder.Services.AddSingleton<ReactionCommandHandler>();
builder.Services.AddSingleton<OfflinePushTrigger>(sp => new OfflinePushTrigger(
    sp.GetRequiredService<IGlobalPresenceStore>(),
    (command, ct) => sp.GetRequiredService<IRealtimeMessageBus>()
        .PublishPushDeliveryAsync(command, ct),
    sp.GetRequiredService<IOptions<PushOptions>>(),
    sp.GetRequiredService<ILogger<OfflinePushTrigger>>()));
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
// 主线四：附件 / 关系后端已接入 RealtimeServices
// （FinalizeAttachmentUploadAsync / MutateRelationshipAsync / QueryRelationshipListAsync）。
builder.Services.AddSingleton<IAttachmentBackend, RealtimeAttachmentBackend>();
builder.Services.AddSingleton<IRelationshipBackend, RealtimeRelationshipBackend>();
builder.Services.AddSingleton<AttachmentCommandHandler>();
builder.Services.AddSingleton<RelationshipCommandHandler>();
// CALL-E2E-2：通话信令控制面（RealtimeCallBackend 经 IRealtimeMessageBus.SendCallCommandAsync 转发）。
builder.Services.AddSingleton<ICallBackend, RealtimeCallBackend>();
builder.Services.AddSingleton<CallCommandHandler>();
builder.Services.AddSingleton<CommandDispatcher>();
builder.Services.AddHostedService<RealtimeEventConsumerService>();

// 五-5：Push 消费已移出 TCP Gateway。PushDeliveryConsumerService / PushDispatcher / IPushProvider
// 仅在独立 PushWorker 进程注册（见 PushWorker/Program.cs）。
// Gateway 仅保留 PushTokenCommandHandler（依赖 IPushTokenStore，已在 AddGatewayInfrastructure 注册），
// 负责 Token 注册/注销协议命令。真实 FCM/APNs/WebPush 的 HTTP/2 连接、限流、重试不再与 10k TCP 连接竞争资源。
// PushOptions 仍在此验证配置（早失败），但不注册任何 Push 消费服务。

builder.Services.AddHostedService<EphemeralPresenceTypingConsumerService>();
builder.Services.AddHostedService<TcpGatewayService>();
builder.Services.AddHostedService<PresenceMaintenanceService>();

await builder.Build().RunAsync();
