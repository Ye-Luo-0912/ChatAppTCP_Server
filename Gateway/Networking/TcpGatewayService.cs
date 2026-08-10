using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net.Sockets;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Core.Server;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Messaging;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Ephemeral;
using ChatApp.TcpGateway.Gateway.Networking.Executor;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Gateway.Networking.Transport;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using ChatApp.TcpGateway.Observability.Tracing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.TcpGateway.Gateway.Networking;

internal sealed partial class TcpGatewayService : BackgroundService
{
    private readonly TcpGatewayOptions _options;
    private readonly IRealtimeAuthenticator _authenticator;
    private readonly IPayloadCodec<AuthenticationRequest> _authenticationRequestCodec;
    private readonly IPayloadCodec<AuthenticationResponse> _authenticationResponseCodec;
    private readonly IPayloadCodec<MessageAcknowledgement> _messageAcknowledgementCodec;
    // TypingUpdate / PresenceChanged 编解码器由 DI 注入（JsonPayloadCodec 实现 IPayloadCodec），
    // 替代早期在构造函数内 new JsonPayloadCodec 的硬编码方式，确保 AOT/trim 友好且可被测试替身替换。
    private readonly IPayloadCodec<TypingUpdate> _typingUpdateCodec;
    private readonly IPayloadCodec<PresenceChanged> _presenceChangedCodec;
    private readonly IRealtimeMessageBus _messageBus;
    private readonly RealtimeIntegrationOptions _integrationOptions;
    private readonly IDeviceSessionLeaseStore _deviceSessionLeaseStore;
    private readonly IGlobalPresenceStore _globalPresence;
    private readonly GatewayMetrics _metrics;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<TcpGatewayService> _logger;
    private readonly ILogger<TcpClientSession> _sessionLogger;
    private readonly PipeOptions _pipeOptions;
    private readonly ConcurrentDictionary<uint, TcpClientSession> _sessions = new();
    private readonly long[] _sessionCloseCounts =
        new long[Enum.GetValues<SessionCloseReason>().Length];
    // 全局内存预算与过载保护（Listener 生命周期、Accept 循环、连接准入与 drain 已抽取至 TcpListenerHost）
    private readonly TcpListenerHost _listenerHost;
    private readonly GlobalOutboundBudget _globalOutboundBudget;
    private readonly GlobalInboundBudget _globalInboundBudget;
    private readonly UserSessionRegistry _userSessions;
    private readonly PresenceWatcherRegistry _presenceWatchers;
    private readonly TypingFanoutCoordinator _typingFanout;
    // Presence watcher 全局路由目录：分片模式下用于定向投递 Presence 事件。
    // 测试场景下可为 null，回退为 NullWatcherGatewayDirectory（广播模式）。
    private readonly IWatcherGatewayDirectory _watcherDirectory;
    // 协议握手、断线重连、Error/GoAway 帧所需依赖。
    // 通过 DI 注入；测试场景下可为 null，新功能路径会跳过。
    private readonly IServerIdentity? _serverIdentity;
    private readonly IResumeTokenStore? _resumeTokenStore;
    private readonly IPayloadCodec<ClientHello>? _clientHelloCodec;
    private readonly IPayloadCodec<ServerHello>? _serverHelloCodec;
    private readonly IPayloadCodec<GoAway>? _goAwayCodec;
    private readonly IPayloadCodec<ResumeResponse>? _resumeResponseCodec;
    private readonly IPayloadCodec<ProtocolErrorFrame>? _protocolErrorFrameCodec;
    // 已迁移到 PushTokenCommandHandler / ReactionCommandHandler 的命令由 dispatcher 接管。
    // 测试路径（直接构造 service 而不注入 dispatcher）下为 null，此时 Push/Reaction 命令
    // 走 default 分支以 ProtocolViolation 关闭，符合既有测试预期。
    private readonly CommandDispatcher? _commandDispatcher;

    // Session 生命周期协调器：登录注册、同设备替换、Resume 恢复、设备租约管理、
    // Presence 上下线广播与连接销毁清理。从本 service 抽取以消除 God Service 散落逻辑。
    // 在构造时内部创建并传入已注入的依赖，service 构造函数签名不变，既有测试无需修改。
    private readonly SessionLifecycleCoordinator _lifecycleCoordinator;

    // Typing 扇出宿主：pump + emission consumer + fanout 三件套。
    // 从本 service 抽取以消除 God Service 中散落的 typing 时间轮驱动逻辑。
    private readonly TypingFanoutHost _typingFanoutHost;

    // 每连接数据面运行时：Pipe reader/writer + 全局 SessionCommandExecutor 调度。
    // 从本 service 抽取以消除 God Service 中散落的 per-connection 数据路径。
    // 单例：所有连接共享同一实例，通过 RunAsync 的 session 参数区分连接。
    private readonly SessionRuntime _sessionRuntime;

    // 心跳扫描协调器：Redis 设备租约 TTL 刷新 + Redis Presence 刷新（分桶）。
    // V2：认证/空闲超时已迁移到全局 DeadlineWheel，本协调器不再执行超时扫描。
    // 从本 service 抽取以消除 God Service 中散落的后台扫描循环。
    private readonly HeartbeatCoordinator _heartbeatCoordinator;

    // 心跳分桶注册表：连接建立/认证/断开时维护，HeartbeatCoordinator 每 tick 只枚举一个桶。
    // 替代每 tick _sessions.Values.ToArray() 全量复制，10k 连接下每 tick 仅遍历 ~333 条目。
    private readonly HeartbeatBucketRegistry _heartbeatBuckets;

    // 全局 DeadlineWheel：替代每连接 Auth/Idle Timer。所有连接共享一个时间轮 + 单 PeriodicTimer。
    // 仅管理 Auth/Idle 超时（低频，符合其全局锁设计假设）。发送超时已迁移到 SendTimeoutTracker。
    private readonly DeadlineWheel _deadlineWheel;
    // 全局发送超时扫描器：替代每帧 DeadlineWheel.Register。周期扫描活跃发送方集合，
    // 不为每帧创建闭包、不竞争 DeadlineWheel 全局锁、不增长 _fired 集合。
    private readonly SendTimeoutTracker _sendTimeoutTracker;
    // P1-5：全局帧装配超时扫描器：替代 DeadlineWheel 管理高频 Header/Payload 装配超时。
    // 周期扫描活跃装配集合，不为每次装配创建闭包、不竞争 DeadlineWheel 全局锁。
    private readonly FrameAssemblyTimeoutTracker _frameAssemblyTracker;

    // 全局命令执行器：替代每连接 OrderedWrite/Query/Ephemeral Channel + Consumer Task。
    // 共享 worker 池，按 connectionId 串行保序，跨连接并行。
    private readonly SessionCommandExecutor _orderedWriteExecutor;
    private readonly SessionCommandExecutor _queryExecutor;
    private readonly EphemeralCommandPipeline _ephemeralPipeline;
    private readonly TypingActorPipeline? _typingActorPipeline;
    // Typing 授权失效桥接器：解耦 RelationshipListHandler（DI 创建）与 TypingActorPipeline
    // （本 service 内部创建）。Specialized 未启用时持有 null，调用为 no-op。
    private readonly TypingAuthorizationInvalidatorAccessor _typingInvalidatorAccessor;

    // OnDemandSendPump 模式专用：共享出站 pump 协调器（ready queue + worker 池）。
    // PersistentSendLoop/PerSessionDrain 模式下为 null。
    private readonly OutboundPumpCoordinator? _outboundPump;
    // PerSessionDrain 模式标志：构造时确定，传递给每个 TcpClientSession。
    private readonly bool _usePerSessionDrain;

    // 会话控制命令处理器：AuthenticationRequest + ClientHello 握手/鉴权流程。
    // 从本 service 抽取以消除 God Service 中散落的连接状态机逻辑。
    // 不走 CommandDispatcher：依赖 _listenerHost 准入回调与 _lifecycleCoordinator（内部创建）。
    private readonly SessionControlHandler _sessionControlHandler;

    public TcpGatewayService(
        IOptions<TcpGatewayOptions> options,
        IRealtimeAuthenticator authenticator,
        IPayloadCodec<AuthenticationRequest> authenticationRequestCodec,
        IPayloadCodec<AuthenticationResponse> authenticationResponseCodec,
        IPayloadCodec<MessageAcknowledgement> messageAcknowledgementCodec,
        IPayloadCodec<TypingUpdate> typingUpdateCodec,
        IPayloadCodec<PresenceChanged> presenceChangedCodec,
        IRealtimeMessageBus messageBus,
        RealtimeIntegrationOptions integrationOptions,
        IDeviceSessionLeaseStore deviceSessionLeaseStore,
        IGlobalPresenceStore globalPresence,
        UserSessionRegistry userSessions,
        PresenceWatcherRegistry presenceWatchers,
        TypingFanoutCoordinator typingFanout,
        GatewayMetrics metrics,
        TimeProvider timeProvider,
        ILogger<TcpGatewayService> logger,
        ILogger<TcpClientSession> sessionLogger,
        IServerIdentity? serverIdentity = null,
        IResumeTokenStore? resumeTokenStore = null,
        IPayloadCodec<ClientHello>? clientHelloCodec = null,
        IPayloadCodec<ServerHello>? serverHelloCodec = null,
        IPayloadCodec<GoAway>? goAwayCodec = null,
        IPayloadCodec<ResumeResponse>? resumeResponseCodec = null,
        IPayloadCodec<ProtocolErrorFrame>? protocolErrorFrameCodec = null,
        IWatcherGatewayDirectory? watcherDirectory = null,
        CommandDispatcher? commandDispatcher = null,
        IPayloadCodec<TypingNotify>? typingNotifyCodec = null,
        IDirectConversationAuthorizer? directConversationAuthorizer = null,
        IRedisCircuitBreaker? circuitBreaker = null,
        TypingAuthorizationInvalidatorAccessor? typingInvalidatorAccessor = null,
        IFrozenUserCache? frozenUserCache = null)
    {
        _options = options.Value;
        _authenticator = authenticator;
        _authenticationRequestCodec = authenticationRequestCodec;
        _authenticationResponseCodec = authenticationResponseCodec;
        _messageAcknowledgementCodec = messageAcknowledgementCodec;
        _typingUpdateCodec = typingUpdateCodec;
        _presenceChangedCodec = presenceChangedCodec;
        _messageBus = messageBus;
        _integrationOptions = integrationOptions;
        _deviceSessionLeaseStore = deviceSessionLeaseStore;
        _globalPresence = globalPresence;
        _userSessions = userSessions;
        _presenceWatchers = presenceWatchers;
        _typingFanout = typingFanout;
        _metrics = metrics;
        _timeProvider = timeProvider;
        _logger = logger;
        _sessionLogger = sessionLogger;
        _serverIdentity = serverIdentity;
        _resumeTokenStore = resumeTokenStore;
        _clientHelloCodec = clientHelloCodec;
        _serverHelloCodec = serverHelloCodec;
        _goAwayCodec = goAwayCodec;
        _resumeResponseCodec = resumeResponseCodec;
        _protocolErrorFrameCodec = protocolErrorFrameCodec;
        _watcherDirectory = watcherDirectory ?? NullWatcherGatewayDirectory.Instance;
        _commandDispatcher = commandDispatcher;

        _pipeOptions = new PipeOptions(
            pool: MemoryPool<byte>.Shared,
            pauseWriterThreshold: _options.PipePauseWriterThreshold,
            resumeWriterThreshold: _options.PipeResumeWriterThreshold,
            minimumSegmentSize: _options.ReceiveBufferSize,
            readerScheduler: PipeScheduler.ThreadPool,
            writerScheduler: PipeScheduler.ThreadPool,
            useSynchronizationContext: false);

        // 初始化全局内存预算与过载保护
        _globalOutboundBudget = new GlobalOutboundBudget(
            _options.GlobalMaxOutboundQueuedBytes);
        _globalInboundBudget = new GlobalInboundBudget(
            _options.GlobalMaxInboundBufferedBytes);

        // 注册预算与进程级资源 ObservableGauge。
        // committed/max 揭示预算使用比例；working_set/avg_per_session/unaccounted 揭示
        // GlobalInboundBudget 未跟踪的"隐藏"内存（Pipe 未计入 segment、池化余量、对象开销），
        // 避免将 committed 误用为物理内存硬上限。
        // ObservableGauge 的委托由 Meter 在 scrape 时回调，须捕获稳定引用。
        _metrics.RegisterInboundBudgetObservers(
            () => _globalInboundBudget.CurrentBytes,
            () => _globalInboundBudget.MaxBytes);
        _metrics.RegisterOutboundBudgetObservers(
            () => _globalOutboundBudget.CurrentBytes,
            () => _globalOutboundBudget.MaxBytes);
        _metrics.RegisterResourceObservers(
            () => Environment.WorkingSet,
            () => _sessions.Count,
            () => _globalInboundBudget.CurrentBytes);

        // Listener 生命周期、Accept 循环、连接准入与 drain 由 TcpListenerHost 持有。
        // 通过 OnConnectionAccepted 回调将已准入的 (connectionId, socket, remoteIp) 交给本服务创建 session。
        _listenerHost = new TcpListenerHost(
            _options,
            _metrics,
            _logger,
            _goAwayCodec,
            () => _sessions.Values,
            OnConnectionAccepted);

        // Session 生命周期协调器：内部创建并复用已注入依赖，避免 service 构造函数签名变化。
        _lifecycleCoordinator = new SessionLifecycleCoordinator(
            _deviceSessionLeaseStore,
            _globalPresence,
            _resumeTokenStore,
            _userSessions,
            _presenceWatchers,
            _messageBus,
            _integrationOptions,
            _options,
            _metrics,
            _timeProvider,
            _logger,
            _presenceChangedCodec,
            circuitBreaker,
            frozenUserCache);

        // Typing 扇出宿主：内部创建并复用已注入依赖。
        _typingFanoutHost = new TypingFanoutHost(
            _options,
            _typingFanout,
            _userSessions,
            _messageBus,
            _integrationOptions,
            _metrics,
            _timeProvider,
            _logger,
            _typingUpdateCodec);

        // 全局 DeadlineWheel：替代每连接 Auth/Idle Timer。
        // 单 PeriodicTimer + 分桶时间轮，所有连接共享。单调时钟避免墙钟回拨死锁。
        // 仅管理 Auth/Idle 超时（低频）。发送超时由 SendTimeoutTracker 独立扫描管理。
        _deadlineWheel = new DeadlineWheel(_timeProvider);
        // 全局发送超时扫描器：替代每帧 DeadlineWheel.Register（消除闭包分配 + 全局锁竞争）。
        _sendTimeoutTracker = new SendTimeoutTracker(_timeProvider);
        // P1-5：全局帧装配超时扫描器：替代 DeadlineWheel 管理高频 Header/Payload 装配超时。
        _frameAssemblyTracker = new FrameAssemblyTimeoutTracker(_timeProvider);

        // 全局命令执行器：OrderedWrite lane。
        // 共享 worker 池，按 connectionId 串行保序，跨连接并行。Burst 限制防止单连接独占 worker。
        // perUserConcurrency=0：写操作不按用户限流。
        _orderedWriteExecutor = new SessionCommandExecutor(
            (command, token) => ProcessScheduledCommandAsync(
                command, token),
            workerCount: Math.Max(2, Environment.ProcessorCount),
            burstLimit: 8,
            perConnectionCapacity: _options.CommandSchedulerOrderedWriteCapacity,
            globalCapacity: Math.Max(1024, _options.CommandSchedulerOrderedWriteCapacity * 256),
            commandTimeout: TimeSpan.Zero,
            perUserConcurrency: 0,
            onFatalError: null,
            logger: _logger);

        // 全局命令执行器：Query lane。
        // 同连接串行（与 OrderedWrite 独立），可叠加每用户并发上限与查询超时。
        // 当前配置与 OrderedWrite 相同；如需更细策略可独立调整 workerCount/timeout/perUserConcurrency。
        _queryExecutor = new SessionCommandExecutor(
            (command, token) => ProcessScheduledCommandAsync(
                command, token),
            workerCount: Math.Max(2, Environment.ProcessorCount),
            burstLimit: 8,
            perConnectionCapacity: _options.CommandSchedulerQueryCapacity,
            globalCapacity: Math.Max(1024, _options.CommandSchedulerQueryCapacity * 256),
            commandTimeout: TimeSpan.Zero,
            perUserConcurrency: 0,
            onFatalError: null,
            logger: _logger);

        // Ephemeral lane 可切换到轻量 ActorRuntime；旧 SessionCommandExecutor 保留为 A/B 回退。
        // 当 Specialized TypingActor 启用时，TypingNotify 是唯一 Ephemeral C2S 命令，
        // 会被快路径截获而不进入 _ephemeralPipeline。为避免两套 Ephemeral Runtime 并存
        // （浪费 worker 池、重复注册指标），将 Generic EphemeralPipeline 设为 Disabled：
        // 不创建 Legacy Worker、不创建 Generic Actor、不为连接创建 Ephemeral ConnectionQueue。
        // Register/Unregister 为真正 no-op，Start/Stop 为 no-op。
        // 同时要求 EnableEphemeralPresenceAndTyping=true：功能关闭时 Typing 命令应走
        // FeatureNotNegotiated 路径，而非被 Specialized Pipeline 截获。
        var specializedTypingEnabled =
            _options.UseTypingActorPipeline &&
            _options.UseActorRuntimeForEphemeralCommands &&
            _options.EnableEphemeralPresenceAndTyping &&
            typingNotifyCodec is not null;

        // Specialized Typing 启用时强制 Disabled；否则按 options 推导（Legacy/GenericActor）。
        var ephemeralMode = specializedTypingEnabled
            ? EphemeralPipelineMode.Disabled
            : _options.ResolveEphemeralPipelineMode();

        _ephemeralPipeline = new EphemeralCommandPipeline(
            _options,
            ephemeralMode,
            ProcessScheduledCommandAsync,
            _metrics,
            _timeProvider,
            _logger);

        // Typing 领域 Actor：TCP Read 直接解析并路由到 LatestOnly Actor，
        // 不创建通用 SessionCommand。仅在 UseTypingActorPipeline=true 且注入 codec 时启用。
        // 复用 EphemeralActor 配置（Shard/Ingress/Async/IdleTimeout/OperationTimeout）。
        _typingInvalidatorAccessor = typingInvalidatorAccessor
            ?? new TypingAuthorizationInvalidatorAccessor();
        if (specializedTypingEnabled)
        {
            _typingActorPipeline = new TypingActorPipeline(
                _options,
                typingNotifyCodec!,
                directConversationAuthorizer,
                _typingFanout,
                _metrics,
                _timeProvider,
                _logger);
            // 注册到桥接器，使 RelationshipListHandler 能在关系变更时失效 Actor 内授权缓存。
            _typingInvalidatorAccessor.SetInstance(_typingActorPipeline);
        }

        // OnDemandSendPump 模式：创建共享出站 pump 协调器。
        // PersistentSendLoop 模式（默认）保持 null，每连接保留永久 SendLoop Task。
        // PerSessionDrain 模式也保持 null，每连接按需启动自有 drain Task（无共享 worker 池）。
        // ready queue 容量 ≥ MaxConnections，保证每连接至多一份引用时不会阻塞 TrySchedule。
        // worker 数在 ExecuteAsync 中按 OnDemandSendWorkerCount 推导并传入 StartAsync。
        if (_options.OutboundSendMode == Configuration.OutboundSendMode.OnDemandSendPump)
        {
            _outboundPump = new OutboundPumpCoordinator(
                burstLimit: _options.OnDemandSendBurstLimit,
                readyQueueCapacity: Math.Max(_options.MaxConnections, 1024),
                logger: _logger);
        }
        var usePerSessionDrain =
            _options.OutboundSendMode == Configuration.OutboundSendMode.PerSessionDrain;
        _usePerSessionDrain = usePerSessionDrain;

        // 注册 Runtime V2 共享执行器 ObservableGauge。
        // DeadlineWheel 始终存在；OutboundPumpCoordinator 仅 OnDemandSendPump 模式下非 null，
        // PersistentSendLoop 模式下出站相关 provider 传 null，对应指标不注册。
        _metrics.RegisterRuntimeV2Observers(
            activeDeadlinesProvider: () => _deadlineWheel.ActiveDeadlineCount,
            sendTimeoutActiveSendersProvider: () => _sendTimeoutTracker.ActiveSenderCount,
            frameAssemblyActiveProvider: () => _frameAssemblyTracker.ActiveAssemblyCount,
            outboundPumpReadyQueueProvider: _outboundPump is null ? null : () => _outboundPump.ReadyQueueCount,
            outboundPumpTotalScheduledProvider: _outboundPump is null ? null : () => _outboundPump.TotalScheduled,
            outboundPumpWorkerCountProvider: _outboundPump is null ? null : () => _outboundPump.WorkerCount);

        // Actor Runtime 指标注册：优先 Specialized TypingActor，其次 Generic Ephemeral Pipeline。
        // 二者不会同时启用（Specialized 启用时 Generic 的 ActorRuntime 已被关闭）。
        if (_typingActorPipeline is not null)
        {
            var typingSnapshot = () => _typingActorPipeline.Snapshot;
            // 通用 gateway.actor.* 指标仍注册（基线观测），但 Specialized 启用时数据来自 Typing Actor。
            _metrics.RegisterActorRuntimeObservers(
                activeActorsProvider: () => typingSnapshot().ActiveActors,
                busyActorsProvider: () => typingSnapshot().BusyActors,
                pendingIngressProvider: () => typingSnapshot().PendingIngress,
                pendingMailboxProvider: () => typingSnapshot().PendingMailbox,
                pendingAsyncProvider: () => typingSnapshot().PendingAsyncOperations,
                totalProcessedProvider: () => typingSnapshot().TotalProcessed);

            // Specialized 专属指标：typing_actor.* 覆盖 replaced/admission/async 等领域维度，
            // typing_auth.* 覆盖授权 I/O DomainWorkLane 状态与耗时。避免开启 Specialized 后
            // 仪表盘仍只显示 Generic Actor 空闲数据。
            _metrics.RegisterTypingActorRuntimeObservers(
                activeActorsProvider: () => typingSnapshot().ActiveActors,
                busyActorsProvider: () => typingSnapshot().BusyActors,
                ingressPendingProvider: () => typingSnapshot().PendingIngress,
                replacedProvider: () => typingSnapshot().TotalReplaced,
                admissionRejectedProvider: () => typingSnapshot().TotalActiveActorAdmissionRejected,
                pendingDeadlinesProvider: () => typingSnapshot().PendingDeadlines,
                activationsProvider: () => typingSnapshot().TotalActivations,
                deactivationsProvider: () => typingSnapshot().TotalDeactivations,
                mailboxFullProvider: () => typingSnapshot().TotalMailboxFull,
                shardOverloadedProvider: () => typingSnapshot().TotalShardOverloaded,
                asyncSubmittedProvider: () => typingSnapshot().TotalAsyncOperationsSubmitted,
                asyncCompletedProvider: () => typingSnapshot().TotalAsyncOperationsCompleted,
                asyncRejectedProvider: () => typingSnapshot().TotalAsyncOperationsRejected);

            var authSnapshot = () => _typingActorPipeline.AuthLaneSnapshot;
            _metrics.RegisterTypingAuthLaneObservers(
                queuedProvider: () => authSnapshot().QueuedCount,
                inflightProvider: () => authSnapshot().InflightCount,
                rejectedProvider: () => authSnapshot().TotalRejected,
                timeoutProvider: () => authSnapshot().TotalTimeout);
        }
        else if (_ephemeralPipeline.UsesActorRuntime)
        {
            _metrics.RegisterActorRuntimeObservers(
                activeActorsProvider: () => _ephemeralPipeline.Snapshot.ActiveActors,
                busyActorsProvider: () => _ephemeralPipeline.Snapshot.BusyActors,
                pendingIngressProvider: () => _ephemeralPipeline.Snapshot.PendingIngress,
                pendingMailboxProvider: () => _ephemeralPipeline.Snapshot.PendingMailbox,
                pendingAsyncProvider: () => _ephemeralPipeline.Snapshot.PendingAsyncOperations,
                totalProcessedProvider: () => _ephemeralPipeline.Snapshot.TotalProcessed);
        }

        // 每连接数据面运行时：内部创建并注入协议级回调。
        // ProcessPacketAsync / SendProtocolError / RejectOversizedPayload 仍由本 service 持有，
        // 因为它们依赖大量协议 codec 与 _commandDispatcher / _lifecycleCoordinator。
        _sessionRuntime = new SessionRuntime(
            _options,
            _pipeOptions,
            _globalInboundBudget,
            _orderedWriteExecutor,
            _queryExecutor,
            _ephemeralPipeline,
            _typingActorPipeline,
            _metrics,
            _timeProvider,
            _logger,
            _deadlineWheel,
            _frameAssemblyTracker,
            ProcessPacketAsync,
            SendProtocolError,
            RejectOversizedPayload);

        // 心跳分桶注册表：替代每 tick 全量 _sessions.Values.ToArray()。
        // 连接建立 → RegisterConnection；认证成功 → RegisterUser；断开 → Unregister。
        var bucketCount = Math.Max(1, _options.HeartbeatBucketCount);
        _heartbeatBuckets = new HeartbeatBucketRegistry(bucketCount);

        // 心跳扫描协调器：内部创建并复用已注入依赖。
        _heartbeatCoordinator = new HeartbeatCoordinator(
            _options,
            _timeProvider,
            _listenerHost,
            _heartbeatBuckets,
            _lifecycleCoordinator,
            _metrics,
            _logger);

        // 八.4：注册心跳队列 ObservableGauge——queue.depth 与 queue.oldest_age。
        // 委托捕获 _heartbeatCoordinator 引用，避免 GC 回收（与 RegisterInboundBudgetObservers 同模式）。
        _metrics.RegisterHeartbeatQueueObservers(
            () => _heartbeatCoordinator.CurrentQueueDepth,
            () => _heartbeatCoordinator.CurrentOldestQueueAgeMs);

        // 会话控制命令处理器：内部创建，注入握手/鉴权所需 codec 与准入/生命周期依赖。
        _sessionControlHandler = new SessionControlHandler(
            _options,
            _authenticator,
            _authenticationRequestCodec,
            _authenticationResponseCodec,
            _clientHelloCodec,
            _serverHelloCodec,
            _resumeResponseCodec,
            _protocolErrorFrameCodec,
            _serverIdentity,
            _listenerHost,
            _lifecycleCoordinator,
            _metrics,
            _logger);
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await base.StartAsync(cancellationToken).ConfigureAwait(false);
        await _listenerHost.ListenerReady
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // 委托 TcpListenerHost 完成 drain 协调：设置 _isDraining、取消 Accept、
        // 关闭 listener、广播 GoAway 并等待 drain 超时。
        await _listenerHost.StopAsync(cancellationToken)
            .ConfigureAwait(false);
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var execution = CancellationTokenSource.CreateLinkedTokenSource(
            stoppingToken);
        var executionToken = execution.Token;

        // 启动全局执行器（OrderedWrite/Query/Ephemeral 共享 worker 池）与 DeadlineWheel。
        // 在心跳与 listener 启动前就绪，避免首个连接到达时执行器未启动。
        await _orderedWriteExecutor.StartAsync(executionToken)
            .ConfigureAwait(false);
        await _queryExecutor.StartAsync(executionToken)
            .ConfigureAwait(false);
        await _ephemeralPipeline.StartAsync(executionToken)
            .ConfigureAwait(false);

        // Specialized Typing Actor Runtime：在 EphemeralCommandPipeline 之后启动，
        // 确保授权 DomainWorkLane 与 Actor Consumer 就绪后再开始接受 TypingNotify。
        // 启动顺序：Typing Authorization Lane → Typing Actor Runtime → Session Listener。
        if (_typingActorPipeline is not null)
        {
            await _typingActorPipeline.StartAsync(executionToken)
                .ConfigureAwait(false);
        }

        var deadlineWheelTask = _deadlineWheel.StartAsync(executionToken);

        // SendTimeoutTracker：启动定时扫描线程，检测卡死的 Socket Send。
        // 必须在 Listener 启动前启动，确保所有 Session 注册的发送超时能被检测。
        await _sendTimeoutTracker.StartAsync(executionToken)
            .ConfigureAwait(false);

        // P1-5：FrameAssemblyTimeoutTracker：启动定时扫描线程，检测慢速帧装配。
        // 必须在 Listener 启动前启动，确保所有 Session 注册的装配超时能被检测。
        await _frameAssemblyTracker.StartAsync(executionToken)
            .ConfigureAwait(false);

        // OnDemandSendPump 模式：启动共享出站 worker 池。
        // PersistentSendLoop 模式下 _outboundPump=null，跳过。
        if (_outboundPump is not null)
        {
            var pumpWorkerCount = _options.OnDemandSendWorkerCount > 0
                ? _options.OnDemandSendWorkerCount
                : Math.Max(2, Environment.ProcessorCount);
            await _outboundPump.StartAsync(pumpWorkerCount, executionToken)
                .ConfigureAwait(false);
        }

        // 心跳扫描（超时关闭 + 租约刷新 + Presence 刷新）委托 HeartbeatCoordinator 驱动。
        var heartbeatTask = _heartbeatCoordinator.RunAsync(executionToken);
        // Typing 时间轮 pump 与发射消费委托 TypingFanoutHost 驱动。
        var typingFanoutTask = _typingFanoutHost.RunAsync(executionToken);

        try
        {
            // Bind/Listen/Accept 循环与 drain 等待均委托 TcpListenerHost。
            // Accept 成功且准入通过后通过 OnConnectionAccepted 回调创建 session 并启动 HandleClientAsync。
            await _listenerHost.RunAsync(executionToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (executionToken.IsCancellationRequested)
        {
            // Normal host shutdown.
        }
        catch (Exception exception)
        {
            Environment.ExitCode = 1;
            _logger.GatewayFatal(exception);
            throw;
        }
        finally
        {
            await execution.CancelAsync();

            foreach (var session in _sessions.Values)
            {
                session.Close(SessionCloseReason.ApplicationStopping);
            }

            await _listenerHost.WaitForClientTasksAsync()
                .ConfigureAwait(false);

            _logger.SessionCloseSummary(BuildSessionCloseSummary());

            await heartbeatTask.ConfigureAwait(false);
            await typingFanoutTask.ConfigureAwait(false);

            // SendTimeoutTracker：所有 Session 已关闭后停止扫描。
            // 须在执行器停止前停止，避免扫描已释放的执行器资源。
            await _sendTimeoutTracker.StopAsync()
                .ConfigureAwait(false);

            // P1-5：FrameAssemblyTimeoutTracker：所有 Session 已关闭后停止扫描。
            await _frameAssemblyTracker.StopAsync()
                .ConfigureAwait(false);

            // 停止执行器：取消 worker 循环并排空残留命令（释放缓冲区与入站预算）。
            await _orderedWriteExecutor.StopAsync(CancellationToken.None)
                .ConfigureAwait(false);
            await _queryExecutor.StopAsync(CancellationToken.None)
                .ConfigureAwait(false);
            await _ephemeralPipeline.StopAsync(CancellationToken.None)
                .ConfigureAwait(false);
            // Typing 领域 Actor：在 EphemeralCommandPipeline 之后停止，确保 fanout 依赖已就绪。
            if (_typingActorPipeline is not null)
            {
                await _typingActorPipeline.StopAsync(CancellationToken.None)
                    .ConfigureAwait(false);
            }

            // OnDemandSendPump：停止共享出站 worker 池。
            // 须在所有 session.Close 后调用，确保 in-flight pump 已退出。
            if (_outboundPump is not null)
            {
                await _outboundPump.StopAsync(CancellationToken.None)
                    .ConfigureAwait(false);
            }

            // DeadlineWheel：等待驱动循环退出。
            try
            {
                await deadlineWheelTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Normal shutdown.
            }

            _logger.GatewayStopped();
        }
    }

    public override void Dispose()
    {
        _listenerHost.Dispose();
        // 全局执行器与 DeadlineWheel 是单例服务级资源，停机后释放。
        // StopAsync 已在 ExecuteAsync finally 中等待 worker 退出，此处仅释放 CTS 等托管资源。
        _orderedWriteExecutor.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _queryExecutor.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _ephemeralPipeline.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _typingActorPipeline?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _deadlineWheel.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _sendTimeoutTracker.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _frameAssemblyTracker.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _outboundPump?.Dispose();
        base.Dispose();
    }

    /// <summary>
    /// TcpListenerHost 回调：在 Accept 成功且通过连接准入后调用，负责创建 session
    /// 并启动 <see cref="HandleClientAsync"/>。返回的 Task 由 Host 注册到 _clientTasks 用于停机 WhenAll。
    /// 返回 null 表示 session 创建失败（如 connectionId 冲突），Host 会回滚准入与槽位。
    /// </summary>
    private async ValueTask<Task?> OnConnectionAccepted(
        uint connectionId,
        Socket socket,
        string remoteIp,
        CancellationToken stoppingToken)
    {
        TcpClientSession? session = null;
        var registrations = default(SessionCommandRegistrationSet);
        var registrationsOwned = false;
        var heartbeatRegistered = false;
        try
        {
            socket.NoDelay = true;
            socket.SetSocketOption(
                SocketOptionLevel.Socket,
                SocketOptionName.KeepAlive,
                optionValue: true);

            session = new TcpClientSession(
                socket,
                connectionId,
                _options.OutboundQueueCapacity,
                _options.MaxOutboundQueuedBytes,
                _options.SendTimeout,
                _timeProvider,
                _metrics,
                _sessionLogger,
                _globalOutboundBudget,
                _options.AuthenticationTimeout,
                deadlineWheel: _deadlineWheel,
                idleTimeout: _options.IdleTimeout,
                outboundPump: _outboundPump,
                sendTimeoutTracker: _sendTimeoutTracker,
                frameAssemblyTracker: _frameAssemblyTracker,
                usePerSessionDrain: _usePerSessionDrain,
                outboundQueueMode: _options.OutboundQueueMode);

            // 三条 lane 必须全部取得本 session 的 opaque lease 后才能暴露连接。
            // 任一 lane 冲突时 helper 只回滚本次已成功的租约，不会按裸 ID 删除旧连接。
            if (!SessionCommandRegistrationSet.TryRegister(
                    connectionId,
                    session.UserId,
                    _orderedWriteExecutor,
                    _queryExecutor,
                    _ephemeralPipeline,
                    out registrations))
            {
                session.Close(SessionCloseReason.TransportError);
                await session.DisposeAsync().ConfigureAwait(false);
                return null;
            }
            registrationsOwned = true;

            // 注册到心跳分桶注册表（连接桶，按 connectionId 分桶用于租约刷新）。
            // 用户桶在认证成功后由 ProcessPacketAsync 的 auth 转换钩子注册。
            _heartbeatBuckets.RegisterConnection(session);
            heartbeatRegistered = true;

            if (!_sessions.TryAdd(connectionId, session))
            {
                // 注册表冲突（极罕见的 connectionId 碰撞）：注销执行器连接与心跳桶，
                // 丢弃队列中残留命令并释放缓冲区与入站预算。
                registrations.Unregister();
                registrationsOwned = false;
                _heartbeatBuckets.Unregister(session);
                heartbeatRegistered = false;
                session.Close(SessionCloseReason.TransportError);
                await session.DisposeAsync().ConfigureAwait(false);
                return null;
            }

            _metrics.ConnectionAccepted();

            var clientTask = HandleClientAsync(
                session,
                remoteIp,
                registrations,
                stoppingToken);
            registrationsOwned = false;
            return clientTask;
        }
        catch
        {
            if (registrationsOwned)
                registrations.Unregister();
            if (heartbeatRegistered && session is not null)
                _heartbeatBuckets.Unregister(session);

            if (session is not null)
            {
                session.Close(SessionCloseReason.TransportError);
                try
                {
                    await session.DisposeAsync().ConfigureAwait(false);
                }
                catch
                {
                    // Preserve the original connection-accept exception.
                }
            }
            else
            {
                socket.Dispose();
            }
            throw;
        }
    }

    private async Task HandleClientAsync(
        TcpClientSession session,
        string remoteIp,
        SessionCommandRegistrationSet registrations,
        CancellationToken cancellationToken)
    {
        // 每连接数据面（Pipe + 调度器 + fill/read 双任务）委托 SessionRuntime。
        // 数据面清理（pipeLease/scheduler/session.DisposeAsync）由 SessionRuntime 内部 finally 完成。
        // 服务级清理（Presence 下线、admission 释放、session 注册表移除）由本方法 finally 处理。
        try
        {
            await _sessionRuntime
                .RunAsync(
                    session,
                    remoteIp,
                    registrations,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            // 嵌套 try/finally：外部生命周期清理（Presence 下线、watcher 移除、设备租约释放）
            // 是 best-effort；本地资源记账（admission/session 注册表/连接槽位）必须不可被
            // Redis、NATS 或业务清理异常阻断。
            try
            {
                await _lifecycleCoordinator.OnDisconnectedAsync(session, CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // 静默吞掉：CancellationToken.None 不会触发取消，保留防御性处理。
            }
            catch (Exception exception)
            {
                _logger.LifecycleCleanupFailed(session.ConnectionId, exception);
            }
            finally
            {
                // 释放准入跟踪器槽位（已迁移至 TcpListenerHost）。
                // P0-4 / 主线二子项2：使用 AdmissionState 三态 CAS 而非 UserId>0——
                // Resume Commit 失败时 UserId 已设置但 AdmissionState 仍为 Unauthenticated，
                // 未认证计数未被递减，需在此递减否则泄漏槽位。
                // TryReleaseAdmission CAS Promoted→Released 返回 true 表示首次释放（防止重复递减）。
                var wasAuthenticated = session.AdmissionPromoted;
                if (wasAuthenticated)
                    session.TryReleaseAdmission();
                _listenerHost.ReleaseAdmission(remoteIp, wasAuthenticated);
                if (!wasAuthenticated)
                    _metrics.UnauthenticatedConnectionClosed();

                if (_sessions.TryRemove(
                        new KeyValuePair<uint, TcpClientSession>(
                            session.ConnectionId,
                            session)))
                {
                    var closeReason = session.CloseReason;
                    Interlocked.Increment(
                        ref _sessionCloseCounts[(int)closeReason]);
                    // CloseReason is a fixed enum, so this remains a bounded
                    // cardinality diagnostic and exposes timeout/transport causes
                    // without logging per-connection identifiers.
                    _metrics.ConnectionClosed(closeReason.ToString());
                    _listenerHost.ReleaseConnectionSlot();
                }

                // 注销执行器连接：丢弃队列中残留命令并释放缓冲区与入站预算。
                // 必须在 session.DisposeAsync 之后执行，确保 in-flight 命令已完成或取消。
                registrations.Unregister();

                // 注销心跳分桶注册表（连接桶 + 用户桶引用计数递减）。
                _heartbeatBuckets.Unregister(session);
            }
        }
    }

    private string BuildSessionCloseSummary()
    {
        var reasons = Enum.GetValues<SessionCloseReason>();
        return string.Join(
            ", ",
            reasons.Select(
                reason => $"{reason}={Volatile.Read(ref _sessionCloseCounts[(int)reason])}"));
    }
}
