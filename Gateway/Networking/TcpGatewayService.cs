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
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Gateway.Networking.Transport;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using ChatApp.TcpGateway.Observability.Tracing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.TcpGateway.Gateway.Networking;

internal sealed class TcpGatewayService : BackgroundService
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

    // 每连接数据面运行时：Pipe reader/writer + SessionCommandScheduler 三件套。
    // 从本 service 抽取以消除 God Service 中散落的 per-connection 数据路径。
    // 单例：所有连接共享同一实例，通过 RunAsync 的 session 参数区分连接。
    private readonly SessionRuntime _sessionRuntime;

    // 心跳扫描协调器：周期扫描超时关闭 + 设备租约 TTL 刷新 + Redis Presence 刷新。
    // 从本 service 抽取以消除 God Service 中散落的后台扫描循环。
    private readonly HeartbeatCoordinator _heartbeatCoordinator;

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
        CommandDispatcher? commandDispatcher = null)
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
            _presenceChangedCodec);

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

        // 每连接数据面运行时：内部创建并注入协议级回调。
        // ProcessPacketAsync / SendProtocolError / RejectOversizedPayload 仍由本 service 持有，
        // 因为它们依赖大量协议 codec 与 _commandDispatcher / _lifecycleCoordinator。
        _sessionRuntime = new SessionRuntime(
            _options,
            _pipeOptions,
            _globalInboundBudget,
            _metrics,
            _logger,
            ProcessPacketAsync,
            SendProtocolError,
            RejectOversizedPayload);

        // 心跳扫描协调器：内部创建并复用已注入依赖。
        _heartbeatCoordinator = new HeartbeatCoordinator(
            _options,
            _timeProvider,
            _listenerHost,
            () => _sessions.Values,
            _lifecycleCoordinator,
            _userSessions,
            _metrics,
            _logger);

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

            await heartbeatTask.ConfigureAwait(false);
            await typingFanoutTask.ConfigureAwait(false);
            _logger.GatewayStopped();
        }
    }

    public override void Dispose()
    {
        _listenerHost.Dispose();
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
        try
        {
            socket.NoDelay = true;
            socket.SetSocketOption(
                SocketOptionLevel.Socket,
                SocketOptionName.KeepAlive,
                optionValue: true);

            var session = new TcpClientSession(
                socket,
                connectionId,
                _options.OutboundQueueCapacity,
                _options.MaxOutboundQueuedBytes,
                _options.SendTimeout,
                _timeProvider,
                _metrics,
                _sessionLogger,
                _globalOutboundBudget,
                _options.AuthenticationTimeout);

            if (!_sessions.TryAdd(connectionId, session))
            {
                session.Close(SessionCloseReason.TransportError);
                await session.DisposeAsync().ConfigureAwait(false);
                return null;
            }

            _metrics.ConnectionAccepted();

            return HandleClientAsync(session, remoteIp, stoppingToken);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    private async Task HandleClientAsync(
        TcpClientSession session,
        string remoteIp,
        CancellationToken cancellationToken)
    {
        // 每连接数据面（Pipe + 调度器 + fill/read 双任务）委托 SessionRuntime。
        // 数据面清理（pipeLease/scheduler/session.DisposeAsync）由 SessionRuntime 内部 finally 完成。
        // 服务级清理（Presence 下线、admission 释放、session 注册表移除）由本方法 finally 处理。
        try
        {
            await _sessionRuntime
                .RunAsync(session, remoteIp, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            // Session 生命周期清理（Presence 下线、watcher 移除、设备租约释放）
            // 委托 SessionLifecycleCoordinator。使用 None token 避免 host stopping 取消时跳过清理。
            await _lifecycleCoordinator.OnDisconnectedAsync(session, CancellationToken.None)
                .ConfigureAwait(false);

            // 释放准入跟踪器槽位（已迁移至 TcpListenerHost）。
            var wasAuthenticated = session.UserId > 0;
            _listenerHost.ReleaseAdmission(remoteIp, wasAuthenticated);
            if (!wasAuthenticated)
                _metrics.UnauthenticatedConnectionClosed();

            if (_sessions.TryRemove(session.ConnectionId, out _))
            {
                _metrics.ConnectionClosed();
                _listenerHost.ReleaseConnectionSlot();
            }
        }
    }

    private async ValueTask ProcessPacketAsync(
        PacketFrame frame,
        TcpClientSession session,
        string remoteIp,
        CancellationToken cancellationToken)
    {
        // 鉴权前置：未认证会话只能处理 PreAuthentication/PreHandshake 命令。
        // 委托 CommandCatalog，避免字面量枚举比较遗漏新增握手命令。
        if (!session.IsAuthenticated &&
            !CommandCatalog.IsPreAuthentication(frame.Command))
        {
            _metrics.ProtocolError();
            SendProtocolError(session, ProtocolErrorCode.AuthRequired, "authentication required");
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        using var activity = GatewayTelemetry.StartCommand(frame.Command);

        // 已迁移到 CommandDispatcher 的命令（全部业务命令）优先走 handler 路径。
        // 测试路径下 dispatcher 为 null，落回下方逻辑。
        if (_commandDispatcher is { } dispatcher)
        {
            var context = new CommandContext(session, remoteIp);
            if (await dispatcher
                    .TryDispatchAsync(frame, context, cancellationToken)
                    .ConfigureAwait(false))
            {
                return;
            }
        }

        // 连接状态机命令（Auth/ClientHello）委托 SessionControlHandler。
        // 不走 dispatcher：依赖 _listenerHost 准入回调与 _lifecycleCoordinator（内部创建）。
        if (await _sessionControlHandler
                .TryHandleAsync(
                    frame.Command,
                    frame.Payload,
                    session,
                    remoteIp,
                    cancellationToken)
                .ConfigureAwait(false))
        {
            return;
        }

        switch (frame.Command)
        {
            case PacketCommand.Heartbeat:
                // 使用静态 pinned Heartbeat ACK 帧，避免每次重复分配。
                // TryQueue 内部 TryRetain 增加 ref count，SendLoop 发送后 Dispose 减少 ref count。
                session.TryQueue(OutboundFrameFactory.GetHeartbeatAck());
                break;

            default:
                _metrics.ProtocolError();
                session.Close(SessionCloseReason.ProtocolViolation);
                break;
        }
    }

    /// <summary>
    /// 发送协议级 Error 帧（PacketCommand.Error = 500）。
    /// 依赖未注入（测试场景）时静默跳过，仅记录指标。
    /// 保留在本 service：SessionRuntime 通过委托回调使用，ProcessPacketAsync 鉴权前置守卫也使用。
    /// </summary>
    private void SendProtocolError(
        TcpClientSession session,
        ProtocolErrorCode code,
        string? message = null,
        bool fatal = false,
        int? retryAfterMs = null,
        ushort? originCommand = null)
    {
        // 测试场景下 _protocolErrorFrameCodec 可能为 null，跳过 Error 帧发送。
        if (_protocolErrorFrameCodec is null)
        {
            return;
        }

        var error = new ProtocolErrorFrame
        {
            Code = code,
            Fatal = fatal || code.IsFatal(),
            RetryAfterMs = retryAfterMs,
            Message = message,
            OriginCommand = originCommand
        };

        using var frame = OutboundFrameFactory.Create(
            PacketCommand.Error,
            _protocolErrorFrameCodec,
            error);
        // Critical 等级：使用 TryQueue 保证发送（满时关闭连接）。
        session.TryQueue(frame, closeAfterSend: fatal ? SessionCloseReason.ProtocolViolation : null);
    }

    private void RejectOversizedPayload(
        TcpClientSession session,
        PacketCommand command)
    {
        if (command == PacketCommand.ChatMessage && session.IsAuthenticated)
        {
            // 内联构造 MessageAcknowledgement 并发送。SendMessageAcknowledgement 辅助方法
            // 已随 ChatMessage 命令迁移至 MessagingCommandHandler 删除，此处保留 _messageAcknowledgementCodec
            // 仅供本路径使用（早投拒绝，发生在 dispatcher 接管命令之前）。
            var acknowledgement = new MessageAcknowledgement
            {
                ClientMessageId = string.Empty,
                CommandId = string.Empty,
                Accepted = false,
                ErrorCode = InboundPayloadEarlyValidator.PayloadTooLargeCode,
                ErrorMessage = $"消息体超过上限 {_options.MaxInboundPayloadBytes} 字节。",
                AcknowledgedUtc = _timeProvider.GetUtcNow().UtcDateTime
            };

            using var outboundFrame = OutboundFrameFactory.Create(
                PacketCommand.MessageAcknowledgement,
                _messageAcknowledgementCodec,
                acknowledgement);
            if (!session.TryQueue(outboundFrame, SessionCloseReason.ProtocolViolation))
            {
                session.Close(SessionCloseReason.ProtocolViolation);
            }
            return;
        }

        session.Close(SessionCloseReason.ProtocolViolation);
    }

}




