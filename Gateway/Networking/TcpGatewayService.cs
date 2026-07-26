using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.History;
using ChatApp.TcpGateway.Core.Messaging.Push;
using ChatApp.TcpGateway.Core.Messaging.Sync;
using ChatApp.TcpGateway.Core.Push;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Core.Server;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Messaging;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using ChatApp.TcpGateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using ChatApp.TcpGateway.Observability.Tracing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RealtimeMessageHistoryQuery =
    ChatApp.Realtime.Abstractions.Messaging.History.MessageHistoryQuery;
using RealtimeConversationListQuery =
    ChatApp.Realtime.Abstractions.Conversations.ConversationListQuery;
using RealtimeConversationMarkReadCommand =
    ChatApp.Realtime.Abstractions.Conversations.ConversationMarkReadCommand;
using RealtimeConversationSetPrefsCommand =
    ChatApp.Realtime.Abstractions.Conversations.ConversationSetPrefsCommand;
using RealtimeMessageRecallCommand =
    ChatApp.Realtime.Abstractions.Messaging.MessageRecallCommand;
using RealtimeMessageEditCommand =
    ChatApp.Realtime.Abstractions.Messaging.MessageEditCommand;
using RealtimeMessageReactionCommand =
    ChatApp.Realtime.Abstractions.Messaging.MessageReactionCommand;
using RealtimeMessageReactionAction =
    ChatApp.Realtime.Abstractions.Messaging.MessageReactionAction;
using RealtimeSyncBootstrapQuery =
    ChatApp.Realtime.Abstractions.Sync.SyncBootstrapQuery;
using RealtimeConversationSyncWatermark =
    ChatApp.Realtime.Abstractions.Sync.ConversationSyncWatermark;

namespace ChatApp.TcpGateway.Gateway.Networking;

internal sealed class TcpGatewayService : BackgroundService
{
    private readonly TcpGatewayOptions _options;
    private readonly IRealtimeAuthenticator _authenticator;
    private readonly IPayloadCodec<AuthenticationRequest> _authenticationRequestCodec;
    private readonly IPayloadCodec<AuthenticationResponse> _authenticationResponseCodec;
    private readonly IPayloadCodec<ChatMessage> _chatMessageCodec;
    private readonly IPayloadCodec<MessageAcknowledgement> _messageAcknowledgementCodec;
    private readonly IPayloadCodec<MessageReceiptRequest> _messageReceiptRequestCodec;
    private readonly IPayloadCodec<MessageReceiptAcknowledgement> _messageReceiptAcknowledgementCodec;
    private readonly IPayloadCodec<MessageHistoryRequest> _messageHistoryRequestCodec;
    private readonly IPayloadCodec<MessageHistoryResponse> _messageHistoryResponseCodec;
    private readonly IPayloadCodec<MessageHistoryItem[]> _messageHistoryItemCodec;
    private readonly IPayloadCodec<ConversationListRequest> _conversationListRequestCodec;
    private readonly IPayloadCodec<ConversationListResponse> _conversationListResponseCodec;
    private readonly IPayloadCodec<ConversationListItem[]> _conversationListItemCodec;
    private readonly IPayloadCodec<ConversationMarkReadRequest> _conversationMarkReadRequestCodec;
    private readonly IPayloadCodec<ConversationMarkReadResponse> _conversationMarkReadResponseCodec;
    private readonly IPayloadCodec<ConversationSetPrefsRequest> _conversationSetPrefsRequestCodec;
    private readonly IPayloadCodec<ConversationSetPrefsResponse> _conversationSetPrefsResponseCodec;
    private readonly IPayloadCodec<MessageRecallRequest> _messageRecallRequestCodec;
    private readonly IPayloadCodec<MessageRecallAcknowledgement> _messageRecallAcknowledgementCodec;
    private readonly IPayloadCodec<MessageEditRequest> _messageEditRequestCodec;
    private readonly IPayloadCodec<MessageEditAcknowledgement> _messageEditAcknowledgementCodec;
    private readonly IPayloadCodec<AddReactionRequest> _addReactionRequestCodec;
    private readonly IPayloadCodec<AddReactionAcknowledgement> _addReactionAcknowledgementCodec;
    private readonly IPayloadCodec<RemoveReactionRequest> _removeReactionRequestCodec;
    private readonly IPayloadCodec<RemoveReactionAcknowledgement> _removeReactionAcknowledgementCodec;
    private readonly IPayloadCodec<SyncBootstrapRequest> _syncBootstrapRequestCodec;
    private readonly IPayloadCodec<SyncBootstrapResponse> _syncBootstrapResponseCodec;
    private readonly JsonPayloadCodec<TypingNotify> _typingNotifyCodec;
    private readonly JsonPayloadCodec<TypingUpdate> _typingUpdateCodec;
    private readonly JsonPayloadCodec<PresenceQueryRequest> _presenceQueryRequestCodec;
    private readonly JsonPayloadCodec<PresenceUnwatchRequest> _presenceUnwatchRequestCodec;
    private readonly JsonPayloadCodec<PresenceSnapshotResponse> _presenceSnapshotResponseCodec;
    private readonly JsonPayloadCodec<PresenceChanged> _presenceChangedCodec;
    private readonly JsonPayloadCodec<CreateGroupRequest> _createGroupRequestCodec;
    private readonly JsonPayloadCodec<CreateGroupResponse> _createGroupResponseCodec;
    private readonly JsonPayloadCodec<AddGroupMembersRequest> _addGroupMembersRequestCodec;
    private readonly JsonPayloadCodec<AddGroupMembersResponse> _addGroupMembersResponseCodec;
    private readonly JsonPayloadCodec<RemoveGroupMemberRequest> _removeGroupMemberRequestCodec;
    private readonly JsonPayloadCodec<RemoveGroupMemberResponse> _removeGroupMemberResponseCodec;
    private readonly JsonPayloadCodec<LeaveGroupRequest> _leaveGroupRequestCodec;
    private readonly JsonPayloadCodec<LeaveGroupResponse> _leaveGroupResponseCodec;
    private readonly JsonPayloadCodec<ChangeMemberRoleRequest> _changeMemberRoleRequestCodec;
    private readonly JsonPayloadCodec<ChangeMemberRoleResponse> _changeMemberRoleResponseCodec;
    private readonly JsonPayloadCodec<ListGroupMembersRequest> _listGroupMembersRequestCodec;
    private readonly JsonPayloadCodec<ListGroupMembersResponse> _listGroupMembersResponseCodec;
    private readonly IRealtimeMessageBus _messageBus;
    private readonly RealtimeIntegrationOptions _integrationOptions;
    private readonly IDeviceSessionLeaseStore _deviceSessionLeaseStore;
    private readonly IGlobalPresenceStore _globalPresence;
    private readonly GatewayMetrics _metrics;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<TcpGatewayService> _logger;
    private readonly ILogger<TcpClientSession> _sessionLogger;
    private readonly PipeOptions _pipeOptions;
    private readonly SemaphoreSlim _connectionSlots;
    private readonly ConcurrentDictionary<uint, TcpClientSession> _sessions = new();
    // 全局内存预算与过载保护
    private readonly ConnectionAdmissionTracker _admissionTracker;
    private readonly GlobalOutboundBudget _globalOutboundBudget;
    private readonly GlobalInboundBudget _globalInboundBudget;
    private readonly ConcurrentDictionary<uint, Task> _clientTasks = new();
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
    private readonly IDirectConversationAuthorizer? _directConversationAuthorizer;
    private readonly IPushTokenStore? _pushTokenStore;
    private readonly JsonPayloadCodec<RegisterPushTokenRequest>? _registerPushTokenRequestCodec;
    private readonly JsonPayloadCodec<RegisterPushTokenResponse>? _registerPushTokenResponseCodec;
    private readonly JsonPayloadCodec<UnregisterPushTokenRequest>? _unregisterPushTokenRequestCodec;
    private readonly JsonPayloadCodec<UnregisterPushTokenResponse>? _unregisterPushTokenResponseCodec;

    private readonly TaskCompletionSource _listenerReady = new(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private Socket? _listener;
    private CancellationTokenSource? _acceptLoopCts;
    private int _isDraining;
    private uint _connectionId;

    public TcpGatewayService(
        IOptions<TcpGatewayOptions> options,
        IRealtimeAuthenticator authenticator,
        IPayloadCodec<AuthenticationRequest> authenticationRequestCodec,
        IPayloadCodec<AuthenticationResponse> authenticationResponseCodec,
        IPayloadCodec<ChatMessage> chatMessageCodec,
        IPayloadCodec<MessageAcknowledgement> messageAcknowledgementCodec,
        IPayloadCodec<MessageReceiptRequest> messageReceiptRequestCodec,
        IPayloadCodec<MessageReceiptAcknowledgement> messageReceiptAcknowledgementCodec,
        IPayloadCodec<MessageHistoryRequest> messageHistoryRequestCodec,
        IPayloadCodec<MessageHistoryResponse> messageHistoryResponseCodec,
        IPayloadCodec<MessageHistoryItem[]> messageHistoryItemCodec,
        IPayloadCodec<ConversationListRequest> conversationListRequestCodec,
        IPayloadCodec<ConversationListResponse> conversationListResponseCodec,
        IPayloadCodec<ConversationListItem[]> conversationListItemCodec,
        IPayloadCodec<ConversationMarkReadRequest> conversationMarkReadRequestCodec,
        IPayloadCodec<ConversationMarkReadResponse> conversationMarkReadResponseCodec,
        IPayloadCodec<ConversationSetPrefsRequest> conversationSetPrefsRequestCodec,
        IPayloadCodec<ConversationSetPrefsResponse> conversationSetPrefsResponseCodec,
        IPayloadCodec<MessageRecallRequest> messageRecallRequestCodec,
        IPayloadCodec<MessageRecallAcknowledgement> messageRecallAcknowledgementCodec,
        IPayloadCodec<MessageEditRequest> messageEditRequestCodec,
        IPayloadCodec<MessageEditAcknowledgement> messageEditAcknowledgementCodec,
        IPayloadCodec<AddReactionRequest> addReactionRequestCodec,
        IPayloadCodec<AddReactionAcknowledgement> addReactionAcknowledgementCodec,
        IPayloadCodec<RemoveReactionRequest> removeReactionRequestCodec,
        IPayloadCodec<RemoveReactionAcknowledgement> removeReactionAcknowledgementCodec,
        IPayloadCodec<SyncBootstrapRequest> syncBootstrapRequestCodec,
        IPayloadCodec<SyncBootstrapResponse> syncBootstrapResponseCodec,
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
        IDirectConversationAuthorizer? directConversationAuthorizer = null,
        IPushTokenStore? pushTokenStore = null,
        IWatcherGatewayDirectory? watcherDirectory = null)
    {
        _options = options.Value;
        _authenticator = authenticator;
        _authenticationRequestCodec = authenticationRequestCodec;
        _authenticationResponseCodec = authenticationResponseCodec;
        _chatMessageCodec = chatMessageCodec;
        _messageAcknowledgementCodec = messageAcknowledgementCodec;
        _messageReceiptRequestCodec = messageReceiptRequestCodec;
        _messageReceiptAcknowledgementCodec = messageReceiptAcknowledgementCodec;
        _messageHistoryRequestCodec = messageHistoryRequestCodec;
        _messageHistoryResponseCodec = messageHistoryResponseCodec;
        _messageHistoryItemCodec = messageHistoryItemCodec;
        _conversationListRequestCodec = conversationListRequestCodec;
        _conversationListResponseCodec = conversationListResponseCodec;
        _conversationListItemCodec = conversationListItemCodec;
        _conversationMarkReadRequestCodec = conversationMarkReadRequestCodec;
        _conversationMarkReadResponseCodec = conversationMarkReadResponseCodec;
        _conversationSetPrefsRequestCodec = conversationSetPrefsRequestCodec;
        _conversationSetPrefsResponseCodec = conversationSetPrefsResponseCodec;
        _messageRecallRequestCodec = messageRecallRequestCodec;
        _messageRecallAcknowledgementCodec = messageRecallAcknowledgementCodec;
        _messageEditRequestCodec = messageEditRequestCodec;
        _messageEditAcknowledgementCodec = messageEditAcknowledgementCodec;
        _addReactionRequestCodec = addReactionRequestCodec;
        _addReactionAcknowledgementCodec = addReactionAcknowledgementCodec;
        _removeReactionRequestCodec = removeReactionRequestCodec;
        _removeReactionAcknowledgementCodec = removeReactionAcknowledgementCodec;
        _syncBootstrapRequestCodec = syncBootstrapRequestCodec;
        _syncBootstrapResponseCodec = syncBootstrapResponseCodec;
        _typingNotifyCodec = new JsonPayloadCodec<TypingNotify>(
            GatewayJsonSerializerContext.Default.TypingNotify);
        _typingUpdateCodec = new JsonPayloadCodec<TypingUpdate>(
            GatewayJsonSerializerContext.Default.TypingUpdate);
        _presenceQueryRequestCodec = new JsonPayloadCodec<PresenceQueryRequest>(
            GatewayJsonSerializerContext.Default.PresenceQueryRequest);
        _presenceUnwatchRequestCodec = new JsonPayloadCodec<PresenceUnwatchRequest>(
            GatewayJsonSerializerContext.Default.PresenceUnwatchRequest);
        _presenceSnapshotResponseCodec = new JsonPayloadCodec<PresenceSnapshotResponse>(
            GatewayJsonSerializerContext.Default.PresenceSnapshotResponse);
        _presenceChangedCodec = new JsonPayloadCodec<PresenceChanged>(
            GatewayJsonSerializerContext.Default.PresenceChanged);
        _createGroupRequestCodec = new JsonPayloadCodec<CreateGroupRequest>(
            GatewayJsonSerializerContext.Default.CreateGroupRequest);
        _createGroupResponseCodec = new JsonPayloadCodec<CreateGroupResponse>(
            GatewayJsonSerializerContext.Default.CreateGroupResponse);
        _addGroupMembersRequestCodec = new JsonPayloadCodec<AddGroupMembersRequest>(
            GatewayJsonSerializerContext.Default.AddGroupMembersRequest);
        _addGroupMembersResponseCodec = new JsonPayloadCodec<AddGroupMembersResponse>(
            GatewayJsonSerializerContext.Default.AddGroupMembersResponse);
        _removeGroupMemberRequestCodec = new JsonPayloadCodec<RemoveGroupMemberRequest>(
            GatewayJsonSerializerContext.Default.RemoveGroupMemberRequest);
        _removeGroupMemberResponseCodec = new JsonPayloadCodec<RemoveGroupMemberResponse>(
            GatewayJsonSerializerContext.Default.RemoveGroupMemberResponse);
        _leaveGroupRequestCodec = new JsonPayloadCodec<LeaveGroupRequest>(
            GatewayJsonSerializerContext.Default.LeaveGroupRequest);
        _leaveGroupResponseCodec = new JsonPayloadCodec<LeaveGroupResponse>(
            GatewayJsonSerializerContext.Default.LeaveGroupResponse);
        _changeMemberRoleRequestCodec = new JsonPayloadCodec<ChangeMemberRoleRequest>(
            GatewayJsonSerializerContext.Default.ChangeMemberRoleRequest);
        _changeMemberRoleResponseCodec = new JsonPayloadCodec<ChangeMemberRoleResponse>(
            GatewayJsonSerializerContext.Default.ChangeMemberRoleResponse);
        _listGroupMembersRequestCodec = new JsonPayloadCodec<ListGroupMembersRequest>(
            GatewayJsonSerializerContext.Default.ListGroupMembersRequest);
        _listGroupMembersResponseCodec = new JsonPayloadCodec<ListGroupMembersResponse>(
            GatewayJsonSerializerContext.Default.ListGroupMembersResponse);
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
        _directConversationAuthorizer = directConversationAuthorizer;
        _pushTokenStore = pushTokenStore;
        _watcherDirectory = watcherDirectory ?? NullWatcherGatewayDirectory.Instance;
        _registerPushTokenRequestCodec = new JsonPayloadCodec<RegisterPushTokenRequest>(
            GatewayJsonSerializerContext.Default.RegisterPushTokenRequest);
        _registerPushTokenResponseCodec = new JsonPayloadCodec<RegisterPushTokenResponse>(
            GatewayJsonSerializerContext.Default.RegisterPushTokenResponse);
        _unregisterPushTokenRequestCodec = new JsonPayloadCodec<UnregisterPushTokenRequest>(
            GatewayJsonSerializerContext.Default.UnregisterPushTokenRequest);
        _unregisterPushTokenResponseCodec = new JsonPayloadCodec<UnregisterPushTokenResponse>(
            GatewayJsonSerializerContext.Default.UnregisterPushTokenResponse);

        _pipeOptions = new PipeOptions(
            pool: MemoryPool<byte>.Shared,
            pauseWriterThreshold: _options.PipePauseWriterThreshold,
            resumeWriterThreshold: _options.PipeResumeWriterThreshold,
            minimumSegmentSize: _options.ReceiveBufferSize,
            readerScheduler: PipeScheduler.ThreadPool,
            writerScheduler: PipeScheduler.ThreadPool,
            useSynchronizationContext: false);

        _connectionSlots = new SemaphoreSlim(
            _options.MaxConnections,
            _options.MaxConnections);

        // 初始化全局内存预算与过载保护
        _admissionTracker = new ConnectionAdmissionTracker(
            _options.MaxUnauthenticatedConnections,
            _options.MaxConnectionsPerIp,
            _options.MaxAuthenticationAttemptsPerIp,
            _options.AuthenticationRateWindow);
        _globalOutboundBudget = new GlobalOutboundBudget(
            _options.GlobalMaxOutboundQueuedBytes);
        _globalInboundBudget = new GlobalInboundBudget(
            _options.GlobalMaxInboundBufferedBytes);
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await base.StartAsync(cancellationToken).ConfigureAwait(false);
        await _listenerReady.Task
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // 1. 先进入 draining：停止接入新连接，再通知已有连接排空。
        Interlocked.Exchange(ref _isDraining, 1);
        try
        {
            _acceptLoopCts?.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Accept loop 可能已结束。
        }

        var listener = Interlocked.Exchange(ref _listener, null);
        listener?.Dispose();

        // 2. 优雅停机：通知所有活跃连接重连其他实例。
        if (_goAwayCodec is not null && !_sessions.IsEmpty)
        {
            var drainTimeout = _options.GoAwayDrainTimeout;
            var goAway = new GoAway
            {
                RetryAfterMs = (int)drainTimeout.TotalMilliseconds,
                Reason = "shutdown",
                ServerHint = null
            };

            foreach (var session in _sessions.Values)
            {
                using var frame = OutboundFrameFactory.Create(
                    PacketCommand.GoAway,
                    _goAwayCodec,
                    goAway);
                session.TryQueue(frame);
            }

            // 等待客户端断开或超时（期间不再 Accept）。
            try
            {
                await Task.Delay(drainTimeout, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // 停机令牌已取消，立即进入强制关闭流程。
            }
        }

        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var execution = CancellationTokenSource.CreateLinkedTokenSource(
            stoppingToken);
        var executionToken = execution.Token;

        var endpoint = new IPEndPoint(
            IPAddress.Parse(_options.ListenAddress),
            _options.Port);

        var listener = new Socket(
            endpoint.AddressFamily,
            SocketType.Stream,
            ProtocolType.Tcp);

        listener.SetSocketOption(
            SocketOptionLevel.Socket,
            SocketOptionName.ReuseAddress,
            optionValue: true);
        try
        {
            listener.Bind(endpoint);
            listener.Listen(_options.ListenBacklog);
        }
        catch (Exception exception)
        {
            listener.Dispose();
            _listenerReady.TrySetException(exception);
            throw;
        }

        Volatile.Write(ref _listener, listener);
        _listenerReady.TrySetResult();

        using var acceptLoopCts = CancellationTokenSource.CreateLinkedTokenSource(
            executionToken);
        Volatile.Write(ref _acceptLoopCts, acceptLoopCts);

        _logger.GatewayStarted(endpoint, _options.MaxConnections);
        var heartbeatTask = RunHeartbeatLoopAsync(executionToken);
        // Typing 时间轮 pump 与发射消费由本机宿主驱动，替代旧的每状态 Task.Delay 过期。
        var typingFanoutTask = RunTypingFanoutLoopAsync(executionToken);

        try
        {
            while (!executionToken.IsCancellationRequested)
            {
                if (Volatile.Read(ref _isDraining) != 0 ||
                    acceptLoopCts.IsCancellationRequested)
                {
                    // 已停止接入：等待 StopAsync 完成 GoAway 排空后再由 base.StopAsync 取消 executionToken。
                    try
                    {
                        await Task.Delay(Timeout.Infinite, executionToken)
                            .ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                        when (executionToken.IsCancellationRequested)
                    {
                    }

                    break;
                }

                Socket socket;
                try
                {
                    socket = await listener
                        .AcceptAsync(acceptLoopCts.Token)
                        .ConfigureAwait(false);
                }
                catch (ObjectDisposedException)
                {
                    // StopAsync 已关闭 listener；转入排空等待。
                    continue;
                }
                catch (OperationCanceledException)
                    when (acceptLoopCts.IsCancellationRequested &&
                          !executionToken.IsCancellationRequested)
                {
                    // Accept 已取消但仍在 draining；转入排空等待。
                    continue;
                }
                catch (SocketException) when (Volatile.Read(ref _isDraining) != 0 ||
                                              acceptLoopCts.IsCancellationRequested)
                {
                    continue;
                }

                if (Volatile.Read(ref _isDraining) != 0)
                {
                    socket.Dispose();
                    continue;
                }

                if (!await _connectionSlots.WaitAsync(0, CancellationToken.None))
                {
                    _metrics.ConnectionRejected();
                    socket.Dispose();
                    continue;
                }

                await StartClientAsync(socket, executionToken)
                    .ConfigureAwait(false);
            }
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
            Volatile.Write(ref _acceptLoopCts, null);
            await execution.CancelAsync();
            Volatile.Write(ref _listener, null);
            listener.Dispose();

            foreach (var session in _sessions.Values)
            {
                session.Close(SessionCloseReason.ApplicationStopping);
            }

            var activeTasks = _clientTasks.Values.ToArray();
            if (activeTasks.Length != 0)
            {
                await Task.WhenAll(activeTasks).ConfigureAwait(false);
            }

            await heartbeatTask.ConfigureAwait(false);
            await typingFanoutTask.ConfigureAwait(false);
            _logger.GatewayStopped();
        }
    }

    public override void Dispose()
    {
        Volatile.Read(ref _listener)?.Dispose();
        _connectionSlots.Dispose();
        base.Dispose();
    }

    private async ValueTask StartClientAsync(Socket socket, CancellationToken stoppingToken)
    {
        // 提取远程 IP 用于准入检查。
        string remoteIp = "unknown";
        try
        {
            if (socket.RemoteEndPoint is IPEndPoint ep)
                remoteIp = ep.Address.ToString();
        }
        catch
        {
            // 获取失败时用 "unknown" 作为 key，仍受全局未认证限制。
        }

        // 连接准入检查（未认证数 + 每 IP 连接数 + 每 IP 认证失败率）。
        var admission = _admissionTracker.TryAdmit(remoteIp);
        if (admission != AdmissionResult.Admitted)
        {
            _metrics.ConnectionRejected();
            switch (admission)
            {
                case AdmissionResult.RejectedUnauthenticatedLimit:
                    _metrics.ConnectionRejectedUnauthLimit();
                    break;
                case AdmissionResult.RejectedPerIpConnectionLimit:
                    _metrics.ConnectionRejectedPerIpLimit();
                    break;
                case AdmissionResult.RejectedPerIpAuthRateLimit:
                    _metrics.AuthenticationRejectedPerIpRate();
                    break;
            }
            socket.Dispose();
            _connectionSlots.Release();
            return;
        }

        _metrics.UnauthenticatedConnectionAccepted();

        try
        {
            socket.NoDelay = true;
            socket.SetSocketOption(
                SocketOptionLevel.Socket,
                SocketOptionName.KeepAlive,
                optionValue: true);

            var connectionId = NextConnectionId();
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
                _connectionSlots.Release();
                _admissionTracker.Release(remoteIp, wasAuthenticated: false);
                _metrics.UnauthenticatedConnectionClosed();
                return;
            }

            _metrics.ConnectionAccepted();

            var clientTask = HandleClientAsync(
                session,
                remoteIp,
                stoppingToken);
            _clientTasks[connectionId] = clientTask;

            _ = clientTask.ContinueWith(
                static (completedTask, state) =>
                {
                    var context = (ClientTaskContext)state!;
                    context.Tasks.TryRemove(
                        context.ConnectionId,
                        out _);
                },
                new ClientTaskContext(_clientTasks, connectionId),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
        catch
        {
            socket.Dispose();
            _connectionSlots.Release();
            _admissionTracker.Release(remoteIp, wasAuthenticated: false);
            _metrics.UnauthenticatedConnectionClosed();
            throw;
        }
    }

    private async Task HandleClientAsync(
        TcpClientSession session,
        string remoteIp,
        CancellationToken cancellationToken)
    {
        // 链接 Session lifetime token 与宿主 stopping token。
        // 连接关闭时取消所有业务调用，避免后端资源继续被占用。
        using var sessionCts = CancellationTokenSource.CreateLinkedTokenSource(
            session.LifetimeToken, cancellationToken);
        var sessionToken = sessionCts.Token;

        // 每会话命令调度器。将命令分发到 OrderedWrite/Query/Ephemeral 三条 lane，
        // 避免慢请求阻塞同连接的其他命令（队头阻塞）。
        // Control 命令（Auth/Heartbeat/PresenceUnwatch）由读循环内联处理。
        var scheduler = new SessionCommandScheduler(
            (command, token) => ProcessScheduledCommandAsync(
                command, session, remoteIp, token),
            _options.CommandSchedulerOrderedWriteCapacity,
            _options.CommandSchedulerQueryCapacity,
            _options.CommandSchedulerEphemeralCapacity,
            sessionToken,
            ex => _logger.TransportFailed(GatewayTransportOperation.ClientProcessing, session.ConnectionId, ex));

        var pipe = new Pipe(_pipeOptions);
        var pipeLease = new SessionInboundPipeLease(_globalInboundBudget);
        var fillTask = FillPipeAsync(
            session,
            pipe.Writer,
            pipeLease,
            sessionToken);
        var readTask = ReadPipeAsync(
            pipe.Reader,
            session,
            remoteIp,
            scheduler,
            pipeLease,
            sessionToken);

        try
        {
            await Task.WhenAll(fillTask, readTask).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested ||
                  !session.IsConnected)
        {
            // Expected shutdown path.
        }
        catch (SocketException)
        {
            session.Close(SessionCloseReason.TransportError);
        }
        catch (ObjectDisposedException)
        {
            session.Close(SessionCloseReason.TransportError);
        }
        catch (Exception exception)
        {
            _logger.TransportFailed(
                GatewayTransportOperation.ClientProcessing,
                session.ConnectionId,
                exception);
            session.Close(SessionCloseReason.TransportError);
        }
        finally
        {
            pipeLease.ReleaseAll();
            session.Close(
                cancellationToken.IsCancellationRequested
                    ? SessionCloseReason.ApplicationStopping
                    : SessionCloseReason.RemoteClosed);

            var wentOffline = _userSessions.Remove(session);
            if (wentOffline)
            {
                if (_options.EnableEphemeralPresenceAndTyping)
                    await PublishPresenceChangedAsync(session.UserId, isOnline: false, CancellationToken.None)
                        .ConfigureAwait(false);
                _presenceWatchers.RemoveWatcher(session.UserId);
            }

            if (session is { UserId: > 0, DeviceIdHash: { } deviceHash }
                && !string.IsNullOrWhiteSpace(session.SessionId))
            {
                try
                {
                    // 使用 ConnectionLeaseId 作为所有权令牌释放租约。
                    await _deviceSessionLeaseStore
                        .ReleaseIfOwnerAsync(
                            session.UserId,
                            deviceHash,
                            session.ConnectionLeaseId,
                            CancellationToken.None)
                        .ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    _logger.SessionRevocationFailed(
                        session.ConnectionId,
                        session.SessionId,
                        exception);
                }
            }

            // 释放准入跟踪器槽位。
            var wasAuthenticated = session.UserId > 0;
            _admissionTracker.Release(remoteIp, wasAuthenticated);
            if (!wasAuthenticated)
                _metrics.UnauthenticatedConnectionClosed();

            if (_sessions.TryRemove(session.ConnectionId, out _))
            {
                _metrics.ConnectionClosed();
                _connectionSlots.Release();
            }

            // 先停止命令调度器（等待 lane 消费者退出并归还租用缓冲区），
            // 再释放 Session。避免 Session 释放后调度器仍访问其字段。
            await scheduler.DisposeAsync().ConfigureAwait(false);

            await session.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task FillPipeAsync(
        TcpClientSession session,
        PipeWriter writer,
        SessionInboundPipeLease pipeLease,
        CancellationToken cancellationToken)
    {
        Exception? completionError = null;
        try
        {
            while (!cancellationToken.IsCancellationRequested &&
                   session.IsConnected)
            {
                var memory = writer.GetMemory(
                    _options.ReceiveBufferSize);
                var bytesRead = await session
                    .ReceiveAsync(memory, cancellationToken)
                    .ConfigureAwait(false);

                if (bytesRead == 0)
                {
                    session.Close(SessionCloseReason.RemoteClosed);
                    break;
                }

                if (!pipeLease.TryReserve(bytesRead))
                {
                    session.Close(SessionCloseReason.InboundBudgetExceeded);
                    break;
                }

                writer.Advance(bytesRead);
                var result = await writer
                    .FlushAsync(cancellationToken)
                    .ConfigureAwait(false);

                if (result.IsCanceled || result.IsCompleted)
                {
                    break;
                }
            }
        }
        catch (Exception exception)
        {
            completionError = exception;
            throw;
        }
        finally
        {
            await writer.CompleteAsync(completionError)
                .ConfigureAwait(false);
        }
    }

    private async Task ReadPipeAsync(
        PipeReader reader,
        TcpClientSession session,
        string remoteIp,
        SessionCommandScheduler scheduler,
        SessionInboundPipeLease pipeLease,
        CancellationToken cancellationToken)
    {
        Exception? completionError = null;
        try
        {
            while (!cancellationToken.IsCancellationRequested &&
                   session.IsConnected)
            {
                var result = await reader
                    .ReadAsync(cancellationToken)
                    .ConfigureAwait(false);
                var readBuffer = result.Buffer;
                var buffer = readBuffer;

                // 跟踪已消费位置。Inline 命令处理后更新此位置；
                // 入队命令在复制 payload 后也更新此位置，使 Pipe 可立即回收内存。
                var consumed = buffer.Start;

                while (session.IsConnected)
                {
                    // 未认证状态下，在等待完整 Payload 前立即拒绝非认证命令。
                    // 攻击者可能声明 ChatMessage（上限 64 KiB）等命令并慢速发送，
                    // 旧实现在完整 Payload 到达后才由 ProcessPacketAsync 拒绝，浪费缓冲与连接。
                    if (!session.IsAuthenticated &&
                        PacketParser.TryPeekCommand(buffer, out var peekedCommand))
                    {
                        // RequireClientHello=true 时，认证前必须先完成 ClientHello 握手。
                        // ClientHello / AuthenticationRequest / Resume 均在 Inline lane 串行处理，
                        // 同一 TCP 段内多帧也不会乱序越过握手状态机。
                        if (_options.RequireClientHello &&
                            peekedCommand == PacketCommand.AuthenticationRequest &&
                            !session.HasCompletedHandshake)
                        {
                            _metrics.ProtocolError();
                            SendProtocolError(
                                session,
                                ProtocolErrorCode.ProtocolViolation,
                                "ClientHello required before authentication",
                                fatal: true,
                                originCommand: (ushort)peekedCommand);
                            session.Close(SessionCloseReason.ProtocolViolation);
                            return;
                        }

                        if (!PacketProtocol.IsAuthenticationCommand(peekedCommand))
                        {
                            _metrics.ProtocolError();
                            SendProtocolError(
                                session,
                                ProtocolErrorCode.ProtocolViolation,
                                "command not allowed before authentication",
                                fatal: true,
                                originCommand: (ushort)peekedCommand);
                            session.Close(SessionCloseReason.ProtocolViolation);
                            return;
                        }
                    }

                    var parseStatus = PacketParser.TryParse(
                        ref buffer,
                        out var frame);

                    if (parseStatus == PacketParseStatus.NeedMoreData)
                    {
                        break;
                    }

                    if (parseStatus == PacketParseStatus.InvalidPacket)
                    {
                        _metrics.ProtocolError();
                        SendProtocolError(
                            session,
                            ProtocolErrorCode.ProtocolViolation,
                            "invalid packet structure",
                            fatal: true);
                        session.Close(
                            SessionCloseReason.ProtocolViolation);
                        return;
                    }

                    var payloadLength = (int)frame.Payload.Length;
                    if (!InboundPayloadEarlyValidator.IsPayloadWithinLimit(
                            payloadLength,
                            _options.MaxInboundPayloadBytes))
                    {
                        _metrics.ProtocolError();
                        RejectOversizedPayload(session, frame.Command);
                        return;
                    }

                    var frameByteCount = PacketProtocol.HeaderSize +
                                         payloadLength;
                    var packetCost = PacketProtocol.GetCommandCost(frame.Command);
                    if (!session.RecordInboundTraffic(
                            _options.MaxPacketsPerSecond,
                            _options.MaxInboundBytesPerSecond,
                            frameByteCount,
                            packetCost))
                    {
                        // 限流为可重试错误：跳过当前帧，不关闭连接。
                        // 客户端收到 RateLimited + RetryAfter 后应退避重试。
                        _metrics.ProtocolError();
                        SendProtocolError(
                            session,
                            ProtocolErrorCode.RateLimited,
                            "inbound rate limit exceeded",
                            retryAfterMs: 1000,
                            originCommand: (ushort)frame.Command);
                        consumed = buffer.Start;
                        continue;
                    }

                    _metrics.PacketReceived();

                    // 按 lane 分类调度。委托 CommandCatalog（单一事实源）。
                    var lane = CommandCatalog.GetLane(frame.Command);

                    if (lane == CommandLane.Inline)
                    {
                        // Control 命令内联处理：ClientHello/Auth/Heartbeat/PresenceUnwatch。
                        // 握手、认证、恢复必须在同一读循环内严格串行，禁止入 OrderedWrite。
                        try
                        {
                            await ProcessPacketAsync(
                                    frame,
                                    session,
                                    remoteIp,
                                    cancellationToken)
                                .ConfigureAwait(false);
                        }
                        catch (JsonException)
                        {
                            _metrics.ProtocolError();
                            session.Close(
                                SessionCloseReason.ProtocolViolation);
                            return;
                        }
                    }
                    else if (lane == CommandLane.Ephemeral)
                    {
                        // 复制出 Pipe 前预留全局入站预算（所有权从 Pipe 转到 lane 缓冲）。
                        if (!_globalInboundBudget.TryReserve(payloadLength))
                        {
                            session.Close(SessionCloseReason.InboundBudgetExceeded);
                            return;
                        }

                        // Ephemeral 命令（Typing）使用普通分配 + DropOldest。
                        var buffer2 = payloadLength > 0
                            ? new byte[payloadLength]
                            : Array.Empty<byte>();

                        if (payloadLength > 0)
                            frame.Payload.CopyTo(buffer2);

                        var command = new SessionCommand
                        {
                            Command = frame.Command,
                            RentedBuffer = buffer2,
                            PayloadLength = payloadLength,
                            IsPooled = false,
                            ReservedInboundBytes = payloadLength,
                            InboundBudget = _globalInboundBudget
                        };

                        // TryEnqueueEphemeral 非阻塞：返回 false 仅在调度器已关闭时。
                        if (!scheduler.TryEnqueueEphemeral(command))
                        {
                            _globalInboundBudget.Release(payloadLength);
                            return;
                        }
                    }
                    else
                    {
                        if (!_globalInboundBudget.TryReserve(payloadLength))
                        {
                            session.Close(SessionCloseReason.InboundBudgetExceeded);
                            return;
                        }

                        // 复制 payload 到 ArrayPool 租用缓冲区，立即释放 Pipe。
                        var rented = payloadLength > 0
                            ? ArrayPool<byte>.Shared.Rent(payloadLength)
                            : Array.Empty<byte>();

                        if (payloadLength > 0)
                            frame.Payload.CopyTo(rented);

                        var command = new SessionCommand
                        {
                            Command = frame.Command,
                            RentedBuffer = rented,
                            PayloadLength = payloadLength,
                            IsPooled = true,
                            ReservedInboundBytes = payloadLength,
                            InboundBudget = _globalInboundBudget
                        };

                        try
                        {
                            if (lane == CommandLane.Query)
                            {
                                await scheduler.EnqueueQueryAsync(
                                        command, cancellationToken)
                                    .ConfigureAwait(false);
                            }
                            else
                            {
                                await scheduler.EnqueueOrderedAsync(
                                        command, cancellationToken)
                                    .ConfigureAwait(false);
                            }
                        }
                        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                        {
                            // 会话关闭中，归还缓冲区与入站预算并退出。
                            if (rented.Length > 0)
                                ArrayPool<byte>.Shared.Return(rented);
                            _globalInboundBudget.Release(payloadLength);
                            throw;
                        }
                        catch (ChannelClosedException)
                        {
                            // 调度器已关闭，归还缓冲区与入站预算并退出。
                            if (rented.Length > 0)
                                ArrayPool<byte>.Shared.Return(rented);
                            _globalInboundBudget.Release(payloadLength);
                            return;
                        }
                    }

                    // 标记此帧已消费（Pipe 可回收对应内存）。
                    consumed = buffer.Start;
                }

                var consumedBytes = (int)readBuffer
                    .Slice(readBuffer.Start, consumed)
                    .Length;
                pipeLease.Release(consumedBytes);
                reader.AdvanceTo(consumed, buffer.End);

                if (result.IsCanceled)
                {
                    break;
                }

                if (result.IsCompleted)
                {
                    if (!buffer.IsEmpty)
                    {
                        _metrics.ProtocolError();
                    }

                    break;
                }
            }
        }
        catch (Exception exception)
        {
            completionError = exception;
            throw;
        }
        finally
        {
            await reader.CompleteAsync(completionError)
                .ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 调度器消费者回调。从租用缓冲区构造 PacketFrame，
    /// 调用既有 ProcessPacketAsync，并捕获异常关闭会话。
    /// </summary>
    private async ValueTask ProcessScheduledCommandAsync(
        SessionCommand command,
        TcpClientSession session,
        string remoteIp,
        CancellationToken cancellationToken)
    {
        try
        {
            var frame = new PacketFrame(
                command.Command,
                command.AsPayloadSequence());
            await ProcessPacketAsync(
                    frame,
                    session,
                    remoteIp,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (JsonException)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
        }
        catch (SocketException)
        {
            session.Close(SessionCloseReason.TransportError);
        }
        catch (Exception ex)
        {
            _logger.TransportFailed(
                GatewayTransportOperation.ClientProcessing,
                session.ConnectionId,
                ex);
            session.Close(SessionCloseReason.TransportError);
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

        switch (frame.Command)
        {
            case PacketCommand.AuthenticationRequest:
                await HandleAuthenticationAsync(
                        frame.Payload,
                        session,
                        remoteIp,
                        cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PacketCommand.ClientHello:
                await HandleClientHelloAsync(
                        frame.Payload,
                        session,
                        remoteIp,
                        cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PacketCommand.ChatMessage:
                await HandleChatMessageAsync(
                        frame.Payload,
                        session,
                        cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PacketCommand.MessageReceipt:
                await HandleMessageReceiptAsync(
                        frame.Payload,
                        session,
                        cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PacketCommand.MessageHistoryRequest:
                await HandleMessageHistoryRequestAsync(
                        frame.Payload,
                        session,
                        cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PacketCommand.ConversationListRequest:
                await HandleConversationListRequestAsync(
                        frame.Payload,
                        session,
                        cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PacketCommand.ConversationMarkReadRequest:
                await HandleConversationMarkReadRequestAsync(
                        frame.Payload,
                        session,
                        cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PacketCommand.ConversationSetPrefsRequest:
                await HandleConversationSetPrefsRequestAsync(
                    frame.Payload,
                    session,
                    cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PacketCommand.MessageRecallRequest:
                await HandleMessageRecallRequestAsync(
                    frame.Payload,
                    session,
                    cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PacketCommand.MessageEditRequest:
                await HandleMessageEditRequestAsync(
                    frame.Payload,
                    session,
                    cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PacketCommand.AddReactionRequest:
                await HandleAddReactionRequestAsync(
                        frame.Payload,
                        session,
                        cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PacketCommand.RemoveReactionRequest:
                await HandleRemoveReactionRequestAsync(
                        frame.Payload,
                        session,
                        cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PacketCommand.SyncBootstrapRequest:
                await HandleSyncBootstrapRequestAsync(
                        frame.Payload,
                        session,
                        cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PacketCommand.CreateGroupRequest:
                await HandleCreateGroupRequestAsync(frame.Payload, session, cancellationToken)
                    .ConfigureAwait(false);
                break;
            case PacketCommand.AddGroupMembersRequest:
                await HandleAddGroupMembersRequestAsync(frame.Payload, session, cancellationToken)
                    .ConfigureAwait(false);
                break;
            case PacketCommand.RemoveGroupMemberRequest:
                await HandleRemoveGroupMemberRequestAsync(frame.Payload, session, cancellationToken)
                    .ConfigureAwait(false);
                break;
            case PacketCommand.LeaveGroupRequest:
                await HandleLeaveGroupRequestAsync(frame.Payload, session, cancellationToken)
                    .ConfigureAwait(false);
                break;
            case PacketCommand.ChangeMemberRoleRequest:
                await HandleChangeMemberRoleRequestAsync(frame.Payload, session, cancellationToken)
                    .ConfigureAwait(false);
                break;
            case PacketCommand.ListGroupMembersRequest:
                await HandleListGroupMembersRequestAsync(frame.Payload, session, cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PacketCommand.TypingNotify:
                await HandleTypingNotifyAsync(frame.Payload, session, cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PacketCommand.PresenceQuery:
                await HandlePresenceQueryAsync(frame.Payload, session, cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PacketCommand.PresenceUnwatch:
                await HandlePresenceUnwatchAsync(frame.Payload, session, cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PacketCommand.RegisterPushTokenRequest:
                await HandleRegisterPushTokenRequestAsync(
                        frame.Payload,
                        session,
                        cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PacketCommand.UnregisterPushTokenRequest:
                await HandleUnregisterPushTokenRequestAsync(
                        frame.Payload,
                        session,
                        cancellationToken)
                    .ConfigureAwait(false);
                break;

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

    private async ValueTask HandleAuthenticationAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        string remoteIp,
        CancellationToken cancellationToken)
    {
        if (session.IsAuthenticated)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        // 连接状态机：RequireClientHello 时必须先完成握手（含 Resume 路径），再接受认证。
        if (_options.RequireClientHello && !session.HasCompletedHandshake)
        {
            _metrics.ProtocolError();
            SendProtocolError(
                session,
                ProtocolErrorCode.ProtocolViolation,
                "ClientHello required before authentication",
                fatal: true,
                originCommand: (ushort)PacketCommand.AuthenticationRequest);
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var request = _authenticationRequestCodec.Deserialize(payload);
        if (request is null ||
            string.IsNullOrWhiteSpace(request.AccessToken))
        {
            _admissionTracker.RecordAuthenticationFailure(remoteIp);
            SendAuthenticationFailure(
                session,
                "AccessToken 为空",
                AuthenticationFailureKind.InvalidCredentials);
            return;
        }

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken);
        timeout.CancelAfter(_options.AuthenticationTimeout);

        RealtimeAuthenticationResult result;
        try
        {
            result = await _authenticator
                .AuthenticateAsync(
                    request.AccessToken,
                    request.DeviceIdHash,
                    timeout.Token)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (!cancellationToken.IsCancellationRequested)
        {
            session.Close(SessionCloseReason.AuthenticationTimedOut);
            return;
        }

        if (!result.Succeeded)
        {
            _admissionTracker.RecordAuthenticationFailure(remoteIp);
            SendAuthenticationFailure(
                session,
                result.ErrorMessage ?? "Token 无效或已过期",
                result.FailureKind);
            return;
        }

        // 认证成功，递减未认证计数，释放槽位给新连接。
        _admissionTracker.MarkAuthenticated();
        _metrics.UnauthenticatedConnectionClosed();

        session.Authenticate(
            result.UserId,
            result.SessionId,
            result.DeviceIdHash,
            result.DeviceId);
        var becameOnline = _userSessions.Add(session);
        if (becameOnline && _options.EnableEphemeralPresenceAndTyping)
            await PublishPresenceChangedAsync(result.UserId, isOnline: true, cancellationToken)
                .ConfigureAwait(false);

        if (_options.ReplaceSameDeviceSession)
        {
            await ReplaceSameDeviceSessionsAsync(session, cancellationToken)
                .ConfigureAwait(false);
        }

        var response = new AuthenticationResponse
        {
            Success = true,
            UserId = result.UserId,
            SessionId = session.SessionId,
            DeviceIdHash = result.DeviceIdHash,
            DeviceId = result.DeviceId
        };

        // 颁发 ResumeToken 供后续断线重连使用。
        if (_options.EnableResume && _resumeTokenStore is not null)
        {
            try
            {
                response.ResumeToken = await _resumeTokenStore.IssueAsync(
                    new ResumeContext
                    {
                        UserId = result.UserId,
                        SessionId = session.SessionId ?? $"tcp-{session.ConnectionId}",
                        ConnectionLeaseId = session.ConnectionLeaseId,
                        DeviceId = result.DeviceId,
                        DeviceIdHash = result.DeviceIdHash
                    },
                    _options.ResumeTokenTtl,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.TransportFailed(
                GatewayTransportOperation.ClientProcessing,
                session.ConnectionId,
                ex);
            }
        }

        using var responseFrame = OutboundFrameFactory.Create(
            PacketCommand.AuthenticationResponse,
            _authenticationResponseCodec,
            response);
        session.TryQueue(responseFrame);
    }

    private async ValueTask HandleClientHelloAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        string remoteIp,
        CancellationToken cancellationToken)
    {
        // 重复 ClientHello 视为协议违例：已认证或已完成握手的会话不应再次发起握手。
        if (session.IsAuthenticated || session.HasCompletedHandshake)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        // 依赖未注入（测试场景）时静默跳过握手，回退到旧 v1 行为。
        if (_clientHelloCodec is null || _serverHelloCodec is null || _serverIdentity is null)
        {
            return;
        }

        var hello = _clientHelloCodec.Deserialize(payload);
        if (hello is null)
        {
            SendProtocolError(session, ProtocolErrorCode.InvalidPayload, "invalid ClientHello");
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        // 协议版本协商：客户端版本须 <= 服务端当前版本。
        if (hello.ProtocolVersion > PacketProtocol.CurrentProtocolVersion)
        {
            SendProtocolError(
                session,
                ProtocolErrorCode.UnsupportedVersion,
                $"unsupported protocol version {hello.ProtocolVersion}",
                fatal: true);
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        // 断线重连：客户端携带 ResumeToken 时尝试恢复。
        if (_options.EnableResume && !string.IsNullOrWhiteSpace(hello.ResumeToken))
        {
            var resumed = await TryResumeSessionAsync(
                hello.ResumeToken!,
                session,
                remoteIp,
                cancellationToken).ConfigureAwait(false);

            if (resumed)
                return; // 恢复成功，ResumeResponse 已发送。

            // 恢复失败：记录准入失败用于限流统计，再发送 Error 帧，客户端应走完整认证流程。
            _admissionTracker.RecordAuthenticationFailure(remoteIp);
            SendProtocolError(
                session,
                ProtocolErrorCode.ResumeFailed,
                "resume token invalid or expired");
            // 继续发送 ServerHello，客户端可选择重新认证。
        }

        // 发送 ServerHello 握手响应。
        var serverHello = new ServerHello
        {
            ProtocolVersion = PacketProtocol.CurrentProtocolVersion,
            FeatureBits = _serverIdentity.FeatureBits,
            ServerDeviceId = _serverIdentity.ServerDeviceId,
            ServerTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            HeartbeatIntervalMs = (int)_options.IdleTimeout.TotalMilliseconds / 2,
            MaxPayloadBytes = _options.MaxInboundPayloadBytes,
            ResumeSupported = _options.EnableResume,
            PayloadFormat = "json"
        };

        using var helloFrame = OutboundFrameFactory.Create(
            PacketCommand.ServerHello,
            _serverHelloCodec,
            serverHello);
        session.TryQueue(helloFrame);
        session.MarkHandshakeCompleted();
    }

    private async ValueTask<bool> TryResumeSessionAsync(
        string resumeToken,
        TcpClientSession session,
        string remoteIp,
        CancellationToken cancellationToken)
    {
        // 依赖未注入时直接返回 false（不应进入此路径，外层已检查）。
        if (_resumeTokenStore is null || _resumeResponseCodec is null)
        {
            return false;
        }

        ResumeContext? context;
        try
        {
            context = await _resumeTokenStore
                .TryValidateAsync(resumeToken, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.ResumeTokenLookup,
                ex);
            return false;
        }

        if (context is null)
            return false;

        // 恢复会话：复用原 UserId/SessionId/DeviceId。
        session.Authenticate(
            context.UserId,
            context.SessionId,
            context.DeviceIdHash,
            context.DeviceId);
        session.MarkHandshakeCompleted();

        _admissionTracker.MarkAuthenticated();
        _metrics.UnauthenticatedConnectionClosed();

        if (_userSessions.Add(session) && _options.EnableEphemeralPresenceAndTyping)
        {
            await PublishPresenceChangedAsync(context.UserId, isOnline: true, cancellationToken)
                .ConfigureAwait(false);
        }

        // 设备租约接管：原 ConnectionLeaseId 已随旧连接释放，这里用新 Session 的 ConnectionLeaseId 重新获取。
        try
        {
            await _deviceSessionLeaseStore.TakeOverAsync(
                context.UserId,
                context.DeviceIdHash ?? 0,
                context.SessionId,
                session.ConnectionLeaseId,
                _options.IdleTimeout + TimeSpan.FromMinutes(5),
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.TransportFailed(
                GatewayTransportOperation.ClientProcessing,
                session.ConnectionId,
                ex);
        }

        // 颁发新的 ResumeToken（旧 Token 已被消费）。
        string? newToken = null;
        if (_options.EnableResume)
        {
            try
            {
                newToken = await _resumeTokenStore.IssueAsync(
                    new ResumeContext
                    {
                        UserId = context.UserId,
                        SessionId = context.SessionId,
                        ConnectionLeaseId = session.ConnectionLeaseId,
                        DeviceId = context.DeviceId,
                        DeviceIdHash = context.DeviceIdHash
                    },
                    _options.ResumeTokenTtl,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.TransportFailed(
                GatewayTransportOperation.ClientProcessing,
                session.ConnectionId,
                ex);
            }
        }

        var response = new ResumeResponse
        {
            Success = true,
            ResumeToken = newToken,
            UserId = context.UserId,
            SessionId = context.SessionId,
            DeviceId = context.DeviceId,
            LastConversationSequence = null // 后续可从同步服务查询
        };

        using var responseFrame = OutboundFrameFactory.Create(
            PacketCommand.ResumeResponse,
            _resumeResponseCodec,
            response);
        session.TryQueue(responseFrame);

        return true;
    }

    private async ValueTask ReplaceSameDeviceSessionsAsync(
        TcpClientSession incoming,
        CancellationToken cancellationToken)
    {
        // 1) 本机旧连接立即踢下线。
        var localVictims = _userSessions.TakeOverSameDevice(incoming);
        var occurredAtMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
        foreach (var victim in localVictims)
            await RevokeSessionAsync(victim, occurredAtMs, cancellationToken).ConfigureAwait(false);

        // 2) Redis/Garnet 设备租约：发现跨 Gateway 的旧 SessionId 并广播 SessionRevoked。
        if (incoming.DeviceIdHash is not { } deviceHash
            || string.IsNullOrWhiteSpace(incoming.SessionId)
            || incoming.UserId <= 0)
        {
            return;
        }

        // TTL 略长于空闲超时，避免正常心跳间隙丢租约；断开时 ReleaseIfOwner。
        var leaseTtl = _options.IdleTimeout + TimeSpan.FromMinutes(5);
        string? previousSessionId;
        try
        {
            // 传入 ConnectionLeaseId 作为所有权令牌。
            previousSessionId = await _deviceSessionLeaseStore
                .TakeOverAsync(
                    incoming.UserId,
                    deviceHash,
                    incoming.SessionId,
                    incoming.ConnectionLeaseId,
                    leaseTtl,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logger.SessionRevocationFailed(
                incoming.ConnectionId,
                incoming.SessionId,
                exception);
            return;
        }

        if (string.IsNullOrWhiteSpace(previousSessionId)
            || string.Equals(previousSessionId, incoming.SessionId, StringComparison.Ordinal))
        {
            return;
        }

        // 本机已踢过的 SessionId 不必再发；跨实例依赖此事件。
        var alreadyLocal = localVictims.Any(v =>
            string.Equals(v.SessionId, previousSessionId, StringComparison.Ordinal));
        if (alreadyLocal)
            return;

        try
        {
            await _messageBus
                .PublishEventAsync(
                    new RealtimeEvent
                    {
                        EventId = RealtimeEventContracts.CreateSessionRevokedEventId(
                            incoming.UserId,
                            previousSessionId,
                            occurredAtMs),
                        Type = RealtimeEventType.SessionRevoked,
                        TargetUserId = incoming.UserId,
                        SessionId = previousSessionId,
                        OccurredAtMs = occurredAtMs
                    },
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logger.SessionRevocationFailed(
                incoming.ConnectionId,
                previousSessionId,
                exception);
        }
    }

    private async ValueTask RevokeSessionAsync(
        TcpClientSession victim,
        long occurredAtMs,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(victim.SessionId))
        {
            victim.Close(SessionCloseReason.SessionRevoked);
            return;
        }

        try
        {
            await _messageBus
                .PublishEventAsync(
                    new RealtimeEvent
                    {
                        EventId = RealtimeEventContracts.CreateSessionRevokedEventId(
                            victim.UserId,
                            victim.SessionId,
                            occurredAtMs),
                        Type = RealtimeEventType.SessionRevoked,
                        TargetUserId = victim.UserId,
                        SessionId = victim.SessionId,
                        OccurredAtMs = occurredAtMs
                    },
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logger.SessionRevocationFailed(
                victim.ConnectionId,
                victim.SessionId,
                exception);
        }

        // 本机立即断开；跨 Gateway 实例依赖 SessionRevoked 事件。
        victim.Close(SessionCloseReason.SessionRevoked);
    }

    private void SendAuthenticationFailure(
        TcpClientSession session,
        string message,
        AuthenticationFailureKind failureKind)
    {
        _metrics.AuthenticationFailed(failureKind);

        var response = new AuthenticationResponse
        {
            Success = false,
            ErrorMessage = message
        };

        using var responseFrame = OutboundFrameFactory.Create(
            PacketCommand.AuthenticationResponse,
            _authenticationResponseCodec,
            response);

        if (!session.TryQueue(
                responseFrame,
                SessionCloseReason.AuthenticationRejected))
        {
            session.Close(
                SessionCloseReason.AuthenticationRejected);
        }
    }

    /// <summary>
    /// 发送协议级 Error 帧（PacketCommand.Error = 500）。
    /// 依赖未注入（测试场景）时静默跳过，仅记录指标。
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

    private async ValueTask HandleChatMessageAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession sender,
        CancellationToken cancellationToken)
    {
        if (!InboundPayloadEarlyValidator.TryValidateChatMessage(
                payload,
                _options.MaxChatAttachments,
                ChatMessageLimits.MaxAttachmentIdLength,
                out var earlyErrorCode,
                out var earlyErrorMessage))
        {
            _metrics.ProtocolError();
            SendMessageAcknowledgement(
                sender,
                clientMessageId: string.Empty,
                commandId: string.Empty,
                accepted: false,
                errorCode: earlyErrorCode,
                errorMessage: earlyErrorMessage,
                closeAfterSend: SessionCloseReason.ProtocolViolation);
            return;
        }

        var message = _chatMessageCodec.Deserialize(payload);
        var hasAttachments = message?.AttachmentIds is { Count: > 0 };
        var hasReply = !string.IsNullOrWhiteSpace(message?.ReplyToMessageId);
        var hasForward = !string.IsNullOrWhiteSpace(message?.ForwardedFromMessageId);
        var isGroup = !string.IsNullOrWhiteSpace(message?.ConversationId)
                      && Realtime.Abstractions.Conversations.ConversationId.IsGroup(
                          message!.ConversationId);
        if (message is null ||
            (!isGroup && message.TargetUserId <= 0) ||
            (isGroup && message.ConversationId!.Length > 64) ||
            (string.IsNullOrWhiteSpace(message.Content) && !hasAttachments) ||
            message.MessageId?.Length > ChatMessageLimits.MaxClientMessageIdLength ||
            (message.AttachmentIds is { Count: > 0 } &&
             message.AttachmentIds.Count > _options.MaxChatAttachments) ||
            (message.AttachmentIds?.Any(static id =>
                string.IsNullOrWhiteSpace(id) ||
                id.Length > ChatMessageLimits.MaxAttachmentIdLength) == true) ||
            (hasReply && hasForward) ||
            (hasReply && (message.ReplyToMessageId!.Length >
                          ChatMessageLimits.MaxReplyToMessageIdLength
                          || message.ReplyToSenderUserId is null or <= 0)) ||
            (!hasReply && (message.ReplyToSenderUserId is not null
                           || !string.IsNullOrWhiteSpace(message.ReplyToPreview))) ||
            (hasForward && (message.ForwardedFromMessageId!.Length >
                            ChatMessageLimits.MaxForwardedFromMessageIdLength
                            || message.ForwardedFromSenderUserId is null or <= 0)) ||
            (!hasForward && (message.ForwardedFromSenderUserId is not null
                             || !string.IsNullOrWhiteSpace(message.ForwardedFromPreview))))
        {
            _metrics.ProtocolError();
            var rejectedMessageId = message?.MessageId is { Length: > 0 and <= ChatMessageLimits.MaxClientMessageIdLength }
                ? message.MessageId
                : string.Empty;
            SendMessageAcknowledgement(
                sender,
                clientMessageId: rejectedMessageId,
                commandId: string.Empty,
                accepted: false,
                errorCode: "invalid_message",
                errorMessage: "聊天消息参数无效。",
                closeAfterSend: SessionCloseReason.ProtocolViolation);
            return;
        }

        var clientMessageId = string.IsNullOrWhiteSpace(message.MessageId)
            ? Guid.CreateVersion7().ToString("N")
            : message.MessageId;
        var commandId = CreateCommandId(
            sender.UserId,
            clientMessageId);
        var command = new IncomingMessageCommand
        {
            CommandId = commandId,
            ClientMessageId = clientMessageId,
            SenderUserId = sender.UserId,
            SenderSessionId = sender.SessionId
                ?? $"tcp-{sender.ConnectionId}",
            ReceiverUserId = isGroup ? 0 : message.TargetUserId,
            ConversationId = isGroup ? message.ConversationId!.Trim() : null,
            Content = message.Content ?? string.Empty,
            AttachmentIds = message.AttachmentIds,
            ReplyToMessageId = string.IsNullOrWhiteSpace(message.ReplyToMessageId)
                ? null
                : message.ReplyToMessageId.Trim(),
            ReplyToSenderUserId = message.ReplyToSenderUserId,
            ReplyToPreview = string.IsNullOrWhiteSpace(message.ReplyToPreview)
                ? null
                : TruncateReplyPreview(message.ReplyToPreview),
            ForwardedFromMessageId = string.IsNullOrWhiteSpace(message.ForwardedFromMessageId)
                ? null
                : message.ForwardedFromMessageId.Trim(),
            ForwardedFromSenderUserId = message.ForwardedFromSenderUserId,
            ForwardedFromPreview = string.IsNullOrWhiteSpace(message.ForwardedFromPreview)
                ? null
                : TruncateForwardedPreview(message.ForwardedFromPreview),
            MentionedUserIds = NormalizeMentionedUserIds(message.MentionedUserIds, isGroup, sender.UserId),
            MentionedRoles = NormalizeMentionedRoles(message.MentionedRoles, isGroup),
            ReceivedAtMs = _timeProvider
                .GetUtcNow()
                .ToUnixTimeMilliseconds()
        };

        try
        {
            await _messageBus
                .PublishIncomingMessageAsync(
                    command,
                    cancellationToken)
                .ConfigureAwait(false);
            _metrics.MessagePublished();

            SendMessageAcknowledgement(
                sender,
                clientMessageId,
                commandId,
                accepted: true);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.MessagePublishFailed();
            _metrics.CommandFailed(PacketCommand.ChatMessage);
            _logger.CommandFailed(
                PacketCommand.ChatMessage,
                sender.ConnectionId,
                commandId,
                exception);

            SendMessageAcknowledgement(
                sender,
                clientMessageId,
                commandId,
                accepted: false,
                errorCode: "message_bus_unavailable",
                errorMessage: "消息服务暂时不可用，请使用相同 ClientMessageId 重试。");
        }
    }

    private async ValueTask HandleMessageReceiptAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession receiver,
        CancellationToken cancellationToken)
    {
        var request = _messageReceiptRequestCodec.Deserialize(payload);
        if (request is null ||
            string.IsNullOrWhiteSpace(request.MessageId) ||
            request.MessageId.Length > 64 ||
            !Enum.IsDefined(request.State))
        {
            _metrics.ProtocolError();
            receiver.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var receiptType = (MessageReceiptType)(byte)request.State;
        var commandId = CreateReceiptCommandId(
            receiver.UserId,
            request.MessageId,
            receiptType);
        var command = new MessageReceiptCommand
        {
            CommandId = commandId,
            MessageId = request.MessageId,
            ReceiverUserId = receiver.UserId,
            ReceiverSessionId = receiver.SessionId
                ?? $"tcp-{receiver.ConnectionId}",
            ReceiptType = receiptType,
            OccurredAtMs = _timeProvider
                .GetUtcNow()
                .ToUnixTimeMilliseconds()
        };

        try
        {
            await _messageBus
                .PublishMessageReceiptAsync(command, cancellationToken)
                .ConfigureAwait(false);
            _metrics.ReceiptPublished();
            SendMessageReceiptAcknowledgement(
                receiver,
                command,
                accepted: true);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.ReceiptPublishFailed();
            _metrics.CommandFailed(PacketCommand.MessageReceipt);
            _logger.CommandFailed(
                PacketCommand.MessageReceipt,
                receiver.ConnectionId,
                commandId,
                exception);
            SendMessageReceiptAcknowledgement(
                receiver,
                command,
                accepted: false,
                errorCode: "message_bus_unavailable",
                errorMessage: "消息服务暂时不可用，请重试相同回执。");
        }
    }

    private async ValueTask HandleMessageHistoryRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _messageHistoryRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
        var hasBeforeTime = request.BeforeReceivedAtMs.HasValue;
        var hasBeforeMessage = !string.IsNullOrWhiteSpace(
            request.BeforeMessageId);
        var hasAfterTime = request.AfterReceivedAtMs.HasValue;
        var hasAfterMessage = !string.IsNullOrWhiteSpace(
            request.AfterMessageId);
        if (requestId.Length > 64
            || request.Limit < 0
            || request.Limit > PacketProtocol.HistoryPageMaxItems
            || hasBeforeTime != hasBeforeMessage
            || hasAfterTime != hasAfterMessage
            || (hasBeforeTime && hasAfterTime)
            || (hasAfterTime && string.IsNullOrWhiteSpace(request.ConversationId))
            || request.BeforeReceivedAtMs is <= 0
            || request.AfterReceivedAtMs is <= 0
            || request.BeforeMessageId?.Length > 64
            || request.AfterMessageId?.Length > 64
            || request.ConversationId?.Length > 64)
        {
            _metrics.HistoryQueryFailed();
            SendMessageHistoryResponse(
                session,
                new MessageHistoryResponse
                {
                    RequestId = requestId.Length <= 64
                        ? requestId
                        : string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_history_request",
                    ErrorMessage = "历史消息请求参数无效。"
                });
            return;
        }

        var query = new RealtimeMessageHistoryQuery
        {
            RequestId = requestId,
            UserId = session.UserId,
            ConversationId = request.ConversationId,
            BeforeReceivedAtMs = request.BeforeReceivedAtMs,
            BeforeMessageId = request.BeforeMessageId,
            AfterReceivedAtMs = request.AfterReceivedAtMs,
            AfterMessageId = request.AfterMessageId,
            Limit = request.Limit
        };

        try
        {
            var page = await _messageBus
                .QueryMessageHistoryAsync(query, cancellationToken)
                .ConfigureAwait(false);
            _metrics.HistoryQueryCompleted();

            var mappedItems = page.Items
                .Select(static item => new MessageHistoryItem
                {
                    MessageId = item.MessageId,
                    ClientMessageId = item.ClientMessageId,
                    SenderUserId = item.SenderUserId,
                    ReceiverUserId = item.ReceiverUserId,
                    ConversationId = item.ConversationId,
                    Content = item.Content,
                    ReceivedAtMs = item.ReceivedAtMs,
                    DeliveredAtMs = item.DeliveredAtMs,
                    ReadAtMs = item.ReadAtMs,
                    RecalledAtMs = item.RecalledAtMs,
                    EditVersion = item.EditVersion,
                    EditedAtMs = item.EditedAtMs,
                    ChangedAtMs = item.ChangedAtMs,
                    Attachments = AttachmentWireMapper.Map(item.Attachments),
                    Reactions = item.Reactions?
                        .Select(static reaction => new MessageReactionSummary
                        {
                            Emoji = reaction.Emoji,
                            Count = reaction.Count,
                            ReactedByMe = reaction.ReactedByMe
                        })
                        .ToArray(),
                    ReplyToMessageId = item.ReplyToMessageId,
                    ReplyToSenderUserId = item.ReplyToSenderUserId,
                    ReplyToPreview = item.ReplyToPreview,
                    ForwardedFromMessageId = item.ForwardedFromMessageId,
                    ForwardedFromSenderUserId = item.ForwardedFromSenderUserId,
                    ForwardedFromPreview = item.ForwardedFromPreview
                })
                .ToArray();

            var originalNextCursor = page.NextCursor is null
                ? null
                : new MessageHistoryCursor
                {
                    ReceivedAtMs = page.NextCursor.ReceivedAtMs,
                    MessageId = page.NextCursor.MessageId
                };

            // 按字节预算截断，确保响应可装入单帧 TCP Payload。
            // 截断时以第 k 条（最后保留条目）派生新 NextCursor，HasMore=true。
            var response = ResponseByteBudget.Truncate(
                new MessageHistoryResponse
                {
                    RequestId = page.RequestId,
                    Succeeded = page.Succeeded,
                    ErrorCode = page.ErrorCode,
                    ErrorMessage = page.ErrorMessage,
                    Items = mappedItems,
                    NextCursor = originalNextCursor,
                    HasMore = page.HasMore
                },
                mappedItems.Length,
                _messageHistoryResponseCodec,
                PacketProtocol.WireResponseSoftLimit,
                PacketProtocol.WireResponseHardLimit,
                static (original, k) =>
                {
                    if (k >= original.Items.Count)
                    {
                        return original;
                    }

                    var prefix = k <= 0
                        ? Array.Empty<MessageHistoryItem>()
                        : original.Items.Take(k).ToArray();
                    var cursor = k > 0
                        ? new MessageHistoryCursor
                        {
                            ReceivedAtMs = prefix[k - 1].ReceivedAtMs,
                            MessageId = prefix[k - 1].MessageId
                        }
                        : null;
                    return original with
                    {
                        Items = prefix,
                        NextCursor = cursor,
                        HasMore = true
                    };
                });

            SendMessageHistoryResponse(session, response);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.HistoryQueryFailed();
            _metrics.CommandFailed(PacketCommand.MessageHistoryRequest);
            _logger.CommandFailed(
                PacketCommand.MessageHistoryRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendMessageHistoryResponse(
                session,
                new MessageHistoryResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "history_service_unavailable",
                    ErrorMessage = "历史消息服务暂时不可用，请稍后重试。"
                });
        }
    }

    private void SendMessageHistoryResponse(
        TcpClientSession session,
        MessageHistoryResponse response)
    {
        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.MessageHistoryPage,
            _messageHistoryResponseCodec,
            response);
        session.TryQueue(outboundFrame);
    }

    private async ValueTask HandleConversationListRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _conversationListRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
        var hasCursorId = !string.IsNullOrWhiteSpace(request.BeforeConversationId);
        var hasCursorPinned = request.BeforeIsPinned.HasValue;
        if (requestId.Length > 64
            || request.Limit < 0
            || request.Limit > PacketProtocol.ConversationListMaxItems
            || hasCursorId != hasCursorPinned
            || request.BeforeLastMessageAtMs is <= 0
            || request.BeforePinnedAtMs is <= 0
            || request.BeforeConversationId?.Length > 64)
        {
            _metrics.HistoryQueryFailed();
            SendConversationListResponse(
                session,
                new ConversationListResponse
                {
                    RequestId = requestId.Length <= 64
                        ? requestId
                        : string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_conversation_list_request",
                    ErrorMessage = "会话列表请求参数无效。"
                });
            return;
        }

        var query = new RealtimeConversationListQuery
        {
            RequestId = requestId,
            UserId = session.UserId,
            BeforeIsPinned = request.BeforeIsPinned,
            BeforePinnedAtMs = request.BeforePinnedAtMs,
            BeforeLastMessageAtMs = request.BeforeLastMessageAtMs,
            BeforeConversationId = request.BeforeConversationId,
            Limit = request.Limit
        };

        try
        {
            var page = await _messageBus
                .QueryConversationListAsync(query, cancellationToken)
                .ConfigureAwait(false);
            _metrics.HistoryQueryCompleted();

            var mappedItems = page.Items
                .Select(static item => new ConversationListItem
                {
                    ConversationId = item.ConversationId,
                    Type = (ConversationType)(byte)item.Type,
                    PeerUserId = item.PeerUserId,
                    Title = item.Title,
                    LastMessageId = item.LastMessageId,
                    LastMessagePreview = item.LastMessagePreview,
                    LastMessageAtMs = item.LastMessageAtMs,
                    LastSenderUserId = item.LastSenderUserId,
                    UnreadCount = item.UnreadCount,
                    LastReadMessageId = item.LastReadMessageId,
                    LastReadAtMs = item.LastReadAtMs,
                    IsPinned = item.IsPinned,
                    PinnedAtMs = item.PinnedAtMs,
                    IsMuted = item.IsMuted,
                    MutedUntilMs = item.MutedUntilMs
                })
                .ToArray();

            var originalNextCursor = page.NextCursor is null
                ? null
                : new ConversationListCursor
                {
                    IsPinned = page.NextCursor.IsPinned,
                    PinnedAtMs = page.NextCursor.PinnedAtMs,
                    LastMessageAtMs = page.NextCursor.LastMessageAtMs,
                    ConversationId = page.NextCursor.ConversationId
                };

            // 按字节预算截断，确保响应可装入单帧 TCP Payload。
            // 截断时以第 k 条（最后保留条目）派生新 NextCursor，HasMore=true。
            var response = ResponseByteBudget.Truncate(
                new ConversationListResponse
                {
                    RequestId = page.RequestId,
                    Succeeded = page.Succeeded,
                    ErrorCode = page.ErrorCode,
                    ErrorMessage = page.ErrorMessage,
                    Items = mappedItems,
                    NextCursor = originalNextCursor,
                    HasMore = page.HasMore
                },
                mappedItems.Length,
                _conversationListResponseCodec,
                PacketProtocol.WireResponseSoftLimit,
                PacketProtocol.WireResponseHardLimit,
                static (original, k) =>
                {
                    if (k >= original.Items.Count)
                    {
                        return original;
                    }

                    var prefix = k <= 0
                        ? Array.Empty<ConversationListItem>()
                        : original.Items.Take(k).ToArray();
                    var cursor = k > 0
                        ? new ConversationListCursor
                        {
                            IsPinned = prefix[k - 1].IsPinned,
                            PinnedAtMs = prefix[k - 1].PinnedAtMs,
                            LastMessageAtMs = prefix[k - 1].LastMessageAtMs,
                            ConversationId = prefix[k - 1].ConversationId
                        }
                        : null;
                    return original with
                    {
                        Items = prefix,
                        NextCursor = cursor,
                        HasMore = true
                    };
                });

            SendConversationListResponse(session, response);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.HistoryQueryFailed();
            _metrics.CommandFailed(PacketCommand.ConversationListRequest);
            _logger.CommandFailed(
                PacketCommand.ConversationListRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendConversationListResponse(
                session,
                new ConversationListResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "conversation_list_unavailable",
                    ErrorMessage = "会话列表服务暂时不可用，请稍后重试。"
                });
        }
    }

    private async ValueTask HandleConversationMarkReadRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _conversationMarkReadRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
        var hasCursorTime = request.ReadAtMs.HasValue;
        var hasCursorMessage = !string.IsNullOrWhiteSpace(request.ReadMessageId);
        if (requestId.Length > 64
            || string.IsNullOrWhiteSpace(request.ConversationId)
            || request.ConversationId.Length > 64
            || hasCursorTime != hasCursorMessage
            || request.ReadAtMs is <= 0
            || request.ReadMessageId?.Length > 64)
        {
            SendConversationMarkReadResponse(
                session,
                new ConversationMarkReadResponse
                {
                    RequestId = requestId.Length <= 64
                        ? requestId
                        : string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_conversation_mark_read_request",
                    ErrorMessage = "会话已读请求参数无效。"
                });
            return;
        }

        var command = new RealtimeConversationMarkReadCommand
        {
            RequestId = requestId,
            UserId = session.UserId,
            ConversationId = request.ConversationId,
            ReadAtMs = request.ReadAtMs,
            ReadMessageId = request.ReadMessageId
        };

        try
        {
            var result = await _messageBus
                .MarkConversationReadAsync(command, cancellationToken)
                .ConfigureAwait(false);

            SendConversationMarkReadResponse(
                session,
                new ConversationMarkReadResponse
                {
                    RequestId = result.RequestId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId,
                    UnreadCount = result.UnreadCount,
                    LastReadMessageId = result.LastReadMessageId,
                    LastReadAtMs = result.LastReadAtMs,
                    Changed = result.Changed
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.ConversationMarkReadRequest);
            _logger.CommandFailed(
                PacketCommand.ConversationMarkReadRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendConversationMarkReadResponse(
                session,
                new ConversationMarkReadResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "conversation_mark_read_unavailable",
                    ErrorMessage = "会话已读服务暂时不可用，请稍后重试。"
                });
        }
    }

    private async ValueTask HandleConversationSetPrefsRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _conversationSetPrefsRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
        if (requestId.Length > 64
            || string.IsNullOrWhiteSpace(request.ConversationId)
            || request.ConversationId.Length > 64
            || (request.Pinned is null && request.Muted is null)
            || request.MutedUntilMs is <= 0)
        {
            SendConversationSetPrefsResponse(
                session,
                new ConversationSetPrefsResponse
                {
                    RequestId = requestId.Length <= 64
                        ? requestId
                        : string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_conversation_set_prefs_request",
                    ErrorMessage = "会话偏好请求参数无效。"
                });
            return;
        }

        var command = new RealtimeConversationSetPrefsCommand
        {
            RequestId = requestId,
            UserId = session.UserId,
            ConversationId = request.ConversationId,
            Pinned = request.Pinned,
            Muted = request.Muted,
            MutedUntilMs = request.MutedUntilMs
        };

        try
        {
            var result = await _messageBus
                .SetConversationPrefsAsync(command, cancellationToken)
                .ConfigureAwait(false);

            SendConversationSetPrefsResponse(
                session,
                new ConversationSetPrefsResponse
                {
                    RequestId = result.RequestId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId,
                    IsPinned = result.IsPinned,
                    IsMuted = result.IsMuted,
                    MutedUntilMs = result.MutedUntilMs,
                    Changed = result.Changed
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.ConversationSetPrefsRequest);
            _logger.CommandFailed(
                PacketCommand.ConversationSetPrefsRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendConversationSetPrefsResponse(
                session,
                new ConversationSetPrefsResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "conversation_set_prefs_unavailable",
                    ErrorMessage = "会话偏好服务暂时不可用，请稍后重试。"
                });
        }
    }

    private void SendConversationListResponse(
        TcpClientSession session,
        ConversationListResponse response)
    {
        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.ConversationListPage,
            _conversationListResponseCodec,
            response);
        session.TryQueue(outboundFrame);
    }

    private async ValueTask HandleSyncBootstrapRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _syncBootstrapRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
        if (requestId.Length > 64
            || request.ListLimit < 0
            || request.ListLimit > PacketProtocol.ConversationListMaxItems
            || request.HistoryLimitPerConversation < 0
            || request.HistoryLimitPerConversation > PacketProtocol.SyncMaxHistoryPerConversation
            || request.MaxConversationsWithHistory < 0
            || request.MaxConversationsWithHistory > PacketProtocol.SyncMaxConversationsWithHistory
            || request.Watermarks?.Count > PacketProtocol.SyncMaxWatermarks
            || request.Watermarks?.Any(static watermark =>
                string.IsNullOrWhiteSpace(watermark.ConversationId)
                || watermark.ConversationId.Length > 64
                || string.IsNullOrWhiteSpace(watermark.AfterMessageId)
                || watermark.AfterMessageId.Length > 64
                || watermark.AfterReceivedAtMs <= 0) == true)
        {
            _metrics.HistoryQueryFailed();
            SendSyncBootstrapResponse(
                session,
                new SyncBootstrapResponse
                {
                    RequestId = requestId.Length <= 64
                        ? requestId
                        : string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_sync_bootstrap_request",
                    ErrorMessage = "同步引导请求参数无效。"
                });
            return;
        }

        var query = new RealtimeSyncBootstrapQuery
        {
            RequestId = requestId,
            UserId = session.UserId,
            DeviceIdHash = session.DeviceIdHash,
            ListLimit = request.ListLimit,
            HistoryLimitPerConversation = request.HistoryLimitPerConversation,
            MaxConversationsWithHistory = request.MaxConversationsWithHistory,
            Watermarks = request.Watermarks?
                .Select(static watermark => new RealtimeConversationSyncWatermark
                {
                    ConversationId = watermark.ConversationId,
                    AfterReceivedAtMs = watermark.AfterReceivedAtMs,
                    AfterMessageId = watermark.AfterMessageId
                })
                .ToArray()
        };

        try
        {
            var page = await _messageBus
                .QuerySyncBootstrapAsync(query, cancellationToken)
                .ConfigureAwait(false);
            _metrics.HistoryQueryCompleted();

            var mappedConversations = page.Conversations
                .Select(static item => new ConversationListItem
                {
                    ConversationId = item.ConversationId,
                    Type = (ConversationType)(byte)item.Type,
                    PeerUserId = item.PeerUserId,
                    Title = item.Title,
                    LastMessageId = item.LastMessageId,
                    LastMessagePreview = item.LastMessagePreview,
                    LastMessageAtMs = item.LastMessageAtMs,
                    LastSenderUserId = item.LastSenderUserId,
                    UnreadCount = item.UnreadCount,
                    LastReadMessageId = item.LastReadMessageId,
                    LastReadAtMs = item.LastReadAtMs,
                    IsPinned = item.IsPinned,
                    PinnedAtMs = item.PinnedAtMs,
                    IsMuted = item.IsMuted,
                    MutedUntilMs = item.MutedUntilMs
                })
                .ToArray();

            var originalConversationsCursor = page.ConversationsNextCursor is null
                ? null
                : new ConversationListCursor
                {
                    IsPinned = page.ConversationsNextCursor.IsPinned,
                    PinnedAtMs = page.ConversationsNextCursor.PinnedAtMs,
                    LastMessageAtMs = page.ConversationsNextCursor.LastMessageAtMs,
                    ConversationId = page.ConversationsNextCursor.ConversationId
                };

            var mappedCatchUps = page.CatchUps
                .Select(static catchUp => new ConversationHistoryCatchUp
                {
                    ConversationId = catchUp.ConversationId,
                    Items = catchUp.Items
                        .Select(static item => new MessageHistoryItem
                        {
                            MessageId = item.MessageId,
                            ClientMessageId = item.ClientMessageId,
                            SenderUserId = item.SenderUserId,
                            ReceiverUserId = item.ReceiverUserId,
                            ConversationId = item.ConversationId,
                            Content = item.Content,
                            ReceivedAtMs = item.ReceivedAtMs,
                            DeliveredAtMs = item.DeliveredAtMs,
                            ReadAtMs = item.ReadAtMs,
                            RecalledAtMs = item.RecalledAtMs,
                            EditVersion = item.EditVersion,
                            EditedAtMs = item.EditedAtMs,
                            ChangedAtMs = item.ChangedAtMs,
                            Attachments = AttachmentWireMapper.Map(item.Attachments),
                            Reactions = item.Reactions?
                                .Select(static reaction => new MessageReactionSummary
                                {
                                    Emoji = reaction.Emoji,
                                    Count = reaction.Count,
                                    ReactedByMe = reaction.ReactedByMe
                                })
                                .ToArray(),
                            ReplyToMessageId = item.ReplyToMessageId,
                            ReplyToSenderUserId = item.ReplyToSenderUserId,
                            ReplyToPreview = item.ReplyToPreview,
                            ForwardedFromMessageId = item.ForwardedFromMessageId,
                            ForwardedFromSenderUserId = item.ForwardedFromSenderUserId,
                            ForwardedFromPreview = item.ForwardedFromPreview
                        })
                        .ToArray(),
                    HasMore = catchUp.HasMore,
                    NextCursor = catchUp.NextCursor is null
                        ? null
                        : new MessageHistoryCursor
                        {
                            ReceivedAtMs = catchUp.NextCursor.ReceivedAtMs,
                            MessageId = catchUp.NextCursor.MessageId
                        }
                })
                .ToArray();

            var mappedResets = page.ResetsRequired
                .Select(static reset => new SyncCursorResetRequired
                {
                    ConversationId = reset.ConversationId,
                    Reason = (SyncCursorResetReason)(byte)reset.Reason,
                    TipMessageId = reset.TipMessageId,
                    TipReceivedAtMs = reset.TipReceivedAtMs,
                    ClientAfterReceivedAtMs = reset.ClientAfterReceivedAtMs,
                    ClientAfterMessageId = reset.ClientAfterMessageId
                })
                .ToArray();

            // 按字节预算截断 SyncBootstrap 响应。
            var conversationsBudget = PacketProtocol.WireResponseSoftLimit / 2;
            var perCatchUpBudget = mappedCatchUps.Length > 0
                ? (PacketProtocol.WireResponseSoftLimit - conversationsBudget) / mappedCatchUps.Length
                : 0;

            var truncatedConversations = ResponseByteBudget.TruncateArray(
                mappedConversations,
                _conversationListItemCodec,
                conversationsBudget,
                PacketProtocol.WireResponseHardLimit,
                static (items, k) => k <= 0
                    ? Array.Empty<ConversationListItem>()
                    : items.Take(k).ToArray());

            var conversationsWasTruncated = truncatedConversations.Length < mappedConversations.Length;
            var conversationsCursor = conversationsWasTruncated
                ? (truncatedConversations.Length > 0
                    ? new ConversationListCursor
                    {
                        IsPinned = truncatedConversations[^1].IsPinned,
                        PinnedAtMs = truncatedConversations[^1].PinnedAtMs,
                        LastMessageAtMs = truncatedConversations[^1].LastMessageAtMs,
                        ConversationId = truncatedConversations[^1].ConversationId
                    }
                    : null)
                : originalConversationsCursor;
            var conversationsHasMore = conversationsWasTruncated || page.ConversationsHasMore;

            var truncatedCatchUps = new ConversationHistoryCatchUp[mappedCatchUps.Length];
            for (var i = 0; i < mappedCatchUps.Length; i++)
            {
                var catchUp = mappedCatchUps[i];
                var truncatedItems = ResponseByteBudget.TruncateArray(
                    catchUp.Items,
                    _messageHistoryItemCodec,
                    perCatchUpBudget,
                    PacketProtocol.WireResponseHardLimit,
                    static (items, k) => k <= 0
                        ? Array.Empty<MessageHistoryItem>()
                        : items.Take(k).ToArray());

                var itemsWereTruncated = truncatedItems.Length < catchUp.Items.Count;
                var catchUpCursor = itemsWereTruncated
                    ? (truncatedItems.Length > 0
                        ? new MessageHistoryCursor
                        {
                            ReceivedAtMs = truncatedItems[^1].ReceivedAtMs,
                            MessageId = truncatedItems[^1].MessageId
                        }
                        : null)
                    : catchUp.NextCursor;

                truncatedCatchUps[i] = catchUp with
                {
                    Items = truncatedItems,
                    NextCursor = catchUpCursor,
                    HasMore = itemsWereTruncated || catchUp.HasMore
                };
            }

            var response = new SyncBootstrapResponse
            {
                RequestId = page.RequestId,
                Succeeded = page.Succeeded,
                ErrorCode = page.ErrorCode,
                ErrorMessage = page.ErrorMessage,
                ServerTimeMs = page.ServerTimeMs,
                Conversations = truncatedConversations,
                ConversationsNextCursor = conversationsCursor,
                ConversationsHasMore = conversationsHasMore,
                CatchUps = truncatedCatchUps,
                ResetsRequired = mappedResets
            };

            var totalSize = ResponseByteBudget.MeasurePayload(
                _syncBootstrapResponseCodec,
                response,
                PacketProtocol.WireResponseHardLimit);
            while (totalSize < 0 && truncatedCatchUps.Length > 0)
            {
                truncatedCatchUps = truncatedCatchUps[..^1];
                response = response with { CatchUps = truncatedCatchUps };
                totalSize = ResponseByteBudget.MeasurePayload(
                    _syncBootstrapResponseCodec,
                    response,
                    PacketProtocol.WireResponseHardLimit);
            }

            SendSyncBootstrapResponse(session, response);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.HistoryQueryFailed();
            _metrics.CommandFailed(PacketCommand.SyncBootstrapRequest);
            _logger.CommandFailed(
                PacketCommand.SyncBootstrapRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendSyncBootstrapResponse(
                session,
                new SyncBootstrapResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "sync_bootstrap_unavailable",
                    ErrorMessage = "同步引导服务暂时不可用，请稍后重试。"
                });
        }
    }

    private void SendSyncBootstrapResponse(
        TcpClientSession session,
        SyncBootstrapResponse response)
    {
        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.SyncBootstrapResponse,
            _syncBootstrapResponseCodec,
            response);
        session.TryQueue(outboundFrame);
    }

    private void SendConversationMarkReadResponse(
        TcpClientSession session,
        ConversationMarkReadResponse response)
    {
        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.ConversationMarkReadResponse,
            _conversationMarkReadResponseCodec,
            response);
        session.TryQueue(outboundFrame);
    }

    private async ValueTask HandleCreateGroupRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _createGroupRequestCodec.Deserialize(payload);
        if (request is null
            || string.IsNullOrWhiteSpace(request.RequestId)
            || request.RequestId.Length > 64
            || string.IsNullOrWhiteSpace(request.Title)
            || request.Title.Trim().Length > 128)
        {
            _metrics.ProtocolError();
            SendCreateGroupResponse(session, new CreateGroupResponse
            {
                RequestId = request?.RequestId ?? string.Empty,
                Succeeded = false,
                ErrorCode = "invalid_request",
                ErrorMessage = "创建群请求参数无效。"
            });
            return;
        }

        try
        {
            var result = await _messageBus.MutateGroupConversationAsync(
                    new Realtime.Abstractions.Conversations.GroupConversationCommand
                    {
                        RequestId = request.RequestId,
                        ActorUserId = session.UserId,
                        Operation = Realtime.Abstractions.Conversations.GroupConversationOperation.Create,
                        Title = request.Title.Trim(),
                        MemberUserIds = request.MemberUserIds,
                        ActorSessionId = session.SessionId
                    },
                    cancellationToken)
                .ConfigureAwait(false);
            SendCreateGroupResponse(session, new CreateGroupResponse
            {
                RequestId = result.RequestId,
                Succeeded = result.Succeeded,
                ErrorCode = result.ErrorCode,
                ErrorMessage = result.ErrorMessage,
                ConversationId = result.ConversationId,
                Title = result.Title,
                Members = MapMembers(result.Members)
            });
        }
        catch (Exception ex)
        {
            _metrics.CommandFailed(PacketCommand.CreateGroupRequest);
            _logger.CommandFailed(
                PacketCommand.CreateGroupRequest,
                session.ConnectionId,
                request.RequestId,
                ex);
            SendCreateGroupResponse(session, new CreateGroupResponse
            {
                RequestId = request.RequestId,
                Succeeded = false,
                ErrorCode = "group_unavailable",
                ErrorMessage = "群服务暂时不可用。"
            });
        }
    }

    private async ValueTask HandleAddGroupMembersRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _addGroupMembersRequestCodec.Deserialize(payload);
        if (request is null
            || string.IsNullOrWhiteSpace(request.RequestId)
            || string.IsNullOrWhiteSpace(request.ConversationId)
            || request.MemberUserIds is not { Count: > 0 })
        {
            SendGroupMutateResponse(
                session,
                PacketCommand.AddGroupMembersResponse,
                _addGroupMembersResponseCodec,
                new AddGroupMembersResponse
                {
                    RequestId = request?.RequestId ?? string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_request",
                    ErrorMessage = "添加成员请求参数无效。"
                });
            return;
        }

        await SendGroupCommandAsync(
                session,
                PacketCommand.AddGroupMembersRequest,
                new Realtime.Abstractions.Conversations.GroupConversationCommand
                {
                    RequestId = request.RequestId,
                    ActorUserId = session.UserId,
                    Operation = Realtime.Abstractions.Conversations.GroupConversationOperation.AddMembers,
                    ConversationId = request.ConversationId.Trim(),
                    MemberUserIds = request.MemberUserIds,
                    ActorSessionId = session.SessionId
                },
                result => new AddGroupMembersResponse
                {
                    RequestId = result.RequestId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId,
                    Members = MapMembers(result.Members)
                },
                PacketCommand.AddGroupMembersResponse,
                _addGroupMembersResponseCodec,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask HandleRemoveGroupMemberRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _removeGroupMemberRequestCodec.Deserialize(payload);
        if (request is null
            || string.IsNullOrWhiteSpace(request.RequestId)
            || string.IsNullOrWhiteSpace(request.ConversationId)
            || request.TargetUserId <= 0)
        {
            SendGroupMutateResponse(
                session,
                PacketCommand.RemoveGroupMemberResponse,
                _removeGroupMemberResponseCodec,
                new RemoveGroupMemberResponse
                {
                    RequestId = request?.RequestId ?? string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_request",
                    ErrorMessage = "移除成员请求参数无效。"
                });
            return;
        }

        await SendGroupCommandAsync(
                session,
                PacketCommand.RemoveGroupMemberRequest,
                new Realtime.Abstractions.Conversations.GroupConversationCommand
                {
                    RequestId = request.RequestId,
                    ActorUserId = session.UserId,
                    Operation = Realtime.Abstractions.Conversations.GroupConversationOperation.RemoveMember,
                    ConversationId = request.ConversationId.Trim(),
                    TargetUserId = request.TargetUserId,
                    ActorSessionId = session.SessionId
                },
                result => new RemoveGroupMemberResponse
                {
                    RequestId = result.RequestId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId
                },
                PacketCommand.RemoveGroupMemberResponse,
                _removeGroupMemberResponseCodec,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask HandleLeaveGroupRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _leaveGroupRequestCodec.Deserialize(payload);
        if (request is null
            || string.IsNullOrWhiteSpace(request.RequestId)
            || string.IsNullOrWhiteSpace(request.ConversationId))
        {
            SendGroupMutateResponse(
                session,
                PacketCommand.LeaveGroupResponse,
                _leaveGroupResponseCodec,
                new LeaveGroupResponse
                {
                    RequestId = request?.RequestId ?? string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_request",
                    ErrorMessage = "退群请求参数无效。"
                });
            return;
        }

        await SendGroupCommandAsync(
                session,
                PacketCommand.LeaveGroupRequest,
                new Realtime.Abstractions.Conversations.GroupConversationCommand
                {
                    RequestId = request.RequestId,
                    ActorUserId = session.UserId,
                    Operation = Realtime.Abstractions.Conversations.GroupConversationOperation.Leave,
                    ConversationId = request.ConversationId.Trim(),
                    ActorSessionId = session.SessionId
                },
                result => new LeaveGroupResponse
                {
                    RequestId = result.RequestId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId
                },
                PacketCommand.LeaveGroupResponse,
                _leaveGroupResponseCodec,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask HandleChangeMemberRoleRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _changeMemberRoleRequestCodec.Deserialize(payload);
        if (request is null
            || string.IsNullOrWhiteSpace(request.RequestId)
            || string.IsNullOrWhiteSpace(request.ConversationId)
            || request.TargetUserId <= 0)
        {
            SendGroupMutateResponse(
                session,
                PacketCommand.ChangeMemberRoleResponse,
                _changeMemberRoleResponseCodec,
                new ChangeMemberRoleResponse
                {
                    RequestId = request?.RequestId ?? string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_request",
                    ErrorMessage = "角色变更请求参数无效。"
                });
            return;
        }

        await SendGroupCommandAsync(
                session,
                PacketCommand.ChangeMemberRoleRequest,
                new Realtime.Abstractions.Conversations.GroupConversationCommand
                {
                    RequestId = request.RequestId,
                    ActorUserId = session.UserId,
                    Operation = Realtime.Abstractions.Conversations.GroupConversationOperation.ChangeRole,
                    ConversationId = request.ConversationId.Trim(),
                    TargetUserId = request.TargetUserId,
                    NewRole = (Realtime.Abstractions.Conversations.ConversationMemberRole)(byte)request.NewRole,
                    ActorSessionId = session.SessionId
                },
                result => new ChangeMemberRoleResponse
                {
                    RequestId = result.RequestId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId
                },
                PacketCommand.ChangeMemberRoleResponse,
                _changeMemberRoleResponseCodec,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask HandleListGroupMembersRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _listGroupMembersRequestCodec.Deserialize(payload);
        if (request is null
            || string.IsNullOrWhiteSpace(request.RequestId)
            || string.IsNullOrWhiteSpace(request.ConversationId))
        {
            SendGroupMutateResponse(
                session,
                PacketCommand.ListGroupMembersResponse,
                _listGroupMembersResponseCodec,
                new ListGroupMembersResponse
                {
                    RequestId = request?.RequestId ?? string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_request",
                    ErrorMessage = "成员列表请求参数无效。"
                });
            return;
        }

        await SendGroupCommandAsync(
                session,
                PacketCommand.ListGroupMembersRequest,
                new Realtime.Abstractions.Conversations.GroupConversationCommand
                {
                    RequestId = request.RequestId,
                    ActorUserId = session.UserId,
                    Operation = Realtime.Abstractions.Conversations.GroupConversationOperation.ListMembers,
                    ConversationId = request.ConversationId.Trim(),
                    ActorSessionId = session.SessionId
                },
                result => new ListGroupMembersResponse
                {
                    RequestId = result.RequestId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId,
                    Members = MapMembers(result.Members)
                },
                PacketCommand.ListGroupMembersResponse,
                _listGroupMembersResponseCodec,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask SendGroupCommandAsync<TResponse>(
        TcpClientSession session,
        PacketCommand requestCommand,
        Realtime.Abstractions.Conversations.GroupConversationCommand command,
        Func<Realtime.Abstractions.Conversations.GroupConversationResult, TResponse> map,
        PacketCommand responseCommand,
        IPayloadCodec<TResponse> responseCodec,
        CancellationToken cancellationToken)
    {
        try
        {
            var result = await _messageBus.MutateGroupConversationAsync(command, cancellationToken)
                .ConfigureAwait(false);
            SendGroupMutateResponse(session, responseCommand, responseCodec, map(result));
        }
        catch (Exception ex)
        {
            _metrics.CommandFailed(requestCommand);
            _logger.CommandFailed(
                requestCommand,
                session.ConnectionId,
                command.RequestId,
                ex);
            SendGroupMutateResponse(
                session,
                responseCommand,
                responseCodec,
                map(Realtime.Abstractions.Conversations.GroupConversationResult.Failed(
                    command.RequestId,
                    "group_unavailable",
                    "群服务暂时不可用。")));
        }
    }

    private void SendCreateGroupResponse(TcpClientSession session, CreateGroupResponse response) =>
        SendGroupMutateResponse(
            session,
            PacketCommand.CreateGroupResponse,
            _createGroupResponseCodec,
            response);

    private static void SendGroupMutateResponse<TResponse>(
        TcpClientSession session,
        PacketCommand command,
        IPayloadCodec<TResponse> codec,
        TResponse response)
    {
        using var frame = OutboundFrameFactory.Create(command, codec, response);
        session.TryQueue(frame);
    }

    private static ConversationMemberItem[]? MapMembers(
        IReadOnlyList<Realtime.Abstractions.Conversations.ConversationMemberItem>? members)
    {
        if (members is null)
            return null;
        return members
            .Select(static m => new ConversationMemberItem
            {
                UserId = m.UserId,
                Role = (ConversationMemberRole)(byte)m.Role,
                JoinedAtMs = m.JoinedAtMs
            })
            .ToArray();
    }

    private void SendConversationSetPrefsResponse(
        TcpClientSession session,
        ConversationSetPrefsResponse response)
    {
        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.ConversationSetPrefsResponse,
            _conversationSetPrefsResponseCodec,
            response);
        session.TryQueue(outboundFrame);
    }

    private async ValueTask HandleMessageRecallRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _messageRecallRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
        if (requestId.Length > 64
            || string.IsNullOrWhiteSpace(request.MessageId)
            || request.MessageId.Length > 64)
        {
            SendMessageRecallAcknowledgement(
                session,
                new MessageRecallAcknowledgement
                {
                    RequestId = requestId.Length <= 64
                        ? requestId
                        : string.Empty,
                    MessageId = request.MessageId,
                    Succeeded = false,
                    ErrorCode = "invalid_message_recall_request",
                    ErrorMessage = "消息撤回请求参数无效。"
                });
            return;
        }

        var command = new RealtimeMessageRecallCommand
        {
            RequestId = requestId,
            MessageId = request.MessageId,
            SenderUserId = session.UserId,
            SenderSessionId = session.SessionId
                ?? $"tcp-{session.ConnectionId}",
            OccurredAtMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds()
        };

        try
        {
            var result = await _messageBus
                .RecallMessageAsync(command, cancellationToken)
                .ConfigureAwait(false);

            SendMessageRecallAcknowledgement(
                session,
                new MessageRecallAcknowledgement
                {
                    RequestId = result.RequestId,
                    MessageId = result.MessageId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId,
                    RecalledAtMs = result.RecalledAtMs
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.MessageRecallRequest);
            _logger.CommandFailed(
                PacketCommand.MessageRecallRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendMessageRecallAcknowledgement(
                session,
                new MessageRecallAcknowledgement
                {
                    RequestId = requestId,
                    MessageId = request.MessageId,
                    Succeeded = false,
                    ErrorCode = "message_recall_unavailable",
                    ErrorMessage = "消息撤回服务暂时不可用，请稍后重试。"
                });
        }
    }

    private void SendMessageRecallAcknowledgement(
        TcpClientSession session,
        MessageRecallAcknowledgement response)
    {
        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.MessageRecallAck,
            _messageRecallAcknowledgementCodec,
            response);
        session.TryQueue(outboundFrame);
    }

    private async ValueTask HandleMessageEditRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _messageEditRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
        if (requestId.Length > 64
            || string.IsNullOrWhiteSpace(request.MessageId)
            || request.MessageId.Length > 64
            || request.Content.Length > 65_536)
        {
            SendMessageEditAcknowledgement(
                session,
                new MessageEditAcknowledgement
                {
                    RequestId = requestId.Length <= 64
                        ? requestId
                        : string.Empty,
                    MessageId = request.MessageId,
                    Succeeded = false,
                    ErrorCode = "invalid_message_edit_request",
                    ErrorMessage = "消息编辑请求参数无效。"
                });
            return;
        }

        var command = new RealtimeMessageEditCommand
        {
            RequestId = requestId,
            MessageId = request.MessageId,
            Content = request.Content,
            SenderUserId = session.UserId,
            SenderSessionId = session.SessionId
                ?? $"tcp-{session.ConnectionId}",
            OccurredAtMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds()
        };

        try
        {
            var result = await _messageBus
                .EditMessageAsync(command, cancellationToken)
                .ConfigureAwait(false);

            SendMessageEditAcknowledgement(
                session,
                new MessageEditAcknowledgement
                {
                    RequestId = result.RequestId,
                    MessageId = result.MessageId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId,
                    Content = result.Content,
                    EditVersion = result.EditVersion,
                    EditedAtMs = result.EditedAtMs
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.MessageEditRequest);
            _logger.CommandFailed(
                PacketCommand.MessageEditRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendMessageEditAcknowledgement(
                session,
                new MessageEditAcknowledgement
                {
                    RequestId = requestId,
                    MessageId = request.MessageId,
                    Succeeded = false,
                    ErrorCode = "message_edit_unavailable",
                    ErrorMessage = "消息编辑服务暂时不可用，请稍后重试。"
                });
        }
    }

    private void SendMessageEditAcknowledgement(
        TcpClientSession session,
        MessageEditAcknowledgement response)
    {
        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.MessageEditAck,
            _messageEditAcknowledgementCodec,
            response);
        session.TryQueue(outboundFrame);
    }

    private async ValueTask HandleAddReactionRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _addReactionRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
        if (requestId.Length > 64
            || string.IsNullOrWhiteSpace(request.MessageId)
            || request.MessageId.Length > 64
            || string.IsNullOrWhiteSpace(request.Emoji)
            || request.Emoji.Trim().Length > 32)
        {
            SendAddReactionAcknowledgement(
                session,
                new AddReactionAcknowledgement
                {
                    RequestId = requestId.Length <= 64 ? requestId : string.Empty,
                    MessageId = request.MessageId,
                    Succeeded = false,
                    ErrorCode = "invalid_add_reaction_request",
                    ErrorMessage = "添加反应请求参数无效。"
                });
            return;
        }

        var command = new RealtimeMessageReactionCommand
        {
            RequestId = requestId,
            MessageId = request.MessageId,
            Emoji = request.Emoji.Trim(),
            Action = RealtimeMessageReactionAction.Add,
            ActorUserId = session.UserId,
            ActorSessionId = session.SessionId ?? $"tcp-{session.ConnectionId}",
            OccurredAtMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds()
        };

        try
        {
            var result = await _messageBus
                .ReactToMessageAsync(command, cancellationToken)
                .ConfigureAwait(false);

            SendAddReactionAcknowledgement(
                session,
                new AddReactionAcknowledgement
                {
                    RequestId = result.RequestId,
                    MessageId = result.MessageId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId,
                    Emoji = result.Emoji,
                    OccurredAtMs = result.OccurredAtMs,
                    EmojiCount = result.EmojiCount
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.AddReactionRequest);
            _logger.CommandFailed(
                PacketCommand.AddReactionRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendAddReactionAcknowledgement(
                session,
                new AddReactionAcknowledgement
                {
                    RequestId = requestId,
                    MessageId = request.MessageId,
                    Succeeded = false,
                    ErrorCode = "message_reaction_unavailable",
                    ErrorMessage = "消息反应服务暂时不可用，请稍后重试。"
                });
        }
    }

    private async ValueTask HandleRemoveReactionRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _removeReactionRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
        if (requestId.Length > 64
            || string.IsNullOrWhiteSpace(request.MessageId)
            || request.MessageId.Length > 64
            || string.IsNullOrWhiteSpace(request.Emoji)
            || request.Emoji.Trim().Length > 32)
        {
            SendRemoveReactionAcknowledgement(
                session,
                new RemoveReactionAcknowledgement
                {
                    RequestId = requestId.Length <= 64 ? requestId : string.Empty,
                    MessageId = request.MessageId,
                    Succeeded = false,
                    ErrorCode = "invalid_remove_reaction_request",
                    ErrorMessage = "移除反应请求参数无效。"
                });
            return;
        }

        var command = new RealtimeMessageReactionCommand
        {
            RequestId = requestId,
            MessageId = request.MessageId,
            Emoji = request.Emoji.Trim(),
            Action = RealtimeMessageReactionAction.Remove,
            ActorUserId = session.UserId,
            ActorSessionId = session.SessionId ?? $"tcp-{session.ConnectionId}",
            OccurredAtMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds()
        };

        try
        {
            var result = await _messageBus
                .ReactToMessageAsync(command, cancellationToken)
                .ConfigureAwait(false);

            SendRemoveReactionAcknowledgement(
                session,
                new RemoveReactionAcknowledgement
                {
                    RequestId = result.RequestId,
                    MessageId = result.MessageId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    ConversationId = result.ConversationId,
                    Emoji = result.Emoji,
                    OccurredAtMs = result.OccurredAtMs,
                    EmojiCount = result.EmojiCount
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.RemoveReactionRequest);
            _logger.CommandFailed(
                PacketCommand.RemoveReactionRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendRemoveReactionAcknowledgement(
                session,
                new RemoveReactionAcknowledgement
                {
                    RequestId = requestId,
                    MessageId = request.MessageId,
                    Succeeded = false,
                    ErrorCode = "message_reaction_unavailable",
                    ErrorMessage = "消息反应服务暂时不可用，请稍后重试。"
                });
        }
    }

    private void SendAddReactionAcknowledgement(
        TcpClientSession session,
        AddReactionAcknowledgement response)
    {
        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.AddReactionAck,
            _addReactionAcknowledgementCodec,
            response);
        session.TryQueue(outboundFrame);
    }

    private void SendRemoveReactionAcknowledgement(
        TcpClientSession session,
        RemoveReactionAcknowledgement response)
    {
        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.RemoveReactionAck,
            _removeReactionAcknowledgementCodec,
            response);
        session.TryQueue(outboundFrame);
    }

    private void SendMessageReceiptAcknowledgement(
        TcpClientSession session,
        MessageReceiptCommand command,
        bool accepted,
        string? errorCode = null,
        string? errorMessage = null)
    {
        var acknowledgement = new MessageReceiptAcknowledgement
        {
            CommandId = command.CommandId,
            MessageId = command.MessageId,
            State = (MessageReceiptState)(byte)command.ReceiptType,
            Accepted = accepted,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage,
            AcknowledgedUtc = _timeProvider.GetUtcNow().UtcDateTime
        };

        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.MessageReceiptAcknowledgement,
            _messageReceiptAcknowledgementCodec,
            acknowledgement);
        session.TryQueue(outboundFrame);
    }

    /// <summary>
    /// 输入状态：本机 UserSessionRegistry 扇出。多网关需后续 ephemeral NATS。
    /// 默认关闭（<see cref="TcpGatewayOptions.EnableEphemeralPresenceAndTyping"/>）。
    /// 要求 ConversationId 与双方用户匹配（私聊成员校验）。
    /// </summary>
    private async ValueTask HandleTypingNotifyAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        if (!_options.EnableEphemeralPresenceAndTyping)
            return;

        var notify = _typingNotifyCodec.Deserialize(payload);
        if (notify is null || string.IsNullOrWhiteSpace(notify.ConversationId))
            return;

        // 从 conversationId 推导 TargetUserId，忽略客户端提交的 TargetUserId。
        if (!TryResolveDirectConversationTarget(
                notify.ConversationId,
                session.UserId,
                out var conversationId,
                out var targetUserId))
        {
            return;
        }

        // 授权校验：检查双方是否好友或同属一会话，且未被拉黑。
        // 授权器未注入（测试场景）时跳过校验，回退到仅会话 ID 解析行为。
        if (_directConversationAuthorizer is not null)
        {
            var allowed = await _directConversationAuthorizer
                .AuthorizeAsync(session.UserId, targetUserId, cancellationToken)
                .ConfigureAwait(false);
            if (!allowed)
                return;
        }

        // 发射路径由协调器统一管理。TryAccept 内部决定是否发射：
        // 限频命中、全局/单用户槽位超限、无活跃 typing 的 isTyping=false 均不发射。
        _typingFanout.TryAccept(
            session.UserId,
            targetUserId,
            conversationId,
            notify.IsTyping);
    }

    private void PublishEphemeralTypingFireAndForget(
        long senderUserId,
        long targetUserId,
        string conversationId,
        bool isTyping)
    {
        var evt = new EphemeralTypingEvent
        {
            OriginInstanceId = _integrationOptions.InstanceId,
            SenderUserId = senderUserId,
            TargetUserId = targetUserId,
            ConversationId = conversationId,
            IsTyping = isTyping
        };
        _ = PublishEphemeralTypingSafeAsync(evt);
    }

    private async Task PublishEphemeralTypingSafeAsync(EphemeralTypingEvent evt)
    {
        try
        {
            await _messageBus.PublishEphemeralTypingAsync(evt).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _metrics.DependencyOperationFailed(
                GatewayDependency.RealtimeService,
                GatewayDependencyOperation.EphemeralTypingPublish);
            _logger.DependencyOperationFailed(
                GatewayDependency.RealtimeService,
                GatewayDependencyOperation.EphemeralTypingPublish,
                ex);
        }
    }

    private void FanoutTypingUpdate(
        long senderUserId,
        long targetUserId,
        string conversationId,
        bool isTyping)
    {
        var targets = _userSessions.GetSnapshot(targetUserId);
        if (targets.Length == 0)
            return;

        var update = new TypingUpdate
        {
            SenderUserId = senderUserId,
            ConversationId = conversationId,
            IsTyping = isTyping
        };

        using var frame = OutboundFrameFactory.Create(
            PacketCommand.TypingUpdate,
            _typingUpdateCodec,
            update);
        foreach (var target in targets)
            target.TryQueueEphemeral(frame);
    }

    /// <summary>
    /// Typing 时间轮 pump 与发射消费统一由本任务驱动。
    /// pump 按 tick 推进过期扫描；消费方从 <see cref="TypingFanoutCoordinator.ReadEmissionsAsync"/>
    /// 拉取合并后的最新状态执行本机扇出与 ephemeral 发布。
    /// </summary>
    private async Task RunTypingFanoutLoopAsync(CancellationToken cancellationToken)
    {
        if (!_options.EnableEphemeralPresenceAndTyping)
            return;

        var pumpTask = RunTypingPumpAsync(cancellationToken);
        var consumeTask = RunTypingEmissionConsumerAsync(cancellationToken);
        await Task.WhenAll(pumpTask, consumeTask).ConfigureAwait(false);
    }

    private async Task RunTypingPumpAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(
            TypingFanoutCoordinator.DefaultTickInterval,
            _timeProvider);

        try
        {
            while (await timer
                       .WaitForNextTickAsync(cancellationToken)
                       .ConfigureAwait(false))
            {
                _typingFanout.PumpExpired();
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            // Normal host shutdown.
        }
    }

    private async Task RunTypingEmissionConsumerAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var emission in _typingFanout
                               .ReadEmissionsAsync(cancellationToken)
                               .ConfigureAwait(false))
            {
                try
                {
                    FanoutTypingUpdate(
                        emission.SenderUserId,
                        emission.TargetUserId,
                        emission.ConversationId,
                        emission.IsTyping);
                    PublishEphemeralTypingFireAndForget(
                        emission.SenderUserId,
                        emission.TargetUserId,
                        emission.ConversationId,
                        emission.IsTyping);
                }
                catch (Exception ex)
                {
                    _metrics.EphemeralEventDropped("typing_fanout_failed");
                    _logger.DependencyOperationFailed(
                        GatewayDependency.RealtimeService,
                        GatewayDependencyOperation.EphemeralTypingPublish,
                        ex);
                }
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            // Normal host shutdown.
        }
    }

    /// <summary>
    /// 从 conversationId 解析私聊会话的另一方用户 Id。
    /// <para>
    /// 以 conversationId 为权威源，忽略客户端提交的 TargetUserId：
    /// <list type="bullet">
    /// <item>解析 dm:lo:hi 格式，校验 sender（session.UserId）必须是会话成员。</item>
    /// <item>target 为会话另一方，防止客户端伪造 TargetUserId 向任意用户发送 Typing。</item>
    /// </list>
    /// 后续可在此处插入 membership/block 缓存查询，检查会话存在性、成员关系、拉黑状态。
    /// </para>
    /// </summary>
    private static bool TryResolveDirectConversationTarget(
        string? conversationId,
        long senderUserId,
        out string normalizedId,
        out long targetUserId)
    {
        normalizedId = string.Empty;
        targetUserId = 0;

        if (string.IsNullOrWhiteSpace(conversationId) || senderUserId <= 0)
            return false;

        var trimmed = conversationId.Trim();
        if (!Realtime.Abstractions.Conversations.ConversationId.TryParseDirect(
                trimmed,
                out var userLo,
                out var userHi))
        {
            return false;
        }

        // sender 必须是会话成员。
        if (senderUserId != userLo && senderUserId != userHi)
            return false;

        // target 为另一方。
        targetUserId = senderUserId == userLo ? userHi : userLo;
        normalizedId = trimmed;
        return true;
    }

    private async ValueTask HandlePresenceQueryAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _presenceQueryRequestCodec.Deserialize(payload);
        if (request is null || string.IsNullOrWhiteSpace(request.RequestId))
            return;

        if (!_options.EnableEphemeralPresenceAndTyping)
        {
            using var disabled = OutboundFrameFactory.Create(
                PacketCommand.PresenceSnapshot,
                _presenceSnapshotResponseCodec,
                new PresenceSnapshotResponse
                {
                    RequestId = request.RequestId.Trim(),
                    Items = []
                });
            session.TryQueue(disabled);
            return;
        }

        var requested = (request.UserIds ?? Array.Empty<long>())
            .Where(id => id > 0 && id != session.UserId)
            .Distinct()
            .Take(100)
            .ToArray();

        long[] allowedIds;
        try
        {
            var auth = await _messageBus
                .AuthorizePresenceAsync(
                    new PresenceAuthorizeQuery
                    {
                        WatcherUserId = session.UserId,
                        TargetUserIds = requested
                    },
                    cancellationToken)
                .ConfigureAwait(false);

            // 授权结果与原始请求集合做交集。
            // 授权服务若返回请求范围外的用户（实现 bug 或协议变更），Gateway 不得订阅或返回其在线状态。
            // 此处使用 HashSet 做 O(1) 交集，保留 requested 顺序以便客户端映射。
            if (auth.AllowedUserIds is null || auth.AllowedUserIds.Count == 0)
            {
                allowedIds = [];
            }
            else if (requested.Length == 0)
            {
                allowedIds = [];
            }
            else
            {
                var requestedSet = requested.Length <= 64
                    ? null
                    : new HashSet<long>(requested);
                var result = new List<long>(Math.Min(requested.Length, auth.AllowedUserIds.Count));
                foreach (var id in auth.AllowedUserIds)
                {
                    if (id <= 0)
                        continue;
                    // 交集：id 必须在 requested 中。
                    if (requestedSet is not null)
                    {
                        if (requestedSet.Contains(id) && !result.Contains(id))
                            result.Add(id);
                    }
                    else
                    {
                        // 小集合线性扫描避免 HashSet 分配。
                        var found = false;
                        foreach (var rid in requested)
                        {
                            if (rid == id)
                            {
                                found = true;
                                break;
                            }
                        }
                        if (found && !result.Contains(id))
                            result.Add(id);
                    }
                }
                allowedIds = result.ToArray();
            }
        }
        catch (Exception ex)
        {
            _metrics.PresenceQueryFailed();
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.PresenceAuthorize,
                ex);
            allowedIds = [];
        }

        _presenceWatchers.WatchMany(allowedIds, session.UserId);

        // 分片路由：将被观察用户与本实例的对应关系登记到全局 watcher 目录，
        // 供 Presence 事件发布方定向投递。失败不阻断查询响应。
        if (allowedIds.Length > 0)
        {
            try
            {
                await _watcherDirectory
                    .RegisterWatchersAsync(
                        session.UserId,
                        allowedIds,
                        _integrationOptions.InstanceId,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.DependencyOperationFailed(
                    GatewayDependency.Redis,
                    GatewayDependencyOperation.WatcherDirectoryQuery,
                    ex);
            }
        }

        IReadOnlyDictionary<long, bool> onlineMap;
        try
        {
            onlineMap = await _globalPresence
                .GetOnlineManyAsync(allowedIds, cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            onlineMap = new Dictionary<long, bool>();
        }

        var items = new PresenceSnapshotItem[allowedIds.Length];
        for (var i = 0; i < allowedIds.Length; i++)
        {
            var userId = allowedIds[i];
            var localOnline = _userSessions.GetSnapshot(userId).Length > 0;
            var globalOnline = onlineMap.TryGetValue(userId, out var on) && on;
            items[i] = new PresenceSnapshotItem
            {
                UserId = userId,
                IsOnline = localOnline || globalOnline
            };
        }

        var response = new PresenceSnapshotResponse
        {
            RequestId = request.RequestId.Trim(),
            Items = items
        };

        using var outbound = OutboundFrameFactory.Create(
            PacketCommand.PresenceSnapshot,
            _presenceSnapshotResponseCodec,
            response);
        session.TryQueue(outbound);
    }

    private async ValueTask HandlePresenceUnwatchAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        if (!_options.EnableEphemeralPresenceAndTyping)
            return;

        var request = _presenceUnwatchRequestCodec.Deserialize(payload);
        if (request?.UserIds is null || request.UserIds.Count == 0)
            return;

        var selfUserId = session.UserId;
        var userIds = request.UserIds
            .Where(id => id > 0 && id != selfUserId)
            .Distinct()
            .Take(100)
            .ToArray();
        _presenceWatchers.UnwatchMany(userIds, selfUserId);

        // 分片路由：从全局 watcher 目录注销对应关系。失败不阻断客户端请求。
        if (userIds.Length > 0)
        {
            try
            {
                await _watcherDirectory
                    .UnregisterWatchersAsync(
                        selfUserId,
                        userIds,
                        _integrationOptions.InstanceId,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.DependencyOperationFailed(
                    GatewayDependency.Redis,
                    GatewayDependencyOperation.WatcherDirectoryQuery,
                    ex);
            }
        }
    }

    /// <summary>
    /// 注册设备推送令牌。按 (userId, deviceIdHash) 幂等覆盖；超出每用户上限时按最旧淘汰。
    /// deviceIdHash 取自认证会话，忽略客户端传入；token 字符串长度上限由 <see cref="PushTokenLimits"/> 限制。
    /// </summary>
    private async ValueTask HandleRegisterPushTokenRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        if (_pushTokenStore is null || _registerPushTokenRequestCodec is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var request = _registerPushTokenRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
        if (requestId.Length > PushTokenLimits.MaxRequestIdLength
            || !Enum.IsDefined(request.Platform)
            || request.Platform == 0
            || string.IsNullOrWhiteSpace(request.Token)
            || request.Token.Length > PushTokenLimits.MaxTokenLength
            || (request.AppDeviceLabel is { Length: > PushTokenLimits.MaxAppDeviceLabelLength })
            || session.DeviceIdHash is null or 0)
        {
            SendRegisterPushTokenResponse(
                session,
                new RegisterPushTokenResponse
                {
                    RequestId = requestId.Length <= PushTokenLimits.MaxRequestIdLength
                        ? requestId
                        : string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_push_token_request",
                    ErrorMessage = "推送令牌注册请求参数无效。"
                });
            return;
        }

        try
        {
            var activeCount = await _pushTokenStore
                .RegisterAsync(
                    session.UserId,
                    session.DeviceIdHash!.Value,
                    request.Platform,
                    request.Token,
                    string.IsNullOrWhiteSpace(request.AppDeviceLabel)
                        ? null
                        : request.AppDeviceLabel,
                    cancellationToken)
                .ConfigureAwait(false);

            SendRegisterPushTokenResponse(
                session,
                new RegisterPushTokenResponse
                {
                    RequestId = requestId,
                    Succeeded = true,
                    ActiveTokenCount = activeCount
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.RegisterPushTokenRequest);
            _logger.CommandFailed(
                PacketCommand.RegisterPushTokenRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendRegisterPushTokenResponse(
                session,
                new RegisterPushTokenResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "push_token_store_unavailable",
                    ErrorMessage = "推送令牌存储暂不可用。"
                });
        }
    }

    /// <summary>
    /// 注销推送令牌。未传 Token 时按当前连接 deviceIdHash 注销该设备全部令牌；
    /// 传 Token 时按字符串精确注销（可跨设备，适合平台令牌失效场景）。
    /// </summary>
    private async ValueTask HandleUnregisterPushTokenRequestAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        if (_pushTokenStore is null || _unregisterPushTokenRequestCodec is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var request = _unregisterPushTokenRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
        if (requestId.Length > PushTokenLimits.MaxRequestIdLength
            || (request.Token is { Length: > PushTokenLimits.MaxTokenLength })
            || (string.IsNullOrWhiteSpace(request.Token) && session.DeviceIdHash is null or 0))
        {
            SendUnregisterPushTokenResponse(
                session,
                new UnregisterPushTokenResponse
                {
                    RequestId = requestId.Length <= PushTokenLimits.MaxRequestIdLength
                        ? requestId
                        : string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_push_token_request",
                    ErrorMessage = "推送令牌注销请求参数无效。"
                });
            return;
        }

        try
        {
            int activeCount;
            if (!string.IsNullOrWhiteSpace(request.Token))
            {
                activeCount = await _pushTokenStore
                    .UnregisterByTokenAsync(
                        session.UserId,
                        request.Token,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                activeCount = await _pushTokenStore
                    .UnregisterByDeviceAsync(
                        session.UserId,
                        session.DeviceIdHash!.Value,
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            SendUnregisterPushTokenResponse(
                session,
                new UnregisterPushTokenResponse
                {
                    RequestId = requestId,
                    Succeeded = true,
                    ActiveTokenCount = activeCount
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.UnregisterPushTokenRequest);
            _logger.CommandFailed(
                PacketCommand.UnregisterPushTokenRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendUnregisterPushTokenResponse(
                session,
                new UnregisterPushTokenResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "push_token_store_unavailable",
                    ErrorMessage = "推送令牌存储暂不可用。"
                });
        }
    }

    private void SendRegisterPushTokenResponse(
        TcpClientSession session,
        RegisterPushTokenResponse response)
    {
        if (_registerPushTokenResponseCodec is null)
            return;

        using var frame = OutboundFrameFactory.Create(
            PacketCommand.RegisterPushTokenResponse,
            _registerPushTokenResponseCodec,
            response);
        session.TryQueue(frame);
    }

    private void SendUnregisterPushTokenResponse(
        TcpClientSession session,
        UnregisterPushTokenResponse response)
    {
        if (_unregisterPushTokenResponseCodec is null)
            return;

        using var frame = OutboundFrameFactory.Create(
            PacketCommand.UnregisterPushTokenResponse,
            _unregisterPushTokenResponseCodec,
            response);
        session.TryQueue(frame);
    }

    private async Task PublishPresenceChangedAsync(
        long userId,
        bool isOnline,
        CancellationToken cancellationToken)
    {
        // 只在全局状态转换（0->1 或 1->0）时广播与发布跨网关事件。
        // 旧实现每实例本地首连/断开都无条件广播，导致多实例登录时互相覆盖、误报下线。
        PresenceTransition transition;
        if (isOnline)
            transition = await _globalPresence
                .SetOnlineAsync(userId, _integrationOptions.InstanceId, cancellationToken)
                .ConfigureAwait(false);
        else
            transition = await _globalPresence
                .SetOfflineAsync(userId, _integrationOptions.InstanceId, cancellationToken)
                .ConfigureAwait(false);

        if (transition == PresenceTransition.None)
            return;

        var globalIsOnline = transition == PresenceTransition.WentOnline;
        BroadcastPresenceChangedLocal(userId, globalIsOnline);

        try
        {
            await _messageBus
                .PublishEphemeralPresenceAsync(
                    new EphemeralPresenceEvent
                    {
                        OriginInstanceId = _integrationOptions.InstanceId,
                        UserId = userId,
                        IsOnline = globalIsOnline
                    },
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _metrics.DependencyOperationFailed(
                GatewayDependency.RealtimeService,
                GatewayDependencyOperation.EphemeralPresencePublish);
            _logger.DependencyOperationFailed(
                GatewayDependency.RealtimeService,
                GatewayDependencyOperation.EphemeralPresencePublish,
                ex);
        }
    }

    private void BroadcastPresenceChangedLocal(long userId, bool isOnline)
    {
        var watchers = _presenceWatchers.GetWatchers(userId);
        if (watchers.Length == 0)
            return;

        var update = new PresenceChanged
        {
            UserId = userId,
            IsOnline = isOnline
        };

        using var frame = OutboundFrameFactory.Create(
            PacketCommand.PresenceChanged,
            _presenceChangedCodec,
            update);
        foreach (var watcherId in watchers)
        {
            foreach (var watcherSession in _userSessions.GetSnapshot(watcherId))
                watcherSession.TryQueueEphemeral(frame);
        }
    }

    private void SendMessageAcknowledgement(
        TcpClientSession session,
        string clientMessageId,
        string commandId,
        bool accepted,
        string? errorCode = null,
        string? errorMessage = null,
        SessionCloseReason? closeAfterSend = null)
    {
        var acknowledgement = new MessageAcknowledgement
        {
            ClientMessageId = clientMessageId,
            CommandId = commandId,
            Accepted = accepted,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage,
            AcknowledgedUtc = _timeProvider.GetUtcNow().UtcDateTime
        };

        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.MessageAcknowledgement,
            _messageAcknowledgementCodec,
            acknowledgement);
        if (!session.TryQueue(outboundFrame, closeAfterSend) &&
            closeAfterSend is { } reason)
        {
            session.Close(reason);
        }
    }

    private void RejectOversizedPayload(
        TcpClientSession session,
        PacketCommand command)
    {
        if (command == PacketCommand.ChatMessage && session.IsAuthenticated)
        {
            SendMessageAcknowledgement(
                session,
                clientMessageId: string.Empty,
                commandId: string.Empty,
                accepted: false,
                errorCode: InboundPayloadEarlyValidator.PayloadTooLargeCode,
                errorMessage: $"消息体超过上限 {_options.MaxInboundPayloadBytes} 字节。",
                closeAfterSend: SessionCloseReason.ProtocolViolation);
            return;
        }

        session.Close(SessionCloseReason.ProtocolViolation);
    }

    private static string TruncateReplyPreview(string preview)
    {
        var trimmed = preview.Trim();
        return trimmed.Length <= ChatMessageLimits.MaxReplyPreviewLength
            ? trimmed
            : trimmed[..ChatMessageLimits.MaxReplyPreviewLength];
    }

    private static string TruncateForwardedPreview(string preview)
    {
        var trimmed = preview.Trim();
        return trimmed.Length <= ChatMessageLimits.MaxForwardedFromPreviewLength
            ? trimmed
            : trimmed[..ChatMessageLimits.MaxForwardedFromPreviewLength];
    }

    /// <summary>
    /// 规整 @ 用户 Id 列表：非群聊返回 null；去重、去自提及非正 Id；超额截断。
    /// </summary>
    internal static List<long>? NormalizeMentionedUserIds(
        IReadOnlyList<long>? raw,
        bool isGroup,
        long senderUserId)
    {
        if (!isGroup || raw is null || raw.Count == 0)
            return null;

        var seen = new HashSet<long>();
        var result = new List<long>(Math.Min(raw.Count, ChatMessageLimits.MaxMentionedUserIds));
        foreach (var id in raw)
        {
            if (id <= 0 || id == senderUserId)
                continue;
            if (seen.Add(id))
                result.Add(id);
            if (result.Count >= ChatMessageLimits.MaxMentionedUserIds)
                break;
        }

        return result.Count == 0 ? null : result;
    }

    /// <summary>
    /// 规整 @ 角色列表：非群聊返回 null；去空白项与重复项；按长度与数量上限截断。
    /// </summary>
    internal static List<string>? NormalizeMentionedRoles(
        IReadOnlyList<string>? raw,
        bool isGroup)
    {
        if (!isGroup || raw is null || raw.Count == 0)
            return null;

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<string>(Math.Min(raw.Count, ChatMessageLimits.MaxMentionedRoles));
        foreach (var role in raw)
        {
            if (string.IsNullOrWhiteSpace(role))
                continue;
            var trimmed = role.Trim();
            if (trimmed.Length > ChatMessageLimits.MaxMentionedRoleLength)
                trimmed = trimmed[..ChatMessageLimits.MaxMentionedRoleLength];
            if (seen.Add(trimmed))
                result.Add(trimmed);
            if (result.Count >= ChatMessageLimits.MaxMentionedRoles)
                break;
        }

        return result.Count == 0 ? null : result;
    }

    // 20（long 含符号最大位数）+ 1（':'）+ clientMessageId 最大 UTF8 字节数 + 余量。
    private const int CommandIdScratchBytes =
        20 + 1 + (ChatMessageLimits.MaxClientMessageIdLength * 3) + 16;

    private static string CreateCommandId(
        long senderUserId,
        string clientMessageId)
    {
        var maxIdBytes = Encoding.UTF8.GetMaxByteCount(clientMessageId.Length);
        if (20 + 1 + maxIdBytes > CommandIdScratchBytes)
            return CreateCommandIdSlow(senderUserId, clientMessageId);

        Span<byte> scratch = stackalloc byte[CommandIdScratchBytes];
        var written = 0;
        senderUserId.TryFormat(scratch, out var idLen);
        written += idLen;
        scratch[written++] = (byte)':';
        written += Encoding.UTF8.GetBytes(clientMessageId, scratch[written..]);

        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(scratch[..written], hash);
        return Convert.ToHexStringLower(hash);
    }

    private static string CreateCommandIdSlow(
        long senderUserId,
        string clientMessageId)
    {
        var source = Encoding.UTF8.GetBytes(
            $"{senderUserId}:{clientMessageId}");
        return Convert.ToHexStringLower(
            SHA256.HashData(source));
    }

    // 20（long）+ 1（':'）+ messageId 最大 UTF8 字节数 + 1（':'）+ 3（byte）+ 余量。
    private const int ReceiptCommandIdScratchBytes =
        20 + 1 + (64 * 3) + 1 + 3 + 16;

    private static string CreateReceiptCommandId(
        long receiverUserId,
        string messageId,
        MessageReceiptType receiptType)
    {
        var maxIdBytes = Encoding.UTF8.GetMaxByteCount(messageId.Length);
        if (20 + 1 + maxIdBytes + 1 + 3 > ReceiptCommandIdScratchBytes)
            return CreateReceiptCommandIdSlow(receiverUserId, messageId, receiptType);

        Span<byte> scratch = stackalloc byte[ReceiptCommandIdScratchBytes];
        var written = 0;
        receiverUserId.TryFormat(scratch, out var idLen);
        written += idLen;
        scratch[written++] = (byte)':';
        written += Encoding.UTF8.GetBytes(messageId, scratch[written..]);
        scratch[written++] = (byte)':';
        ((byte)receiptType).TryFormat(scratch[written..], out var typeLen);
        written += typeLen;

        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(scratch[..written], hash);
        return Convert.ToHexStringLower(hash);
    }

    private static string CreateReceiptCommandIdSlow(
        long receiverUserId,
        string messageId,
        MessageReceiptType receiptType)
    {
        var source = Encoding.UTF8.GetBytes(
            $"{receiverUserId}:{messageId}:{(byte)receiptType}");
        return Convert.ToHexStringLower(
            SHA256.HashData(source));
    }
    private async Task RunHeartbeatLoopAsync(
        CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(
            _options.HeartbeatScanInterval,
            _timeProvider);
        // 限制并发 Redis 往返，避免 10k 连接串行扫心跳。
        const int maxRefreshConcurrency = 32;
        using var refreshGate = new SemaphoreSlim(maxRefreshConcurrency, maxRefreshConcurrency);

        try
        {
            while (await timer
                       .WaitForNextTickAsync(cancellationToken)
                       .ConfigureAwait(false))
            {
                _admissionTracker.SweepExpiredEntries(DateTimeOffset.UtcNow);

                var sessions = _sessions.Values.ToArray();
                var presenceRefreshUsers = new HashSet<long>();
                var refreshTasks = new List<Task>(sessions.Length);

                foreach (var session in sessions)
                {
                    if (!session.IsAuthenticated &&
                        session.ConnectionAge >
                        _options.AuthenticationTimeout)
                    {
                        session.Close(
                            SessionCloseReason.AuthenticationTimedOut);
                    }
                    else if (session.LastInboundAge >
                             _options.IdleTimeout)
                    {
                        session.Close(
                            SessionCloseReason.IdleTimedOut);
                    }
                    else if (session is { IsAuthenticated: true, UserId: > 0 })
                    {
                        var userId = session.UserId;

                        // 设备租约刷新：独立条件。
                        // 仅当启用同设备替换且会话携带 DeviceIdHash 时才续期租约。
                        // 缺少 DeviceIdHash 的已认证连接不持有设备租约，不应续期。
                        if (_options.ReplaceSameDeviceSession
                            && session.DeviceIdHash is { } deviceHash)
                        {
                            var leaseTtl = _options.IdleTimeout + TimeSpan.FromMinutes(5);
                            var leaseId = session.ConnectionLeaseId;
                            refreshTasks.Add(RefreshLeaseWithGateAsync(
                                refreshGate,
                                userId,
                                deviceHash,
                                leaseId,
                                leaseTtl,
                                cancellationToken));
                        }

                        // Presence 刷新：独立条件，按用户去重。
                        // 不应依赖 ReplaceSameDeviceSession 或 DeviceIdHash：
                        // 关闭同设备替换或无 DeviceIdHash 的已认证连接仍需续期 Redis 全局在线状态，
                        // 否则 TTL（5 分钟）过期后用户会被误判离线。
                        // 同一用户多设备会话只刷新一次（presenceRefreshUsers 去重）。
                        if (_options.EnableEphemeralPresenceAndTyping
                            && presenceRefreshUsers.Add(userId)
                            && _userSessions.GetSnapshot(userId).Length > 0)
                        {
                            refreshTasks.Add(RefreshPresenceWithGateAsync(
                                refreshGate,
                                userId,
                                cancellationToken));
                        }
                    }
                }

                if (refreshTasks.Count > 0)
                {
                    try
                    {
                        await Task.WhenAll(refreshTasks).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch
                    {
                        // 单个刷新失败已在内部记录；不中断心跳循环。
                    }
                }
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            // Normal host shutdown.
        }
    }

    private async Task RefreshLeaseWithGateAsync(
        SemaphoreSlim gate,
        long userId,
        ulong deviceHash,
        string leaseId,
        TimeSpan leaseTtl,
        CancellationToken cancellationToken)
    {
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await _deviceSessionLeaseStore
                .RefreshIfOwnerAsync(
                    userId,
                    deviceHash,
                    leaseId,
                    leaseTtl,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.DeviceLeaseRefresh,
                exception);
        }
        finally
        {
            gate.Release();
        }
    }

    private async Task RefreshPresenceWithGateAsync(
        SemaphoreSlim gate,
        long userId,
        CancellationToken cancellationToken)
    {
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await _globalPresence
                .RefreshOnlineAsync(
                    userId,
                    _integrationOptions.InstanceId,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.PresenceRefresh,
                exception);
        }
        finally
        {
            gate.Release();
        }
    }

    private uint NextConnectionId()
    {
        while (true)
        {
            var next = Interlocked.Increment(ref _connectionId);
            if (next != 0)
            {
                return next;
            }
        }
    }

    private sealed record ClientTaskContext(
        ConcurrentDictionary<uint, Task> Tasks,
        uint ConnectionId);
}




