using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.History;
using ChatApp.TcpGateway.Core.Messaging.Sync;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Gateway.Messaging;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using ChatApp.TcpGateway.Networking.Sessions;
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

internal sealed partial class TcpGatewayService : BackgroundService
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
    private readonly ConcurrentDictionary<uint, Task> _clientTasks = new();
    private readonly UserSessionRegistry _userSessions;
    private readonly PresenceWatcherRegistry _presenceWatchers;
    private readonly TypingFanoutCoordinator _typingFanout;

    private readonly TaskCompletionSource _listenerReady = new(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private Socket? _listener;
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
        ILogger<TcpClientSession> sessionLogger)
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
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await base.StartAsync(cancellationToken).ConfigureAwait(false);
        await _listenerReady.Task
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
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

        LogGatewayStarted(_logger, endpoint, _options.MaxConnections);
        var heartbeatTask = RunHeartbeatLoopAsync(executionToken);
        // P0-2: Typing 时间轮 pump 与发射消费由本机宿主驱动，替代旧的每状态 Task.Delay 过期。
        var typingFanoutTask = RunTypingFanoutLoopAsync(executionToken);

        try
        {
            while (!executionToken.IsCancellationRequested)
            {
                var socket = await listener
                    .AcceptAsync(executionToken)
                    .ConfigureAwait(false);

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
            LogGatewayFatal(_logger, exception);
            throw;
        }
        finally
        {
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
            LogGatewayStopped(_logger);
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
                _sessionLogger);

            if (!_sessions.TryAdd(connectionId, session))
            {
                session.Close(SessionCloseReason.TransportError);
                await session.DisposeAsync().ConfigureAwait(false);
                _connectionSlots.Release();
                return;
            }

            _metrics.ConnectionAccepted();

            var clientTask = HandleClientAsync(
                session,
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
            throw;
        }
    }

    private async Task HandleClientAsync(
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var pipe = new Pipe(_pipeOptions);
        var fillTask = FillPipeAsync(
            session,
            pipe.Writer,
            cancellationToken);
        var readTask = ReadPipeAsync(
            pipe.Reader,
            session,
            cancellationToken);

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
            LogClientProcessingError(
                _logger,
                session.ConnectionId,
                exception);
            session.Close(SessionCloseReason.TransportError);
        }
        finally
        {
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
                    await _deviceSessionLeaseStore
                        .ReleaseIfOwnerAsync(
                            session.UserId,
                            deviceHash,
                            session.SessionId,
                            CancellationToken.None)
                        .ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    LogSessionRevokePublishFailed(
                        _logger,
                        session.ConnectionId,
                        session.SessionId,
                        exception);
                }
            }

            if (_sessions.TryRemove(session.ConnectionId, out _))
            {
                _metrics.ConnectionClosed();
                _connectionSlots.Release();
            }

            await session.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task FillPipeAsync(
        TcpClientSession session,
        PipeWriter writer,
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
                var buffer = result.Buffer;

                while (session.IsConnected)
                {
                    // P0-5：未认证状态下，在等待完整 Payload 前立即拒绝非认证命令。
                    // 攻击者可能声明 ChatMessage（上限 64 KiB）等命令并慢速发送，
                    // 旧实现在完整 Payload 到达后才由 ProcessPacketAsync 拒绝，浪费缓冲与连接。
                    if (!session.IsAuthenticated &&
                        PacketParser.TryPeekCommand(buffer, out var peekedCommand) &&
                        !PacketProtocol.IsAuthenticationCommand(peekedCommand))
                    {
                        _metrics.ProtocolError();
                        session.Close(SessionCloseReason.ProtocolViolation);
                        return;
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
                        session.Close(
                            SessionCloseReason.ProtocolViolation);
                        return;
                    }

                    var payloadLength = frame.Payload.Length;
                    if (!InboundPayloadEarlyValidator.IsPayloadWithinLimit(
                            payloadLength,
                            _options.MaxInboundPayloadBytes))
                    {
                        _metrics.ProtocolError();
                        RejectOversizedPayload(session, frame.Command);
                        return;
                    }

                    var frameByteCount = PacketProtocol.HeaderSize +
                                         (int)payloadLength;
                    if (!session.RecordInboundTraffic(
                            _options.MaxPacketsPerSecond,
                            _options.MaxInboundBytesPerSecond,
                            frameByteCount))
                    {
                        session.Close(
                            SessionCloseReason.RateLimitExceeded);
                        return;
                    }

                    _metrics.PacketReceived();

                    try
                    {
                        await ProcessPacketAsync(
                                frame,
                                session,
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

                reader.AdvanceTo(buffer.Start, buffer.End);

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

    private async ValueTask ProcessPacketAsync(
        PacketFrame frame,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        if (!session.IsAuthenticated &&
            frame.Command != PacketCommand.AuthenticationRequest)
        {
            _metrics.ProtocolError();
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
                HandleTypingNotify(frame.Payload, session);
                break;

            case PacketCommand.PresenceQuery:
                await HandlePresenceQueryAsync(frame.Payload, session, cancellationToken)
                    .ConfigureAwait(false);
                break;

            case PacketCommand.PresenceUnwatch:
                HandlePresenceUnwatch(frame.Payload, session);
                break;

            case PacketCommand.Heartbeat:
                using (var acknowledgement =
                       OutboundFrameFactory.CreateEmpty(
                           PacketCommand.HeartbeatAcknowledgement))
                {
                    session.TryQueue(acknowledgement);
                }

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
        CancellationToken cancellationToken)
    {
        if (session.IsAuthenticated)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var request = _authenticationRequestCodec.Deserialize(payload);
        if (request is null ||
            string.IsNullOrWhiteSpace(request.AccessToken))
        {
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
            SendAuthenticationFailure(
                session,
                result.ErrorMessage ?? "Token 无效或已过期",
                result.FailureKind);
            return;
        }

        session.Authenticate(
            result.UserId,
            result.SessionId,
            result.DeviceIdHash);
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
            DeviceIdHash = result.DeviceIdHash
        };

        using var responseFrame = OutboundFrameFactory.Create(
            PacketCommand.AuthenticationResponse,
            _authenticationResponseCodec,
            response);
        session.TryQueue(responseFrame);
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
            previousSessionId = await _deviceSessionLeaseStore
                .TakeOverAsync(
                    incoming.UserId,
                    deviceHash,
                    incoming.SessionId,
                    leaseTtl,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            LogSessionRevokePublishFailed(
                _logger,
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
            LogSessionRevokePublishFailed(
                _logger,
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
            LogSessionRevokePublishFailed(
                _logger,
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
            LogMessagePublishFailed(
                _logger,
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
            LogReceiptPublishFailed(
                _logger,
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

            // P0-6：按字节预算截断，确保响应可装入单帧 TCP Payload。
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
            LogHistoryQueryFailed(
                _logger,
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

            // P0-6：按字节预算截断，确保响应可装入单帧 TCP Payload。
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
            LogConversationListQueryFailed(
                _logger,
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
            LogConversationMarkReadFailed(
                _logger,
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
            LogConversationSetPrefsFailed(
                _logger,
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

            // P0-6：按字节预算截断 SyncBootstrap 响应。
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
            LogSyncBootstrapQueryFailed(
                _logger,
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
            LogCreateGroupFailed(_logger, request.RequestId, ex);
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
            LogGroupCommandFailed(_logger, command.RequestId, ex);
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
            LogMessageRecallFailed(
                _logger,
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
            LogMessageEditFailed(
                _logger,
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
            LogAddReactionFailed(
                _logger,
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
            LogRemoveReactionFailed(
                _logger,
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
    private void HandleTypingNotify(
        ReadOnlySequence<byte> payload,
        TcpClientSession session)
    {
        if (!_options.EnableEphemeralPresenceAndTyping)
            return;

        var notify = _typingNotifyCodec.Deserialize(payload);
        if (notify is null
            || notify.TargetUserId <= 0
            || notify.TargetUserId == session.UserId)
        {
            return;
        }

        if (!TryAuthorizeDirectConversation(
                notify.ConversationId,
                session.UserId,
                notify.TargetUserId,
                out var conversationId))
        {
            return;
        }

        // P0-2: 发射路径由协调器统一管理。TryAccept 内部决定是否发射：
        // 限频命中、全局/单用户槽位超限、无活跃 typing 的 isTyping=false 均不发射。
        // 本机扇出与 ephemeral 发布由 RunTypingEmissionConsumerAsync 消费 ReadEmissionsAsync 完成，
        // 过期由时间轮 pump 负责，不再为此处创建独立 Task.Delay。
        _typingFanout.TryAccept(
            session.UserId,
            notify.TargetUserId,
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
            LogEphemeralTypingPublishFailed(_logger, evt.TargetUserId, ex);
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
            target.TryQueue(frame);
    }

    /// <summary>
    /// P0-2: Typing 时间轮 pump 与发射消费统一由本任务驱动。
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
                    LogTypingFanoutFailed(
                        _logger,
                        emission.SenderUserId,
                        emission.ConversationId,
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

    private static bool TryAuthorizeDirectConversation(
        string? conversationId,
        long userA,
        long userB,
        out string normalizedId)
    {
        normalizedId = string.Empty;
        if (string.IsNullOrWhiteSpace(conversationId))
            return false;

        var expected = Realtime.Abstractions.Conversations.ConversationId.CreateDirect(userA, userB);
        if (!string.Equals(conversationId.Trim(), expected, StringComparison.Ordinal))
            return false;

        normalizedId = expected;
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
            allowedIds =
            [
                .. auth.AllowedUserIds
                    .Where(static id => id > 0)
                    .Distinct()
            ];
        }
        catch (Exception ex)
        {
            LogPresenceAuthorizeFailed(_logger, session.UserId, ex);
            allowedIds = [];
        }

        _presenceWatchers.WatchMany(allowedIds, session.UserId);

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

    private void HandlePresenceUnwatch(
        ReadOnlySequence<byte> payload,
        TcpClientSession session)
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
    }

    private async Task PublishPresenceChangedAsync(
        long userId,
        bool isOnline,
        CancellationToken cancellationToken)
    {
        if (isOnline)
            await _globalPresence
                .SetOnlineAsync(userId, _integrationOptions.InstanceId, cancellationToken)
                .ConfigureAwait(false);
        else
            await _globalPresence
                .SetOfflineAsync(userId, _integrationOptions.InstanceId, cancellationToken)
                .ConfigureAwait(false);

        BroadcastPresenceChangedLocal(userId, isOnline);

        try
        {
            await _messageBus
                .PublishEphemeralPresenceAsync(
                    new EphemeralPresenceEvent
                    {
                        OriginInstanceId = _integrationOptions.InstanceId,
                        UserId = userId,
                        IsOnline = isOnline
                    },
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogEphemeralPresencePublishFailed(_logger, userId, ex);
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
                watcherSession.TryQueue(frame);
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

        try
        {
            while (await timer
                       .WaitForNextTickAsync(cancellationToken)
                       .ConfigureAwait(false))
            {
                foreach (var session in _sessions.Values)
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
                    else if (_options.EnableEphemeralPresenceAndTyping
                             && session is { IsAuthenticated: true, UserId: > 0 }
                             && _userSessions.GetSnapshot(session.UserId).Length > 0)
                    {
                        _ = _globalPresence.RefreshOnlineAsync(
                            session.UserId,
                            _integrationOptions.InstanceId,
                            CancellationToken.None);
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

    [LoggerMessage(
        EventId = 40,
        Level = LogLevel.Warning,
        Message = "创建群失败 RequestId={RequestId}")]
    private static partial void LogCreateGroupFailed(
        ILogger logger,
        string requestId,
        Exception exception);

    [LoggerMessage(
        EventId = 41,
        Level = LogLevel.Warning,
        Message = "群操作失败 RequestId={RequestId}")]
    private static partial void LogGroupCommandFailed(
        ILogger logger,
        string requestId,
        Exception exception);

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Information,
        Message = "TCP gateway listening on {Endpoint}; maximum connections: {MaxConnections}.")]
    private static partial void LogGatewayStarted(
        ILogger logger,
        IPEndPoint endpoint,
        int maxConnections);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Information,
        Message = "TCP gateway stopped.")]
    private static partial void LogGatewayStopped(ILogger logger);

    [LoggerMessage(
        EventId = 3,
        Level = LogLevel.Error,
        Message = "Connection {ConnectionId} failed during processing.")]
    private static partial void LogClientProcessingError(
        ILogger logger,
        uint connectionId,
        Exception exception);
    [LoggerMessage(
        EventId = 4,
        Level = LogLevel.Critical,
        Message = "TCP gateway stopped due to a fatal error.")]
    private static partial void LogGatewayFatal(
        ILogger logger,
        Exception exception);

    [LoggerMessage(
        EventId = 5,
        Level = LogLevel.Warning,
        Message = "Connection {ConnectionId} could not publish message command {CommandId}.")]
    private static partial void LogMessagePublishFailed(
        ILogger logger,
        uint connectionId,
        string commandId,
        Exception exception);

    [LoggerMessage(
        EventId = 6,
        Level = LogLevel.Warning,
        Message = "Connection {ConnectionId} could not publish receipt command {CommandId}.")]
    private static partial void LogReceiptPublishFailed(
        ILogger logger,
        uint connectionId,
        string commandId,
        Exception exception);
    [LoggerMessage(
        EventId = 7,
        Level = LogLevel.Warning,
        Message = "Connection {ConnectionId} could not query message history for request {RequestId}.")]
    private static partial void LogHistoryQueryFailed(
        ILogger logger,
        uint connectionId,
        string requestId,
        Exception exception);

    [LoggerMessage(
        EventId = 12,
        Level = LogLevel.Warning,
        Message = "Connection {ConnectionId} could not publish session revoke for session {SessionId}.")]
    private static partial void LogSessionRevokePublishFailed(
        ILogger logger,
        uint connectionId,
        string sessionId,
        Exception exception);

    [LoggerMessage(
        EventId = 8,
        Level = LogLevel.Warning,
        Message = "Connection {ConnectionId} could not query conversation list for request {RequestId}.")]
    private static partial void LogConversationListQueryFailed(
        ILogger logger,
        uint connectionId,
        string requestId,
        Exception exception);

    [LoggerMessage(
        EventId = 9,
        Level = LogLevel.Warning,
        Message = "Connection {ConnectionId} could not mark conversation read for request {RequestId}.")]
    private static partial void LogConversationMarkReadFailed(
        ILogger logger,
        uint connectionId,
        string requestId,
        Exception exception);

    [LoggerMessage(
        EventId = 11,
        Level = LogLevel.Warning,
        Message = "Connection {ConnectionId} could not set conversation prefs for request {RequestId}.")]
    private static partial void LogConversationSetPrefsFailed(
        ILogger logger,
        uint connectionId,
        string requestId,
        Exception exception);

    [LoggerMessage(
        EventId = 12,
        Level = LogLevel.Warning,
        Message = "Connection {ConnectionId} could not recall message for request {RequestId}.")]
    private static partial void LogMessageRecallFailed(
        ILogger logger,
        uint connectionId,
        string requestId,
        Exception exception);

    [LoggerMessage(
        EventId = 62,
        Level = LogLevel.Warning,
        Message = "Connection {ConnectionId} could not edit message for request {RequestId}.")]
    private static partial void LogMessageEditFailed(
        ILogger logger,
        uint connectionId,
        string requestId,
        Exception exception);

    [LoggerMessage(
        EventId = 63,
        Level = LogLevel.Warning,
        Message = "Connection {ConnectionId} could not add reaction for request {RequestId}.")]
    private static partial void LogAddReactionFailed(
        ILogger logger,
        uint connectionId,
        string requestId,
        Exception exception);

    [LoggerMessage(
        EventId = 64,
        Level = LogLevel.Warning,
        Message = "Connection {ConnectionId} could not remove reaction for request {RequestId}.")]
    private static partial void LogRemoveReactionFailed(
        ILogger logger,
        uint connectionId,
        string requestId,
        Exception exception);

    [LoggerMessage(
        EventId = 10,
        Level = LogLevel.Warning,
        Message = "Connection {ConnectionId} could not query sync bootstrap for request {RequestId}.")]
    private static partial void LogSyncBootstrapQueryFailed(
        ILogger logger,
        uint connectionId,
        string requestId,
        Exception exception);

    [LoggerMessage(
        EventId = 13,
        Level = LogLevel.Debug,
        Message = "Typing 发射扇出失败 Sender={SenderUserId} Conversation={ConversationId}")]
    private static partial void LogTypingFanoutFailed(
        ILogger logger,
        long senderUserId,
        string conversationId,
        Exception exception);

    [LoggerMessage(
        EventId = 14,
        Level = LogLevel.Debug,
        Message = "Ephemeral Typing 发布失败 Target={TargetUserId}")]
    private static partial void LogEphemeralTypingPublishFailed(
        ILogger logger,
        long targetUserId,
        Exception exception);

    [LoggerMessage(
        EventId = 15,
        Level = LogLevel.Warning,
        Message = "Presence 好友鉴权失败，返回空快照 UserId={UserId}")]
    private static partial void LogPresenceAuthorizeFailed(
        ILogger logger,
        long userId,
        Exception exception);

    [LoggerMessage(
        EventId = 16,
        Level = LogLevel.Debug,
        Message = "Ephemeral Presence 发布失败 UserId={UserId}")]
    private static partial void LogEphemeralPresencePublishFailed(
        ILogger logger,
        long userId,
        Exception exception);

    private sealed record ClientTaskContext(
        ConcurrentDictionary<uint, Task> Tasks,
        uint ConnectionId);
}




