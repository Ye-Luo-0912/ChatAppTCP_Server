using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Integration;
using System.Text.Json;
using ChatApp.TcpGateway.Configuration;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.History;
using ChatApp.TcpGateway.Core.Messaging.Sync;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Diagnostics;
using ChatApp.TcpGateway.Networking.Buffers;
using ChatApp.TcpGateway.Networking.Sessions;
using ChatApp.TcpGateway.Messaging;
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
using RealtimeSyncBootstrapQuery =
    ChatApp.Realtime.Abstractions.Sync.SyncBootstrapQuery;
using RealtimeConversationSyncWatermark =
    ChatApp.Realtime.Abstractions.Sync.ConversationSyncWatermark;

namespace ChatApp.TcpGateway.Networking;

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
    private readonly IPayloadCodec<ConversationListRequest> _conversationListRequestCodec;
    private readonly IPayloadCodec<ConversationListResponse> _conversationListResponseCodec;
    private readonly IPayloadCodec<ConversationMarkReadRequest> _conversationMarkReadRequestCodec;
    private readonly IPayloadCodec<ConversationMarkReadResponse> _conversationMarkReadResponseCodec;
    private readonly IPayloadCodec<ConversationSetPrefsRequest> _conversationSetPrefsRequestCodec;
    private readonly IPayloadCodec<ConversationSetPrefsResponse> _conversationSetPrefsResponseCodec;
    private readonly IPayloadCodec<MessageRecallRequest> _messageRecallRequestCodec;
    private readonly IPayloadCodec<MessageRecallAcknowledgement> _messageRecallAcknowledgementCodec;
    private readonly IPayloadCodec<SyncBootstrapRequest> _syncBootstrapRequestCodec;
    private readonly IPayloadCodec<SyncBootstrapResponse> _syncBootstrapResponseCodec;
    private readonly IRealtimeMessageBus _messageBus;
    private readonly IDeviceSessionLeaseStore _deviceSessionLeaseStore;
    private readonly GatewayMetrics _metrics;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<TcpGatewayService> _logger;
    private readonly ILogger<TcpClientSession> _sessionLogger;
    private readonly PipeOptions _pipeOptions;
    private readonly SemaphoreSlim _connectionSlots;
    private readonly ConcurrentDictionary<uint, TcpClientSession> _sessions = new();
    private readonly ConcurrentDictionary<uint, Task> _clientTasks = new();
    private readonly UserSessionRegistry _userSessions;

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
        IPayloadCodec<ConversationListRequest> conversationListRequestCodec,
        IPayloadCodec<ConversationListResponse> conversationListResponseCodec,
        IPayloadCodec<ConversationMarkReadRequest> conversationMarkReadRequestCodec,
        IPayloadCodec<ConversationMarkReadResponse> conversationMarkReadResponseCodec,
        IPayloadCodec<ConversationSetPrefsRequest> conversationSetPrefsRequestCodec,
        IPayloadCodec<ConversationSetPrefsResponse> conversationSetPrefsResponseCodec,
        IPayloadCodec<MessageRecallRequest> messageRecallRequestCodec,
        IPayloadCodec<MessageRecallAcknowledgement> messageRecallAcknowledgementCodec,
        IPayloadCodec<SyncBootstrapRequest> syncBootstrapRequestCodec,
        IPayloadCodec<SyncBootstrapResponse> syncBootstrapResponseCodec,
        IRealtimeMessageBus messageBus,
        IDeviceSessionLeaseStore deviceSessionLeaseStore,
        UserSessionRegistry userSessions,
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
        _conversationListRequestCodec = conversationListRequestCodec;
        _conversationListResponseCodec = conversationListResponseCodec;
        _conversationMarkReadRequestCodec = conversationMarkReadRequestCodec;
        _conversationMarkReadResponseCodec = conversationMarkReadResponseCodec;
        _conversationSetPrefsRequestCodec = conversationSetPrefsRequestCodec;
        _conversationSetPrefsResponseCodec = conversationSetPrefsResponseCodec;
        _messageRecallRequestCodec = messageRecallRequestCodec;
        _messageRecallAcknowledgementCodec = messageRecallAcknowledgementCodec;
        _syncBootstrapRequestCodec = syncBootstrapRequestCodec;
        _syncBootstrapResponseCodec = syncBootstrapResponseCodec;
        _messageBus = messageBus;
        _deviceSessionLeaseStore = deviceSessionLeaseStore;
        _userSessions = userSessions;
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

        try
        {
            while (!executionToken.IsCancellationRequested)
            {
                var socket = await listener
                    .AcceptAsync(executionToken)
                    .ConfigureAwait(false);

                if (!_connectionSlots.Wait(0, CancellationToken.None))
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
            execution.Cancel();
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

            _userSessions.Remove(session);

            if (session.UserId > 0
                && session.DeviceIdHash is ulong deviceHash
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

            case PacketCommand.SyncBootstrapRequest:
                await HandleSyncBootstrapRequestAsync(
                        frame.Payload,
                        session,
                        cancellationToken)
                    .ConfigureAwait(false);
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
        _userSessions.Add(session);

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
        if (incoming.DeviceIdHash is not ulong deviceHash
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
        if (message is null ||
            message.TargetUserId <= 0 ||
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
            ReceiverUserId = message.TargetUserId,
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

            SendMessageHistoryResponse(
                session,
                new MessageHistoryResponse
                {
                    RequestId = page.RequestId,
                    Succeeded = page.Succeeded,
                    ErrorCode = page.ErrorCode,
                    ErrorMessage = page.ErrorMessage,
                    Items = page.Items
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
                            Attachments = AttachmentWireMapper.Map(item.Attachments),
                            ReplyToMessageId = item.ReplyToMessageId,
                            ReplyToSenderUserId = item.ReplyToSenderUserId,
                            ReplyToPreview = item.ReplyToPreview,
                            ForwardedFromMessageId = item.ForwardedFromMessageId,
                            ForwardedFromSenderUserId = item.ForwardedFromSenderUserId,
                            ForwardedFromPreview = item.ForwardedFromPreview
                        })
                        .ToArray(),
                    NextCursor = page.NextCursor is null
                        ? null
                        : new MessageHistoryCursor
                        {
                            ReceivedAtMs = page.NextCursor.ReceivedAtMs,
                            MessageId = page.NextCursor.MessageId
                        },
                    HasMore = page.HasMore
                });
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

            SendConversationListResponse(
                session,
                new ConversationListResponse
                {
                    RequestId = page.RequestId,
                    Succeeded = page.Succeeded,
                    ErrorCode = page.ErrorCode,
                    ErrorMessage = page.ErrorMessage,
                    Items = page.Items
                        .Select(static item => new Core.Messaging.Conversations.ConversationListItem
                        {
                            ConversationId = item.ConversationId,
                            Type = (Core.Messaging.Conversations.ConversationType)(byte)item.Type,
                            PeerUserId = item.PeerUserId,
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
                        .ToArray(),
                    NextCursor = page.NextCursor is null
                        ? null
                        : new Core.Messaging.Conversations.ConversationListCursor
                        {
                            IsPinned = page.NextCursor.IsPinned,
                            PinnedAtMs = page.NextCursor.PinnedAtMs,
                            LastMessageAtMs = page.NextCursor.LastMessageAtMs,
                            ConversationId = page.NextCursor.ConversationId
                        },
                    HasMore = page.HasMore
                });
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
            || request.HistoryLimitPerConversation < 0
            || request.MaxConversationsWithHistory < 0
            || request.Watermarks?.Count > 50
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

            SendSyncBootstrapResponse(
                session,
                new SyncBootstrapResponse
                {
                    RequestId = page.RequestId,
                    Succeeded = page.Succeeded,
                    ErrorCode = page.ErrorCode,
                    ErrorMessage = page.ErrorMessage,
                    ServerTimeMs = page.ServerTimeMs,
                    Conversations = page.Conversations
                        .Select(static item => new Core.Messaging.Conversations.ConversationListItem
                        {
                            ConversationId = item.ConversationId,
                            Type = (Core.Messaging.Conversations.ConversationType)(byte)item.Type,
                            PeerUserId = item.PeerUserId,
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
                        .ToArray(),
                    ConversationsNextCursor = page.ConversationsNextCursor is null
                        ? null
                        : new Core.Messaging.Conversations.ConversationListCursor
                        {
                            IsPinned = page.ConversationsNextCursor.IsPinned,
                            PinnedAtMs = page.ConversationsNextCursor.PinnedAtMs,
                            LastMessageAtMs = page.ConversationsNextCursor.LastMessageAtMs,
                            ConversationId = page.ConversationsNextCursor.ConversationId
                        },
                    ConversationsHasMore = page.ConversationsHasMore,
                    CatchUps = page.CatchUps
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
                                    Attachments = AttachmentWireMapper.Map(item.Attachments),
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
                        .ToArray()
                });
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

    private static string CreateCommandId(
        long senderUserId,
        string clientMessageId)
    {
        var source = Encoding.UTF8.GetBytes(
            $"{senderUserId}:{clientMessageId}");
        return Convert.ToHexStringLower(
            SHA256.HashData(source));
    }

    private static string CreateReceiptCommandId(
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
        EventId = 10,
        Level = LogLevel.Warning,
        Message = "Connection {ConnectionId} could not query sync bootstrap for request {RequestId}.")]
    private static partial void LogSyncBootstrapQueryFailed(
        ILogger logger,
        uint connectionId,
        string requestId,
        Exception exception);

    private sealed record ClientTaskContext(
        ConcurrentDictionary<uint, Task> Tasks,
        uint ConnectionId);
}




