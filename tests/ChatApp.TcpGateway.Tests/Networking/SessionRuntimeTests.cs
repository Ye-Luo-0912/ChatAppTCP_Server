using System.Buffers;
using System.Diagnostics.Metrics;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading.Channels;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Ephemeral;
using ChatApp.TcpGateway.Gateway.Networking.Executor;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.TcpGateway.Tests.Networking;

/// <summary>
/// <see cref="SessionRuntime"/> 独立单测：聚焦 <see cref="SessionRuntime.DispatchFrameAsync"/> 的
/// lane 路由、限流、能力门控、负载体积上限与资源所有权转移。
/// <para>
/// 不依赖真实网络 I/O——构造最小 SessionRuntime + 已关闭 socket 的 TcpClientSession，
/// 通过捕获委托回调验证协议错误码、关闭原因与 executor 入队行为。
/// </para>
/// <para>
/// 使用 MeterListener 串行集合避免并行测试污染指标捕获。
/// </para>
/// </summary>
[Collection("MeterListenerSerial")]
public sealed class SessionRuntimeTests
{
    [Fact]
    public async Task DispatchFrameAsync_RejectsOversizedPayload_AndClosesSession()
    {
        await using var captured = CreateRuntime(maxInboundPayloadBytes: 0);
        var runtime = captured.Runtime!;
        await using var session = CreateSession();
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);
        session.Authenticate(userId: 1, sessionId: "s", deviceIdHash: null);

        // 构造超体积 payload（1 字节超过 MaxInboundPayloadBytes=0）。
        var payload = new ReadOnlySequence<byte>(new byte[1]);
        var frame = new PacketFrame(PacketCommand.Heartbeat, payload);

        var keepReading = await runtime.DispatchFrameAsync(
            frame, session, "1.2.3.4", default, CancellationToken.None);

        Assert.False(keepReading);
        Assert.False(session.IsConnected);
        Assert.Equal(SessionCloseReason.ProtocolViolation, session.CloseReason);
        Assert.NotNull(captured.LastOversizedRejectedCommand);
        Assert.Equal(PacketCommand.Heartbeat, captured.LastOversizedRejectedCommand.Value);
    }

    [Fact]
    public async Task DispatchFrameAsync_RejectsUnknownCommand_AndClosesSession()
    {
        await using var captured = CreateRuntime(maxInboundPayloadBytes: 1024);
        var runtime = captured.Runtime!;
        await using var session = CreateSession();
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);
        session.Authenticate(userId: 1, sessionId: "s", deviceIdHash: null);

        // (PacketCommand)9999 不在 catalog 中 → TryGetDescriptor 返回 null → RejectInvalidPacket。
        var unknown = (PacketCommand)9999;
        var frame = new PacketFrame(unknown, ReadOnlySequence<byte>.Empty);

        var keepReading = await runtime.DispatchFrameAsync(
            frame, session, "1.2.3.4", default, CancellationToken.None);

        Assert.False(keepReading);
        Assert.False(session.IsConnected);
        Assert.Equal(SessionCloseReason.ProtocolViolation, session.CloseReason);
        Assert.NotNull(captured.LastProtocolError);
        Assert.Equal(ProtocolErrorCode.ProtocolViolation, captured.LastProtocolError.Value.errorCode);
        Assert.True(captured.LastProtocolError.Value.isFatal);
    }

    [Fact]
    public async Task DispatchFrameAsync_RateLimited_SendsErrorAndKeepsConnectionOpen()
    {
        await using var captured = CreateRuntime(
            maxInboundPayloadBytes: 1024,
            maxPacketsPerSecond: 1,
            maxInboundBytesPerSecond: 1024 * 1024);
        var runtime = captured.Runtime!;
        await using var session = CreateSession();
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);
        session.Authenticate(userId: 1, sessionId: "s", deviceIdHash: null);

        var frame = new PacketFrame(
            PacketCommand.Heartbeat, ReadOnlySequence<byte>.Empty);

        // 第一次：放行（满桶初始化）。
        var first = await runtime.DispatchFrameAsync(
            frame, session, "1.2.3.4", default, CancellationToken.None);
        Assert.True(first);
        Assert.Null(captured.LastProtocolError);

        // 第二次：超出 1 包/秒桶容量 → RateLimited，但 keepReading=true（连接保持）。
        var second = await runtime.DispatchFrameAsync(
            frame, session, "1.2.3.4", default, CancellationToken.None);

        Assert.True(second);
        Assert.True(session.IsConnected);
        Assert.NotNull(captured.LastProtocolError);
        Assert.Equal(ProtocolErrorCode.RateLimited, captured.LastProtocolError.Value.errorCode);
        Assert.False(captured.LastProtocolError.Value.isFatal);
        Assert.NotNull(captured.LastProtocolError.Value.retryAfter);
    }

    [Fact]
    public async Task DispatchFrameAsync_FeatureNotNegotiated_SendsErrorAndKeepsConnectionOpen()
    {
        await using var captured = CreateRuntime(maxInboundPayloadBytes: 1024);
        var runtime = captured.Runtime!;
        await using var session = CreateSession();
        // 协商 CommandCapabilities 但不包含 PresenceAndTyping。
        session.CompleteHandshake(
            protocolVersion: 1,
            featureBits: (uint)GatewayFeature.CommandCapabilities);
        session.Authenticate(userId: 1, sessionId: "s", deviceIdHash: null);

        // TypingNotify 需要 PresenceAndTyping 能力位 → FeatureNotNegotiated。
        var frame = new PacketFrame(
            PacketCommand.TypingNotify, ReadOnlySequence<byte>.Empty);

        var keepReading = await runtime.DispatchFrameAsync(
            frame, session, "1.2.3.4", default, CancellationToken.None);

        Assert.True(keepReading);
        Assert.True(session.IsConnected);
        Assert.NotNull(captured.LastProtocolError);
        Assert.Equal(ProtocolErrorCode.FeatureNotNegotiated, captured.LastProtocolError.Value.errorCode);
        Assert.False(captured.LastProtocolError.Value.isFatal);
    }

    [Fact]
    public async Task DispatchFrameAsync_FeatureAllowed_WhenCommandCapabilitiesNotNegotiated()
    {
        // v1 兼容：未协商 CommandCapabilities 时所有命令放行（无 RequiredFeature 强制）。
        await using var captured = CreateRuntime(maxInboundPayloadBytes: 1024);
        var runtime = captured.Runtime!;
        await using var session = CreateSession();
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);
        session.Authenticate(userId: 1, sessionId: "s", deviceIdHash: null);

        var processedTcs = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        captured.OnProcessPacket = _ =>
        {
            processedTcs.TrySetResult(true);
            return ValueTask.CompletedTask;
        };

        // Inline 命令 Heartbeat（无 RequiredFeature）。
        var frame = new PacketFrame(
            PacketCommand.Heartbeat, ReadOnlySequence<byte>.Empty);

        var keepReading = await runtime.DispatchFrameAsync(
            frame, session, "1.2.3.4", default, CancellationToken.None);

        Assert.True(keepReading);
        Assert.True(session.IsConnected);
        Assert.Null(captured.LastProtocolError);
        Assert.True(processedTcs.Task.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task DispatchFrameAsync_InlineLane_InvokesProcessPacketCallback()
    {
        await using var captured = CreateRuntime(maxInboundPayloadBytes: 1024);
        var runtime = captured.Runtime!;
        await using var session = CreateSession();
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);
        session.Authenticate(userId: 1, sessionId: "s", deviceIdHash: null);

        var invoked = false;
        captured.OnProcessPacket = _ =>
        {
            invoked = true;
            return ValueTask.CompletedTask;
        };

        var frame = new PacketFrame(
            PacketCommand.Heartbeat, ReadOnlySequence<byte>.Empty);

        var keepReading = await runtime.DispatchFrameAsync(
            frame, session, "1.2.3.4", default, CancellationToken.None);

        Assert.True(keepReading);
        Assert.True(invoked);
        Assert.True(session.IsConnected);
    }

    [Fact]
    public async Task DispatchFrameAsync_InlineLaneJsonException_ClosesSession()
    {
        await using var captured = CreateRuntime(maxInboundPayloadBytes: 1024);
        var runtime = captured.Runtime!;
        await using var session = CreateSession();
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);
        session.Authenticate(userId: 1, sessionId: "s", deviceIdHash: null);

        captured.OnProcessPacket = _ => throw new JsonException("malformed payload");

        var frame = new PacketFrame(
            PacketCommand.Heartbeat, ReadOnlySequence<byte>.Empty);

        var keepReading = await runtime.DispatchFrameAsync(
            frame, session, "1.2.3.4", default, CancellationToken.None);

        Assert.False(keepReading);
        Assert.False(session.IsConnected);
        Assert.Equal(SessionCloseReason.ProtocolViolation, session.CloseReason);
    }

    [Fact]
    public async Task DispatchFrameAsync_QueryLane_EnqueuesToQueryExecutor()
    {
        await using var captured = CreateRuntime(maxInboundPayloadBytes: 1024);
        var runtime = captured.Runtime!;
        await using var session = CreateSession(connectionId: 77u);
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);
        session.Authenticate(userId: 1, sessionId: "s", deviceIdHash: null);

        Assert.True(SessionCommandRegistrationSet.TryRegister(
            77u,
            userId: 1,
            captured.OrderedWriteExecutor!,
            captured.QueryExecutor!,
            captured.EphemeralPipeline!,
            out var registrations));

        var payloadBytes = new byte[8];
        var frame = new PacketFrame(
            PacketCommand.MessageHistoryRequest,
            new ReadOnlySequence<byte>(payloadBytes));

        var keepReading = await runtime.DispatchFrameAsync(
            frame, session, "1.2.3.4", registrations, CancellationToken.None);

        Assert.True(keepReading);
        Assert.True(session.IsConnected);
        Assert.Null(captured.LastProtocolError);
        // 启动 executor 处理入队命令并验证 processor 收到。
        await captured.QueryExecutor!.StartAsync(CancellationToken.None);
        var processed = await captured.QueryProcessed.Reader.ReadAsync(
            TestContext.Current.CancellationToken);
        Assert.Equal(PacketCommand.MessageHistoryRequest, processed.Command);
        registrations.Unregister();
    }

    [Fact]
    public async Task DispatchFrameAsync_OrderedWriteLane_EnqueuesToOrderedWriteExecutor()
    {
        await using var captured = CreateRuntime(maxInboundPayloadBytes: 1024);
        var runtime = captured.Runtime!;
        await using var session = CreateSession(connectionId: 88u);
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);
        session.Authenticate(userId: 1, sessionId: "s", deviceIdHash: null);

        Assert.True(SessionCommandRegistrationSet.TryRegister(
            88u,
            userId: 1,
            captured.OrderedWriteExecutor!,
            captured.QueryExecutor!,
            captured.EphemeralPipeline!,
            out var registrations));

        var payloadBytes = new byte[8];
        var frame = new PacketFrame(
            PacketCommand.MessageReceipt,
            new ReadOnlySequence<byte>(payloadBytes));

        var keepReading = await runtime.DispatchFrameAsync(
            frame, session, "1.2.3.4", registrations, CancellationToken.None);

        Assert.True(keepReading);
        Assert.True(session.IsConnected);
        await captured.OrderedWriteExecutor!.StartAsync(CancellationToken.None);
        var processed = await captured.OrderedWriteProcessed.Reader.ReadAsync(
            TestContext.Current.CancellationToken);
        Assert.Equal(PacketCommand.MessageReceipt, processed.Command);
        registrations.Unregister();
    }

    [Fact]
    public async Task DispatchFrameAsync_EphemeralLane_EnqueuesToEphemeralPipeline()
    {
        // 未注入 TypingActorPipeline → Ephemeral lane 走 _ephemeralPipeline.TryEnqueue 路径。
        await using var captured = CreateRuntime(maxInboundPayloadBytes: 1024);
        var runtime = captured.Runtime!;
        await using var session = CreateSession(connectionId: 99u);
        session.CompleteHandshake(
            protocolVersion: 1,
            featureBits: (uint)GatewayFeature.CommandCapabilities |
                         (uint)GatewayFeature.PresenceAndTyping);
        session.Authenticate(userId: 1, sessionId: "s", deviceIdHash: null);

        Assert.True(SessionCommandRegistrationSet.TryRegister(
            99u,
            userId: 1,
            captured.OrderedWriteExecutor!,
            captured.QueryExecutor!,
            captured.EphemeralPipeline!,
            out var registrations));
        await captured.EphemeralPipeline!.StartAsync(CancellationToken.None);

        var payloadBytes = new byte[8];
        var frame = new PacketFrame(
            PacketCommand.TypingNotify,
            new ReadOnlySequence<byte>(payloadBytes));

        var keepReading = await runtime.DispatchFrameAsync(
            frame, session, "1.2.3.4", registrations, CancellationToken.None);

        Assert.True(keepReading);
        Assert.True(session.IsConnected);
        var processed = await captured.EphemeralProcessed.Reader.ReadAsync(
            TestContext.Current.CancellationToken);
        Assert.Equal(PacketCommand.TypingNotify, processed.Command);
        registrations.Unregister();
    }

    [Fact]
    public async Task DispatchFrameAsync_EphemeralLaneEnqueueFailure_DropsSilentlyAndKeepsConnection()
    {
        // Ephemeral lane 入队失败（Disabled 模式 TryEnqueue 返回 false）→ 丢弃当前帧但 keepReading=true（drop/coalesce 语义）。
        await using var captured = CreateRuntime(
            maxInboundPayloadBytes: 1024,
            ephemeralMode: EphemeralPipelineMode.Disabled);
        var runtime = captured.Runtime!;
        await using var session = CreateSession(connectionId: 100u);
        session.CompleteHandshake(
            protocolVersion: 1,
            featureBits: (uint)GatewayFeature.CommandCapabilities |
                         (uint)GatewayFeature.PresenceAndTyping);
        session.Authenticate(userId: 1, sessionId: "s", deviceIdHash: null);

        var frame = new PacketFrame(
            PacketCommand.TypingNotify,
            new ReadOnlySequence<byte>(new byte[8]));

        var keepReading = await runtime.DispatchFrameAsync(
            frame, session, "1.2.3.4", default, CancellationToken.None);

        Assert.True(keepReading);
        Assert.True(session.IsConnected);
    }

    [Fact]
    public async Task DispatchFrameAsync_OrderedWriteLaneEnqueueFailure_ClosesWithOutboundQueueFull()
    {
        // 非 Ephemeral lane 入队失败（连接未注册 → TryEnqueue 返回 false）→ 关闭连接（OutboundQueueFull）。
        await using var captured = CreateRuntime(maxInboundPayloadBytes: 1024);
        var runtime = captured.Runtime!;
        await using var session = CreateSession(connectionId: 101u);
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);
        session.Authenticate(userId: 1, sessionId: "s", deviceIdHash: null);

        // 不注册连接 → TryEnqueue 立即失败。
        var frame = new PacketFrame(
            PacketCommand.MessageReceipt,
            new ReadOnlySequence<byte>(new byte[8]));

        var keepReading = await runtime.DispatchFrameAsync(
            frame, session, "1.2.3.4", default, CancellationToken.None);

        Assert.False(keepReading);
        Assert.False(session.IsConnected);
        Assert.Equal(SessionCloseReason.OutboundQueueFull, session.CloseReason);
    }

    [Fact]
    public async Task DispatchFrameAsync_InboundBudgetExhausted_ClosesWithInboundBudgetExceeded()
    {
        await using var captured = CreateRuntime(
            maxInboundPayloadBytes: 1024,
            globalInboundBudgetBytes: 16);
        var runtime = captured.Runtime!;
        await using var session = CreateSession();
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);
        session.Authenticate(userId: 1, sessionId: "s", deviceIdHash: null);

        // Payload 32 字节 > 全局预算 16 字节 → 入站预算拒绝。
        var frame = new PacketFrame(
            PacketCommand.MessageReceipt,
            new ReadOnlySequence<byte>(new byte[32]));

        var keepReading = await runtime.DispatchFrameAsync(
            frame, session, "1.2.3.4", default, CancellationToken.None);

        Assert.False(keepReading);
        Assert.False(session.IsConnected);
        Assert.Equal(SessionCloseReason.InboundBudgetExceeded, session.CloseReason);
    }

    [Fact]
    public async Task DispatchFrameAsync_OwnedPayloadWithoutBudgetReserved_ThrowsInvalidOperationException()
    {
        await using var captured = CreateRuntime(maxInboundPayloadBytes: 1024);
        var runtime = captured.Runtime!;
        await using var session = CreateSession();
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);
        session.Authenticate(userId: 1, sessionId: "s", deviceIdHash: null);

        var ownedBuffer = ArrayPool<byte>.Shared.Rent(8);
        var frame = new PacketFrame(
            PacketCommand.MessageReceipt,
            new ReadOnlySequence<byte>(ownedBuffer, 0, 8));

        // ownedPayloadBudgetReserved=false 但 ownedPayloadBuffer 非空 → 抛 InvalidOperationException。
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            runtime.DispatchFrameAsync(
                frame, session, "1.2.3.4", default, CancellationToken.None,
                ownedPayloadBuffer: ownedBuffer,
                ownedPayloadBudgetReserved: false).AsTask());
    }

    [Fact]
    public async Task DispatchFrameAsync_OwnedPayloadOnEnqueueFailure_ReleasesBufferAndBudget()
    {
        // Owned payload + 入队失败（连接未注册）：buffer 与 budget 必须归还，避免 ArrayPool/budget 泄漏。
        await using var captured = CreateRuntime(
            maxInboundPayloadBytes: 1024,
            globalInboundBudgetBytes: 1024);
        var runtime = captured.Runtime!;
        await using var session = CreateSession(connectionId: 102u);
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);
        session.Authenticate(userId: 1, sessionId: "s", deviceIdHash: null);

        // 不注册连接 → TryEnqueue 立即失败。
        var ownedBuffer = ArrayPool<byte>.Shared.Rent(8);
        var frame = new PacketFrame(
            PacketCommand.MessageReceipt,
            new ReadOnlySequence<byte>(ownedBuffer, 0, 8));

        var keepReading = await runtime.DispatchFrameAsync(
            frame, session, "1.2.3.4", default, CancellationToken.None,
            ownedPayloadBuffer: ownedBuffer,
            ownedPayloadBudgetReserved: true);

        Assert.False(keepReading);
        Assert.False(session.IsConnected);
        // Budget 必须已归还：再尝试 Reserve 8 字节应成功（未泄漏）。
        Assert.True(captured.GlobalInboundBudget!.TryReserve(8));
        captured.GlobalInboundBudget.Release(8);
    }

    [Fact]
    public async Task DispatchFrameAsync_PacketReceivedMetricIncremented_OnValidFrame()
    {
        await using var captured = CreateRuntime(maxInboundPayloadBytes: 1024);
        var runtime = captured.Runtime!;
        await using var session = CreateSession();
        session.CompleteHandshake(protocolVersion: 1, featureBits: 0);
        session.Authenticate(userId: 1, sessionId: "s", deviceIdHash: null);

        using var listener = new SingleCounterListener("gateway.packets.received");
        var frame = new PacketFrame(
            PacketCommand.Heartbeat, ReadOnlySequence<byte>.Empty);

        await runtime.DispatchFrameAsync(
            frame, session, "1.2.3.4", default, CancellationToken.None);

        Assert.True(listener.WaitForIncrement(TimeSpan.FromSeconds(2)),
            "gateway.packets.received was not incremented");
    }

    private static CapturedCallbacks CreateRuntime(
        int maxInboundPayloadBytes = PacketProtocol.MaxPayloadSize,
        int maxPacketsPerSecond = 200,
        long maxInboundBytesPerSecond = 512 * 1024,
        long globalInboundBudgetBytes = 64 * 1024,
        int orderedWriteCapacity = 16,
        int ephemeralPipelineCapacity = 16,
        EphemeralPipelineMode? ephemeralMode = null)
    {
        var options = new TcpGatewayOptions
        {
            MaxInboundPayloadBytes = maxInboundPayloadBytes,
            MaxPacketsPerSecond = maxPacketsPerSecond,
            MaxInboundBytesPerSecond = maxInboundBytesPerSecond,
            RequireClientHello = false,
            CommandSchedulerEphemeralCapacity = Math.Max(1, ephemeralPipelineCapacity),
            EphemeralActorShardCount = 1,
            EphemeralActorIngressCapacity = Math.Max(16, ephemeralPipelineCapacity * 4),
            EphemeralActorAsyncConcurrency = 1,
            EphemeralActorIdleTimeout = TimeSpan.FromSeconds(1),
            EphemeralActorOperationTimeout = TimeSpan.FromSeconds(1)
        };

        var metrics = new GatewayMetrics();
        var timeProvider = TimeProvider.System;
        var captured = new CapturedCallbacks();

        var orderedWriteProcessed = Channel.CreateUnbounded<SessionCommand>();
        var queryProcessed = Channel.CreateUnbounded<SessionCommand>();
        var ephemeralProcessed = Channel.CreateUnbounded<SessionCommand>();

        var orderedWriteExecutor = new SessionCommandExecutor(
            (cmd, _) =>
            {
                orderedWriteProcessed.Writer.TryWrite(cmd);
                return ValueTask.CompletedTask;
            },
            workerCount: 1,
            burstLimit: 4,
            perConnectionCapacity: orderedWriteCapacity,
            globalCapacity: 16,
            commandTimeout: TimeSpan.Zero,
            perUserConcurrency: 0,
            onFatalError: null);

        var queryExecutor = new SessionCommandExecutor(
            (cmd, _) =>
            {
                queryProcessed.Writer.TryWrite(cmd);
                return ValueTask.CompletedTask;
            },
            workerCount: 1,
            burstLimit: 4,
            perConnectionCapacity: 16,
            globalCapacity: 16,
            commandTimeout: TimeSpan.Zero,
            perUserConcurrency: 0,
            onFatalError: null);

        var ephemeralPipeline = new EphemeralCommandPipeline(
            options,
            ephemeralMode,
            (cmd, _) =>
            {
                ephemeralProcessed.Writer.TryWrite(cmd);
                return ValueTask.CompletedTask;
            },
            metrics,
            timeProvider,
            NullLogger.Instance);

        var globalInboundBudget = new GlobalInboundBudget(globalInboundBudgetBytes);

        captured.QueryExecutor = queryExecutor;
        captured.OrderedWriteExecutor = orderedWriteExecutor;
        captured.EphemeralPipeline = ephemeralPipeline;
        captured.GlobalInboundBudget = globalInboundBudget;
        captured.OrderedWriteProcessed = orderedWriteProcessed;
        captured.QueryProcessed = queryProcessed;
        captured.EphemeralProcessed = ephemeralProcessed;

        var runtime = new SessionRuntime(
            options: options,
            pipeOptions: new PipeOptions(
                pauseWriterThreshold: 32 * 1024,
                resumeWriterThreshold: 16 * 1024,
                minimumSegmentSize: 1024),
            globalInboundBudget: globalInboundBudget,
            orderedWriteExecutor: orderedWriteExecutor,
            queryExecutor: queryExecutor,
            ephemeralPipeline: ephemeralPipeline,
            typingActorPipeline: null,
            metrics: metrics,
            timeProvider: timeProvider,
            logger: NullLogger.Instance,
            deadlineWheel: null!,
            frameAssemblyTracker: null,
            processPacketAsync: (frame, sess, ip, ct) =>
            {
                if (captured.OnProcessPacket is { } cb)
                    return cb(frame);
                return ValueTask.CompletedTask;
            },
            sendProtocolError: (sess, code, msg, fatal, retry, cmd) =>
            {
                captured.LastProtocolError = (code, msg, fatal, retry, cmd);
            },
            rejectOversizedPayload: (sess, cmd) =>
            {
                captured.LastOversizedRejectedCommand = cmd;
                // 与 TcpGatewayService.RejectOversizedPayload 行为一致：
                // 非 ChatMessage 命令直接关闭连接（ProtocolViolation）。
                sess.Close(SessionCloseReason.ProtocolViolation);
            });

        captured.Runtime = runtime;
        return captured;
    }

    private static TcpClientSession CreateSession(uint connectionId = 1u)
    {
        var socket = new Socket(
            AddressFamily.InterNetwork,
            SocketType.Stream,
            ProtocolType.Tcp);

        return new TcpClientSession(
            socket: socket,
            connectionId: connectionId,
            outboundQueueCapacity: 16,
            maxOutboundQueuedBytes: 4096,
            sendTimeout: TimeSpan.FromSeconds(5),
            timeProvider: TimeProvider.System,
            metrics: new GatewayMetrics(),
            logger: NullLogger<TcpClientSession>.Instance,
            globalOutboundBudget: null,
            authenticationTimeout: default,
            deadlineWheel: null,
            idleTimeout: default,
            outboundPump: null);
    }

    private sealed class CapturedCallbacks : IAsyncDisposable
    {
        public SessionRuntime? Runtime { get; set; }
        public SessionCommandExecutor? QueryExecutor { get; set; }
        public SessionCommandExecutor? OrderedWriteExecutor { get; set; }
        public EphemeralCommandPipeline? EphemeralPipeline { get; set; }
        public GlobalInboundBudget? GlobalInboundBudget { get; set; }
        public Channel<SessionCommand> OrderedWriteProcessed { get; set; } = Channel.CreateUnbounded<SessionCommand>();
        public Channel<SessionCommand> QueryProcessed { get; set; } = Channel.CreateUnbounded<SessionCommand>();
        public Channel<SessionCommand> EphemeralProcessed { get; set; } = Channel.CreateUnbounded<SessionCommand>();

        public (ProtocolErrorCode errorCode, string? message, bool isFatal, int? retryAfter, ushort? command)?
            LastProtocolError { get; set; }
        public PacketCommand? LastOversizedRejectedCommand { get; set; }
        public Func<PacketFrame, ValueTask>? OnProcessPacket { get; set; }

        public async ValueTask DisposeAsync()
        {
            OrderedWriteProcessed.Writer.TryComplete();
            QueryProcessed.Writer.TryComplete();
            EphemeralProcessed.Writer.TryComplete();
            if (EphemeralPipeline is not null)
                await EphemeralPipeline.DisposeAsync();
            if (OrderedWriteExecutor is not null)
                await OrderedWriteExecutor.DisposeAsync();
            if (QueryExecutor is not null)
                await QueryExecutor.DisposeAsync();
        }
    }

    /// <summary>
    /// 单一指标监听器：捕获指定 Counter 的累计增量。
    /// 必须串行化（MeterListener 全局监听器会捕获并行测试的测量）。
    /// </summary>
    private sealed class SingleCounterListener : IDisposable
    {
        private readonly MeterListener _listener = new();
        private long _count;
        private readonly string _instrumentName;

        public SingleCounterListener(string instrumentName)
        {
            _instrumentName = instrumentName;
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == GatewayMetrics.MeterName
                    && instrument.Name == _instrumentName)
                {
                    listener.EnableMeasurementEvents(instrument, this);
                }
            };
            _listener.SetMeasurementEventCallback<long>(static (_, measurement, _, state) =>
            {
                if (state is SingleCounterListener l && measurement > 0)
                    Interlocked.Add(ref l._count, measurement);
            });
            _listener.Start();
        }

        public long Count => Volatile.Read(ref _count);

        public bool WaitForIncrement(TimeSpan timeout)
        {
            var deadline = Environment.TickCount64 + (long)timeout.TotalMilliseconds;
            while (Environment.TickCount64 < deadline)
            {
                if (Volatile.Read(ref _count) > 0)
                    return true;
                Thread.Sleep(10);
            }
            return false;
        }

        public void Dispose() => _listener.Dispose();
    }
}
