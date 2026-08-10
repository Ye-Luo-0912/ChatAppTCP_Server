using System.Threading.Channels;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Networking.Executor;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;

namespace ChatApp.TcpGateway.Tests.Networking;

/// <summary>
/// <see cref="SessionCommandExecutor"/> 单元测试。
/// 验证注册/入队/burst 上限/per-connection 串行/跨连接并行/资源释放。
/// </summary>
public sealed class SessionCommandExecutorTests
{
    private static SessionCommand CreateCommand(
        TcpClientSession? session = null,
        int payloadLength = 0)
    {
        session ??= TestSessionFactory.Create();
        return new SessionCommand
        {
            Command = PacketCommand.Heartbeat,
            RentedBuffer = payloadLength > 0
                ? System.Buffers.ArrayPool<byte>.Shared.Rent(payloadLength)
                : Array.Empty<byte>(),
            PayloadLength = payloadLength,
            IsPooled = payloadLength > 0,
            ReservedInboundBytes = 0,
            InboundBudget = null,
            Session = session,
            RemoteIp = "127.0.0.1"
        };
    }

    [Fact]
    public async Task EnqueueBeforeRegisterReturnsFalse()
    {
        var executor = new SessionCommandExecutor(
            (_, _) => ValueTask.CompletedTask,
            workerCount: 1,
            burstLimit: 4,
            perConnectionCapacity: 8,
            globalCapacity: 16,
            commandTimeout: TimeSpan.Zero,
            perUserConcurrency: 0,
            onFatalError: null);

        var session = TestSessionFactory.Create();
        var command = CreateCommand(session);

        // 未注册连接：入队失败。
        var registration = default(SessionCommandExecutor.Registration);
        Assert.False(registration.TryEnqueue(in command));

        await executor.DisposeAsync();
        await session.DisposeAsync();
    }

    [Fact]
    public async Task RegisterIsIdempotent()
    {
        var executor = new SessionCommandExecutor(
            (_, _) => ValueTask.CompletedTask,
            workerCount: 1,
            burstLimit: 4,
            perConnectionCapacity: 8,
            globalCapacity: 16,
            commandTimeout: TimeSpan.Zero,
            perUserConcurrency: 0,
            onFatalError: null);

        Assert.True(executor.TryRegisterConnection(
            connectionId: 1u,
            userId: 100,
            out var registration));
        Assert.True(registration.IsValid);
        // 重复注册同一 connectionId：幂等返回 false。
        Assert.False(executor.TryRegisterConnection(
            connectionId: 1u,
            userId: 100,
            out var duplicate));
        Assert.False(duplicate.IsValid);
        duplicate.Unregister();
        Assert.Equal(1, executor.RegisteredConnectionCount);

        await executor.DisposeAsync();
    }

    [Fact]
    public async Task EnqueueBeyondPerConnectionCapacityReturnsFalse()
    {
        var processed = Channel.CreateUnbounded<SessionCommand>();
        var executor = new SessionCommandExecutor(
            (cmd, _) =>
            {
                processed.Writer.TryWrite(cmd);
                return ValueTask.CompletedTask;
            },
            workerCount: 1,
            burstLimit: 1,
            perConnectionCapacity: 2,
            globalCapacity: 16,
            commandTimeout: TimeSpan.Zero,
            perUserConcurrency: 0,
            onFatalError: null);

        using var cts = new CancellationTokenSource();
        await executor.StartAsync(cts.Token);

        var session = TestSessionFactory.Create();
        Assert.True(executor.TryRegisterConnection(
            session.ConnectionId,
            session.UserId,
            out var registration));

        // 入队 2 条（达到容量上限）。
        var c1 = CreateCommand(session);
        var c2 = CreateCommand(session);
        Assert.True(registration.TryEnqueue(in c1));
        Assert.True(registration.TryEnqueue(in c2));

        // 第 3 条：超过容量，返回 false。
        var c3 = CreateCommand(session);
        Assert.False(registration.TryEnqueue(in c3));

        // 等待前两条被处理。
        await processed.Reader.ReadAsync(TestContext.Current.CancellationToken);
        await processed.Reader.ReadAsync(TestContext.Current.CancellationToken);

        cts.Cancel();
        await executor.StopAsync(CancellationToken.None);
        await executor.DisposeAsync();
        await session.DisposeAsync();
    }

    [Fact]
    public async Task SameConnectionCommandsAreProcessedSerially()
    {
        var concurrent = 0;
        var maxConcurrent = 0;
        var processed = Channel.CreateUnbounded<int>();

        var executor = new SessionCommandExecutor(
            (_, _) =>
            {
                var current = Interlocked.Increment(ref concurrent);
                // 记录峰值并发。
                int observed;
                do
                {
                    observed = Volatile.Read(ref maxConcurrent);
                    if (current <= observed) break;
                } while (Interlocked.CompareExchange(
                    ref maxConcurrent, current, observed) != observed);

                Thread.Sleep(20); // 模拟处理耗时，扩大竞态窗口。
                Interlocked.Decrement(ref concurrent);
                processed.Writer.TryWrite(0);
                return ValueTask.CompletedTask;
            },
            workerCount: 4,
            burstLimit: 1, // burst=1 强制每命令后让出 worker，测试串行性。
            perConnectionCapacity: 16,
            globalCapacity: 32,
            commandTimeout: TimeSpan.Zero,
            perUserConcurrency: 0,
            onFatalError: null);

        using var cts = new CancellationTokenSource();
        await executor.StartAsync(cts.Token);

        var session = TestSessionFactory.Create();
        Assert.True(executor.TryRegisterConnection(
            session.ConnectionId,
            session.UserId,
            out var registration));

        for (var i = 0; i < 8; i++)
        {
            var cmd = CreateCommand(session);
            Assert.True(registration.TryEnqueue(in cmd));
        }

        // 等待 8 条全部处理完。
        for (var i = 0; i < 8; i++)
            await processed.Reader.ReadAsync(TestContext.Current.CancellationToken);

        // 同连接 burst=1 + 单连接同时只一个 worker 处理：峰值并发必须为 1。
        Assert.Equal(1, maxConcurrent);

        cts.Cancel();
        await executor.StopAsync(CancellationToken.None);
        await executor.DisposeAsync();
        await session.DisposeAsync();
    }

    [Fact]
    public async Task UnregisterConnectionReleasesPendingCommands()
    {
        var inboundBudget = new GlobalInboundBudget(maxBytes: 1024);
        var executor = new SessionCommandExecutor(
            (_, _) => ValueTask.CompletedTask,
            workerCount: 1,
            burstLimit: 1,
            perConnectionCapacity: 8,
            globalCapacity: 16,
            commandTimeout: TimeSpan.Zero,
            perUserConcurrency: 0,
            onFatalError: null);

        // 不启动 worker，使入队命令停留在队列中。
        var session = TestSessionFactory.Create();
        Assert.True(executor.TryRegisterConnection(
            session.ConnectionId,
            session.UserId,
            out var registration));

        // 入队带 pooled buffer + inbound budget 的命令。
        var payloadLen = 64;
        var budgetBefore = inboundBudget.CurrentBytes;
        var cmd = new SessionCommand
        {
            Command = PacketCommand.Heartbeat,
            RentedBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(payloadLen),
            PayloadLength = payloadLen,
            IsPooled = true,
            ReservedInboundBytes = payloadLen,
            InboundBudget = inboundBudget,
            Session = session,
            RemoteIp = "127.0.0.1"
        };
        inboundBudget.TryReserve(payloadLen);
        Assert.Equal(budgetBefore + payloadLen, inboundBudget.CurrentBytes);

        Assert.True(registration.TryEnqueue(in cmd));

        // 注销连接：应释放缓冲区与预算。
        registration.Unregister();

        // 预算已归还。
        Assert.Equal(budgetBefore, inboundBudget.CurrentBytes);

        await executor.DisposeAsync();
        await session.DisposeAsync();
    }

    [Fact]
    public async Task StopAsyncDrainsAllConnectionQueues()
    {
        var inboundBudget = new GlobalInboundBudget(maxBytes: 4096);
        var executor = new SessionCommandExecutor(
            (_, _) => ValueTask.CompletedTask,
            workerCount: 1,
            burstLimit: 1,
            perConnectionCapacity: 8,
            globalCapacity: 16,
            commandTimeout: TimeSpan.Zero,
            perUserConcurrency: 0,
            onFatalError: null);

        // 不启动 worker，命令停留在队列中。
        var session = TestSessionFactory.Create();
        Assert.True(executor.TryRegisterConnection(
            session.ConnectionId,
            session.UserId,
            out var registration));

        for (var i = 0; i < 3; i++)
        {
            var len = 32;
            inboundBudget.TryReserve(len);
            var cmd = new SessionCommand
            {
                Command = PacketCommand.Heartbeat,
                RentedBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(len),
                PayloadLength = len,
                IsPooled = true,
                ReservedInboundBytes = len,
                InboundBudget = inboundBudget,
                Session = session,
                RemoteIp = "127.0.0.1"
            };
            Assert.True(registration.TryEnqueue(in cmd));
        }

        Assert.Equal(96, inboundBudget.CurrentBytes);

        // StopAsync 会取消 worker、排空所有连接队列。
        await executor.StopAsync(CancellationToken.None);

        // 队列中残留命令的预算与缓冲区已释放。
        Assert.Equal(0, inboundBudget.CurrentBytes);

        await executor.DisposeAsync();
        await session.DisposeAsync();
    }

    [Fact]
    public async Task IdleRegistrationsDoNotAllocateCommandQueues()
    {
        var executor = new SessionCommandExecutor(
            (_, _) => ValueTask.CompletedTask,
            workerCount: 1,
            burstLimit: 4,
            perConnectionCapacity: 8,
            globalCapacity: 16,
            commandTimeout: TimeSpan.Zero,
            perUserConcurrency: 0,
            onFatalError: null);

        await using var session = TestSessionFactory.Create();
        const int connectionCount = 10_000;
        var registrations = new SessionCommandExecutor.Registration[connectionCount];
        for (uint connectionId = 1; connectionId <= connectionCount; connectionId++)
        {
            Assert.True(executor.TryRegisterConnection(
                connectionId,
                userId: 0,
                out registrations[connectionId - 1]));
        }

        Assert.Equal(connectionCount, executor.RegisteredConnectionCount);
        Assert.Equal(0, executor.AllocatedCommandQueueCount);

        var command = CreateCommand(session);
        Assert.True(registrations[0].TryEnqueue(in command));
        Assert.Equal(1, executor.AllocatedCommandQueueCount);

        foreach (var registration in registrations)
            registration.Unregister();

        Assert.Equal(0, executor.RegisteredConnectionCount);
        Assert.Equal(0, executor.AllocatedCommandQueueCount);
        await executor.DisposeAsync();
    }

    [Fact]
    public async Task UnregisterRacingFirstEnqueueNeverStrandsOwnedResources()
    {
        var budget = new GlobalInboundBudget(maxBytes: 4096);
        var executor = new SessionCommandExecutor(
            (_, _) => ValueTask.CompletedTask,
            workerCount: 1,
            burstLimit: 1,
            perConnectionCapacity: 8,
            globalCapacity: 16,
            commandTimeout: TimeSpan.Zero,
            perUserConcurrency: 0,
            onFatalError: null);

        await using var session = TestSessionFactory.Create();
        const int iterations = 256;
        for (uint connectionId = 1; connectionId <= iterations; connectionId++)
        {
            Assert.True(executor.TryRegisterConnection(
                connectionId,
                userId: 0,
                out var registration));
            Assert.True(budget.TryReserve(1));

            var command = new SessionCommand
            {
                Command = PacketCommand.Heartbeat,
                RentedBuffer = new byte[1],
                PayloadLength = 1,
                IsPooled = false,
                ReservedInboundBytes = 1,
                InboundBudget = budget,
                Session = session,
                RemoteIp = "127.0.0.1"
            };

            using var start = new Barrier(participantCount: 2);
            var enqueueTask = Task.Run(() =>
            {
                start.SignalAndWait(TestContext.Current.CancellationToken);
                var local = command;
                if (!registration.TryEnqueue(in local))
                    SessionCommandResources.Release(in local);
            }, TestContext.Current.CancellationToken);
            var unregisterTask = Task.Run(() =>
            {
                start.SignalAndWait(TestContext.Current.CancellationToken);
                registration.Unregister();
            }, TestContext.Current.CancellationToken);

            await Task.WhenAll(enqueueTask, unregisterTask);

            // 幂等关闭，并验证已捕获旧 holder 的生产者不能留下命令。
            registration.Unregister();
            var probe = CreateCommand(session);
            Assert.False(registration.TryEnqueue(in probe));
        }

        Assert.Equal(0, budget.CurrentBytes);
        Assert.Equal(0, executor.RegisteredConnectionCount);
        await executor.DisposeAsync();
    }

    [Fact]
    public async Task MultipleProducersPreservePerProducerFifoAndReleaseEveryCommand()
    {
        const int producerCount = 4;
        const int commandsPerProducer = 64;
        var expectedCount = producerCount * commandsPerProducer;
        var budget = new GlobalInboundBudget(maxBytes: expectedCount * 2L);
        var processed = Channel.CreateUnbounded<(byte Producer, byte Sequence)>();
        var executor = new SessionCommandExecutor(
            (command, _) =>
            {
                processed.Writer.TryWrite((
                    command.RentedBuffer[0],
                    command.RentedBuffer[1]));
                return ValueTask.CompletedTask;
            },
            workerCount: 4,
            burstLimit: 3,
            perConnectionCapacity: expectedCount,
            globalCapacity: 16,
            commandTimeout: TimeSpan.Zero,
            perUserConcurrency: 0,
            onFatalError: null);

        await using var session = TestSessionFactory.Create();
        Assert.True(executor.TryRegisterConnection(
            connectionId: 77,
            userId: 0,
            out var registration));
        await executor.StartAsync(TestContext.Current.CancellationToken);

        using var start = new Barrier(participantCount: producerCount);
        var producers = new Task[producerCount];
        for (var producerIndex = 0; producerIndex < producerCount; producerIndex++)
        {
            var producer = (byte)producerIndex;
            producers[producerIndex] = Task.Run(() =>
            {
                start.SignalAndWait(TestContext.Current.CancellationToken);
                for (byte sequence = 0; sequence < commandsPerProducer; sequence++)
                {
                    Assert.True(budget.TryReserve(2));
                    var command = new SessionCommand
                    {
                        Command = PacketCommand.Heartbeat,
                        RentedBuffer = [producer, sequence],
                        PayloadLength = 2,
                        IsPooled = false,
                        ReservedInboundBytes = 2,
                        InboundBudget = budget,
                        Session = session,
                        RemoteIp = "127.0.0.1"
                    };
                    Assert.True(registration.TryEnqueue(in command));
                }
            }, TestContext.Current.CancellationToken);
        }

        await Task.WhenAll(producers);

        var observed = new List<(byte Producer, byte Sequence)>(expectedCount);
        for (var i = 0; i < expectedCount; i++)
        {
            observed.Add(await processed.Reader.ReadAsync(
                TestContext.Current.CancellationToken));
        }

        Assert.True(SpinWait.SpinUntil(
            () => budget.CurrentBytes == 0,
            TimeSpan.FromSeconds(5)));
        Assert.Equal(expectedCount, observed.Distinct().Count());
        for (byte producer = 0; producer < producerCount; producer++)
        {
            Assert.Equal(
                Enumerable.Range(0, commandsPerProducer).Select(value => (byte)value),
                observed
                    .Where(item => item.Producer == producer)
                    .Select(item => item.Sequence));
        }

        await executor.StopAsync(CancellationToken.None);
        await executor.DisposeAsync();
    }

    [Fact]
    public async Task UnregisterDrainsPendingWhileInflightCommandRetainsSingleOwnership()
    {
        var processorEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseProcessor = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var processed = 0;
        var budget = new GlobalInboundBudget(maxBytes: 1024);
        var executor = new SessionCommandExecutor(
            async (_, cancellationToken) =>
            {
                Interlocked.Increment(ref processed);
                processorEntered.TrySetResult();
                await releaseProcessor.Task.WaitAsync(cancellationToken);
            },
            workerCount: 1,
            burstLimit: 8,
            perConnectionCapacity: 16,
            globalCapacity: 16,
            commandTimeout: TimeSpan.Zero,
            perUserConcurrency: 0,
            onFatalError: null);

        await using var session = TestSessionFactory.Create();
        Assert.True(executor.TryRegisterConnection(
            connectionId: 91,
            userId: 0,
            out var registration));
        await executor.StartAsync(TestContext.Current.CancellationToken);

        for (var i = 0; i < 9; i++)
        {
            Assert.True(budget.TryReserve(1));
            var command = new SessionCommand
            {
                Command = PacketCommand.Heartbeat,
                RentedBuffer = new byte[1],
                PayloadLength = 1,
                IsPooled = false,
                ReservedInboundBytes = 1,
                InboundBudget = budget,
                Session = session,
                RemoteIp = "127.0.0.1"
            };
            Assert.True(registration.TryEnqueue(in command));
        }

        await processorEntered.Task.WaitAsync(TestContext.Current.CancellationToken);
        registration.Unregister();

        // 8 条 pending 已由关闭路径释放；in-flight 仍由 processor 独占。
        Assert.Equal(1, budget.CurrentBytes);
        var rejected = CreateCommand(session);
        Assert.False(registration.TryEnqueue(in rejected));

        releaseProcessor.TrySetResult();
        Assert.True(SpinWait.SpinUntil(
            () => budget.CurrentBytes == 0,
            TimeSpan.FromSeconds(5)));
        Assert.Equal(1, Volatile.Read(ref processed));

        await executor.StopAsync(CancellationToken.None);
        Assert.False(executor.TryRegisterConnection(
            92,
            userId: 0,
            out _));
        await executor.DisposeAsync();
    }
}

/// <summary>
/// 测试专用 TcpClientSession 工厂：构造最小可用的 session 实例。
/// </summary>
internal static class TestSessionFactory
{
    public static TcpClientSession Create()
    {
        // 使用已关闭的 socket 构造测试 session：避免真实网络 I/O。
        // TcpClientSession 不在构造时访问 socket，仅在 Receive/Send 时使用。
        var socket = new System.Net.Sockets.Socket(
            System.Net.Sockets.AddressFamily.InterNetwork,
            System.Net.Sockets.SocketType.Stream,
            System.Net.Sockets.ProtocolType.Tcp);

        var metrics = new GatewayMetrics();
        var logger = Microsoft.Extensions.Logging.Abstractions
            .NullLogger<TcpClientSession>.Instance;

        return new TcpClientSession(
            socket: socket,
            connectionId: 1u,
            outboundQueueCapacity: 16,
            maxOutboundQueuedBytes: 4096,
            sendTimeout: TimeSpan.FromSeconds(5),
            timeProvider: TimeProvider.System,
            metrics: metrics,
            logger: logger,
            globalOutboundBudget: null,
            authenticationTimeout: default,
            deadlineWheel: null,
            idleTimeout: default,
            outboundPump: null);
    }
}
