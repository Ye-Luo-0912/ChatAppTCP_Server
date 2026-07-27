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
        Assert.False(executor.TryEnqueue(connectionId: 999u, in command));

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

        Assert.True(executor.TryRegisterConnection(connectionId: 1u, userId: 100));
        // 重复注册同一 connectionId：幂等返回 false。
        Assert.False(executor.TryRegisterConnection(connectionId: 1u, userId: 100));

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
        executor.TryRegisterConnection(session.ConnectionId, session.UserId);

        // 入队 2 条（达到容量上限）。
        var c1 = CreateCommand(session);
        var c2 = CreateCommand(session);
        Assert.True(executor.TryEnqueue(session.ConnectionId, in c1));
        Assert.True(executor.TryEnqueue(session.ConnectionId, in c2));

        // 第 3 条：超过容量，返回 false。
        var c3 = CreateCommand(session);
        Assert.False(executor.TryEnqueue(session.ConnectionId, in c3));

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
        executor.TryRegisterConnection(session.ConnectionId, session.UserId);

        for (var i = 0; i < 8; i++)
        {
            var cmd = CreateCommand(session);
            Assert.True(executor.TryEnqueue(session.ConnectionId, in cmd));
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
        executor.TryRegisterConnection(session.ConnectionId, session.UserId);

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

        Assert.True(executor.TryEnqueue(session.ConnectionId, in cmd));

        // 注销连接：应释放缓冲区与预算。
        executor.UnregisterConnection(session.ConnectionId);

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
        executor.TryRegisterConnection(session.ConnectionId, session.UserId);

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
            Assert.True(executor.TryEnqueue(session.ConnectionId, in cmd));
        }

        Assert.Equal(96, inboundBudget.CurrentBytes);

        // StopAsync 会取消 worker、排空所有连接队列。
        await executor.StopAsync(CancellationToken.None);

        // 队列中残留命令的预算与缓冲区已释放。
        Assert.Equal(0, inboundBudget.CurrentBytes);

        await executor.DisposeAsync();
        await session.DisposeAsync();
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
