using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Ephemeral;
using ChatApp.TcpGateway.Gateway.Networking.Executor;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.TcpGateway.Tests.Networking;

/// <summary>
/// 覆盖 TcpGatewayService 使用的三 lane 注册事务：部分失败只回滚当前 session，
/// 旧 session 的延迟 finally 不能删除复用同一 connectionId 的后继 holder。
/// </summary>
public sealed class SessionCommandRegistrationSetTests
{
    [Theory]
    [InlineData((int)CommandLane.OrderedWrite)]
    [InlineData((int)CommandLane.Query)]
    [InlineData((int)CommandLane.Ephemeral)]
    public async Task PartialLaneRegistrationFailureRollsBackOnlyNewLeases(
        int blockedLaneValue)
    {
        var blockedLane = (CommandLane)blockedLaneValue;
        await using var fixture = new RegistrationFixture();
        await using var session = TestSessionFactory.Create();
        const uint connectionId = 41;

        var orderedBlocker = default(SessionCommandExecutor.Registration);
        var queryBlocker = default(SessionCommandExecutor.Registration);
        var ephemeralBlocker = default(EphemeralCommandPipeline.Registration);
        var blocked = blockedLane switch
        {
            CommandLane.OrderedWrite => fixture.Ordered.TryRegisterConnection(
                connectionId, 7, out orderedBlocker),
            CommandLane.Query => fixture.Query.TryRegisterConnection(
                connectionId, 7, out queryBlocker),
            CommandLane.Ephemeral => fixture.Ephemeral.TryRegisterConnection(
                connectionId, 7, out ephemeralBlocker),
            _ => false
        };
        Assert.True(blocked);

        Assert.False(SessionCommandRegistrationSet.TryRegister(
            connectionId,
            userId: 8,
            fixture.Ordered,
            fixture.Query,
            fixture.Ephemeral,
            out var failed));
        Assert.False(failed.IsComplete);

        Assert.Equal(
            blockedLane == CommandLane.OrderedWrite ? 1 : 0,
            fixture.Ordered.RegisteredConnectionCount);
        Assert.Equal(
            blockedLane == CommandLane.Query ? 1 : 0,
            fixture.Query.RegisteredConnectionCount);
        Assert.Equal(
            blockedLane == CommandLane.Ephemeral ? 1 : 0,
            fixture.Ephemeral.RegisteredConnectionCount);

        var budget = new GlobalInboundBudget(16);
        var command = CreateOwnedCommand(session, budget);
        var accepted = blockedLane switch
        {
            CommandLane.OrderedWrite => orderedBlocker.TryEnqueue(in command),
            CommandLane.Query => queryBlocker.TryEnqueue(in command),
            CommandLane.Ephemeral => ephemeralBlocker.TryEnqueue(in command),
            _ => false
        };
        Assert.True(accepted);
        Assert.Equal(1, budget.CurrentBytes);

        orderedBlocker.Unregister();
        queryBlocker.Unregister();
        ephemeralBlocker.Unregister();
        Assert.Equal(0, budget.CurrentBytes);
    }

    [Fact]
    public async Task CollisionAndOldCleanupCannotDeleteReplacementRegistrations()
    {
        await using var fixture = new RegistrationFixture();
        await using var session = TestSessionFactory.Create();
        const uint connectionId = 73;

        Assert.True(SessionCommandRegistrationSet.TryRegister(
            connectionId,
            userId: 1,
            fixture.Ordered,
            fixture.Query,
            fixture.Ephemeral,
            out var first));

        // OnConnectionAccepted 遇到同 ID 时必须失败，且不能触碰原 session 的三条 lane。
        Assert.False(SessionCommandRegistrationSet.TryRegister(
            connectionId,
            userId: 2,
            fixture.Ordered,
            fixture.Query,
            fixture.Ephemeral,
            out _));
        Assert.Equal(1, fixture.Ordered.RegisteredConnectionCount);
        Assert.Equal(1, fixture.Query.RegisteredConnectionCount);
        Assert.Equal(1, fixture.Ephemeral.RegisteredConnectionCount);

        first.Unregister();
        Assert.True(SessionCommandRegistrationSet.TryRegister(
            connectionId,
            userId: 2,
            fixture.Ordered,
            fixture.Query,
            fixture.Ephemeral,
            out var replacement));

        // 模拟旧 HandleClientAsync finally 在 ID 复用后迟到/重复执行。
        first.Unregister();
        Assert.Equal(1, fixture.Ordered.RegisteredConnectionCount);
        Assert.Equal(1, fixture.Query.RegisteredConnectionCount);
        Assert.Equal(1, fixture.Ephemeral.RegisteredConnectionCount);

        var budget = new GlobalInboundBudget(16);
        foreach (var lane in new[]
                 {
                     CommandLane.OrderedWrite,
                     CommandLane.Query,
                     CommandLane.Ephemeral
                 })
        {
            var command = CreateOwnedCommand(session, budget);
            Assert.True(replacement.TryEnqueue(lane, in command));
        }

        Assert.Equal(3, budget.CurrentBytes);
        replacement.Unregister();
        Assert.Equal(0, budget.CurrentBytes);
        Assert.Equal(0, fixture.Ordered.RegisteredConnectionCount);
        Assert.Equal(0, fixture.Query.RegisteredConnectionCount);
        Assert.Equal(0, fixture.Ephemeral.RegisteredConnectionCount);
    }

    [Fact]
    public async Task OldServiceCleanupCannotRemoveReplacementHeartbeatEntry()
    {
        var registry = new HeartbeatBucketRegistry(bucketCount: 4);
        await using var first = TestSessionFactory.Create();
        await using var replacement = TestSessionFactory.Create();
        Assert.Equal(first.ConnectionId, replacement.ConnectionId);

        registry.RegisterConnection(first);
        registry.RegisterConnection(replacement);

        // 模拟旧 HandleClientAsync finally 晚于新 session 注册完成。
        registry.Unregister(first);
        var remaining = Assert.Single(registry.GetConnectionBucket(
            (int)(replacement.ConnectionId % 4)));
        Assert.Same(replacement, remaining);

        registry.Unregister(replacement);
        Assert.Equal(0, registry.TotalConnections);
    }

    private static SessionCommand CreateOwnedCommand(
        TcpClientSession session,
        GlobalInboundBudget budget)
    {
        Assert.True(budget.TryReserve(1));
        return new SessionCommand
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
    }

    private sealed class RegistrationFixture : IAsyncDisposable
    {
        private readonly GatewayMetrics _metrics = new();

        public SessionCommandExecutor Ordered { get; } = CreateExecutor();
        public SessionCommandExecutor Query { get; } = CreateExecutor();
        public EphemeralCommandPipeline Ephemeral { get; }

        public RegistrationFixture()
        {
            Ephemeral = new EphemeralCommandPipeline(
                new TcpGatewayOptions
                {
                    CommandSchedulerEphemeralCapacity = 8
                },
                EphemeralPipelineMode.Legacy,
                static (_, _) => ValueTask.CompletedTask,
                _metrics,
                TimeProvider.System,
                NullLogger.Instance);
        }

        public async ValueTask DisposeAsync()
        {
            await Ephemeral.DisposeAsync();
            await Query.DisposeAsync();
            await Ordered.DisposeAsync();
            _metrics.Dispose();
        }

        private static SessionCommandExecutor CreateExecutor() => new(
            static (_, _) => ValueTask.CompletedTask,
            workerCount: 1,
            burstLimit: 4,
            perConnectionCapacity: 8,
            globalCapacity: 16,
            commandTimeout: TimeSpan.Zero,
            perUserConcurrency: 0,
            onFatalError: null);
    }
}
