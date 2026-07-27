using System.Buffers;
using System.Net.Sockets;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Ephemeral;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.TcpGateway.Tests.Networking;

public sealed class EphemeralCommandPipelineTests
{
    [Fact]
    public async Task ActorPipelineProcessesCommandAndReleasesInboundOwnership()
    {
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        await using var session = new TcpClientSession(
            new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp),
            connectionId: 7,
            outboundQueueCapacity: 4,
            maxOutboundQueuedBytes: 128 * 1024,
            sendTimeout: TimeSpan.FromSeconds(1),
            TimeProvider.System,
            metrics,
            NullLogger<TcpClientSession>.Instance);

        var processed = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        await using var pipeline = new EphemeralCommandPipeline(
            new TcpGatewayOptions
            {
                UseActorRuntimeForEphemeralCommands = true,
                CommandSchedulerEphemeralCapacity = 4,
                EphemeralActorShardCount = 1,
                EphemeralActorIngressCapacity = 16,
                EphemeralActorAsyncConcurrency = 1,
                EphemeralActorIdleTimeout = TimeSpan.FromSeconds(1),
                EphemeralActorOperationTimeout = TimeSpan.FromSeconds(1)
            },
            (command, _) =>
            {
                processed.TrySetResult(true);
                return ValueTask.CompletedTask;
            },
            metrics,
            TimeProvider.System,
            NullLogger.Instance);

        var budget = new GlobalInboundBudget(1024);
        Assert.True(budget.TryReserve(32));
        var rented = ArrayPool<byte>.Shared.Rent(32);
        var command = new SessionCommand
        {
            Command = PacketCommand.TypingNotify,
            RentedBuffer = rented,
            PayloadLength = 32,
            IsPooled = true,
            ReservedInboundBytes = 32,
            InboundBudget = budget,
            Session = session,
            RemoteIp = "127.0.0.1"
        };

        await pipeline.StartAsync(ct);
        Assert.True(pipeline.TryRegisterConnection(7, 0));
        Assert.True(pipeline.TryEnqueue(7, in command));
        await processed.Task.WaitAsync(TimeSpan.FromSeconds(2), ct);
        await WaitUntilAsync(
            () => budget.CurrentBytes == 0 &&
                  pipeline.Snapshot.PendingAsyncOperations == 0 &&
                  pipeline.Snapshot.BusyActors == 0 &&
                  pipeline.Snapshot.TotalProcessed >= 2,
            TimeSpan.FromSeconds(2),
            ct);

        Assert.Equal(0, budget.CurrentBytes);
        Assert.True(pipeline.Snapshot.TotalProcessed >= 2);
        await pipeline.StopAsync(ct);
    }

    [Fact]
    public async Task ActorPipelineFailureStillReleasesBudgetAndResumesActor()
    {
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        await using var session = new TcpClientSession(
            new Socket(
                AddressFamily.InterNetwork,
                SocketType.Stream,
                ProtocolType.Tcp),
            connectionId: 8,
            outboundQueueCapacity: 4,
            maxOutboundQueuedBytes: 128 * 1024,
            sendTimeout: TimeSpan.FromSeconds(1),
            TimeProvider.System,
            metrics,
            NullLogger<TcpClientSession>.Instance);

        await using var pipeline = new EphemeralCommandPipeline(
            new TcpGatewayOptions
            {
                UseActorRuntimeForEphemeralCommands = true,
                CommandSchedulerEphemeralCapacity = 4,
                EphemeralActorShardCount = 1,
                EphemeralActorIngressCapacity = 16,
                EphemeralActorAsyncConcurrency = 1,
                EphemeralActorIdleTimeout = TimeSpan.FromSeconds(1),
                EphemeralActorOperationTimeout = TimeSpan.FromSeconds(1)
            },
            static (_, _) => ValueTask.FromException(
                new InvalidOperationException("expected")),
            metrics,
            TimeProvider.System,
            NullLogger.Instance);

        var budget = new GlobalInboundBudget(1024);
        Assert.True(budget.TryReserve(32));
        var rented = ArrayPool<byte>.Shared.Rent(32);
        var command = new SessionCommand
        {
            Command = PacketCommand.TypingNotify,
            RentedBuffer = rented,
            PayloadLength = 32,
            IsPooled = true,
            ReservedInboundBytes = 32,
            InboundBudget = budget,
            Session = session,
            RemoteIp = "127.0.0.1"
        };

        await pipeline.StartAsync(ct);
        Assert.True(pipeline.TryEnqueue(8, in command));
        await WaitUntilAsync(
            () => budget.CurrentBytes == 0 &&
                  pipeline.Snapshot.PendingAsyncOperations == 0 &&
                  pipeline.Snapshot.BusyActors == 0,
            TimeSpan.FromSeconds(2),
            ct);

        Assert.Equal(0, budget.CurrentBytes);
        Assert.True(pipeline.Snapshot.TotalProcessed >= 2);
        await pipeline.StopAsync(ct);
    }

    private static async Task WaitUntilAsync(
        Func<bool> predicate,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (predicate())
                return;
            await Task.Delay(10, cancellationToken);
        }

        Assert.True(predicate(), "Condition was not reached before timeout.");
    }
}
