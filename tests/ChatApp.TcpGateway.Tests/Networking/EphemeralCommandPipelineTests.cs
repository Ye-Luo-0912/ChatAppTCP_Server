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
        Assert.True(pipeline.TryRegisterConnection(7, 0, out var registration));
        Assert.True(registration.TryEnqueue(in command));
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
        Assert.True(pipeline.TryRegisterConnection(8, 0, out var registration));
        Assert.True(registration.TryEnqueue(in command));
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

    [Fact]
    public async Task ActorRegistrationGenerationRejectsOldLeaseAfterConnectionIdReuse()
    {
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        await using var session = new TcpClientSession(
            new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp),
            connectionId: 17,
            outboundQueueCapacity: 4,
            maxOutboundQueuedBytes: 128 * 1024,
            sendTimeout: TimeSpan.FromSeconds(1),
            TimeProvider.System,
            metrics,
            NullLogger<TcpClientSession>.Instance);

        var processed = new TaskCompletionSource(
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
            (_, _) =>
            {
                processed.TrySetResult();
                return ValueTask.CompletedTask;
            },
            metrics,
            TimeProvider.System,
            NullLogger.Instance);

        await pipeline.StartAsync(ct);
        Assert.True(pipeline.TryRegisterConnection(17, 1, out var first));
        first.Unregister();
        Assert.True(pipeline.TryRegisterConnection(17, 2, out var replacement));

        // 旧 session finally 重复清理时不能移除/Deactivate 新 generation。
        first.Unregister();
        Assert.Equal(1, pipeline.RegisteredConnectionCount);

        var stale = new SessionCommand
        {
            Command = PacketCommand.TypingNotify,
            RentedBuffer = Array.Empty<byte>(),
            PayloadLength = 0,
            IsPooled = false,
            Session = session,
            RemoteIp = "127.0.0.1"
        };
        Assert.False(first.TryEnqueue(in stale));

        var budget = new GlobalInboundBudget(16);
        Assert.True(budget.TryReserve(1));
        var current = new SessionCommand
        {
            Command = PacketCommand.TypingNotify,
            RentedBuffer = new byte[1],
            PayloadLength = 1,
            IsPooled = false,
            ReservedInboundBytes = 1,
            InboundBudget = budget,
            Session = session,
            RemoteIp = "127.0.0.1"
        };
        Assert.True(replacement.TryEnqueue(in current));
        await processed.Task.WaitAsync(TimeSpan.FromSeconds(2), ct);
        await WaitUntilAsync(
            () => budget.CurrentBytes == 0,
            TimeSpan.FromSeconds(2),
            ct);

        replacement.Unregister();
        Assert.Equal(0, pipeline.RegisteredConnectionCount);
        await pipeline.StopAsync(ct);
    }

    [Fact]
    public async Task ActorStopClosesAdmissionAndClearsRacingRegistrations()
    {
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        await using var pipeline = CreateActorPipeline(metrics);
        await pipeline.StartAsync(ct);
        Assert.True(pipeline.TryRegisterConnection(1, 0, out var existing));

        const int racerCount = 16;
        var raced = new EphemeralCommandPipeline.Registration[racerCount];
        using var barrier = new Barrier(racerCount + 1);
        var registrationTasks = new Task[racerCount];
        for (var index = 0; index < racerCount; index++)
        {
            var slot = index;
            registrationTasks[index] = Task.Run(() =>
            {
                barrier.SignalAndWait(ct);
                pipeline.TryRegisterConnection(
                    (uint)(100 + slot),
                    0,
                    out raced[slot]);
            }, ct);
        }

        var stopTask = Task.Run(async () =>
        {
            barrier.SignalAndWait(ct);
            await pipeline.StopAsync(ct);
        }, ct);

        await Task.WhenAll(registrationTasks.Append(stopTask));

        Assert.Equal(0, pipeline.RegisteredConnectionCount);
        Assert.False(pipeline.TryRegisterConnection(999, 0, out var rejected));
        Assert.False(rejected.IsValid);
        var probe = default(SessionCommand);
        Assert.False(existing.TryEnqueue(in probe));
        foreach (var registration in raced)
            Assert.False(registration.TryEnqueue(in probe));
    }

    [Fact]
    public async Task ActorDisposeClosesAdmissionAndClearsRacingRegistrations()
    {
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        var pipeline = CreateActorPipeline(metrics);
        try
        {
            await pipeline.StartAsync(ct);
            Assert.True(pipeline.TryRegisterConnection(1, 0, out var existing));

            const int racerCount = 16;
            var raced = new EphemeralCommandPipeline.Registration[racerCount];
            using var barrier = new Barrier(racerCount + 1);
            var registrationTasks = new Task[racerCount];
            for (var index = 0; index < racerCount; index++)
            {
                var slot = index;
                registrationTasks[index] = Task.Run(() =>
                {
                    barrier.SignalAndWait(ct);
                    pipeline.TryRegisterConnection(
                        (uint)(200 + slot),
                        0,
                        out raced[slot]);
                }, ct);
            }

            var disposeTask = Task.Run(async () =>
            {
                barrier.SignalAndWait(ct);
                await pipeline.DisposeAsync();
            }, ct);

            await Task.WhenAll(registrationTasks.Append(disposeTask));

            Assert.Equal(0, pipeline.RegisteredConnectionCount);
            Assert.False(pipeline.TryRegisterConnection(999, 0, out var rejected));
            Assert.False(rejected.IsValid);
            var probe = default(SessionCommand);
            Assert.False(existing.TryEnqueue(in probe));
            foreach (var registration in raced)
                Assert.False(registration.TryEnqueue(in probe));
        }
        finally
        {
            await pipeline.DisposeAsync();
        }
    }

    private static EphemeralCommandPipeline CreateActorPipeline(
        GatewayMetrics metrics)
        => new(
            new TcpGatewayOptions
            {
                UseActorRuntimeForEphemeralCommands = true,
                CommandSchedulerEphemeralCapacity = 8,
                EphemeralActorShardCount = 1,
                EphemeralActorIngressCapacity = 64,
                EphemeralActorAsyncConcurrency = 1,
                EphemeralActorIdleTimeout = TimeSpan.FromSeconds(1),
                EphemeralActorOperationTimeout = TimeSpan.FromSeconds(1)
            },
            static (_, _) => ValueTask.CompletedTask,
            metrics,
            TimeProvider.System,
            NullLogger.Instance);

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
