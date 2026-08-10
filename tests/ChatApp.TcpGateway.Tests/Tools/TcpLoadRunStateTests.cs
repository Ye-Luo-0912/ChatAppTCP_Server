using System.Diagnostics;
using ChatApp.TcpGateway.LoadGenerator;

namespace ChatApp.TcpGateway.Tests.Tools;

public sealed class TcpLoadRunStateTests
{
    [Fact]
    public void DeliveryDrainRequiresAckAndDeliveryForTheSameMessage()
    {
        var state = CreateState(expectedSenders: 1);
        Assert.True(state.TryTrack(
            "message-1",
            Stopwatch.GetTimestamp(),
            new HashSet<int> { 1 }));
        state.RecordSent();
        state.SealSending();

        var acknowledgement = state.RecordChatAcknowledged("message-1");

        Assert.Equal(MessageSignalRecordKind.Recorded, acknowledgement.Kind);
        Assert.Equal(1, state.OutstandingCount);
        Assert.False(state.IsDeliveryDrainCompleted);

        var delivery = state.RecordChatDelivered(
            "message-1",
            recipientClientIndex: 1,
            isSlowReader: false);

        Assert.Equal(MessageSignalRecordKind.Completed, delivery.Kind);
        Assert.Equal(0, state.OutstandingCount);
        Assert.True(state.IsDeliveryDrainCompleted);
        Assert.Equal(1, state.AcknowledgementLatency.Snapshot().Count);
        Assert.Equal(1, state.DeliveryLatency.Snapshot().Count);
        Assert.Equal(1, state.Latency.Snapshot().Count);
    }

    [Fact]
    public void DeliveryBeforeAckAlsoKeepsTheMessageOutstanding()
    {
        var state = CreateState(expectedSenders: 1);
        Assert.True(state.TryTrack(
            "message-1",
            Stopwatch.GetTimestamp(),
            new HashSet<int> { 1 }));
        state.RecordSent();
        state.SealSending();

        var delivery = state.RecordChatDelivered(
            "message-1",
            recipientClientIndex: 1,
            isSlowReader: true);

        Assert.Equal(MessageSignalRecordKind.Recorded, delivery.Kind);
        Assert.Equal(1, state.OutstandingCount);
        Assert.False(state.IsDeliveryDrainCompleted);

        var acknowledgement = state.RecordChatAcknowledged("message-1");

        Assert.Equal(MessageSignalRecordKind.Completed, acknowledgement.Kind);
        Assert.Equal(0, state.OutstandingCount);
        Assert.True(state.IsDeliveryDrainCompleted);
        Assert.Equal(1, state.SlowLatency.Snapshot().Count);
    }

    [Fact]
    public void DuplicateTerminalSignalsAreCountedAndAbortTheRun()
    {
        using var lifecycleCancellation = new CancellationTokenSource();
        var state = CreateState(expectedSenders: 1);
        state.AttachLifecycle(lifecycleCancellation);
        Assert.True(state.TryTrack(
            "message-1",
            Stopwatch.GetTimestamp(),
            new HashSet<int> { 1 }));
        state.RecordSent();

        Assert.Equal(
            MessageSignalRecordKind.Recorded,
            state.RecordChatAcknowledged("message-1").Kind);
        Assert.Equal(
            MessageSignalRecordKind.DuplicateOrUntracked,
            state.RecordChatAcknowledged("message-1").Kind);

        Assert.Equal(
            MessageSignalRecordKind.Completed,
            state.RecordChatDelivered(
                "message-1",
                recipientClientIndex: 1,
                isSlowReader: false).Kind);
        Assert.Equal(
            MessageSignalRecordKind.DuplicateOrUntracked,
            state.RecordChatDelivered(
                "message-1",
                recipientClientIndex: 1,
                isSlowReader: false).Kind);

        var counters = state.SnapshotCounters();
        Assert.Equal(1, counters.Acknowledged);
        Assert.Equal(1, counters.Received);
        Assert.Equal(1, counters.DuplicateAcknowledgements);
        Assert.Equal(1, counters.DuplicateDeliveries);
        Assert.NotNull(state.RuntimeFailure);
        Assert.True(lifecycleCancellation.IsCancellationRequested);
    }

    [Fact]
    public void CrossGatewayExternalDelivery_IsCountedOnce_AndDuplicateAbortsTheRun()
    {
        using var lifecycleCancellation = new CancellationTokenSource();
        var state = new LoadRunState(
            expectedClients: 2,
            maxInflight: 16,
            inflightTtl: TimeSpan.FromMinutes(1),
            expectedChatSenders: 1,
            allowAckOnlyTracking: true);
        state.AttachLifecycle(lifecycleCancellation);

        var externalMessageId = LoadMessageCorrelation.Create(
            Stopwatch.GetTimestamp() - Stopwatch.Frequency / 20);
        var first = state.RecordChatDelivered(
            externalMessageId,
            recipientClientIndex: 0,
            isSlowReader: false);

        Assert.Equal(MessageSignalRecordKind.Recorded, first.Kind);
        Assert.True(first.ElapsedMilliseconds > 0);
        Assert.Equal(1, state.SnapshotCounters().Received);
        Assert.Equal(1, state.DeliveryLatency.Snapshot().Count);
        Assert.Equal(1, state.SnapshotDeliveryIds().Count);
        Assert.Null(state.RuntimeFailure);

        var duplicate = state.RecordChatDelivered(
            externalMessageId,
            recipientClientIndex: 0,
            isSlowReader: false);

        Assert.Equal(MessageSignalRecordKind.DuplicateOrUntracked, duplicate.Kind);
        Assert.Equal(1, state.SnapshotCounters().Received);
        Assert.Equal(1, state.SnapshotCounters().DuplicateDeliveries);
        Assert.NotNull(state.RuntimeFailure);
        Assert.True(lifecycleCancellation.IsCancellationRequested);
    }

    [Fact]
    public void CrossGatewayDeliveryWithoutCorrelatableTimestampFailsInsteadOfRecordingZeroLatency()
    {
        using var lifecycleCancellation = new CancellationTokenSource();
        var state = new LoadRunState(
            expectedClients: 1,
            maxInflight: 16,
            inflightTtl: TimeSpan.FromMinutes(1),
            expectedChatSenders: 0,
            allowAckOnlyTracking: true);
        state.AttachLifecycle(lifecycleCancellation);

        var result = state.RecordChatDelivered(
            "not-a-load-correlation-id",
            recipientClientIndex: 0,
            isSlowReader: false);

        Assert.Equal(MessageSignalRecordKind.DuplicateOrUntracked, result.Kind);
        Assert.Equal(0, state.DeliveryLatency.Snapshot().Count);
        Assert.Equal(0, state.SnapshotDeliveryIds().Count);
        Assert.Equal(1, state.SnapshotCounters().DuplicateDeliveries);
        Assert.NotNull(state.RuntimeFailure);
        Assert.True(lifecycleCancellation.IsCancellationRequested);
    }

    [Fact]
    public async Task WarmupRuntimeFailureCancelsLifecycleAndPreventsMeasurement()
    {
        using var lifecycleCancellation = new CancellationTokenSource();
        var state = CreateState(expectedSenders: 0);
        state.AttachLifecycle(lifecycleCancellation);

        state.FailRuntime("warmup heartbeat failed");
        var started = state.StartMeasurement(
            new TargetPlan(
                "peer-ring",
                UniqueUsers: 2,
                new Dictionary<int, long>(),
                new Dictionary<int, IReadOnlySet<int>>(),
                Error: null),
            measurementAllowed: true,
            CancellationToken.None);
        var context = await state.WaitForMeasurementAsync();

        Assert.False(started);
        Assert.True(lifecycleCancellation.IsCancellationRequested);
        Assert.Equal("warmup heartbeat failed", context.PreparationError);
    }

    [Fact]
    public void DuplicateCountersFailTheSemanticGate()
    {
        var options = CreateChatOptions();
        var targetPlan = new TargetPlan(
            "peer-ring",
            UniqueUsers: 2,
            new Dictionary<int, long>(),
            new Dictionary<int, IReadOnlySet<int>>(),
            Error: null);

        var gate = LoadGateEvaluator.Evaluate(
            options,
            targetPlan,
            measurementStarted: true,
            successfulConnections: 2,
            sent: 1,
            expectedDeliveries: 1,
            received: 1,
            acknowledged: 1,
            rejected: 0,
            duplicateAcknowledgements: 1,
            duplicateDeliveries: 1,
            latencySamples: 1,
            trackingExpired: 0,
            trackingDropped: 0,
            deliveryDrainCompleted: true,
            runtimeFailure: null);

        Assert.False(gate.Passed);
        Assert.Contains(
            gate.Failures,
            static failure => failure.Contains("duplicate", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(
            gate.Failures,
            static failure => failure.Contains("peer deliveries", StringComparison.Ordinal));
    }

    [Fact]
    public void OneDeliveryPerExpectedDeviceIsNotADuplicate()
    {
        var state = CreateState(expectedSenders: 1);
        Assert.True(state.TryTrack(
            "message-1",
            Stopwatch.GetTimestamp(),
            new HashSet<int> { 1, 2 }));
        state.RecordSent();
        state.SealSending();

        Assert.Equal(
            MessageSignalRecordKind.Recorded,
            state.RecordChatAcknowledged("message-1").Kind);
        Assert.Equal(
            MessageSignalRecordKind.Recorded,
            state.RecordChatDelivered(
                "message-1",
                recipientClientIndex: 1,
                isSlowReader: false).Kind);
        Assert.Equal(
            MessageSignalRecordKind.Completed,
            state.RecordChatDelivered(
                "message-1",
                recipientClientIndex: 2,
                isSlowReader: false).Kind);

        var counters = state.SnapshotCounters();
        Assert.Equal(2, counters.ExpectedDeliveries);
        Assert.Equal(2, counters.Received);
        Assert.Equal(0, counters.DuplicateDeliveries);
        Assert.True(state.IsDeliveryDrainCompleted);
    }

    [Fact]
    public void PeerRingExpectsTargetDevicesAndNonOriginSenderDevices()
    {
        var state = new LoadRunState(
            expectedClients: 4,
            maxInflight: 16,
            inflightTtl: TimeSpan.FromMinutes(1),
            expectedChatSenders: 1);
        state.CompletePreparation(0, Identity(userId: 10, session: "sender-origin"), null);
        state.CompletePreparation(1, Identity(userId: 10, session: "sender-device-2"), null);
        state.CompletePreparation(2, Identity(userId: 20, session: "target-device-1"), null);
        state.CompletePreparation(3, Identity(userId: 20, session: "target-device-2"), null);
        var options = CreateChatOptions() with
        {
            Connections = 4,
            AccessTokens = ["token-1", "token-2", "token-3", "token-4"]
        };

        var plan = state.CreateTargetPlan(options);

        Assert.Null(plan.Error);
        Assert.Equal(20, plan.TargetUserIds[0]);
        Assert.Equal(
            new HashSet<int> { 1, 2, 3 },
            plan.ExpectedRecipientClientIndexes[0]);
    }

    [Fact]
    public void FixedTargetMayBeANonSendingConnectedUser()
    {
        var state = new LoadRunState(
            expectedClients: 2,
            maxInflight: 16,
            inflightTtl: TimeSpan.FromMinutes(1),
            expectedChatSenders: 1);
        state.CompletePreparation(0, Identity(userId: 10, session: "sender"), null);
        state.CompletePreparation(1, Identity(userId: 20, session: "target"), null);
        var options = CreateChatOptions() with { TargetUserId = 20 };

        var plan = state.CreateTargetPlan(options);

        Assert.Null(plan.Error);
        Assert.Equal(20, plan.TargetUserIds[0]);
        Assert.Equal(
            new HashSet<int> { 1 },
            plan.ExpectedRecipientClientIndexes[0]);
    }

    [Fact]
    public void SenderEchoExcludesEveryConnectionReusingTheOriginSession()
    {
        var state = new LoadRunState(
            expectedClients: 3,
            maxInflight: 16,
            inflightTtl: TimeSpan.FromMinutes(1),
            expectedChatSenders: 1);
        state.CompletePreparation(0, Identity(userId: 10, session: "shared-session"), null);
        state.CompletePreparation(1, Identity(userId: 10, session: "shared-session"), null);
        state.CompletePreparation(2, Identity(userId: 20, session: "target-session"), null);
        var options = CreateChatOptions() with
        {
            Connections = 3,
            AccessTokens = ["token-1", "token-2", "token-3"]
        };

        var plan = state.CreateTargetPlan(options);

        Assert.Null(plan.Error);
        Assert.Equal(
            new HashSet<int> { 2 },
            plan.ExpectedRecipientClientIndexes[0]);
    }

    [Fact]
    public void SlowReadersRequireAWriteOnlyHeartbeatInterval()
    {
        var exception = Assert.Throws<ArgumentException>(() =>
            LoadOptions.Parse(
            [
                "--mode", "chat",
                "--connections", "2",
                "--duration-seconds", "1",
                "--token", "token",
                "--slow-readers", "1",
                "--inactive-heartbeat-seconds", "0"
            ]));

        Assert.Contains("slow readers require", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void InactiveHeartbeatClientsRequireAKeepAliveInterval()
    {
        var exception = Assert.Throws<ArgumentException>(() =>
            LoadOptions.Parse(
            [
                "--mode", "heartbeat",
                "--connections", "2",
                "--duration-seconds", "1",
                "--token", "token",
                "--active-senders", "1",
                "--inactive-heartbeat-seconds", "0"
            ]));

        Assert.Contains(
            "inactive authenticated clients require",
            exception.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SenderCompletionTimeoutCannotBeOverwrittenBySealSending()
    {
        var state = CreateState(expectedSenders: 1);

        state.FailSenderCompletionTimeout(TimeSpan.FromSeconds(60));
        state.CompleteChatSender(clientIndex: 0);
        state.SealSending();

        Assert.False(state.IsDeliveryDrainCompleted);
        Assert.Contains("senders did not stop", state.RuntimeFailure, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("chat receiver")]
    [InlineData("chat sender")]
    [InlineData("inactive chat heartbeat")]
    public async Task PairedLoopFaultImmediatelyCancelsTheSharedLifecycle(
        string loopName)
    {
        using var lifecycleCancellation = new CancellationTokenSource();
        var state = CreateState(expectedSenders: 0);
        state.AttachLifecycle(lifecycleCancellation);
        var sibling = LoadLoopCoordinator.ObserveAsync(
            Task.Delay(Timeout.InfiniteTimeSpan, lifecycleCancellation.Token),
            clientIndex: 1,
            "paired sibling",
            state,
            lifecycleCancellation.Token);

        var failing = LoadLoopCoordinator.ObserveAsync(
            Task.FromException(new EndOfStreamException("peer closed")),
            clientIndex: 0,
            loopName,
            state,
            CancellationToken.None);

        await Assert.ThrowsAsync<EndOfStreamException>(() => failing);
        Assert.True(lifecycleCancellation.IsCancellationRequested);
        Assert.Contains(loopName, state.RuntimeFailure, StringComparison.Ordinal);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sibling);
    }

    [Fact]
    public async Task MeasurementDeadlineCancellationIsNotReportedAsALoopFault()
    {
        using var lifecycleCancellation = new CancellationTokenSource();
        using var measurementCancellation = new CancellationTokenSource();
        var state = CreateState(expectedSenders: 0);
        state.AttachLifecycle(lifecycleCancellation);
        measurementCancellation.Cancel();

        var observed = LoadLoopCoordinator.ObserveAsync(
            Task.FromCanceled(measurementCancellation.Token),
            clientIndex: 0,
            "chat sender",
            state,
            measurementCancellation.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => observed);
        Assert.Null(state.RuntimeFailure);
        Assert.False(lifecycleCancellation.IsCancellationRequested);
    }

    private static LoadRunState CreateState(int expectedSenders) =>
        new(
            expectedClients: 0,
            maxInflight: 16,
            inflightTtl: TimeSpan.FromMinutes(1),
            expectedChatSenders: expectedSenders);

    private static AuthenticatedIdentity Identity(long userId, string session) =>
        new(userId, session, DeviceIdHash: null);

    private static LoadOptions CreateChatOptions() =>
        new(
            Host: "127.0.0.1",
            Port: 8888,
            Connections: 2,
            Duration: TimeSpan.FromSeconds(1),
            Mode: LoadMode.Chat,
            AccessTokens: ["token-1", "token-2"],
            DeviceIdHash: null,
            TargetUserId: null,
            ActiveSenders: 1,
            MessagesPerSecond: 1,
            PayloadBytes: 16,
            SlowReaders: 0,
            ConnectionsPerSecond: 0,
            Stabilization: TimeSpan.Zero,
            ConnectTimeout: TimeSpan.FromSeconds(1),
            MaxInflight: 16,
            InflightTtl: TimeSpan.FromMinutes(1),
            DeliveryDrain: TimeSpan.FromSeconds(1),
            InactiveHeartbeatInterval: TimeSpan.FromSeconds(1),
            MinimumAcknowledgementRatio: 1,
            MinimumDeliveryRatio: 1,
            SlowlorisPhase: null,
            SlowlorisDelayMs: 100,
            TargetRingFilePath: null,
            ReportDirectory: null);
}
