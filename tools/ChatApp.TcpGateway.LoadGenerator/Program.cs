using System.Diagnostics;
using System.Globalization;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.LoadGenerator;
using ChatApp.TcpGateway.LoadGenerator.Diagnostics;

LoadOptions options;
try
{
    options = LoadOptions.Parse(args);
}
catch (Exception exception)
{
    Console.Error.WriteLine(exception.Message);
    Console.Error.WriteLine(
        "Usage: --mode connection|heartbeat|chat|invalid-packet|slowloris " +
        "--host 127.0.0.1 --port 8888 --connections 100 " +
        "--duration-seconds 10 [--token TOKEN] [--token-file PATH] " +
        "[--target-user-id ID] [--active-senders N] " +
        "[--messages-per-second 10] " +
        "[--payload-bytes 128] [--slow-readers 0] " +
        "[--connections-per-second 0] [--stabilization-seconds 0] " +
        "[--connect-timeout-seconds 30] [--max-inflight 1000000] " +
        "[--inflight-ttl-seconds 120] [--delivery-drain-seconds 30] " +
        "[--inactive-heartbeat-seconds 30] " +
        "[--min-ack-ratio 0.95] " +
        "[--min-delivery-ratio 0.90] [--slowloris-phase header|payload] " +
        "[--slowloris-delay-ms 1000] [--report-directory PATH]");
    return 2;
}

var totalStartedAt = Stopwatch.GetTimestamp();
// item 五：跨 Gateway 配对时，每个 sender 的预期接收者集合为空（目标投递在
// 另一 Gateway 上，由接收侧 LoadGenerator 观测）。必须启用 ack-only 跟踪，
// 否则 TryTrack 会因“无本地可观测接收者”而判定运行时失败。
var runState = new LoadRunState(
    options.Connections,
    options.MaxInflight,
    options.InflightTtl,
    options.Mode == LoadMode.Chat ? options.ActiveSenders : 0,
    allowAckOnlyTracking: options.TargetRingFilePath is not null);
using var lifecycleCancellation = new CancellationTokenSource();
runState.AttachLifecycle(lifecycleCancellation);
var sharedChatPayload = options.Mode == LoadMode.Chat
    ? new string('x', options.PayloadBytes)
    : string.Empty;

var rampStartedAt = Stopwatch.GetTimestamp();
var clients = await CreateClientsAsync(options, runState, sharedChatPayload)
    .ConfigureAwait(false);
await runState.AllPreparationCompleted.ConfigureAwait(false);
var rampElapsed = Stopwatch.GetElapsedTime(rampStartedAt);

var targetPlan = runState.CreateTargetPlan(options);
var measurementAllowed = targetPlan.Error is null &&
                         runState.RuntimeFailure is null &&
                         !lifecycleCancellation.IsCancellationRequested;
var stabilizationElapsed = TimeSpan.Zero;
if (measurementAllowed && options.Stabilization > TimeSpan.Zero)
{
    var stabilizationStartedAt = Stopwatch.GetTimestamp();
    try
    {
        await Task.Delay(options.Stabilization, lifecycleCancellation.Token)
            .ConfigureAwait(false);
    }
    catch (OperationCanceledException)
        when (lifecycleCancellation.IsCancellationRequested)
    {
        measurementAllowed = false;
    }
    finally
    {
        stabilizationElapsed = Stopwatch.GetElapsedTime(stabilizationStartedAt);
    }
}

using var measurementCancellation =
    CancellationTokenSource.CreateLinkedTokenSource(
        lifecycleCancellation.Token);
measurementAllowed = measurementAllowed &&
                     runState.RuntimeFailure is null &&
                     !lifecycleCancellation.IsCancellationRequested;
if (measurementAllowed)
    measurementCancellation.CancelAfter(options.Duration);
else
    measurementCancellation.Cancel();
var measurementStarted = runState.StartMeasurement(
    targetPlan,
    measurementAllowed,
    measurementCancellation.Token);
TimeSpan measurementElapsed;
var deliveryDrainElapsed = TimeSpan.Zero;
var deliveryDrainCompleted = options.Mode != LoadMode.Chat;
if (measurementStarted)
{
    var measurementStartedAt = Stopwatch.GetTimestamp();
    await WaitForCancellationAsync(measurementCancellation.Token)
        .ConfigureAwait(false);
    measurementElapsed = Stopwatch.GetElapsedTime(measurementStartedAt);

    if (options.Mode == LoadMode.Chat)
    {
        var drainStartedAt = Stopwatch.GetTimestamp();
        var drainTimedOut = false;

        if (options.DeliveryDrain == TimeSpan.Zero)
        {
            runState.StopClients();
        }
        else
        {
            try
            {
                await runState.AllChatSendersCompleted
                    .WaitAsync(options.DeliveryDrain)
                    .ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                drainTimedOut = true;
                runState.FailSenderCompletionTimeout(options.DeliveryDrain);
                runState.StopClients();
            }
        }

        // Once shutdown has been requested, every sender observes a cancelled
        // lifetime token. Wait for their finally blocks so Sent is immutable.
        await runState.AllChatSendersCompleted.ConfigureAwait(false);
        runState.SealSending();
        deliveryDrainCompleted = runState.IsDeliveryDrainCompleted;
        var crossGatewayDrain = options.TargetRingFilePath is not null;

        // Each cross-Gateway child observes deliveries produced by another process.
        // Keep receiver connections alive for the entire drain window so ACKs on this
        // process cannot terminate it before the counterpart's final deliveries arrive.
        if (crossGatewayDrain &&
            options.DeliveryDrain > TimeSpan.Zero &&
            !drainTimedOut &&
            runState.RuntimeFailure is null)
        {
            var remaining = options.DeliveryDrain -
                Stopwatch.GetElapsedTime(drainStartedAt);
            try
            {
                if (remaining > TimeSpan.Zero)
                    await Task.Delay(remaining, lifecycleCancellation.Token)
                        .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (lifecycleCancellation.IsCancellationRequested)
            {
            }

            deliveryDrainCompleted = runState.IsDeliveryDrainCompleted;
        }

        if (!crossGatewayDrain &&
            !deliveryDrainCompleted &&
            options.DeliveryDrain > TimeSpan.Zero &&
            !drainTimedOut &&
            runState.RuntimeFailure is null)
        {
            var remaining = options.DeliveryDrain -
                Stopwatch.GetElapsedTime(drainStartedAt);
            try
            {
                if (remaining > TimeSpan.Zero)
                {
                    await runState.DeliveryDrainCompleted
                        .WaitAsync(
                            remaining,
                            lifecycleCancellation.Token)
                        .ConfigureAwait(false);
                }
            }
            catch (TimeoutException)
            {
                // The configured drain bound is a normal semantic-gate result.
            }
            catch (OperationCanceledException)
                when (lifecycleCancellation.IsCancellationRequested)
            {
                // Runtime failure cancels the drain immediately.
            }

            deliveryDrainCompleted = runState.IsDeliveryDrainCompleted;
        }

        deliveryDrainElapsed = Stopwatch.GetElapsedTime(drainStartedAt);
    }
}
else
{
    measurementElapsed = TimeSpan.Zero;
}

runState.StopClients();
var results = await Task.WhenAll(clients).ConfigureAwait(false);
runState.PruneExpired();
var totalElapsed = Stopwatch.GetElapsedTime(totalStartedAt);

var successfulConnections = results.Count(
    static result => result.Connected);
var failedConnections = results.Length - successfulConnections;
var tcpConnectSucceeded = results.Count(
    static result => result.TcpConnectSucceeded);
var tcpConnectFailed = results.Length - tcpConnectSucceeded;
var authSucceeded = results.Count(
    static result => result.AuthSucceeded);
var authInvalidToken = results.Count(
    static result => result.AuthFailureKind == AuthFailureKind.InvalidToken);
var authDependencyUnavailable = results.Count(
    static result => result.AuthFailureKind == AuthFailureKind.DependencyUnavailable);
var authOtherFailure = results.Count(
    static result => result.AuthFailureKind == AuthFailureKind.Other);
var authSucceededWithoutResumeToken = results.Count(
    static result => result.AuthSucceededWithoutResumeToken);
var chatSendFailed = results.Count(
    static result => result.ChatSendFailed);
var chatReceiveFailed = results.Count(
    static result => result.ChatReceiveFailed);
var serverClosed = results.Count(
    static result => result.ServerClosed);
var protocolRejected = results.Count(
    static result => result.ProtocolRejected);
var completedNormally = results.Count(
    static result => result.CompletedNormally);
var counters = runState.SnapshotCounters();
var sent = counters.Sent;
var expectedDeliveries = counters.ExpectedDeliveries;
var received = counters.Received;
var acknowledged = counters.Acknowledged;
var rejected = counters.Rejected;
var duplicateAcknowledgements = counters.DuplicateAcknowledgements;
var duplicateDeliveries = counters.DuplicateDeliveries;
var crossGateway = options.TargetRingFilePath is not null;
var reportExpectedDeliveries = crossGateway ? sent : expectedDeliveries;
var latency = runState.Latency.Snapshot();
var acknowledgementLatency = runState.AcknowledgementLatency.Snapshot();
var deliveryLatency = runState.DeliveryLatency.Snapshot();
var acknowledgementIdFingerprint = runState.SnapshotAcknowledgementIds();
var deliveryIdFingerprint = runState.SnapshotDeliveryIds();
var healthyLatency = runState.HealthyLatency.Snapshot();
var slowLatency = runState.SlowLatency.Snapshot();
var healthyCount = results.Count(
    static result => !result.IsSlowReader && result.Connected);
var slowCount = results.Count(
    static result => result.IsSlowReader && result.Connected);

var collectedErrors = new List<string>();
if (targetPlan.Error is not null)
    collectedErrors.Add(targetPlan.Error);
if (runState.RuntimeFailure is not null)
    collectedErrors.Add(runState.RuntimeFailure);
collectedErrors.AddRange(results
    .Where(static result => result.Error is not null)
    .Select(static result => result.Error!));
var errorSamples = collectedErrors
    .Distinct(StringComparer.Ordinal)
    .Take(8)
    .ToArray();

var gate = LoadGateEvaluator.Evaluate(
    options,
    targetPlan,
    measurementStarted,
    successfulConnections,
    sent,
    expectedDeliveries,
    received,
    acknowledged,
    rejected,
    duplicateAcknowledgements,
    duplicateDeliveries,
    crossGateway ? acknowledgementLatency.Count : latency.Count,
    runState.TrackingExpired,
    runState.TrackingDropped,
    deliveryDrainCompleted,
    runState.RuntimeFailure);

Console.WriteLine(
    string.Create(CultureInfo.InvariantCulture, $"Mode: {options.Mode}"));
Console.WriteLine(
    string.Create(
        CultureInfo.InvariantCulture,
        $"Connections: {successfulConnections} succeeded, {failedConnections} failed"));
Console.WriteLine(
    string.Create(
        CultureInfo.InvariantCulture,
        $"Phases: ramp={rampElapsed.TotalSeconds:F2}s, " +
        $"stabilization={stabilizationElapsed.TotalSeconds:F2}s, " +
        $"measurement={measurementElapsed.TotalSeconds:F2}s, " +
        $"delivery-drain={deliveryDrainElapsed.TotalSeconds:F2}s " +
        $"(completed={deliveryDrainCompleted})"));
Console.WriteLine(
    string.Create(
        CultureInfo.InvariantCulture,
        $"Target strategy: {targetPlan.Strategy}; unique users: {targetPlan.UniqueUsers}"));
if (options.Mode is LoadMode.Heartbeat or LoadMode.Chat)
{
    Console.WriteLine(
        string.Create(
            CultureInfo.InvariantCulture,
            $"Active senders: {options.ActiveSenders}/{options.Connections}"));
}
Console.WriteLine(
    string.Create(
        CultureInfo.InvariantCulture,
        $"Peak active connections: {runState.PeakActiveConnections}"));

if (options.Mode == LoadMode.Heartbeat)
{
    Console.WriteLine(
        string.Create(
            CultureInfo.InvariantCulture,
            $"Heartbeat round trips: {latency.Count}"));
}
else if (options.Mode == LoadMode.Chat)
{
    Console.WriteLine(
        string.Create(
            CultureInfo.InvariantCulture,
            $"Chat messages: {sent} sent, {acknowledged} MQ-accepted, {rejected} rejected, {received}/{reportExpectedDeliveries} expected recipient deliveries received"));
    Console.WriteLine(
        string.Create(
            CultureInfo.InvariantCulture,
            $"Duplicate/untracked terminal frames: {duplicateAcknowledgements} ACK, " +
            $"{duplicateDeliveries} delivery"));
    Console.WriteLine(
        string.Create(
            CultureInfo.InvariantCulture,
            $"In-flight: {runState.OutstandingCount} outstanding, " +
            $"{runState.TrackingExpired} TTL-expired, " +
            $"{runState.TrackingDropped} tracking-dropped"));
    Console.WriteLine(
        string.Create(
            CultureInfo.InvariantCulture,
            $"Slow readers: {options.SlowReaders}"));
}

if (measurementElapsed > TimeSpan.Zero)
{
    Console.WriteLine(
        string.Create(
            CultureInfo.InvariantCulture,
            $"Throughput: {sent / measurementElapsed.TotalSeconds:F0} sent/s, " +
            $"{received / measurementElapsed.TotalSeconds:F0} delivered/s"));
}

if (options.Mode == LoadMode.Chat)
{
    if (acknowledgementLatency.Count != 0)
    {
        Console.WriteLine(
            string.Create(
                CultureInfo.InvariantCulture,
                $"MQ ACK latency ms p50={acknowledgementLatency.P50Ms:F3}, " +
                $"p95={acknowledgementLatency.P95Ms:F3}, " +
                $"p99={acknowledgementLatency.P99Ms:F3}"));
    }

    if (deliveryLatency.Count != 0)
    {
        Console.WriteLine(
            string.Create(
                CultureInfo.InvariantCulture,
                $"Peer delivery latency ms p50={deliveryLatency.P50Ms:F3}, " +
                $"p95={deliveryLatency.P95Ms:F3}, " +
                $"p99={deliveryLatency.P99Ms:F3}"));
    }
}
else if (latency.Count != 0)
{
    Console.WriteLine(
        string.Create(
            CultureInfo.InvariantCulture,
            $"Latency ms p50={latency.P50Ms:F3}, " +
            $"p95={latency.P95Ms:F3}, p99={latency.P99Ms:F3}"));
}

foreach (var error in errorSamples)
    Console.Error.WriteLine(error);
foreach (var failure in gate.Failures)
    Console.Error.WriteLine($"Gate failed: {failure}");

var report = TcpLoadReport.Create(
    options,
    rampElapsed,
    stabilizationElapsed,
    measurementElapsed,
    deliveryDrainElapsed,
    deliveryDrainCompleted,
    totalElapsed,
    targetPlan,
    successfulConnections,
    failedConnections,
    tcpConnectSucceeded,
    tcpConnectFailed,
    authSucceeded,
    authInvalidToken,
    authDependencyUnavailable,
    authOtherFailure,
    authSucceededWithoutResumeToken,
    chatSendFailed,
    chatReceiveFailed,
    serverClosed,
    protocolRejected,
    completedNormally,
    runState.PeakActiveConnections,
    sent,
    reportExpectedDeliveries,
    received,
    acknowledged,
    rejected,
    duplicateAcknowledgements,
    duplicateDeliveries,
    acknowledgementIdFingerprint,
    deliveryIdFingerprint,
    runState.OutstandingCount,
    runState.TrackingExpired,
    runState.TrackingDropped,
    latency,
    acknowledgementLatency,
    deliveryLatency,
    healthyLatency,
    slowLatency,
    healthyCount,
    slowCount,
    gate,
    runState.RuntimeFailure,
    errorSamples);
var reportPaths = TcpLoadReportWriter.WriteFiles(
    report,
    options.ReportDirectory);
if (reportPaths is not null)
{
    Console.WriteLine($"JSON report: {reportPaths.JsonPath}");
    Console.WriteLine($"Markdown report: {reportPaths.MarkdownPath}");
}

Console.WriteLine(gate.Passed ? "Semantic gate: PASSED" : "Semantic gate: FAILED");
return gate.Passed ? 0 : 1;

static async Task<ClientResult> RunClientAsync(
    LoadOptions options,
    int clientIndex,
    LoadRunState runState,
    string sharedChatPayload)
{
    var result = new ClientResult
    {
        IsSlowReader = options.Mode == LoadMode.Chat &&
                       clientIndex >= options.Connections - options.SlowReaders
    };
    var preparationCompleted = false;
    var connectionAccepted = false;

    try
    {
        await using var client = new ProtocolClient();
        AuthenticatedIdentity? identity = null;
        try
        {
            using var setupCancellation =
                CancellationTokenSource.CreateLinkedTokenSource(
                    runState.LifetimeCancellationToken);
            setupCancellation.CancelAfter(options.ConnectTimeout);
            await client.ConnectAsync(
                    options.Host,
                    options.Port,
                    result.IsSlowReader,
                    setupCancellation.Token)
                .ConfigureAwait(false);
            result.TcpConnectSucceeded = true;
            connectionAccepted = true;
            runState.OnConnectionAccepted();

            if (options.Mode is LoadMode.Heartbeat or LoadMode.Chat)
            {
                var token = options.AccessTokens[
                    clientIndex % options.AccessTokens.Count];
                var auth = await client.AuthenticateAsync(
                        token,
                        AddDeviceOffset(options.DeviceIdHash, clientIndex),
                        setupCancellation.Token)
                    .ConfigureAwait(false);
                if (!auth.Succeeded)
                {
                    result.AuthFailureKind = auth.FailureKind;
                    result.Error = auth.ErrorMessage;
                    runState.CompletePreparation(
                        clientIndex,
                        identity: null,
                        auth.ErrorMessage);
                    preparationCompleted = true;
                    return result;
                }

                identity = auth.Identity;
                result.AuthSucceeded = true;
                result.AuthSucceededWithoutResumeToken = !auth.ResumeTokenIssued;
            }

            result.Connected = true;
            runState.CompletePreparation(clientIndex, identity, error: null);
            preparationCompleted = true;
        }
        catch (OperationCanceledException)
            when (runState.LifetimeCancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(
                runState.RuntimeFailure ?? "The shared load lifecycle was canceled.",
                runState.LifetimeCancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException(
                $"Client {clientIndex} connection/authentication exceeded " +
                $"{options.ConnectTimeout.TotalSeconds:F0}s.");
        }

        MeasurementContext measurement;
        if (result.IsSlowReader &&
            options.InactiveHeartbeatInterval > TimeSpan.Zero)
        {
            measurement = await WaitForMeasurementWithWriteOnlyHeartbeatAsync(
                    client,
                    clientIndex - (options.Connections - options.SlowReaders),
                    options.SlowReaders,
                    options.InactiveHeartbeatInterval,
                    options.ConnectTimeout,
                    runState)
                .ConfigureAwait(false);
        }
        else if ((options.Mode is LoadMode.Heartbeat or LoadMode.Chat) &&
                 options.InactiveHeartbeatInterval > TimeSpan.Zero)
        {
            measurement = await WaitForMeasurementWithHeartbeatAsync(
                    client,
                    clientIndex,
                    result,
                    options.Mode,
                    options.InactiveHeartbeatInterval,
                    options.ConnectTimeout,
                    runState)
                .ConfigureAwait(false);
        }
        else
        {
            measurement = await runState.WaitForMeasurementAsync()
                .ConfigureAwait(false);
        }
        if (measurement.PreparationError is not null)
        {
            result.Error = measurement.PreparationError;
            return result;
        }

        try
        {
            switch (options.Mode)
            {
                case LoadMode.Connection:
                    await client.WaitForRemoteCloseAsync(
                            measurement.SendingCancellationToken)
                        .ConfigureAwait(false);
                    throw new EndOfStreamException(
                        "Gateway closed the connection-only client before the " +
                        "measurement window ended.");
                case LoadMode.InvalidPacket:
                    await client.SendInvalidPacketAndWaitForCloseAsync(
                            measurement.SendingCancellationToken)
                        .ConfigureAwait(false);
                    break;
                case LoadMode.Slowloris:
                    await RunSlowlorisAsync(
                            client,
                            options,
                            result,
                            runState,
                            measurement.SendingCancellationToken)
                        .ConfigureAwait(false);
                    break;
                case LoadMode.Heartbeat:
                    if (clientIndex < options.ActiveSenders)
                    {
                        await RunHeartbeatAsync(
                                client,
                                clientIndex,
                                options.ActiveSenders,
                                options.MessagesPerSecond,
                                result,
                                runState,
                                measurement.SendingCancellationToken)
                            .ConfigureAwait(false);
                    }
                    else
                    {
                        await RunInactiveHeartbeatKeepAliveAsync(
                                client,
                                clientIndex - options.ActiveSenders,
                                options.Connections - options.ActiveSenders,
                                options.InactiveHeartbeatInterval,
                                measurement.LifetimeCancellationToken)
                            .ConfigureAwait(false);
                    }
                    break;
                case LoadMode.Chat:
                    if (result.IsSlowReader)
                    {
                        await RunSlowReaderHeartbeatAsync(
                                client,
                                clientIndex -
                                (options.Connections - options.SlowReaders),
                                options.SlowReaders,
                                options.InactiveHeartbeatInterval,
                                measurement.LifetimeCancellationToken)
                            .ConfigureAwait(false);
                        break;
                    }

                    var receiver = LoadLoopCoordinator.ObserveAsync(
                        RunChatReceiverAsync(
                            client,
                            clientIndex,
                            result,
                            runState,
                            measurement.LifetimeCancellationToken),
                        clientIndex,
                        "chat receiver",
                        runState,
                        measurement.LifetimeCancellationToken);
                    if (clientIndex < options.ActiveSenders)
                    {
                        var targetUserId = measurement.TargetUserIds[clientIndex];
                        var expectedRecipientClientIndexes =
                            measurement.ExpectedRecipientClientIndexes[clientIndex];
                        var sender = LoadLoopCoordinator.ObserveAsync(
                            RunChatSenderAsync(
                                client,
                                clientIndex,
                                options.ActiveSenders,
                                targetUserId,
                                sharedChatPayload,
                                options.MessagesPerSecond,
                                result,
                                runState,
                                expectedRecipientClientIndexes,
                                measurement.SendingCancellationToken,
                                measurement.LifetimeCancellationToken),
                            clientIndex,
                            "chat sender",
                            runState,
                            measurement.SendingCancellationToken);
                        await Task.WhenAll(
                                sender,
                                receiver)
                            .ConfigureAwait(false);
                    }
                    else
                    {
                        var inactiveClientIndex = clientIndex - options.ActiveSenders;
                        var inactiveClientCount = options.Connections -
                                                  options.ActiveSenders -
                                                  options.SlowReaders;
                        var heartbeat = LoadLoopCoordinator.ObserveAsync(
                            RunInactiveChatHeartbeatAsync(
                                client,
                                inactiveClientIndex,
                                inactiveClientCount,
                                options.InactiveHeartbeatInterval,
                                measurement.LifetimeCancellationToken),
                            clientIndex,
                            "inactive chat heartbeat",
                            runState,
                            measurement.LifetimeCancellationToken);
                        await Task.WhenAll(
                                heartbeat,
                                receiver)
                            .ConfigureAwait(false);
                    }
                    break;
                default:
                    throw new InvalidOperationException(
                        $"Unsupported load mode {options.Mode}.");
            }
        }
        catch (OperationCanceledException)
            when (measurement.SendingCancellationToken.IsCancellationRequested ||
                  measurement.LifetimeCancellationToken.IsCancellationRequested)
        {
            // Normal measurement/drain phase transition or coordinated shutdown.
        }
        result.CompletedNormally = true;
    }
    catch (Exception exception)
    {
        result.Error = exception.Message;
        result.Connected = false;
        ClassifyPhaseFailure(result, exception);
        if (!preparationCompleted)
        {
            runState.CompletePreparation(
                clientIndex,
                identity: null,
                exception.Message);
            preparationCompleted = true;
            runState.FailRuntime(
                $"Client {clientIndex} failed during preparation: {exception.Message}");
        }
        else
        {
            runState.FailRuntime(
                $"Client {clientIndex} failed during measurement/drain: {exception.Message}");
        }
    }
    finally
    {
        if (!preparationCompleted)
        {
            runState.CompletePreparation(
                clientIndex,
                identity: null,
                result.Error ?? "Client preparation ended unexpectedly.");
        }

        if (connectionAccepted)
            runState.OnConnectionClosed();

        if (options.Mode == LoadMode.Chat &&
            clientIndex < options.ActiveSenders)
        {
            runState.CompleteChatSender(clientIndex);
        }
    }

    return result;
}

static async Task RunHeartbeatAsync(
    ProtocolClient client,
    int clientIndex,
    int totalClients,
    double messagesPerSecond,
    ClientResult result,
    LoadRunState runState,
    CancellationToken cancellationToken)
{
    await DelayInitialOperationAsync(
            clientIndex,
            totalClients,
            messagesPerSecond,
            cancellationToken)
        .ConfigureAwait(false);
    using var timer = new PeriodicTimer(
        TimeSpan.FromSeconds(1d / messagesPerSecond));

    while (!cancellationToken.IsCancellationRequested)
    {
        var startedAt = Stopwatch.GetTimestamp();
        await client.SendHeartbeatAsync(cancellationToken)
            .ConfigureAwait(false);
        runState.RecordSent();
        await client.ReceiveHeartbeatAsync(cancellationToken)
            .ConfigureAwait(false);
        runState.RecordAcknowledged();
        runState.RecordLatency(
            Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds,
            result.IsSlowReader);

        if (!await timer.WaitForNextTickAsync(cancellationToken)
                .ConfigureAwait(false))
        {
            break;
        }
    }
}

static async Task RunInactiveHeartbeatKeepAliveAsync(
    ProtocolClient client,
    int clientIndex,
    int totalClients,
    TimeSpan interval,
    CancellationToken cancellationToken)
{
    if (interval <= TimeSpan.Zero)
    {
        throw new InvalidOperationException(
            "Inactive heartbeat clients require a positive heartbeat interval.");
    }

    var phase = TimeSpan.FromTicks(
        interval.Ticks * clientIndex / Math.Max(1, totalClients));
    if (phase > TimeSpan.Zero)
        await Task.Delay(phase, cancellationToken).ConfigureAwait(false);

    using var timer = new PeriodicTimer(interval);
    while (!cancellationToken.IsCancellationRequested)
    {
        await client.SendHeartbeatAsync(cancellationToken)
            .ConfigureAwait(false);
        await client.ReceiveHeartbeatAsync(cancellationToken)
            .ConfigureAwait(false);
        if (!await timer.WaitForNextTickAsync(cancellationToken)
                .ConfigureAwait(false))
        {
            break;
        }
    }
}

static async Task RunSlowlorisAsync(
    ProtocolClient client,
    LoadOptions options,
    ClientResult result,
    LoadRunState runState,
    CancellationToken cancellationToken)
{
    try
    {
        var closedByGateway = await client.SendSlowlorisAndWaitForCloseAsync(
                options.SlowlorisPhase!.Value,
                options.SlowlorisDelayMs,
                cancellationToken)
            .ConfigureAwait(false);
        if (closedByGateway)
        {
            runState.RecordAcknowledged();
            return;
        }
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        // The shared measurement window ended before the gateway closed it.
    }

    result.Error = "Gateway did not close the slowloris connection within the deadline.";
    result.Connected = false;
}

static async Task RunChatSenderAsync(
    ProtocolClient client,
    int clientIndex,
    int totalClients,
    long targetUserId,
    string content,
    double messagesPerSecond,
    ClientResult result,
    LoadRunState runState,
    IReadOnlySet<int> expectedRecipientClientIndexes,
    CancellationToken sendingCancellationToken,
    CancellationToken lifetimeCancellationToken)
{
    try
    {
        await DelayInitialOperationAsync(
                clientIndex,
                totalClients,
                messagesPerSecond,
                sendingCancellationToken)
            .ConfigureAwait(false);
        using var timer = new PeriodicTimer(
            TimeSpan.FromSeconds(1d / messagesPerSecond));

        while (!sendingCancellationToken.IsCancellationRequested)
        {
            var startedAt = Stopwatch.GetTimestamp();
            var messageId = LoadMessageCorrelation.Create(startedAt);
            var tracked = runState.TryTrack(
                messageId,
                startedAt,
                expectedRecipientClientIndexes);

            try
            {
                // A send admitted before the measurement deadline may finish;
                // only runtime failure/overall shutdown cancels the write itself.
                await client.SendChatMessageAsync(
                        new ChatMessage
                        {
                            MessageId = messageId,
                            TargetUserId = targetUserId,
                            Content = content
                        },
                        lifetimeCancellationToken)
                    .ConfigureAwait(false);
                runState.RecordSent();
            }
            catch
            {
                if (tracked)
                    runState.Discard(messageId);
                throw;
            }

            if (!await timer.WaitForNextTickAsync(
                        sendingCancellationToken)
                    .ConfigureAwait(false))
            {
                break;
            }
        }
    }
    finally
    {
        runState.CompleteChatSender(clientIndex);
    }
}

static async Task RunChatReceiverAsync(
    ProtocolClient client,
    int clientIndex,
    ClientResult result,
    LoadRunState runState,
    CancellationToken cancellationToken)
{
    while (!cancellationToken.IsCancellationRequested)
    {
        var inbound = await client
            .ReceiveChatInboundAsync(cancellationToken)
            .ConfigureAwait(false);
        HandleChatInbound(inbound, clientIndex, result, runState);
    }
}

static void HandleChatInbound(
    ChatInboundFrame inbound,
    int clientIndex,
    ClientResult result,
    LoadRunState runState)
{
    if (inbound.IsHeartbeatAcknowledgement)
        return;

    if (inbound.Acknowledgement is { } acknowledgement)
    {
        if (acknowledgement.Accepted)
        {
            runState.RecordChatAcknowledged(
                acknowledgement.ClientMessageId);
        }
        else
        {
            runState.RecordRejected();
            runState.FailRuntime(
                $"Gateway rejected a chat operation: " +
                $"{acknowledgement.ErrorCode ?? "unknown_error"}.");
        }

        return;
    }

    var message = inbound.Message
        ?? throw new InvalidDataException(
            "Chat inbound frame contained no payload.");
    runState.RecordChatDelivered(
        message.ClientMessageId ?? message.MessageId,
        clientIndex,
        result.IsSlowReader);
}

static async Task<MeasurementContext>
    WaitForMeasurementWithWriteOnlyHeartbeatAsync(
        ProtocolClient client,
        int clientIndex,
        int totalClients,
        TimeSpan interval,
        TimeSpan heartbeatTimeout,
        LoadRunState runState)
{
    var measurement = runState.WaitForMeasurementAsync();
    var lifetimeToken = runState.LifetimeCancellationToken;
    var phase = TimeSpan.FromTicks(
        interval.Ticks * clientIndex / Math.Max(1, totalClients));
    if (phase > TimeSpan.Zero &&
        await Task.WhenAny(
                measurement,
                Task.Delay(phase, lifetimeToken))
            .ConfigureAwait(false) == measurement)
    {
        return await measurement.ConfigureAwait(false);
    }

    while (!measurement.IsCompleted)
    {
        using var heartbeatCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(lifetimeToken);
        heartbeatCancellation.CancelAfter(heartbeatTimeout);
        try
        {
            // A slow reader is write-only from authentication onward. It never
            // consumes warmup ACKs or a business frame at the measurement edge.
            await client.SendHeartbeatAsync(heartbeatCancellation.Token)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (!lifetimeToken.IsCancellationRequested &&
                  heartbeatCancellation.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Pre-measurement slow-reader heartbeat exceeded " +
                $"{heartbeatTimeout.TotalSeconds:F0}s.");
        }

        var delay = Task.Delay(interval, lifetimeToken);
        if (await Task.WhenAny(measurement, delay).ConfigureAwait(false) == measurement)
            break;
        lifetimeToken.ThrowIfCancellationRequested();
    }

    return await measurement.ConfigureAwait(false);
}

static async Task<MeasurementContext> WaitForMeasurementWithHeartbeatAsync(
    ProtocolClient client,
    int clientIndex,
    ClientResult result,
    LoadMode mode,
    TimeSpan interval,
    TimeSpan heartbeatTimeout,
    LoadRunState runState)
{
    var measurement = runState.WaitForMeasurementAsync();
    var lifetimeToken = runState.LifetimeCancellationToken;
    while (!measurement.IsCompleted)
    {
        var delay = Task.Delay(interval, lifetimeToken);
        if (await Task.WhenAny(measurement, delay).ConfigureAwait(false) == measurement)
            break;

        lifetimeToken.ThrowIfCancellationRequested();
        using var heartbeatCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(lifetimeToken);
        heartbeatCancellation.CancelAfter(heartbeatTimeout);
        try
        {
            await client.SendHeartbeatAsync(heartbeatCancellation.Token)
                .ConfigureAwait(false);
            if (mode == LoadMode.Chat)
            {
                while (true)
                {
                    var inbound = await client
                        .ReceiveChatInboundAsync(heartbeatCancellation.Token)
                        .ConfigureAwait(false);
                    if (inbound.IsHeartbeatAcknowledgement)
                        break;

                    // Measurement can begin while this bounded warmup heartbeat
                    // is in flight. Route any early terminal frame instead of
                    // consuming it as the heartbeat acknowledgement.
                    HandleChatInbound(inbound, clientIndex, result, runState);
                }
            }
            else
            {
                await client.ReceiveHeartbeatAsync(heartbeatCancellation.Token)
                    .ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
            when (!lifetimeToken.IsCancellationRequested &&
                  heartbeatCancellation.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Pre-measurement heartbeat exceeded " +
                $"{heartbeatTimeout.TotalSeconds:F0}s.");
        }
    }

    return await measurement.ConfigureAwait(false);
}

static async Task RunSlowReaderHeartbeatAsync(
    ProtocolClient client,
    int clientIndex,
    int totalClients,
    TimeSpan interval,
    CancellationToken cancellationToken)
{
    if (interval <= TimeSpan.Zero)
    {
        throw new InvalidOperationException(
            "Slow readers require a positive heartbeat interval so an idle " +
            "disconnect cannot be mistaken for a healthy slow consumer.");
    }

    // Deliberately never consume heartbeat ACKs or chat deliveries: this keeps
    // the connection a true slow consumer while inbound heartbeat writes keep
    // the protocol session alive and surface a closed socket as a run failure.
    var phase = TimeSpan.FromTicks(
        interval.Ticks * clientIndex / Math.Max(1, totalClients));
    if (phase > TimeSpan.Zero)
        await Task.Delay(phase, cancellationToken).ConfigureAwait(false);

    using var timer = new PeriodicTimer(interval);
    while (!cancellationToken.IsCancellationRequested)
    {
        await client.SendHeartbeatAsync(cancellationToken)
            .ConfigureAwait(false);
        if (!await timer.WaitForNextTickAsync(cancellationToken)
                .ConfigureAwait(false))
        {
            break;
        }
    }
}

static async Task RunInactiveChatHeartbeatAsync(
    ProtocolClient client,
    int clientIndex,
    int totalClients,
    TimeSpan interval,
    CancellationToken cancellationToken)
{
    if (interval <= TimeSpan.Zero)
    {
        await WaitForDurationAsync(cancellationToken).ConfigureAwait(false);
        return;
    }

    var phase = TimeSpan.FromTicks(
        interval.Ticks * clientIndex / Math.Max(1, totalClients));
    if (phase > TimeSpan.Zero)
        await Task.Delay(phase, cancellationToken).ConfigureAwait(false);

    using var timer = new PeriodicTimer(interval);
    while (!cancellationToken.IsCancellationRequested)
    {
        await client.SendHeartbeatAsync(cancellationToken)
            .ConfigureAwait(false);
        if (!await timer.WaitForNextTickAsync(cancellationToken)
                .ConfigureAwait(false))
        {
            break;
        }
    }
}

static async Task WaitForDurationAsync(CancellationToken cancellationToken)
{
    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)
        .ConfigureAwait(false);
}

static async Task WaitForCancellationAsync(CancellationToken cancellationToken)
{
    try
    {
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)
            .ConfigureAwait(false);
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        // Expected timer completion.
    }
}

static async Task DelayInitialOperationAsync(
    int clientIndex,
    int totalClients,
    double messagesPerSecond,
    CancellationToken cancellationToken)
{
    var intervalSeconds = 1d / messagesPerSecond;
    var phaseSeconds = intervalSeconds * clientIndex / totalClients;
    if (phaseSeconds <= 0d)
        return;

    await Task.Delay(
            TimeSpan.FromSeconds(phaseSeconds),
            cancellationToken)
        .ConfigureAwait(false);
}

static ulong? AddDeviceOffset(ulong? deviceIdHash, int clientIndex)
{
    if (deviceIdHash is null)
        return null;
    return unchecked(deviceIdHash.Value + (ulong)clientIndex);
}

static void ClassifyPhaseFailure(ClientResult result, Exception exception)
{
    if (exception is EndOfStreamException)
    {
        result.ServerClosed = true;
        return;
    }

    if (exception is InvalidDataException)
    {
        result.ProtocolRejected = true;
        return;
    }

    if (exception is TimeoutException)
    {
        result.ChatReceiveFailed = true;
        return;
    }

    // 发送/接收阶段无法在此处精确区分来源，统一归为通讯阶段失败。
    result.ChatSendFailed = true;
}

static async Task<Task<ClientResult>[]> CreateClientsAsync(
    LoadOptions options,
    LoadRunState runState,
    string sharedChatPayload)
{
    var clients = new Task<ClientResult>[options.Connections];
    var rampStartedAt = Stopwatch.GetTimestamp();

    for (var index = 0; index < clients.Length; index++)
    {
        if (runState.LifetimeCancellationToken.IsCancellationRequested)
        {
            CompleteUnstartedClients(options, runState, clients, index);
            break;
        }

        if (options.ConnectionsPerSecond > 0 && index > 0)
        {
            var due = TimeSpan.FromSeconds(
                index / (double)options.ConnectionsPerSecond);
            try
            {
                await DelayUntilAsync(
                        rampStartedAt,
                        due,
                        runState.LifetimeCancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (runState.LifetimeCancellationToken.IsCancellationRequested)
            {
                CompleteUnstartedClients(options, runState, clients, index);
                break;
            }
        }

        clients[index] = RunClientAsync(
            options,
            index,
            runState,
            sharedChatPayload);
    }

    return clients;
}

static void CompleteUnstartedClients(
    LoadOptions options,
    LoadRunState runState,
    Task<ClientResult>[] clients,
    int firstIndex)
{
    var error = runState.RuntimeFailure ??
                "The shared load lifecycle was canceled before client preparation.";
    for (var index = firstIndex; index < clients.Length; index++)
    {
        var isSlowReader = options.Mode == LoadMode.Chat &&
                           index >= options.Connections - options.SlowReaders;
        runState.CompletePreparation(index, identity: null, error);
        if (options.Mode == LoadMode.Chat && index < options.ActiveSenders)
            runState.CompleteChatSender(index);
        clients[index] = Task.FromResult(new ClientResult
        {
            Connected = false,
            IsSlowReader = isSlowReader,
            Error = error
        });
    }
}

static async Task DelayUntilAsync(
    long startedAt,
    TimeSpan due,
    CancellationToken cancellationToken)
{
    while (true)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var remaining = due - Stopwatch.GetElapsedTime(startedAt);
        if (remaining <= TimeSpan.Zero)
            return;

        if (remaining > TimeSpan.FromMilliseconds(2))
        {
            await Task.Delay(
                    remaining - TimeSpan.FromMilliseconds(1),
                    cancellationToken)
                .ConfigureAwait(false);
        }
        else
        {
            await Task.Yield();
        }
    }
}

internal sealed class ClientResult
{
    public bool Connected { get; set; }
    public bool IsSlowReader { get; set; }
    public string? Error { get; set; }

    // 分阶段归因：TCP 连接是否建立。
    public bool TcpConnectSucceeded { get; set; }

    // 认证阶段：仅认证模式（heartbeat/chat）有意义。
    public AuthFailureKind AuthFailureKind { get; set; } = AuthFailureKind.None;
    public bool AuthSucceeded { get; set; }
    public bool AuthSucceededWithoutResumeToken { get; set; }

    // 通讯阶段失败：发送或接收阶段抛出的末次异常类型。
    public bool ChatSendFailed { get; set; }
    public bool ChatReceiveFailed { get; set; }
    public bool ServerClosed { get; set; }
    public bool ProtocolRejected { get; set; }
    public bool CompletedNormally { get; set; }
}
