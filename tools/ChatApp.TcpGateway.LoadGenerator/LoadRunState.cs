using System.Collections.Concurrent;
using System.Diagnostics;

namespace ChatApp.TcpGateway.LoadGenerator;

internal sealed class LoadRunState
{
    private readonly int _expectedClients;
    private readonly int _maxInflight;
    private readonly long _inflightTtlTicks;
    private readonly long _pruneIntervalTicks;
    private readonly ConcurrentDictionary<int, byte> _completedPreparation = new();
    private readonly ConcurrentDictionary<int, AuthenticatedIdentity> _identities = new();
    private readonly ConcurrentDictionary<int, string> _preparationErrors = new();
    private readonly ConcurrentDictionary<int, byte> _completedChatSenders = new();
    private readonly ConcurrentDictionary<string, InflightMessageState> _inflight = new();
    private readonly TaskCompletionSource _allPreparationCompleted =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource<MeasurementContext> _measurementStarted =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _allChatSendersCompleted =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _deliveryDrainCompleted =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly int _expectedChatSenders;
    private CancellationTokenSource? _lifecycleCancellation;
    private string? _runtimeFailure;
    private int _preparationCount;
    private int _chatSenderCompletionCount;
    private int _activeConnections;
    private int _peakActiveConnections;
    private int _trackedCount;
    private int _pruneInProgress;
    private long _nextPruneAt;
    private long _trackingDropped;
    private long _trackingExpired;
    private long _sent;
    private long _expectedDeliveries;
    private long _received;
    private long _acknowledged;
    private long _rejected;
    private long _duplicateAcknowledgements;
    private long _duplicateDeliveries;
    private int _sendingSealed;
    private int _senderCompletionTimedOut;
    private int _normalStop;
    // item 五：跨 Gateway 配对时，目标用户在另一 Gateway 上，本地无法观测投递，
    // 因此允许 ack-only 跟踪（无本地可观测 recipient 不再视为失败）。
    private readonly bool _allowAckOnlyTracking;

    public LoadRunState(
        int expectedClients,
        int maxInflight,
        TimeSpan inflightTtl,
        int expectedChatSenders,
        bool allowAckOnlyTracking = false)
    {
        _expectedClients = expectedClients;
        _maxInflight = maxInflight;
        _expectedChatSenders = expectedChatSenders;
        _allowAckOnlyTracking = allowAckOnlyTracking;
        _inflightTtlTicks = Math.Max(
            1L,
            (long)(inflightTtl.TotalSeconds * Stopwatch.Frequency));
        _pruneIntervalTicks = Math.Max(
            1L,
            Math.Min(
                _inflightTtlTicks / 4,
                Stopwatch.Frequency * 10L));
        if (expectedChatSenders == 0)
            _allChatSendersCompleted.TrySetResult();
    }

    public FixedLatencyHistogram Latency { get; } = new();
    public FixedLatencyHistogram AcknowledgementLatency { get; } = new();
    public FixedLatencyHistogram DeliveryLatency { get; } = new();
    public FixedLatencyHistogram HealthyLatency { get; } = new();
    public FixedLatencyHistogram SlowLatency { get; } = new();
    public Task AllPreparationCompleted => _allPreparationCompleted.Task;
    public int PeakActiveConnections => Volatile.Read(ref _peakActiveConnections);
    public int OutstandingCount => Volatile.Read(ref _trackedCount);
    public long TrackingDropped => Volatile.Read(ref _trackingDropped);
    public long TrackingExpired => Volatile.Read(ref _trackingExpired);
    public string? RuntimeFailure => Volatile.Read(ref _runtimeFailure);
    public Task AllChatSendersCompleted => _allChatSendersCompleted.Task;
    public Task DeliveryDrainCompleted => _deliveryDrainCompleted.Task;
    public bool IsDeliveryDrainCompleted =>
        Volatile.Read(ref _senderCompletionTimedOut) == 0 &&
        _deliveryDrainCompleted.Task.IsCompletedSuccessfully;

    public LoadCounterSnapshot SnapshotCounters() =>
        new(
            Volatile.Read(ref _sent),
            Volatile.Read(ref _expectedDeliveries),
            Volatile.Read(ref _received),
            Volatile.Read(ref _acknowledged),
            Volatile.Read(ref _rejected),
            Volatile.Read(ref _duplicateAcknowledgements),
            Volatile.Read(ref _duplicateDeliveries));

    public void RecordSent() => Interlocked.Increment(ref _sent);

    public void RecordAcknowledged()
    {
        Interlocked.Increment(ref _acknowledged);
        TryCompleteDeliveryDrain();
    }

    public void RecordRejected() => Interlocked.Increment(ref _rejected);

    public MessageSignalRecordResult RecordChatAcknowledged(
        string? clientMessageId)
    {
        if (string.IsNullOrWhiteSpace(clientMessageId) ||
            !_inflight.TryGetValue(clientMessageId, out var state))
        {
            return RecordDuplicateAcknowledgement(clientMessageId);
        }

        var completion = state.TryAcknowledge();
        if (completion == MessageSignalCompletion.DuplicateOrUnexpected)
            return RecordDuplicateAcknowledgement(clientMessageId);

        return RecordUniqueSignal(
            state,
            completion,
            Stopwatch.GetTimestamp(),
            ref _acknowledged,
            AcknowledgementLatency,
            isDelivery: false,
            isSlowReader: false);
    }

    public MessageSignalRecordResult RecordChatDelivered(
        string? clientMessageId,
        int recipientClientIndex,
        bool isSlowReader)
    {
        if (string.IsNullOrWhiteSpace(clientMessageId) ||
            !_inflight.TryGetValue(clientMessageId, out var state))
        {
            return RecordDuplicateDelivery(clientMessageId, recipientClientIndex);
        }

        var completion = state.TryDeliver(recipientClientIndex);
        if (completion == MessageSignalCompletion.DuplicateOrUnexpected)
            return RecordDuplicateDelivery(clientMessageId, recipientClientIndex);

        return RecordUniqueSignal(
            state,
            completion,
            Stopwatch.GetTimestamp(),
            ref _received,
            DeliveryLatency,
            isDelivery: true,
            isSlowReader);
    }

    public void OnConnectionAccepted()
    {
        var current = Interlocked.Increment(ref _activeConnections);
        while (true)
        {
            var oldPeak = Volatile.Read(ref _peakActiveConnections);
            if (current <= oldPeak)
                return;
            if (Interlocked.CompareExchange(
                    ref _peakActiveConnections,
                    current,
                    oldPeak) == oldPeak)
            {
                return;
            }
        }
    }

    public void OnConnectionClosed() =>
        Interlocked.Decrement(ref _activeConnections);

    public void CompletePreparation(
        int clientIndex,
        AuthenticatedIdentity? identity,
        string? error)
    {
        if (!_completedPreparation.TryAdd(clientIndex, 0))
            return;

        if (identity is not null)
            _identities[clientIndex] = identity;
        if (!string.IsNullOrWhiteSpace(error))
            _preparationErrors[clientIndex] = error;

        if (Interlocked.Increment(ref _preparationCount) == _expectedClients)
            _allPreparationCompleted.TrySetResult();
    }

    public TargetPlan CreateTargetPlan(LoadOptions options)
    {
        var uniqueUsers = _identities.Values
            .Select(static identity => identity.UserId)
            .Distinct()
            .Order()
            .ToArray();
        var errors = _preparationErrors
            .OrderBy(static pair => pair.Key)
            .Select(static pair => $"client {pair.Key}: {pair.Value}")
            .ToList();

        if (_preparationCount != _expectedClients)
        {
            errors.Add(
                $"Only {_preparationCount}/{_expectedClients} clients completed preparation.");
        }

        if (!_preparationErrors.IsEmpty)
        {
            errors.Insert(
                0,
                $"{_preparationErrors.Count}/{_expectedClients} clients failed connection or authentication.");
        }

        var authenticatedMode = options.Mode is LoadMode.Heartbeat or LoadMode.Chat;
        if (authenticatedMode && _identities.Count != _expectedClients)
        {
            errors.Add(
                $"Only {_identities.Count}/{_expectedClients} clients authenticated successfully.");
        }

        if (options.Mode != LoadMode.Chat)
        {
            return new TargetPlan(
                "not-applicable",
                uniqueUsers.Length,
                new Dictionary<int, long>(),
                new Dictionary<int, IReadOnlySet<int>>(),
                CombineErrors(errors));
        }

        // item 五：跨 Gateway 配对。编排器为每个连接写一行目标用户 id，
        // 目标常落在另一 Gateway 上，因此本地不要求投递可观测（ack-only）。
        if (options.TargetRingFilePath is not null)
        {
            return CreateCrossGatewayTargetPlan(options, errors);
        }

        var targetUserIds = new Dictionary<int, long>(_identities.Count);
        var expectedRecipientClientIndexes =
            new Dictionary<int, IReadOnlySet<int>>(options.ActiveSenders);
        var observableRecipientsByUser = _identities
            .Where(pair => pair.Key < options.Connections - options.SlowReaders)
            .GroupBy(static pair => pair.Value.UserId)
            .ToDictionary(
                static group => group.Key,
                static group => (IReadOnlySet<int>)group
                    .Select(static pair => pair.Key)
                    .ToHashSet());

        void AddExpectedRecipients(int senderClientIndex, long targetUserId)
        {
            if (senderClientIndex >= options.ActiveSenders)
                return;

            if (!observableRecipientsByUser.TryGetValue(
                    targetUserId,
                    out var recipients) ||
                recipients.Count == 0)
            {
                errors.Add(
                    $"Active sender client {senderClientIndex} targets user " +
                    $"{targetUserId}, but no readable load client can observe delivery.");
                return;
            }

            // Realtime emits both the target-user delivery and a sender echo.
            // The gateway skips every local connection whose SessionId equals
            // the event's origin SessionId. Reused sessions are therefore all
            // excluded; when the authenticated SessionId is absent the gateway
            // uses a non-empty fallback event id and no null-session connection
            // is skipped.
            var expectedRecipients = new HashSet<int>(recipients);
            var senderIdentity = _identities[senderClientIndex];
            var senderUserId = senderIdentity.UserId;
            if (observableRecipientsByUser.TryGetValue(
                    senderUserId,
                    out var senderDevices))
            {
                foreach (var senderDeviceIndex in senderDevices)
                {
                    var deviceSessionId = _identities[senderDeviceIndex].SessionId;
                    if (!string.IsNullOrWhiteSpace(senderIdentity.SessionId) &&
                        string.Equals(
                            deviceSessionId,
                            senderIdentity.SessionId,
                            StringComparison.Ordinal))
                    {
                        continue;
                    }

                    expectedRecipients.Add(senderDeviceIndex);
                }
            }

            expectedRecipientClientIndexes[senderClientIndex] = expectedRecipients;
        }

        if (options.TargetUserId is { } fixedTargetUserId)
        {
            foreach (var pair in _identities)
            {
                targetUserIds[pair.Key] = fixedTargetUserId;
                if (pair.Key >= options.ActiveSenders)
                    continue;

                if (pair.Value.UserId == fixedTargetUserId)
                {
                    errors.Add(
                        $"Fixed target user {fixedTargetUserId} equals authenticated " +
                        $"user for active sender client {pair.Key}; self-chat is forbidden.");
                    continue;
                }

                AddExpectedRecipients(pair.Key, fixedTargetUserId);
            }

            return new TargetPlan(
                "fixed-user",
                uniqueUsers.Length,
                targetUserIds,
                expectedRecipientClientIndexes,
                CombineErrors(errors));
        }

        if (uniqueUsers.Length < 2)
        {
            errors.Add(
                "Peer-ring targeting requires at least two distinct authenticated users. " +
                "Provide multiple users in --token-file or an explicit non-self --target-user-id.");
        }
        else
        {
            var targetByUser = new Dictionary<long, long>(uniqueUsers.Length);
            for (var index = 0; index < uniqueUsers.Length; index++)
            {
                targetByUser[uniqueUsers[index]] =
                    uniqueUsers[(index + 1) % uniqueUsers.Length];
            }

            foreach (var pair in _identities)
            {
                var targetUserId = targetByUser[pair.Value.UserId];
                targetUserIds[pair.Key] = targetUserId;
                AddExpectedRecipients(pair.Key, targetUserId);
            }
        }

        return new TargetPlan(
            "peer-ring",
            uniqueUsers.Length,
            targetUserIds,
            expectedRecipientClientIndexes,
            CombineErrors(errors));
    }

    /// <summary>
    /// item 五：跨 Gateway 配对。目标文件每行 = 一个连接的目标用户 id（行号 =
    /// 连接序号）。目标常落在另一 Gateway 上，本地无法观测投递，因此
    /// expected recipients 保持为空（ack-only 跟踪），由接收侧 Gateway 的
    /// LoadGenerator 观测实际投递。
    /// </summary>
    private TargetPlan CreateCrossGatewayTargetPlan(
        LoadOptions options,
        List<string> errors)
    {
        var ring = LoadTargetRing(options.TargetRingFilePath!, _identities.Count, errors);
        if (ring is null)
        {
            return new TargetPlan(
                "peer-ring",
                _identities.Count,
                new Dictionary<int, long>(),
                new Dictionary<int, IReadOnlySet<int>>(),
                CombineErrors(errors));
        }

        // 跨 Gateway 模式下本地不要求投递可观测，为每个 sender 提供空预期集。
        var targetUserIds = new Dictionary<int, long>(_identities.Count);
        var expectedRecipientClientIndexes =
            new Dictionary<int, IReadOnlySet<int>>(options.ActiveSenders);
        for (var senderIndex = 0; senderIndex < options.ActiveSenders; senderIndex++)
            expectedRecipientClientIndexes[senderIndex] = new HashSet<int>();

        foreach (var pair in _identities.OrderBy(static pair => pair.Key))
        {
            var targetUserId = ring[pair.Key];
            if (targetUserId <= 0)
            {
                errors.Add(
                    $"Cross-gateway target ring had an invalid target for client " +
                    $"{pair.Key}: {targetUserId}.");
                continue;
            }

            if (targetUserId == pair.Value.UserId)
            {
                errors.Add(
                    $"Cross-gateway target ring targets self for client {pair.Key}; " +
                    "self-chat is forbidden.");
                continue;
            }

            targetUserIds[pair.Key] = targetUserId;
        }

        return new TargetPlan(
            "peer-ring",
            _identities.Count,
            targetUserIds,
            expectedRecipientClientIndexes,
            CombineErrors(errors));
    }

    private static long[]? LoadTargetRing(
        string path,
        int expectedCount,
        List<string> errors)
    {
        string[] lines;
        try
        {
            lines = File.ReadLines(path)
                .Select(static line => line.Trim())
                .Where(static line => !string.IsNullOrWhiteSpace(line))
                .ToArray();
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            errors.Add($"Failed to read target ring file {path}: {exception.Message}");
            return null;
        }

        if (lines.Length != expectedCount)
        {
            errors.Add(
                $"Target ring file {path} contains {lines.Length} entries; " +
                $"expected exactly {expectedCount} (one per connection).");
            return null;
        }

        var ring = new long[expectedCount];
        for (var index = 0; index < expectedCount; index++)
        {
            if (!long.TryParse(
                    lines[index],
                    System.Globalization.NumberStyles.None,
                    System.Globalization.CultureInfo.InvariantCulture,
                    out var target))
            {
                errors.Add(
                    $"Target ring file {path} line {index} is not a valid user id.");
                return null;
            }

            ring[index] = target;
        }

        return ring;
    }

    public bool StartMeasurement(
        TargetPlan targetPlan,
        bool measurementAllowed,
        CancellationToken sendingCancellationToken)
    {
        var runtimeFailure = RuntimeFailure;
        var mayStart = measurementAllowed &&
                       targetPlan.Error is null &&
                       runtimeFailure is null &&
                       !LifetimeCancellationToken.IsCancellationRequested;
        if (!mayStart)
            Volatile.Read(ref _lifecycleCancellation)?.Cancel();

        _measurementStarted.TrySetResult(
            new MeasurementContext(
                targetPlan.TargetUserIds,
                targetPlan.ExpectedRecipientClientIndexes,
                mayStart
                    ? null
                    : targetPlan.Error ?? runtimeFailure ??
                      "Measurement was canceled before it started.",
                sendingCancellationToken,
                LifetimeCancellationToken));
        return mayStart;
    }

    public CancellationToken LifetimeCancellationToken =>
        Volatile.Read(ref _lifecycleCancellation)?.Token ?? CancellationToken.None;

    public void AttachLifecycle(CancellationTokenSource lifecycleCancellation)
    {
        ArgumentNullException.ThrowIfNull(lifecycleCancellation);
        if (Interlocked.CompareExchange(
                ref _lifecycleCancellation,
                lifecycleCancellation,
                comparand: null) is not null)
        {
            throw new InvalidOperationException("The load lifecycle is already attached.");
        }

        if (RuntimeFailure is not null)
            lifecycleCancellation.Cancel();
    }

    public void FailRuntime(string reason)
    {
        if (Volatile.Read(ref _normalStop) != 0)
            return;
        if (Interlocked.CompareExchange(
                ref _runtimeFailure,
                reason,
                comparand: null) is not null)
        {
            return;
        }

        Volatile.Read(ref _lifecycleCancellation)?.Cancel();
    }

    public void CompleteChatSender(int clientIndex)
    {
        if (!_completedChatSenders.TryAdd(clientIndex, 0))
            return;

        if (Interlocked.Increment(ref _chatSenderCompletionCount) ==
            _expectedChatSenders)
        {
            _allChatSendersCompleted.TrySetResult();
        }
    }

    public void SealSending()
    {
        Volatile.Write(ref _sendingSealed, 1);
        TryCompleteDeliveryDrain();
    }

    public void FailSenderCompletionTimeout(TimeSpan timeout)
    {
        Volatile.Write(ref _senderCompletionTimedOut, 1);
        FailRuntime(
            $"Chat senders did not stop within the configured delivery-drain " +
            $"budget of {timeout.TotalSeconds:F0} seconds.");
    }

    public void StopClients()
    {
        Volatile.Write(ref _normalStop, 1);
        Volatile.Read(ref _lifecycleCancellation)?.Cancel();
    }

    public Task<MeasurementContext> WaitForMeasurementAsync() =>
        _measurementStarted.Task;

    public bool TryTrack(
        string messageId,
        long startedAt,
        IReadOnlySet<int> expectedRecipientClientIndexes)
    {
        ArgumentNullException.ThrowIfNull(expectedRecipientClientIndexes);
        if (expectedRecipientClientIndexes.Count == 0 && !_allowAckOnlyTracking)
        {
            Interlocked.Increment(ref _trackingDropped);
            FailRuntime(
                $"Message {messageId} has no observable delivery recipient.");
            return false;
        }

        PruneExpired(startedAt, force: false);

        while (true)
        {
            var current = Volatile.Read(ref _trackedCount);
            if (current >= _maxInflight)
            {
                // The scheduled prune above is the only O(n) scan on this hot
                // path. Do not rescan the entire full dictionary per sender.
                Interlocked.Increment(ref _trackingDropped);
                FailRuntime(
                    $"In-flight tracking reached the configured limit {_maxInflight}.");
                return false;
            }

            if (Interlocked.CompareExchange(
                    ref _trackedCount,
                    current + 1,
                    current) != current)
            {
                continue;
            }

            if (_inflight.TryAdd(
                    messageId,
                    new InflightMessageState(
                        messageId,
                        startedAt,
                        expectedRecipientClientIndexes)))
            {
                Interlocked.Add(
                    ref _expectedDeliveries,
                    expectedRecipientClientIndexes.Count);
                return true;
            }

            Interlocked.Decrement(ref _trackedCount);
            Interlocked.Increment(ref _trackingDropped);
            FailRuntime(
                $"Duplicate in-flight message id {messageId} could not be tracked.");
            return false;
        }
    }

    public void Discard(string messageId)
    {
        if (_inflight.TryRemove(messageId, out var state))
        {
            Interlocked.Add(
                ref _expectedDeliveries,
                -state.ExpectedDeliveryCount);
            Interlocked.Decrement(ref _trackedCount);
        }
    }

    public void RecordLatency(double milliseconds, bool isSlowReader)
    {
        Latency.Record(milliseconds);
        if (isSlowReader)
            SlowLatency.Record(milliseconds);
        else
            HealthyLatency.Record(milliseconds);
    }

    public void PruneExpired() =>
        PruneExpired(Stopwatch.GetTimestamp(), force: true);

    private void PruneExpired(long now, bool force)
    {
        if (!force && now < Volatile.Read(ref _nextPruneAt))
            return;
        if (Interlocked.CompareExchange(ref _pruneInProgress, 1, 0) != 0)
            return;

        try
        {
            foreach (var pair in _inflight)
            {
                if (now - pair.Value.StartedAt < _inflightTtlTicks ||
                    !_inflight.TryRemove(pair.Key, out _))
                {
                    continue;
                }

                Interlocked.Decrement(ref _trackedCount);
                Interlocked.Increment(ref _trackingExpired);
                FailRuntime(
                    $"In-flight message {pair.Key} exceeded the configured TTL.");
                if (RuntimeFailure is not null)
                    break;
            }

            Volatile.Write(ref _nextPruneAt, now + _pruneIntervalTicks);
        }
        finally
        {
            Volatile.Write(ref _pruneInProgress, 0);
        }
    }

    private static string? CombineErrors(List<string> errors) =>
        errors.Count == 0
            ? null
            : string.Join(" ", errors.Take(8));

    private void TryCompleteDeliveryDrain()
    {
        if (Volatile.Read(ref _sendingSealed) == 0)
            return;
        if (Volatile.Read(ref _senderCompletionTimedOut) != 0)
            return;

        var sent = Volatile.Read(ref _sent);
        var expectedDeliveries = Volatile.Read(ref _expectedDeliveries);
        if (Volatile.Read(ref _acknowledged) >= sent &&
            Volatile.Read(ref _received) >= expectedDeliveries &&
            Volatile.Read(ref _trackedCount) == 0)
        {
            _deliveryDrainCompleted.TrySetResult();
        }
    }

    private MessageSignalRecordResult RecordUniqueSignal(
        InflightMessageState state,
        MessageSignalCompletion completion,
        long signalAt,
        ref long uniqueCounter,
        FixedLatencyHistogram histogram,
        bool isDelivery,
        bool isSlowReader)
    {
        var elapsedMilliseconds = Stopwatch
            .GetElapsedTime(state.StartedAt, signalAt)
            .TotalMilliseconds;
        Interlocked.Increment(ref uniqueCounter);
        histogram.Record(elapsedMilliseconds);
        if (isDelivery)
        {
            // In chat mode the compatibility Latency field is explicitly the
            // peer-delivery latency. ACK latency is reported separately.
            Latency.Record(elapsedMilliseconds);
            if (isSlowReader)
                SlowLatency.Record(elapsedMilliseconds);
            else
                HealthyLatency.Record(elapsedMilliseconds);
        }

        if (completion == MessageSignalCompletion.Completed &&
            _inflight.TryRemove(state.ClientMessageId, out _))
        {
            Interlocked.Decrement(ref _trackedCount);
        }

        TryCompleteDeliveryDrain();
        return new MessageSignalRecordResult(
            completion == MessageSignalCompletion.Completed
                ? MessageSignalRecordKind.Completed
                : MessageSignalRecordKind.Recorded,
            elapsedMilliseconds);
    }

    private MessageSignalRecordResult RecordDuplicateAcknowledgement(
        string? clientMessageId)
    {
        Interlocked.Increment(ref _duplicateAcknowledgements);
        FailRuntime(
            $"Received a duplicate or untracked acknowledgement for client " +
            $"message {clientMessageId ?? "<missing>"}.");
        return MessageSignalRecordResult.DuplicateOrUntracked;
    }

    private MessageSignalRecordResult RecordDuplicateDelivery(
        string? clientMessageId,
        int recipientClientIndex)
    {
        Interlocked.Increment(ref _duplicateDeliveries);
        FailRuntime(
            $"Received a duplicate, unexpected, or untracked delivery for " +
            $"client message {clientMessageId ?? "<missing>"} on recipient " +
            $"client {recipientClientIndex}.");
        return MessageSignalRecordResult.DuplicateOrUntracked;
    }

    private enum MessageSignalCompletion
    {
        Recorded,
        Completed,
        DuplicateOrUnexpected
    }

    private sealed class InflightMessageState(
        string clientMessageId,
        long startedAt,
        IReadOnlySet<int> expectedRecipientClientIndexes)
    {
        private readonly ConcurrentDictionary<int, byte> _deliveredRecipients = new();
        private int _acknowledged;

        public string ClientMessageId { get; } = clientMessageId;
        public long StartedAt { get; } = startedAt;
        public int ExpectedDeliveryCount => expectedRecipientClientIndexes.Count;

        public MessageSignalCompletion TryAcknowledge()
        {
            if (Interlocked.Exchange(ref _acknowledged, 1) != 0)
                return MessageSignalCompletion.DuplicateOrUnexpected;

            return _deliveredRecipients.Count == ExpectedDeliveryCount
                ? MessageSignalCompletion.Completed
                : MessageSignalCompletion.Recorded;
        }

        public MessageSignalCompletion TryDeliver(int recipientClientIndex)
        {
            if (!expectedRecipientClientIndexes.Contains(recipientClientIndex) ||
                !_deliveredRecipients.TryAdd(recipientClientIndex, 0))
            {
                return MessageSignalCompletion.DuplicateOrUnexpected;
            }

            return Volatile.Read(ref _acknowledged) != 0 &&
                   _deliveredRecipients.Count == ExpectedDeliveryCount
                ? MessageSignalCompletion.Completed
                : MessageSignalCompletion.Recorded;
        }
    }
}

internal readonly record struct LoadCounterSnapshot(
    long Sent,
    long ExpectedDeliveries,
    long Received,
    long Acknowledged,
    long Rejected,
    long DuplicateAcknowledgements,
    long DuplicateDeliveries);

internal enum MessageSignalRecordKind
{
    Recorded,
    Completed,
    DuplicateOrUntracked
}

internal readonly record struct MessageSignalRecordResult(
    MessageSignalRecordKind Kind,
    double ElapsedMilliseconds)
{
    public static MessageSignalRecordResult DuplicateOrUntracked { get; } =
        new(MessageSignalRecordKind.DuplicateOrUntracked, 0d);
}

internal sealed record TargetPlan(
    string Strategy,
    int UniqueUsers,
    IReadOnlyDictionary<int, long> TargetUserIds,
    IReadOnlyDictionary<int, IReadOnlySet<int>> ExpectedRecipientClientIndexes,
    string? Error);

internal sealed record MeasurementContext(
    IReadOnlyDictionary<int, long> TargetUserIds,
    IReadOnlyDictionary<int, IReadOnlySet<int>> ExpectedRecipientClientIndexes,
    string? PreparationError,
    CancellationToken SendingCancellationToken,
    CancellationToken LifetimeCancellationToken);
