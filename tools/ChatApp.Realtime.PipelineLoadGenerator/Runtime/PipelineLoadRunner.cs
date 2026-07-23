using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.PipelineLoadGenerator.Configuration;
using ChatApp.Realtime.PipelineLoadGenerator.Diagnostics;

namespace ChatApp.Realtime.PipelineLoadGenerator.Runtime;

internal sealed class PipelineLoadRunner
{
    private readonly IRealtimeMessageBus _messageBus;
    private readonly PipelineEventRouter _eventRouter;
    private readonly PipelineLoadOptions _options;
    private readonly string _content;

    public PipelineLoadRunner(
        IRealtimeMessageBus messageBus,
        PipelineEventRouter eventRouter,
        PipelineLoadOptions options)
    {
        _messageBus = messageBus;
        _eventRouter = eventRouter;
        _options = options;
        _content = new string('x', options.PayloadBytes);
    }

    public async Task<PipelineLoadReport> RunAsync(CancellationToken ct)
    {
        var ping = await _messageBus.PingAsync(ct).ConfigureAwait(false);
        await _eventRouter.StartAsync(ct).ConfigureAwait(false);

        if (_options.Warmup > TimeSpan.Zero)
        {
            Console.WriteLine(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"Warmup: {_options.Warmup.TotalSeconds:F0}s"));
            await RunPhaseAsync(
                    _options.Warmup,
                    measurement: null,
                    failFast: true,
                    ct)
                .ConfigureAwait(false);
            await Task.Delay(TimeSpan.FromMilliseconds(250), ct)
                .ConfigureAwait(false);
        }

        Console.WriteLine(
            string.Create(
                CultureInfo.InvariantCulture,
                $"Measurement: {_options.Duration.TotalSeconds:F0}s; " +
                $"concurrency={_options.Concurrency}; " +
                $"target={(_options.OperationsPerSecond == 0 ? "unlimited" : _options.OperationsPerSecond)} ops/s"));

        var measurement = new PipelineLoadMeasurement();
        var allocatedBefore = GC.GetTotalAllocatedBytes(precise: false);
        var startedAt = Stopwatch.GetTimestamp();
        await RunPhaseAsync(
                _options.Duration,
                measurement,
                failFast: false,
                ct)
            .ConfigureAwait(false);
        var elapsed = Stopwatch.GetElapsedTime(startedAt);
        var allocatedBytes = Math.Max(
            0,
            GC.GetTotalAllocatedBytes(precise: false) - allocatedBefore);

        return PipelineLoadReport.Create(
            _options,
            measurement,
            ping,
            elapsed,
            allocatedBytes);
    }

    private async Task RunPhaseAsync(
        TimeSpan duration,
        PipelineLoadMeasurement? measurement,
        bool failFast,
        CancellationToken ct)
    {
        var deadline = Stopwatch.GetTimestamp()
                       + (long)(duration.TotalSeconds * Stopwatch.Frequency);
        var workers = Enumerable.Range(0, _options.Concurrency)
            .Select(worker => RunWorkerAsync(
                worker,
                deadline,
                measurement,
                failFast,
                ct))
            .ToArray();
        await Task.WhenAll(workers).ConfigureAwait(false);
    }

    private async Task RunWorkerAsync(
        int worker,
        long deadline,
        PipelineLoadMeasurement? measurement,
        bool failFast,
        CancellationToken ct)
    {
        var senderUserId = _options.BaseUserId + worker * 2L;
        var receiverUserId = senderUserId + 1;
        var interval = _options.OperationsPerSecond == 0
            ? TimeSpan.Zero
            : TimeSpan.FromSeconds(
                (double)_options.Concurrency / _options.OperationsPerSecond);
        var nextStart = Stopwatch.GetTimestamp();

        while (Stopwatch.GetTimestamp() < deadline)
        {
            ct.ThrowIfCancellationRequested();
            _eventRouter.EnsureHealthy();

            if (interval > TimeSpan.Zero)
            {
                var delayTicks = nextStart - Stopwatch.GetTimestamp();
                if (delayTicks > 0)
                {
                    await Task.Delay(
                            TimeSpan.FromSeconds(
                                (double)delayTicks / Stopwatch.Frequency),
                            ct)
                        .ConfigureAwait(false);
                }
                nextStart = Math.Max(nextStart, Stopwatch.GetTimestamp())
                            + (long)(interval.TotalSeconds * Stopwatch.Frequency);
            }

            measurement?.RecordStarted();
            try
            {
                await RunOperationAsync(
                        worker,
                        senderUserId,
                        receiverUserId,
                        measurement,
                        ct)
                    .ConfigureAwait(false);
                measurement?.RecordSucceeded();
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                if (failFast)
                    throw new InvalidOperationException(
                        $"Warmup worker {worker} failed.",
                        ex);
                measurement?.RecordFailed(ex);
            }
        }
    }

    private async Task RunOperationAsync(
        int worker,
        long senderUserId,
        long receiverUserId,
        PipelineLoadMeasurement? measurement,
        CancellationToken outerToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(
            outerToken);
        timeout.CancelAfter(_options.OperationTimeout);
        var ct = timeout.Token;
        var totalStartedAt = Stopwatch.GetTimestamp();
        var clientMessageId = Guid.CreateVersion7().ToString("N");
        var messageId = CreateMessageCommandId(
            senderUserId,
            clientMessageId);
        var command = new IncomingMessageCommand
        {
            CommandId = messageId,
            ClientMessageId = clientMessageId,
            SenderUserId = senderUserId,
            SenderSessionId = $"load-{worker}",
            ReceiverUserId = receiverUserId,
            Content = _content,
            ReceivedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        using (var persisted = _eventRouter.Register(
                   messageId,
                   RealtimeEventType.MessageReceived))
        {
            var persistedStartedAt = Stopwatch.GetTimestamp();
            var publishStartedAt = Stopwatch.GetTimestamp();
            await RunStageAsync(
                    "incoming_publish",
                    () => _messageBus.PublishIncomingMessageAsync(command, ct),
                    outerToken)
                .ConfigureAwait(false);
            measurement?.MessagePublishAck.Record(
                Stopwatch.GetElapsedTime(publishStartedAt));
            await RunStageAsync(
                    "message_persisted_event",
                    () => persisted.WaitAsync(ct),
                    outerToken)
                .ConfigureAwait(false);
            measurement?.MessagePersisted.Record(
                Stopwatch.GetElapsedTime(persistedStartedAt));
        }

        var receipt = new MessageReceiptCommand
        {
            CommandId = CreateReceiptCommandId(
                receiverUserId,
                messageId,
                MessageReceiptType.Read),
            MessageId = messageId,
            ReceiverUserId = receiverUserId,
            ReceiverSessionId = $"load-receiver-{worker}",
            ReceiptType = MessageReceiptType.Read,
            OccurredAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        using (var persisted = _eventRouter.Register(
                   messageId,
                   RealtimeEventType.MessageReceiptUpdated))
        {
            var persistedStartedAt = Stopwatch.GetTimestamp();
            var publishStartedAt = Stopwatch.GetTimestamp();
            await RunStageAsync(
                    "receipt_publish",
                    () => _messageBus.PublishMessageReceiptAsync(receipt, ct),
                    outerToken)
                .ConfigureAwait(false);
            measurement?.ReceiptPublishAck.Record(
                Stopwatch.GetElapsedTime(publishStartedAt));
            await RunStageAsync(
                    "receipt_persisted_event",
                    () => persisted.WaitAsync(ct),
                    outerToken)
                .ConfigureAwait(false);
            measurement?.ReceiptPersisted.Record(
                Stopwatch.GetElapsedTime(persistedStartedAt));
        }

        var conversationId = ConversationId.CreateDirect(senderUserId, receiverUserId);

        var historyStartedAt = Stopwatch.GetTimestamp();
        var historyQuery = new MessageHistoryQuery
        {
            RequestId = Guid.CreateVersion7().ToString("N"),
            UserId = receiverUserId,
            ConversationId = conversationId,
            Limit = 10
        };
        var history = await RunStageAsync(
                "history_query",
                () => QueryWithTransientRetryAsync(
                    () => _messageBus.QueryMessageHistoryAsync(historyQuery, ct),
                    static page => page.Succeeded,
                    static page => page.ErrorCode,
                    ct,
                    "history_unavailable",
                    "history_timeout"),
                outerToken)
            .ConfigureAwait(false);
        measurement?.HistoryQuery.Record(
            Stopwatch.GetElapsedTime(historyStartedAt));

        if (!history.Succeeded)
        {
            throw new InvalidOperationException(
                $"Conversation history query failed: {history.ErrorCode ?? "unknown"}.");
        }

        var restored = history.Items.FirstOrDefault(item =>
            string.Equals(item.MessageId, messageId, StringComparison.Ordinal));
        if (restored?.DeliveredAtMs is null || restored.ReadAtMs is null)
        {
            throw new InvalidOperationException(
                "Conversation history did not return the completed read receipt.");
        }

        if (history.HasMore && history.NextCursor is { } nextCursor)
        {
            var pageQuery = new MessageHistoryQuery
            {
                RequestId = Guid.CreateVersion7().ToString("N"),
                UserId = receiverUserId,
                ConversationId = conversationId,
                BeforeReceivedAtMs = nextCursor.ReceivedAtMs,
                BeforeMessageId = nextCursor.MessageId,
                Limit = 10
            };
            var olderPage = await RunStageAsync(
                    "history_query_page",
                    () => QueryWithTransientRetryAsync(
                        () => _messageBus.QueryMessageHistoryAsync(pageQuery, ct),
                        static page => page.Succeeded,
                        static page => page.ErrorCode,
                        ct,
                        "history_unavailable",
                        "history_timeout"),
                    outerToken)
                .ConfigureAwait(false);
            if (!olderPage.Succeeded)
            {
                throw new InvalidOperationException(
                    $"Conversation history page failed: {olderPage.ErrorCode ?? "unknown"}.");
            }
        }

        var listStartedAt = Stopwatch.GetTimestamp();
        var listQuery = new ConversationListQuery
        {
            RequestId = Guid.CreateVersion7().ToString("N"),
            UserId = receiverUserId,
            Limit = 20
        };
        var list = await RunStageAsync(
                "conversation_list_query",
                () => QueryWithTransientRetryAsync(
                    () => _messageBus.QueryConversationListAsync(listQuery, ct),
                    static page => page.Succeeded,
                    static page => page.ErrorCode,
                    ct,
                    "conversation_list_unavailable",
                    "conversation_list_timeout"),
                outerToken)
            .ConfigureAwait(false);
        measurement?.ConversationListQuery.Record(
            Stopwatch.GetElapsedTime(listStartedAt));
        if (!list.Succeeded)
        {
            throw new InvalidOperationException(
                $"Conversation list query failed: {list.ErrorCode ?? "unknown"}.");
        }

        var listed = list.Items.FirstOrDefault(item =>
            string.Equals(item.ConversationId, conversationId, StringComparison.Ordinal));
        if (listed is null ||
            !string.Equals(listed.LastMessageId, messageId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "Conversation list did not contain the latest message summary.");
        }

        var markReadStartedAt = Stopwatch.GetTimestamp();
        var markRead = new ConversationMarkReadCommand
        {
            RequestId = Guid.CreateVersion7().ToString("N"),
            UserId = receiverUserId,
            ConversationId = conversationId,
            ReadAtMs = restored.ReceivedAtMs,
            ReadMessageId = messageId
        };
        var markReadResult = await RunStageAsync(
                "conversation_mark_read",
                () => QueryWithTransientRetryAsync(
                    () => _messageBus.MarkConversationReadAsync(markRead, ct),
                    static result => result.Succeeded,
                    static result => result.ErrorCode,
                    ct,
                    "conversation_mark_read_unavailable",
                    "conversation_mark_read_timeout"),
                outerToken)
            .ConfigureAwait(false);
        measurement?.ConversationMarkRead.Record(
            Stopwatch.GetElapsedTime(markReadStartedAt));
        if (!markReadResult.Succeeded)
        {
            throw new InvalidOperationException(
                $"Conversation mark-read failed: {markReadResult.ErrorCode ?? "unknown"}.");
        }

        if (markReadResult.UnreadCount != 0)
        {
            throw new InvalidOperationException(
                $"Expected unread count 0 after mark-read, got {markReadResult.UnreadCount}.");
        }

        var syncStartedAt = Stopwatch.GetTimestamp();
        var syncQuery = new SyncBootstrapQuery
        {
            RequestId = Guid.CreateVersion7().ToString("N"),
            UserId = receiverUserId,
            ListLimit = 20,
            HistoryLimitPerConversation = 10,
            MaxConversationsWithHistory = 5,
            Watermarks =
            [
                new ConversationSyncWatermark
                {
                    ConversationId = conversationId,
                    AfterReceivedAtMs = restored.ReceivedAtMs,
                    AfterMessageId = messageId
                }
            ]
        };
        var sync = await RunStageAsync(
                "sync_bootstrap",
                () => QueryWithTransientRetryAsync(
                    () => _messageBus.QuerySyncBootstrapAsync(syncQuery, ct),
                    static page => page.Succeeded,
                    static page => page.ErrorCode,
                    ct,
                    "sync_bootstrap_unavailable",
                    "sync_bootstrap_timeout"),
                outerToken)
            .ConfigureAwait(false);
        measurement?.SyncBootstrap.Record(
            Stopwatch.GetElapsedTime(syncStartedAt));
        if (!sync.Succeeded)
        {
            throw new InvalidOperationException(
                $"SyncBootstrap failed: {sync.ErrorCode ?? "unknown"}.");
        }

        if (sync.Conversations.All(item =>
                !string.Equals(item.ConversationId, conversationId, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException(
                "SyncBootstrap conversations did not include the active conversation.");
        }

        measurement?.CompletePipeline.Record(
            Stopwatch.GetElapsedTime(totalStartedAt));
    }

    private static async Task<T> QueryWithTransientRetryAsync<T>(
        Func<Task<T>> query,
        Func<T, bool> succeeded,
        Func<T, string?> errorCode,
        CancellationToken ct,
        params string[] transientCodes)
    {
        var attempt = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var page = await query().ConfigureAwait(false);
            if (succeeded(page))
                return page;

            var code = errorCode(page);
            if (code is null ||
                !transientCodes.Contains(code, StringComparer.Ordinal))
            {
                return page;
            }

            attempt++;
            var delay = TimeSpan.FromMilliseconds(
                Math.Min(2_000, 250 * Math.Pow(2, Math.Min(attempt - 1, 3))) +
                Random.Shared.Next(0, 250));
            await Task.Delay(delay, ct).ConfigureAwait(false);
        }
    }
    private static async Task RunStageAsync(
        string stage,
        Func<Task> action,
        CancellationToken outerToken)
    {
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (OperationCanceledException ex) when (!outerToken.IsCancellationRequested)
        {
            throw new TimeoutException($"Pipeline stage timed out: {stage}.", ex);
        }
    }

    private static async Task<T> RunStageAsync<T>(
        string stage,
        Func<Task<T>> action,
        CancellationToken outerToken)
    {
        try
        {
            return await action().ConfigureAwait(false);
        }
        catch (OperationCanceledException ex) when (!outerToken.IsCancellationRequested)
        {
            throw new TimeoutException($"Pipeline stage timed out: {stage}.", ex);
        }
    }
    private static string CreateMessageCommandId(
        long senderUserId,
        string clientMessageId)
    {
        var source = Encoding.UTF8.GetBytes(
            $"{senderUserId}:{clientMessageId}");
        return Convert.ToHexStringLower(SHA256.HashData(source));
    }

    private static string CreateReceiptCommandId(
        long receiverUserId,
        string messageId,
        MessageReceiptType receiptType)
    {
        var source = Encoding.UTF8.GetBytes(
            $"{receiverUserId}:{messageId}:{(byte)receiptType}");
        return Convert.ToHexStringLower(SHA256.HashData(source));
    }
}
