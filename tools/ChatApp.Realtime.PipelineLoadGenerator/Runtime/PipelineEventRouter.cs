using System.Collections.Concurrent;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Integration;

namespace ChatApp.Realtime.PipelineLoadGenerator.Runtime;

internal sealed class PipelineEventRouter : IAsyncDisposable
{
    private readonly IRealtimeMessageBus _messageBus;
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentDictionary<EventKey, TaskCompletionSource<RealtimeEvent>>
        _pending = new();
    private Task? _consumerTask;

    public PipelineEventRouter(IRealtimeMessageBus messageBus)
    {
        _messageBus = messageBus;
    }

    public async Task StartAsync(CancellationToken ct)
    {
        if (_consumerTask is not null)
            throw new InvalidOperationException("Event router has already started.");

        _consumerTask = ConsumeAsync(_stop.Token);
        var startupDelay = Task.Delay(TimeSpan.FromMilliseconds(500), ct);
        var completed = await Task.WhenAny(_consumerTask, startupDelay)
            .ConfigureAwait(false);
        if (completed == _consumerTask)
            await _consumerTask.ConfigureAwait(false);
    }

    public PendingRealtimeEvent Register(
        string messageId,
        RealtimeEventType eventType)
    {
        EnsureHealthy();
        var key = new EventKey(messageId, eventType);
        var completion = new TaskCompletionSource<RealtimeEvent>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_pending.TryAdd(key, completion))
        {
            throw new InvalidOperationException(
                $"Duplicate event registration: {eventType}/{messageId}");
        }

        return new PendingRealtimeEvent(this, key, completion);
    }

    public void EnsureHealthy()
    {
        if (_consumerTask is { IsFaulted: true })
        {
            throw new InvalidOperationException(
                "Realtime event consumer stopped unexpectedly.",
                _consumerTask.Exception?.GetBaseException());
        }
        if (_consumerTask is { IsCompletedSuccessfully: true })
            throw new InvalidOperationException(
                "Realtime event consumer completed unexpectedly.");
    }

    private async Task ConsumeAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var delivery in _messageBus
                               .ConsumeEventsAsync(ct)
                               .ConfigureAwait(false))
            {
                var evt = delivery.Event;
                if (!string.IsNullOrWhiteSpace(evt.MessageId))
                {
                    var key = new EventKey(evt.MessageId, evt.Type);
                    if (_pending.TryRemove(key, out var completion))
                        completion.TrySetResult(evt);
                }

                await delivery.AckAsync(ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
    }

    private void Remove(
        EventKey key,
        TaskCompletionSource<RealtimeEvent> completion)
    {
        _pending.TryRemove(
            new KeyValuePair<EventKey, TaskCompletionSource<RealtimeEvent>>(
                key,
                completion));
    }

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        if (_consumerTask is not null)
        {
            try
            {
                await _consumerTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        foreach (var completion in _pending.Values)
            completion.TrySetCanceled();
        _pending.Clear();
        _stop.Dispose();
    }

    internal readonly record struct EventKey(
        string MessageId,
        RealtimeEventType EventType);

    internal sealed class PendingRealtimeEvent : IDisposable
    {
        private readonly PipelineEventRouter _owner;
        private readonly EventKey _key;
        private readonly TaskCompletionSource<RealtimeEvent> _completion;

        public PendingRealtimeEvent(
            PipelineEventRouter owner,
            EventKey key,
            TaskCompletionSource<RealtimeEvent> completion)
        {
            _owner = owner;
            _key = key;
            _completion = completion;
        }

        public Task<RealtimeEvent> WaitAsync(CancellationToken ct) =>
            _completion.Task.WaitAsync(ct);

        public void Dispose() => _owner.Remove(_key, _completion);
    }
}
