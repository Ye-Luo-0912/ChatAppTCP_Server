using System.Collections.Concurrent;
using ChatApp.Realtime.PipelineLoadGenerator.Diagnostics;

namespace ChatApp.Realtime.PipelineLoadGenerator.Runtime;

internal sealed class PipelineLoadMeasurement
{
    private const int MaximumErrorSamples = 10;
    private readonly ConcurrentQueue<string> _errors = new();
    private long _started;
    private long _succeeded;
    private long _failed;
    private int _errorSamples;

    public LatencyHistogram MessagePublishAck { get; } = new();
    public LatencyHistogram MessagePersisted { get; } = new();
    public LatencyHistogram ReceiptPublishAck { get; } = new();
    public LatencyHistogram ReceiptPersisted { get; } = new();
    public LatencyHistogram HistoryQuery { get; } = new();
    public LatencyHistogram ConversationListQuery { get; } = new();
    public LatencyHistogram ConversationMarkRead { get; } = new();
    public LatencyHistogram SyncBootstrap { get; } = new();
    public LatencyHistogram CompletePipeline { get; } = new();

    public long Started => Interlocked.Read(ref _started);
    public long Succeeded => Interlocked.Read(ref _succeeded);
    public long Failed => Interlocked.Read(ref _failed);
    public IReadOnlyList<string> Errors => _errors.ToArray();

    public void RecordStarted() => Interlocked.Increment(ref _started);

    public void RecordSucceeded() => Interlocked.Increment(ref _succeeded);

    public void RecordFailed(Exception exception)
    {
        Interlocked.Increment(ref _failed);
        if (Interlocked.Increment(ref _errorSamples) <= MaximumErrorSamples)
        {
            _errors.Enqueue(
                $"{exception.GetType().Name}: {exception.Message}");
        }
    }
}
