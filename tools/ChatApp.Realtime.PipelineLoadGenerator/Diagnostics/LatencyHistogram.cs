namespace ChatApp.Realtime.PipelineLoadGenerator.Diagnostics;

internal sealed class LatencyHistogram
{
    private const double BucketWidthMs = 0.5;
    private const int MaximumTrackedMilliseconds = 60_000;
    private static readonly int BucketCount =
        (int)(MaximumTrackedMilliseconds / BucketWidthMs) + 1;

    private readonly long[] _buckets = new long[BucketCount];
    private long _count;
    private long _totalMicroseconds;
    private long _maximumMicroseconds;

    public void Record(TimeSpan elapsed)
    {
        var microseconds = Math.Max(0, (long)elapsed.TotalMicroseconds);
        var bucket = Math.Clamp(
            (int)Math.Ceiling(elapsed.TotalMilliseconds / BucketWidthMs),
            0,
            _buckets.Length - 1);

        Interlocked.Increment(ref _buckets[bucket]);
        Interlocked.Increment(ref _count);
        Interlocked.Add(ref _totalMicroseconds, microseconds);

        var current = Volatile.Read(ref _maximumMicroseconds);
        while (microseconds > current)
        {
            var observed = Interlocked.CompareExchange(
                ref _maximumMicroseconds,
                microseconds,
                current);
            if (observed == current)
                break;
            current = observed;
        }
    }

    public LatencySnapshot Snapshot()
    {
        var count = Interlocked.Read(ref _count);
        return new LatencySnapshot
        {
            Count = count,
            AverageMs = count == 0
                ? 0
                : Interlocked.Read(ref _totalMicroseconds) / 1000d / count,
            P50Ms = Percentile(count, 0.50),
            P95Ms = Percentile(count, 0.95),
            P99Ms = Percentile(count, 0.99),
            MaximumMs = Interlocked.Read(ref _maximumMicroseconds) / 1000d
        };
    }

    private double Percentile(long count, double percentile)
    {
        if (count == 0)
            return 0;

        var target = (long)Math.Ceiling(count * percentile);
        long accumulated = 0;
        for (var index = 0; index < _buckets.Length; index++)
        {
            accumulated += Volatile.Read(ref _buckets[index]);
            if (accumulated >= target)
                return index * BucketWidthMs;
        }

        return MaximumTrackedMilliseconds;
    }
}
