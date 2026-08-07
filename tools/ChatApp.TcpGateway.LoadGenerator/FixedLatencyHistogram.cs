using System.Numerics;

namespace ChatApp.TcpGateway.LoadGenerator;

/// <summary>
/// Fixed-memory, thread-safe latency histogram. Each power-of-two microsecond
/// range is split into 16 buckets, which gives bounded relative error without
/// retaining individual observations during long soak runs.
/// </summary>
internal sealed class FixedLatencyHistogram
{
    private const int BucketsPerPowerOfTwo = 16;
    private const int PowerOfTwoRanges = 64;
    private readonly long[] _buckets =
        new long[BucketsPerPowerOfTwo * PowerOfTwoRanges];
    private long _count;
    private long _sumMicroseconds;
    private long _maximumMicroseconds;

    public long Count => Volatile.Read(ref _count);

    public void Record(double milliseconds)
    {
        if (!double.IsFinite(milliseconds) || milliseconds < 0d)
            return;

        var microsecondsDouble = Math.Ceiling(milliseconds * 1_000d);
        var microseconds = microsecondsDouble >= long.MaxValue
            ? long.MaxValue
            : Math.Max(1L, (long)microsecondsDouble);
        var bucketIndex = GetBucketIndex((ulong)microseconds);

        Interlocked.Increment(ref _buckets[bucketIndex]);
        Interlocked.Increment(ref _count);
        AddSaturating(ref _sumMicroseconds, microseconds);
        UpdateMaximum(microseconds);
    }

    public LatencyHistogramSnapshot Snapshot()
    {
        var count = Volatile.Read(ref _count);
        if (count == 0)
            return LatencyHistogramSnapshot.Empty;

        var buckets = new long[_buckets.Length];
        for (var index = 0; index < buckets.Length; index++)
            buckets[index] = Volatile.Read(ref _buckets[index]);

        return new LatencyHistogramSnapshot(
            count,
            Volatile.Read(ref _sumMicroseconds) / (double)count / 1_000d,
            FindPercentileMilliseconds(buckets, count, 0.50d),
            FindPercentileMilliseconds(buckets, count, 0.95d),
            FindPercentileMilliseconds(buckets, count, 0.99d),
            Volatile.Read(ref _maximumMicroseconds) / 1_000d);
    }

    private static int GetBucketIndex(ulong microseconds)
    {
        var exponent = BitOperations.Log2(microseconds);
        var rangeStart = 1UL << exponent;
        var offset = microseconds - rangeStart;
        var subBucket = (int)Math.Min(
            BucketsPerPowerOfTwo - 1d,
            offset / (double)rangeStart * BucketsPerPowerOfTwo);
        return exponent * BucketsPerPowerOfTwo + subBucket;
    }

    private static double FindPercentileMilliseconds(
        long[] buckets,
        long count,
        double percentile)
    {
        var target = Math.Max(1L, (long)Math.Ceiling(count * percentile));
        long cumulative = 0;
        for (var index = 0; index < buckets.Length; index++)
        {
            cumulative += buckets[index];
            if (cumulative < target)
                continue;

            var exponent = index / BucketsPerPowerOfTwo;
            var subBucket = index % BucketsPerPowerOfTwo;
            var rangeStart = Math.Pow(2d, exponent);
            var upperMicroseconds = rangeStart *
                (1d + ((subBucket + 1d) / BucketsPerPowerOfTwo));
            return upperMicroseconds / 1_000d;
        }

        return 0d;
    }

    private static void AddSaturating(ref long location, long value)
    {
        while (true)
        {
            var current = Volatile.Read(ref location);
            var updated = current > long.MaxValue - value
                ? long.MaxValue
                : current + value;
            if (Interlocked.CompareExchange(
                    ref location,
                    updated,
                    current) == current)
            {
                return;
            }
        }
    }

    private void UpdateMaximum(long value)
    {
        while (true)
        {
            var current = Volatile.Read(ref _maximumMicroseconds);
            if (value <= current)
                return;
            if (Interlocked.CompareExchange(
                    ref _maximumMicroseconds,
                    value,
                    current) == current)
            {
                return;
            }
        }
    }
}

internal sealed record LatencyHistogramSnapshot(
    long Count,
    double AverageMs,
    double P50Ms,
    double P95Ms,
    double P99Ms,
    double MaximumMs)
{
    public static LatencyHistogramSnapshot Empty { get; } =
        new(0, 0d, 0d, 0d, 0d, 0d);
}
