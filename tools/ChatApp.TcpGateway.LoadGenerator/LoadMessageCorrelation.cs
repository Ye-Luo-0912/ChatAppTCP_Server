using System.Globalization;
using System.Diagnostics;

namespace ChatApp.TcpGateway.LoadGenerator;

/// <summary>
/// Creates load-only client message ids that carry the process-shared monotonic
/// send timestamp. The orchestrator starts every load child on the same host, so
/// a receiving child can measure delivery latency from the exact wire id without
/// IPC or an unbounded cross-process dictionary.
/// </summary>
internal static class LoadMessageCorrelation
{
    private const string Prefix = "lg1-";
    private const int TimestampOffset = 4;
    private const int TimestampLength = 16;
    private const int SeparatorOffset = TimestampOffset + TimestampLength;
    private const int RandomOffset = SeparatorOffset + 1;
    private const int RandomLength = 16;
    private const int MessageIdLength = RandomOffset + RandomLength;

    public static string Create(long startedAt)
    {
        var random = Guid.NewGuid().ToString("N")[..RandomLength];
        return string.Create(
            CultureInfo.InvariantCulture,
            $"{Prefix}{unchecked((ulong)startedAt):x16}-{random}");
    }

    public static bool TryMeasureElapsedMilliseconds(
        string? clientMessageId,
        long signalAt,
        out double elapsedMilliseconds)
    {
        elapsedMilliseconds = 0;
        if (clientMessageId is null ||
            clientMessageId.Length != MessageIdLength ||
            !clientMessageId.StartsWith(Prefix, StringComparison.Ordinal) ||
            clientMessageId[SeparatorOffset] != '-' ||
            !ulong.TryParse(
                clientMessageId.AsSpan(TimestampOffset, TimestampLength),
                NumberStyles.AllowHexSpecifier,
                CultureInfo.InvariantCulture,
                out var encodedStartedAt) ||
            encodedStartedAt > long.MaxValue)
        {
            return false;
        }

        var startedAt = (long)encodedStartedAt;
        if (startedAt <= 0 || signalAt < startedAt)
            return false;

        elapsedMilliseconds = Stopwatch
            .GetElapsedTime(startedAt, signalAt)
            .TotalMilliseconds;
        return double.IsFinite(elapsedMilliseconds) && elapsedMilliseconds >= 0;
    }
}

internal sealed class MessageIdFingerprintAccumulator
{
    private long _count;
    private long _sum;
    private long _xor;

    public void Add(string messageId)
    {
        var hash = unchecked((long)StableHash(messageId));
        Interlocked.Increment(ref _count);
        Interlocked.Add(ref _sum, hash);

        while (true)
        {
            var current = Volatile.Read(ref _xor);
            if (Interlocked.CompareExchange(ref _xor, current ^ hash, current) == current)
                break;
        }
    }

    public MessageIdFingerprintSnapshot Snapshot() => new(
        Volatile.Read(ref _count),
        unchecked((ulong)Volatile.Read(ref _sum)).ToString("x16", CultureInfo.InvariantCulture),
        unchecked((ulong)Volatile.Read(ref _xor)).ToString("x16", CultureInfo.InvariantCulture));

    private static ulong StableHash(string value)
    {
        const ulong offset = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;
        var hash = offset;
        foreach (var character in value)
        {
            hash ^= (byte)character;
            hash *= prime;
            hash ^= (byte)(character >> 8);
            hash *= prime;
        }

        // Final avalanche keeps the commutative sum/xor fingerprint useful even
        // for ids sharing the same timestamp prefix.
        hash ^= hash >> 33;
        hash *= 0xff51afd7ed558ccdUL;
        hash ^= hash >> 33;
        hash *= 0xc4ceb9fe1a85ec53UL;
        return hash ^ (hash >> 33);
    }
}

internal sealed record MessageIdFingerprintSnapshot(
    long Count,
    string SumHex,
    string XorHex);
