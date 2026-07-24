namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

internal sealed class OutboundQueueBudget(long maximumBytes)
{
    private long _currentBytes;

    public long CurrentBytes => Volatile.Read(ref _currentBytes);

    public bool TryReserve(int byteCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(byteCount);

        while (true)
        {
            var current = Volatile.Read(ref _currentBytes);
            if (byteCount > maximumBytes ||
                current > maximumBytes - byteCount)
            {
                return false;
            }

            if (Interlocked.CompareExchange(
                    ref _currentBytes,
                    current + byteCount,
                    current) == current)
            {
                return true;
            }
        }
    }

    public void Release(int byteCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(byteCount);

        var remaining = Interlocked.Add(ref _currentBytes, -byteCount);
        if (remaining >= 0)
        {
            return;
        }

        Interlocked.Add(ref _currentBytes, byteCount);
        throw new InvalidOperationException(
            "Outbound queue byte budget was released too many times.");
    }
}
