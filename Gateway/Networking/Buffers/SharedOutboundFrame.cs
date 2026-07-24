using System.Buffers;

namespace ChatApp.TcpGateway.Gateway.Networking.Buffers;

internal sealed class SharedOutboundFrame : IDisposable
{
    private byte[]? _buffer;
    private int _referenceCount = 1;

    public SharedOutboundFrame(byte[] buffer, int length)
    {
        _buffer = buffer;
        Length = length;
    }

    public int Length { get; }

    public ReadOnlyMemory<byte> Memory
    {
        get
        {
            var buffer = Volatile.Read(ref _buffer)
                ?? throw new ObjectDisposedException(nameof(SharedOutboundFrame));
            return buffer.AsMemory(0, Length);
        }
    }

    public bool TryRetain()
    {
        while (true)
        {
            var current = Volatile.Read(ref _referenceCount);
            if (current <= 0)
            {
                return false;
            }

            if (Interlocked.CompareExchange(
                    ref _referenceCount,
                    current + 1,
                    current) == current)
            {
                return true;
            }
        }
    }

    public void Dispose()
    {
        var remaining = Interlocked.Decrement(ref _referenceCount);
        
        switch (remaining)
        {
            case > 0:
                return;
            case < 0:
                throw new InvalidOperationException("Outbound frame released too many times.");
        }

        var buffer = Interlocked.Exchange(ref _buffer, null);
        if (buffer is not null)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
