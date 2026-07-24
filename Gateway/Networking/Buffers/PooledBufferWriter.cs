using System.Buffers;

namespace ChatApp.TcpGateway.Gateway.Networking.Buffers;

internal sealed class PooledBufferWriter : IBufferWriter<byte>, IDisposable
{
    private readonly int _maximumCapacity;
    private byte[]? _buffer;
    private int _written;

    public PooledBufferWriter(int initialCapacity, int maximumCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(initialCapacity);
        ArgumentOutOfRangeException.ThrowIfLessThan(
            maximumCapacity,
            initialCapacity);

        _maximumCapacity = maximumCapacity;
        _buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
    }

    public int WrittenCount => _written;

    public Span<byte> WrittenSpan => GetBuffer().AsSpan(0, _written);

    public void Advance(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);

        var buffer = GetBuffer();
        if (count > buffer.Length - _written ||
            count > _maximumCapacity - _written)
        {
            throw new InvalidOperationException(
                "Cannot advance beyond the available buffer.");
        }

        _written += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return GetBuffer().AsMemory(_written);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return GetBuffer().AsSpan(_written);
    }

    public SharedOutboundFrame Detach()
    {
        var buffer = Interlocked.Exchange(ref _buffer, null)
            ?? throw new ObjectDisposedException(nameof(PooledBufferWriter));

        var frame = new SharedOutboundFrame(buffer, _written);
        _written = 0;
        return frame;
    }

    public void Dispose()
    {
        var buffer = Interlocked.Exchange(ref _buffer, null);
        if (buffer is not null)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        _written = 0;
    }

    private byte[] GetBuffer() =>
        Volatile.Read(ref _buffer)
        ?? throw new ObjectDisposedException(nameof(PooledBufferWriter));

    private void EnsureCapacity(int sizeHint)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(sizeHint);

        sizeHint = Math.Max(sizeHint, 1);
        var buffer = GetBuffer();
        var requiredCapacity = checked(_written + sizeHint);

        if (requiredCapacity > _maximumCapacity)
        {
            throw new InvalidOperationException(
                $"Payload exceeds the configured maximum of {_maximumCapacity} bytes.");
        }

        if (requiredCapacity <= buffer.Length)
        {
            return;
        }

        var newCapacity = Math.Min(
            Math.Max(requiredCapacity, buffer.Length * 2),
            _maximumCapacity);

        var replacement = ArrayPool<byte>.Shared.Rent(newCapacity);
        buffer.AsSpan(0, _written).CopyTo(replacement);
        _buffer = replacement;
        ArrayPool<byte>.Shared.Return(buffer);
    }
}

