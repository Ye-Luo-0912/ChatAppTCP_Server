using System.Buffers;

namespace ChatApp.TcpGateway.Gateway.Networking.Buffers;

internal sealed class SharedOutboundFrame : IDisposable
{
    private byte[]? _buffer;
    private int _referenceCount = 1;
    private readonly bool _pinned;

    public SharedOutboundFrame(byte[] buffer, int length)
    {
        _buffer = buffer;
        Length = length;
    }

    private SharedOutboundFrame(byte[] buffer, int length, bool pinned)
    {
        _buffer = buffer;
        Length = length;
        _pinned = pinned;
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

    /// <summary>
    /// 创建一个静态（pinned）帧：buffer 不从 ArrayPool 租用，Dispose 永不归还。
    /// 用于 Heartbeat ACK 等固定不变的小帧，避免每次发送重复分配。
    /// </summary>
    public static SharedOutboundFrame CreatePinned(byte[] buffer, int length)
    {
        var frame = new SharedOutboundFrame(buffer, length, pinned: true);
        // pinned 帧引用计数设为高位，确保 Dispose 永不归零。
        Interlocked.Exchange(ref frame._referenceCount, int.MaxValue / 2);
        return frame;
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
        // pinned 帧永不释放 buffer，仅减少引用计数。
        if (_pinned)
        {
            Interlocked.Decrement(ref _referenceCount);
            return;
        }

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
