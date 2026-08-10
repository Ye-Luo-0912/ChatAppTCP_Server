using System.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;

namespace ChatApp.TcpGateway.Tests.Networking;

public sealed class ReceiveBufferDowngradePolicyTests
{
    private static readonly TimeSpan LargeFrameRetention = TimeSpan.FromSeconds(60);

    [Fact]
    public void LargeBuffer_BeforeLargeFrameRetentionExpires_RemainsUpgraded()
    {
        var time = new ManualTimestampProvider();
        var lastLargeFrame = time.GetTimestamp();
        time.Advance(LargeFrameRetention - TimeSpan.FromMilliseconds(1));

        Assert.False(ShouldDowngrade(
            time,
            lastLargeFrame,
            bufferedByteCount: 0,
            partialFrameStartTimestamp: 0));
    }

    [Fact]
    public void LargeBuffer_AfterLargeFrameRetentionAndCompletedSmallFrame_Downgrades()
    {
        var time = new ManualTimestampProvider();
        var lastLargeFrame = time.GetTimestamp();
        time.Advance(LargeFrameRetention);

        // 小帧已完整解析，因此 buffer 为空且不存在 partial frame。
        Assert.True(ShouldDowngrade(
            time,
            lastLargeFrame,
            bufferedByteCount: 0,
            partialFrameStartTimestamp: 0));
    }

    [Fact]
    public void LargeBuffer_AfterLargeFrameRetentionButPartialFrame_NeverDowngrades()
    {
        var time = new ManualTimestampProvider();
        var lastLargeFrame = time.GetTimestamp();
        time.Advance(LargeFrameRetention + TimeSpan.FromSeconds(1));

        Assert.False(ShouldDowngrade(
            time,
            lastLargeFrame,
            bufferedByteCount: 7,
            partialFrameStartTimestamp: time.GetTimestamp()));
    }

    [Fact]
    public void ZeroLargeFrameRetention_DisablesDowngrade()
    {
        var time = new ManualTimestampProvider();
        var lastLargeFrame = time.GetTimestamp();
        time.Advance(TimeSpan.FromHours(1));

        Assert.False(SessionRuntime.ShouldDowngradeReceiveBuffer(
            currentBufferSize: 4096,
            baseBufferSize: 1024,
            bufferedByteCount: 0,
            partialFrameStartTimestamp: 0,
            lastLargeFrameTimestamp: lastLargeFrame,
            largeFrameRetention: TimeSpan.Zero,
            timeProvider: time));
    }

    [Fact]
    public void ReplaceEmptyReceiveBuffer_RentFailure_PreservesOldOwner()
    {
        var oldBuffer = new byte[4096];
        var buffer = oldBuffer;
        var pool = new RecordingArrayPool(throwOnRent: true);

        Assert.Throws<InvalidOperationException>(() =>
            SessionRuntime.ReplaceEmptyReceiveBuffer(
                ref buffer,
                newSize: 1024,
                pool));

        Assert.Same(oldBuffer, buffer);
        Assert.Equal(["Rent"], pool.Operations);
        Assert.Empty(pool.Returned);
    }

    [Fact]
    public void ReplaceEmptyReceiveBuffer_RentsThenSwapsAndReturnsOldOwner()
    {
        var oldBuffer = new byte[4096];
        var replacement = new byte[1024];
        var buffer = oldBuffer;
        var pool = new RecordingArrayPool(replacement: replacement);

        SessionRuntime.ReplaceEmptyReceiveBuffer(
            ref buffer,
            newSize: 1024,
            pool);

        Assert.Same(replacement, buffer);
        Assert.Equal(["Rent", "Return"], pool.Operations);
        Assert.Equal([oldBuffer], pool.Returned);
    }

    private static bool ShouldDowngrade(
        TimeProvider timeProvider,
        long lastLargeFrameTimestamp,
        int bufferedByteCount,
        long partialFrameStartTimestamp) =>
        SessionRuntime.ShouldDowngradeReceiveBuffer(
            currentBufferSize: 4096,
            baseBufferSize: 1024,
            bufferedByteCount,
            partialFrameStartTimestamp,
            lastLargeFrameTimestamp,
            LargeFrameRetention,
            timeProvider);

    private sealed class ManualTimestampProvider : TimeProvider
    {
        private long _ticks = 1;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp() => Volatile.Read(ref _ticks);

        public void Advance(TimeSpan duration) =>
            Interlocked.Add(ref _ticks, duration.Ticks);
    }

    private sealed class RecordingArrayPool : ArrayPool<byte>
    {
        private readonly bool _throwOnRent;
        private readonly byte[] _replacement;

        public RecordingArrayPool(
            bool throwOnRent = false,
            byte[]? replacement = null)
        {
            _throwOnRent = throwOnRent;
            _replacement = replacement ?? new byte[1024];
        }

        public List<string> Operations { get; } = [];
        public List<byte[]> Returned { get; } = [];

        public override byte[] Rent(int minimumLength)
        {
            Operations.Add("Rent");
            if (_throwOnRent)
                throw new InvalidOperationException("Injected rent failure.");
            Assert.True(_replacement.Length >= minimumLength);
            return _replacement;
        }

        public override void Return(byte[] array, bool clearArray = false)
        {
            Operations.Add("Return");
            Returned.Add(array);
        }
    }
}
