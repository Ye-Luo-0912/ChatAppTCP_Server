using ChatApp.ActorRuntime.Primitives;

namespace ChatApp.TcpGateway.Tests.ActorRuntime;

/// <summary>
/// <see cref="SpscRing{T}"/> 单元测试。
/// </summary>
public sealed class SpscRingTests
{
    [Fact]
    public void ConstructorRejectsNonPowerOfTwo()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SpscRing<int>(3));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SpscRing<int>(0));
    }

    [Fact]
    public void TryEnqueueAndDequeue()
    {
        var ring = new SpscRing<string>(4);
        Assert.True(ring.TryEnqueue("a"));
        Assert.True(ring.TryEnqueue("b"));
        Assert.Equal(2, ring.Count);

        Assert.True(ring.TryDequeue(out var a));
        Assert.Equal("a", a);
        Assert.True(ring.TryDequeue(out var b));
        Assert.Equal("b", b);
        Assert.False(ring.TryDequeue(out _));
        Assert.Equal(0, ring.Count);
    }

    [Fact]
    public void TryEnqueueReturnsFalseWhenFull()
    {
        var ring = new SpscRing<int>(2);
        Assert.True(ring.TryEnqueue(1));
        Assert.True(ring.TryEnqueue(2));
        Assert.False(ring.TryEnqueue(3));
    }

    [Fact]
    public void ClearRemovesAllItems()
    {
        var ring = new SpscRing<int>(4);
        ring.TryEnqueue(1);
        ring.TryEnqueue(2);
        ring.Clear();
        Assert.Equal(0, ring.Count);
        Assert.False(ring.TryDequeue(out _));
    }
}
