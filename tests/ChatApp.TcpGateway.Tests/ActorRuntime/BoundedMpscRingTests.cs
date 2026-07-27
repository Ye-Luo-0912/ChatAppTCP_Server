using ChatApp.ActorRuntime.Primitives;

namespace ChatApp.TcpGateway.Tests.ActorRuntime;

/// <summary>
/// <see cref="BoundedMpscRing{T}"/> 单元测试。
/// 验证基本入队/出队、容量上限、多生产者并发安全。
/// </summary>
public sealed class BoundedMpscRingTests
{
    [Fact]
    public void ConstructorRejectsNonPowerOfTwo()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new BoundedMpscRing<int>(3));
        Assert.Throws<ArgumentOutOfRangeException>(() => new BoundedMpscRing<int>(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new BoundedMpscRing<int>(-1));
    }

    [Fact]
    public void TryEnqueueAndTryDequeueSingleItem()
    {
        var ring = new BoundedMpscRing<int>(4);
        Assert.True(ring.TryEnqueue(42));
        Assert.Equal(1, ring.Count);

        Assert.True(ring.TryDequeue(out var item));
        Assert.Equal(42, item);
        Assert.Equal(0, ring.Count);
    }

    [Fact]
    public void TryEnqueueReturnsFalseWhenFull()
    {
        var ring = new BoundedMpscRing<int>(2);
        Assert.True(ring.TryEnqueue(1));
        Assert.True(ring.TryEnqueue(2));
        Assert.False(ring.TryEnqueue(3));
        Assert.Equal(2, ring.Count);
    }

    [Fact]
    public void TryDequeueReturnsFalseWhenEmpty()
    {
        var ring = new BoundedMpscRing<int>(4);
        Assert.False(ring.TryDequeue(out _));
        Assert.Equal(0, ring.Count);
    }

    [Fact]
    public void FifoOrderingPreserved()
    {
        var ring = new BoundedMpscRing<int>(8);
        for (var i = 0; i < 8; i++)
            Assert.True(ring.TryEnqueue(i));

        for (var i = 0; i < 8; i++)
        {
            Assert.True(ring.TryDequeue(out var item));
            Assert.Equal(i, item);
        }
    }

    [Fact]
    public async Task ConcurrentProducersDoNotLoseItems()
    {
        const int capacity = 1024;
        const int producerCount = 4;
        const int itemsPerProducer = 200;
        var ring = new BoundedMpscRing<int>(capacity);
        var ct = TestContext.Current.CancellationToken;
        var producers = new Task[producerCount];

        for (var p = 0; p < producerCount; p++)
        {
            var pid = p;
            producers[p] = Task.Run(() =>
            {
                for (var i = 0; i < itemsPerProducer; i++)
                {
                    var value = pid * itemsPerProducer + i;
                    while (!ring.TryEnqueue(value))
                    {
                        // 队列满：短暂让出再重试
                        Thread.Yield();
                    }
                }
            }, ct);
        }

        // 单消费者并行消费所有项
        var consumed = new List<int>(producerCount * itemsPerProducer);
        var consumeTask = Task.Run(() =>
        {
            while (consumed.Count < producerCount * itemsPerProducer)
            {
                if (ring.TryDequeue(out var item))
                    consumed.Add(item);
                else
                    Thread.Yield();
            }
        }, ct);

        await Task.WhenAll(producers);
        await consumeTask;

        Assert.Equal(producerCount * itemsPerProducer, consumed.Count);
        // 无重复：去重后数量应一致
        var unique = new HashSet<int>(consumed);
        Assert.Equal(consumed.Count, unique.Count);
    }
}
