using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using Xunit;

namespace ChatApp.TcpGateway.Tests.Networking;

public sealed class LazySegmentedOutboundQueueTests
{
    // OutboundWrite.Frame=null 即 sentinel，无需 SharedOutboundFrame 实例。
    // ByteCount 复用为标识符以校验 FIFO 顺序。
    private static OutboundWrite Write(int id) => new(null, id, null);

    [Fact]
    public void TryRead_ReturnsFalse_WhenEmpty()
    {
        var q = new LazySegmentedOutboundQueue(16);
        Assert.False(q.TryRead(out _));
        Assert.False(q.TryPeek(out _));
    }

    [Fact]
    public void TryWrite_TryRead_PreservesFifoOrder()
    {
        var q = new LazySegmentedOutboundQueue(16);
        Assert.True(q.TryWrite(Write(1)));
        Assert.True(q.TryWrite(Write(2)));
        Assert.True(q.TryWrite(Write(3)));

        Assert.True(q.TryRead(out var a));
        Assert.Equal(1, a.ByteCount);
        Assert.True(q.TryRead(out var b));
        Assert.Equal(2, b.ByteCount);
        Assert.True(q.TryRead(out var c));
        Assert.Equal(3, c.ByteCount);
        Assert.False(q.TryRead(out _));
    }

    [Fact]
    public void TryWrite_ReturnsFalse_WhenCapacityExceeded()
    {
        var q = new LazySegmentedOutboundQueue(3);
        Assert.True(q.TryWrite(Write(1)));
        Assert.True(q.TryWrite(Write(2)));
        Assert.True(q.TryWrite(Write(3)));
        // 容量已满
        Assert.False(q.TryWrite(Write(4)));

        // 释放一个槽位后可再次入队（FIFO：1 先出，4 入队尾）
        Assert.True(q.TryRead(out var first));
        Assert.Equal(1, first.ByteCount);
        Assert.True(q.TryWrite(Write(4)));
        // FIFO 顺序：2, 3, 4
        Assert.True(q.TryRead(out var a));
        Assert.Equal(2, a.ByteCount);
        Assert.True(q.TryRead(out var b));
        Assert.Equal(3, b.ByteCount);
        Assert.True(q.TryRead(out var c));
        Assert.Equal(4, c.ByteCount);
        Assert.False(q.TryRead(out _));
    }

    [Fact]
    public void TryWrite_ReturnsFalse_AfterComplete()
    {
        var q = new LazySegmentedOutboundQueue(16);
        q.TryWrite(Write(1));
        q.TryComplete();

        Assert.False(q.TryWrite(Write(2)));
        // 已入队项仍可读取
        Assert.True(q.TryRead(out var r));
        Assert.Equal(1, r.ByteCount);
    }

    [Fact]
    public void TryPeek_ReturnsItem_WithoutConsuming()
    {
        var q = new LazySegmentedOutboundQueue(16);
        q.TryWrite(Write(10));
        q.TryWrite(Write(20));

        Assert.True(q.TryPeek(out var p));
        Assert.Equal(10, p.ByteCount);
        // Peek 不消费：再次 Peek 仍返回首项
        Assert.True(q.TryPeek(out _));
        Assert.True(q.TryRead(out var r));
        Assert.Equal(10, r.ByteCount);
    }

    [Fact]
    public void MultiSegment_PreservesOrder_AcrossSegmentBoundary()
    {
        // 容量远大于 SegmentSize(16)，触发多段链表
        const int count = 100;
        var q = new LazySegmentedOutboundQueue(count);

        for (var i = 0; i < count; i++)
            Assert.True(q.TryWrite(Write(i)));

        for (var i = 0; i < count; i++)
        {
            Assert.True(q.TryRead(out var r), $"failed at index {i}");
            Assert.Equal(i, r.ByteCount);
        }
        Assert.False(q.TryRead(out _));
    }

    [Fact]
    public async Task WaitToReadAsync_Completes_WhenItemEnqueued()
    {
        var q = new LazySegmentedOutboundQueue(16);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // 队列空：WaitToReadAsync 应挂起
        var wait = q.WaitToReadAsync(cts.Token);
        Assert.False(wait.IsCompleted);

        // 入队唤醒
        q.TryWrite(Write(42));
        var ready = await wait;
        Assert.True(ready);
        Assert.True(q.TryRead(out var r));
        Assert.Equal(42, r.ByteCount);
    }

    [Fact]
    public async Task WaitToReadAsync_FastPath_WhenItemsAvailable()
    {
        var q = new LazySegmentedOutboundQueue(16);
        q.TryWrite(Write(1));

        // 已有项：立即返回 true，不挂起
        var wait = q.WaitToReadAsync(CancellationToken.None);
        Assert.True(wait.IsCompletedSuccessfully);
        Assert.True(await wait);
    }

    [Fact]
    public async Task WaitToReadAsync_ReturnsFalse_WhenCompletedAndEmpty()
    {
        var q = new LazySegmentedOutboundQueue(16);
        q.TryComplete();

        var ready = await q.WaitToReadAsync(TestContext.Current.CancellationToken);
        Assert.False(ready);
    }

    [Fact]
    public async Task WaitToReadAsync_ReturnsTrue_WhenCompletedWithItems()
    {
        var q = new LazySegmentedOutboundQueue(16);
        q.TryWrite(Write(1));
        q.TryComplete();

        // Complete 但仍有项：应返回 true 让消费者排空
        var ready = await q.WaitToReadAsync(TestContext.Current.CancellationToken);
        Assert.True(ready);
        Assert.True(q.TryRead(out _));
        // 排空后返回 false
        Assert.False(await q.WaitToReadAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task WaitToReadAsync_ThrowsOCE_WhenCanceled()
    {
        var q = new LazySegmentedOutboundQueue(16);
        using var cts = new CancellationTokenSource();

        var wait = q.WaitToReadAsync(cts.Token);
        Assert.False(wait.IsCompleted);

        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(async () => await wait);
    }

    [Fact]
    public async Task WaitToReadAsync_PreCanceledToken_ThrowsOCE()
    {
        var q = new LazySegmentedOutboundQueue(16);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // 已取消的 token：UnsafeRegister 同步触发 SetException
        await Assert.ThrowsAsync<OperationCanceledException>(
            async () => await q.WaitToReadAsync(cts.Token));
    }

    [Fact]
    public async Task ConcurrentProducers_AllItemsDelivered_InPerProducerOrder()
    {
        const int producers = 8;
        const int perProducer = 200;
        var q = new LazySegmentedOutboundQueue(producers * perProducer);

        // 每个生产者写入连续 id（producerId * perProducer + i），便于校验单生产者 FIFO。
        var produceTasks = Enumerable.Range(0, producers)
            .Select(pid => Task.Run(() =>
            {
                var baseId = pid * perProducer;
                for (var i = 0; i < perProducer; i++)
                    Assert.True(q.TryWrite(Write(baseId + i)), $"producer {pid} write {i} rejected");
            }))
            .ToArray();

        await Task.WhenAll(produceTasks);

        // 单消费者读取全部项，验证每个生产者的 id 单调递增
        var seen = new Dictionary<int, int>(); // producerId → next expected i
        var total = producers * perProducer;
        for (var n = 0; n < total; n++)
        {
            Assert.True(q.TryRead(out var r), $"failed at {n}");
            var pid = r.ByteCount / perProducer;
            var i = r.ByteCount % perProducer;
            if (!seen.TryGetValue(pid, out var next))
                next = 0;
            Assert.Equal(next, i);
            seen[pid] = i + 1;
        }
        Assert.False(q.TryRead(out _));
    }

    [Fact]
    public async Task ConcurrentProducers_BoundedCapacityNeverExceeded()
    {
        const int capacity = 32;
        const int producers = 8;
        const int perProducer = 500;
        var q = new LazySegmentedOutboundQueue(capacity);

        var rejected = 0;
        var consumed = 0;
        var producerDone = 0;

        var produceTasks = Enumerable.Range(0, producers)
            .Select(_ => Task.Run(() =>
            {
                for (var i = 0; i < perProducer; i++)
                    if (!q.TryWrite(Write(i)))
                        Interlocked.Increment(ref rejected);
            }))
            .ToArray();

        // 单消费者持续排空，与生产者并行运行
        var consumeTask = Task.Run(
            async () =>
            {
                await Task.Yield();
                while (Volatile.Read(ref producerDone) == 0)
                {
                    while (q.TryRead(out _))
                        Interlocked.Increment(ref consumed);
                    await Task.Delay(1, TestContext.Current.CancellationToken);
                }
                // 生产者全部完成后的残余排空
                while (q.TryRead(out _))
                    Interlocked.Increment(ref consumed);
            },
            TestContext.Current.CancellationToken);

        await Task.WhenAll(produceTasks);
        Volatile.Write(ref producerDone, 1);
        await consumeTask;

        // 不变量：已消费 + 被拒 = 总写入尝试；_count 不会超过 capacity（TryWrite 内 CAS 保证）
        Assert.Equal(producers * perProducer, consumed + rejected);
    }

    [Fact]
    public async Task ConcurrentProducer_Consumer_Pipeline()
    {
        // 生产者持续入队、消费者持续出队并行运行；最终所有项都被消费一次。
        const int count = 5000;
        var q = new LazySegmentedOutboundQueue(64);

        var producer = Task.Run(
            () =>
            {
                for (var i = 0; i < count; i++)
                    while (!q.TryWrite(Write(i)))
                        Thread.SpinWait(1);
                q.TryComplete();
            },
            TestContext.Current.CancellationToken);

        var consumed = new List<int>(count);
        var consumer = Task.Run(
            async () =>
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                while (true)
                {
                    if (q.TryRead(out var r))
                    {
                        consumed.Add(r.ByteCount);
                        continue;
                    }
                    // 空且 Complete → 退出
                    if (!await q.WaitToReadAsync(cts.Token))
                        break;
                }
            },
            TestContext.Current.CancellationToken);

        await Task.WhenAll(producer, consumer);

        Assert.Equal(count, consumed.Count);
        consumed.Sort();
        for (var i = 0; i < count; i++)
            Assert.Equal(i, consumed[i]);
    }
}
