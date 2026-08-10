using System.Buffers;
using System.Net.Sockets;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Executor;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.TcpGateway.Tests.Networking;

public sealed class TcpClientSessionOnDemandDisposeRaceTests
{
    [Fact(Timeout = 5_000)]
    public async Task DisposeAsync_WaitsForRunningOnDemandPump_BeforeUniqueCloseDrain()
    {
        using var metrics = new GatewayMetrics();
        using var coordinator = new OutboundPumpCoordinator(
            burstLimit: 4,
            readyQueueCapacity: 8,
            NullLogger.Instance);
        using var queue = new PumpReadBarrierOutboundQueue();
        var globalBudget = new GlobalOutboundBudget(4096);
        var session = CreateSession(metrics, globalBudget, coordinator, queue);
        var frame = new SharedOutboundFrame(
            ArrayPool<byte>.Shared.Rent(128),
            length: 128);

        try
        {
            Assert.True(session.TryQueueEphemeral(
                frame,
                EphemeralKey.Presence(userId: 42)));

            // Coordinator 不启动 worker；手动执行 pump，使状态确定性进入 Running，
            // 并在其唯一消费者 TryRead 内暂停。
            var pumpTask = Task.Run(
                async () => await session.PumpOutboundAsync(
                    maxBurst: 4,
                    TestContext.Current.CancellationToken),
                TestContext.Current.CancellationToken);

            Assert.True(queue.FirstReadEntered.Wait(
                TimeSpan.FromSeconds(2),
                TestContext.Current.CancellationToken));

            var disposeTask = session.DisposeAsync().AsTask();
            Assert.False(session.IsConnected);

            // Dispose 必须等待 Running pump 释放消费者所有权，不能并发 TryRead/Drain。
            await Task.Yield();
            Assert.False(disposeTask.IsCompleted);
            Assert.False(queue.ConcurrentConsumerObserved);

            queue.ReleaseFirstRead();
            await pumpTask;
            await disposeTask;

            Assert.False(queue.ConcurrentConsumerObserved);
            Assert.False(session.HasEphemeralEntries);
            Assert.Equal(0, session.OutboundQueuedBytes);
            Assert.Equal(0, globalBudget.CurrentBytes);

            // Pump 的 mailbox 所有权恰好释放 retained ref；原始 owner 仍有效。
            Assert.Equal(128, frame.Memory.Length);
            frame.Dispose();
            Assert.Throws<ObjectDisposedException>(() => _ = frame.Memory);

            await coordinator.StopAsync(TestContext.Current.CancellationToken);
        }
        finally
        {
            queue.ReleaseFirstRead();
            if (session.IsConnected)
                await session.DisposeAsync();

            try
            {
                _ = frame.Memory;
                frame.Dispose();
            }
            catch (ObjectDisposedException)
            {
                // Expected after the exact-release assertion path.
            }
        }
    }

    [Fact(Timeout = 5_000)]
    public async Task DisposeAsync_WaitsForRunningPerSessionDrain_BeforeUniqueCloseDrain()
    {
        using var metrics = new GatewayMetrics();
        using var queue = new PumpReadBarrierOutboundQueue();
        var globalBudget = new GlobalOutboundBudget(4096);
        var session = CreatePerSessionDrainSession(metrics, globalBudget, queue);
        var frame = new SharedOutboundFrame(
            ArrayPool<byte>.Shared.Rent(128),
            length: 128);
        Task<bool>? enqueueTask = null;

        try
        {
            // PerSessionDrain starts inline until its first incomplete await. Run the enqueue
            // on another thread so the barrier can hold the active drain inside TryRead.
            enqueueTask = Task.Run(
                () => session.TryQueueEphemeral(
                    frame,
                    EphemeralKey.Presence(userId: 43)),
                TestContext.Current.CancellationToken);

            Assert.True(queue.FirstReadEntered.Wait(
                TimeSpan.FromSeconds(2),
                TestContext.Current.CancellationToken));

            var disposeTask = session.DisposeAsync().AsTask();
            Assert.False(session.IsConnected);

            // Dispose must await this drain generation before becoming the only FIFO/mailbox
            // close-cleanup owner. A second reader here exposes concurrent drain immediately.
            await Task.Yield();
            Assert.False(disposeTask.IsCompleted);
            Assert.False(queue.ConcurrentConsumerObserved);

            queue.ReleaseFirstRead();
            Assert.True(await enqueueTask);
            await disposeTask;

            Assert.False(queue.ConcurrentConsumerObserved);
            Assert.False(session.HasEphemeralEntries);
            Assert.Equal(0, session.OutboundQueuedBytes);
            Assert.Equal(0, globalBudget.CurrentBytes);

            // Exactly one retained mailbox reference was released; the caller's owner remains.
            Assert.Equal(128, frame.Memory.Length);
            frame.Dispose();
            Assert.Throws<ObjectDisposedException>(() => _ = frame.Memory);
        }
        finally
        {
            queue.ReleaseFirstRead();
            if (enqueueTask is not null)
            {
                try
                {
                    await enqueueTask;
                }
                catch
                {
                    // The primary assertion path reports failures; cleanup only unblocks work.
                }
            }

            if (session.IsConnected)
                await session.DisposeAsync();

            try
            {
                _ = frame.Memory;
                frame.Dispose();
            }
            catch (ObjectDisposedException)
            {
                // Expected after the exact-release assertion path.
            }
        }
    }

    [Fact(Timeout = 5_000)]
    public async Task DisposeAsync_WaitsForPerSessionPublishing_BeforeReadingCompletionCore()
    {
        using var metrics = new GatewayMetrics();
        using var operation = new BarrierDrainOperation(
            blockFirstReset: true,
            blockFirstComplete: false,
            TestContext.Current.CancellationToken);
        var queue = new BoundedChannelOutboundQueue(capacity: 4);
        var globalBudget = new GlobalOutboundBudget(4096);
        var session = CreatePerSessionDrainSession(
            metrics,
            globalBudget,
            queue,
            operation);
        var frame = new SharedOutboundFrame(
            ArrayPool<byte>.Shared.Rent(128),
            length: 128);
        Task<bool>? enqueueTask = null;

        try
        {
            enqueueTask = Task.Run(
                () => session.TryQueueEphemeral(
                    frame,
                    EphemeralKey.Presence(userId: 44)),
                TestContext.Current.CancellationToken);

            // Reset core/ActiveGeneration 已完成，但 publisher 尚未把 phase 发布为 Running。
            Assert.True(operation.FirstResetPublished.Wait(
                TimeSpan.FromSeconds(2),
                TestContext.Current.CancellationToken));

            var disposeTask = session.DisposeAsync().AsTask();
            await Task.Yield();

            // Dispose 必须把 Publishing 当作活跃 owner，不能读取 core 后提前 Drain。
            Assert.False(disposeTask.IsCompleted);
            Assert.True(session.HasEphemeralEntries);
            Assert.Equal(128, session.OutboundQueuedBytes);
            Assert.Equal(128, globalBudget.CurrentBytes);

            operation.ReleaseReset();
            Assert.True(await enqueueTask);
            await disposeTask;

            Assert.False(session.HasEphemeralEntries);
            Assert.Equal(0, session.OutboundQueuedBytes);
            Assert.Equal(0, globalBudget.CurrentBytes);

            Assert.Equal(128, frame.Memory.Length);
            frame.Dispose();
            Assert.Throws<ObjectDisposedException>(() => _ = frame.Memory);
        }
        finally
        {
            operation.ReleaseReset();
            if (enqueueTask is not null)
            {
                try
                {
                    await enqueueTask;
                }
                catch
                {
                    // Primary assertion path reports the failure.
                }
            }

            if (session.IsConnected)
                await session.DisposeAsync();
            DisposeOwnerIfAlive(frame);
        }
    }

    [Fact(Timeout = 5_000)]
    public async Task PerSessionCloseAfterReset_ClearsPendingFromStaleProducer()
    {
        using var metrics = new GatewayMetrics();
        using var operation = new BarrierDrainOperation(
            blockFirstReset: true,
            blockFirstComplete: true,
            TestContext.Current.CancellationToken);
        var queue = new BoundedChannelOutboundQueue(capacity: 4);
        var globalBudget = new GlobalOutboundBudget(4096);
        var session = CreatePerSessionDrainSession(
            metrics,
            globalBudget,
            queue,
            operation);
        var frame = new SharedOutboundFrame(
            ArrayPool<byte>.Shared.Rent(128),
            length: 128);
        Task<bool>? enqueueTask = null;

        try
        {
            enqueueTask = Task.Run(
                () => session.TryQueueEphemeral(
                    frame,
                    EphemeralKey.Presence(userId: 48)),
                TestContext.Current.CancellationToken);

            // Publisher 已完成 Reset，但 phase 仍为 Publishing。
            Assert.True(operation.FirstResetPublished.Wait(
                TimeSpan.FromSeconds(2),
                TestContext.Current.CancellationToken));

            var disposeTask = session.DisposeAsync().AsTask();
            operation.ReleaseReset();

            // Close-after-Reset 分支已发布 Finalizing，并停在 Complete 内。
            Assert.True(operation.FirstCompleteEntered.Wait(
                TimeSpan.FromSeconds(2),
                TestContext.Current.CancellationToken));

            // 模拟 Close 前已通过入口检查的 producer 延迟到达：它能合法地把
            // Finalizing 提升为 Pending，但关闭 finalizer 仍必须归位 Idle。
            Assert.True(session.TryPromotePerSessionFinalizingToPendingForTest());
            operation.ReleaseComplete();

            Assert.True(await enqueueTask);
            await disposeTask.WaitAsync(TestContext.Current.CancellationToken);

            Assert.False(session.HasEphemeralEntries);
            Assert.Equal(0, session.OutboundQueuedBytes);
            Assert.Equal(0, globalBudget.CurrentBytes);

            Assert.Equal(128, frame.Memory.Length);
            frame.Dispose();
            Assert.Throws<ObjectDisposedException>(() => _ = frame.Memory);
        }
        finally
        {
            operation.ReleaseReset();
            operation.ReleaseComplete();
            if (enqueueTask is not null)
            {
                try
                {
                    await enqueueTask;
                }
                catch
                {
                    // Primary assertion path reports the failure.
                }
            }

            if (session.IsConnected)
                await session.DisposeAsync();
            DisposeOwnerIfAlive(frame);
        }
    }

    [Fact(Timeout = 5_000)]
    public async Task PerSessionFinalizing_CompletesOldGeneration_BeforeNextReset()
    {
        using var metrics = new GatewayMetrics();
        using var operation = new BarrierDrainOperation(
            blockFirstReset: false,
            blockFirstComplete: true,
            TestContext.Current.CancellationToken);
        var queue = new FirstReadEmptyOutboundQueue();
        var globalBudget = new GlobalOutboundBudget(4096);
        var session = CreatePerSessionDrainSession(
            metrics,
            globalBudget,
            queue,
            operation);
        var first = new SharedOutboundFrame(
            ArrayPool<byte>.Shared.Rent(128),
            length: 128);
        var second = new SharedOutboundFrame(
            ArrayPool<byte>.Shared.Rent(128),
            length: 128);
        Task<bool>? firstEnqueueTask = null;

        try
        {
            firstEnqueueTask = Task.Run(
                () => session.TryQueue(first),
                TestContext.Current.CancellationToken);

            Assert.True(operation.FirstCompleteEntered.Wait(
                TimeSpan.FromSeconds(2),
                TestContext.Current.CancellationToken));

            // 首次 TryRead 故意报告 FIFO 空，使第一代在仍保留 durable write 的情况下
            // 进入 Finalizing/Complete 屏障。新 producer 只能标 Pending，不能把同一
            // MRVTSC Reset 到第二代。
            Assert.True(session.TryQueueEphemeral(
                second,
                EphemeralKey.Presence(userId: 46)));
            Assert.Equal(1, operation.ResetCallCount);
            Assert.Equal(256, session.OutboundQueuedBytes);
            Assert.Equal(256, globalBudget.CurrentBytes);

            operation.ReleaseComplete();
            Assert.True(operation.SecondResetPublished.Wait(
                TimeSpan.FromSeconds(2),
                TestContext.Current.CancellationToken));
            Assert.Equal(2, operation.ResetCallCount);

            Assert.True(await firstEnqueueTask);
            await session.DisposeAsync();

            Assert.False(queue.ConcurrentConsumerObserved);
            Assert.False(session.HasEphemeralEntries);
            Assert.Equal(0, session.OutboundQueuedBytes);
            Assert.Equal(0, globalBudget.CurrentBytes);

            Assert.Equal(128, first.Memory.Length);
            Assert.Equal(128, second.Memory.Length);
            first.Dispose();
            second.Dispose();
            Assert.Throws<ObjectDisposedException>(() => _ = first.Memory);
            Assert.Throws<ObjectDisposedException>(() => _ = second.Memory);
        }
        finally
        {
            operation.ReleaseComplete();
            if (firstEnqueueTask is not null)
            {
                try
                {
                    await firstEnqueueTask;
                }
                catch
                {
                    // Primary assertion path reports the failure.
                }
            }

            if (session.IsConnected)
                await session.DisposeAsync();
            DisposeOwnerIfAlive(first);
            DisposeOwnerIfAlive(second);
        }
    }

    [Fact(Timeout = 5_000)]
    public async Task DisposeAsync_WaitsForOnDemandFinalizingPeek_BeforeCloseDrain()
    {
        using var metrics = new GatewayMetrics();
        using var coordinator = new OutboundPumpCoordinator(
            burstLimit: 4,
            readyQueueCapacity: 8,
            NullLogger.Instance);
        using var queue = new FinalizingPeekBarrierOutboundQueue();
        var globalBudget = new GlobalOutboundBudget(4096);
        var session = CreateSession(metrics, globalBudget, coordinator, queue);
        var frame = new SharedOutboundFrame(
            ArrayPool<byte>.Shared.Rent(128),
            length: 128);

        try
        {
            Assert.True(session.TryQueueEphemeral(
                frame,
                EphemeralKey.Presence(userId: 47)));

            var pumpTask = Task.Run(
                async () => await session.PumpOutboundAsync(
                    maxBurst: 4,
                    TestContext.Current.CancellationToken),
                TestContext.Current.CancellationToken);

            // Pump 的首个 TryRead 故意返回空，随后在 Finalizing 独占期的 TryPeek 暂停。
            Assert.True(queue.FinalizingPeekEntered.Wait(
                TimeSpan.FromSeconds(2),
                TestContext.Current.CancellationToken));

            var disposeTask = session.DisposeAsync().AsTask();
            await Task.Yield();

            Assert.False(disposeTask.IsCompleted);
            Assert.False(queue.ConcurrentConsumerObserved);
            Assert.Equal(128, session.OutboundQueuedBytes);
            Assert.Equal(128, globalBudget.CurrentBytes);

            queue.ReleaseFinalizingPeek();
            await pumpTask;
            await disposeTask;

            Assert.False(queue.ConcurrentConsumerObserved);
            Assert.False(session.HasEphemeralEntries);
            Assert.Equal(0, session.OutboundQueuedBytes);
            Assert.Equal(0, globalBudget.CurrentBytes);

            Assert.Equal(128, frame.Memory.Length);
            frame.Dispose();
            Assert.Throws<ObjectDisposedException>(() => _ = frame.Memory);

            await coordinator.StopAsync(TestContext.Current.CancellationToken);
        }
        finally
        {
            queue.ReleaseFinalizingPeek();
            if (session.IsConnected)
                await session.DisposeAsync();
            DisposeOwnerIfAlive(frame);
        }
    }

    private static void DisposeOwnerIfAlive(SharedOutboundFrame frame)
    {
        try
        {
            _ = frame.Memory;
            frame.Dispose();
        }
        catch (ObjectDisposedException)
        {
            // Exact-release assertion path already disposed the caller's owner.
        }
    }

    private static TcpClientSession CreateSession(
        GatewayMetrics metrics,
        GlobalOutboundBudget globalBudget,
        OutboundPumpCoordinator coordinator,
        IOutboundQueue outboundQueue)
    {
        var socket = new Socket(
            AddressFamily.InterNetwork,
            SocketType.Stream,
            ProtocolType.Tcp);

        return new TcpClientSession(
            socket: socket,
            connectionId: 1,
            outboundQueueCapacity: 16,
            maxOutboundQueuedBytes: 4096,
            sendTimeout: TimeSpan.FromSeconds(5),
            timeProvider: TimeProvider.System,
            metrics: metrics,
            logger: NullLogger<TcpClientSession>.Instance,
            globalOutboundBudget: globalBudget,
            outboundPump: coordinator,
            outboundQueue: outboundQueue);
    }

    private static TcpClientSession CreatePerSessionDrainSession(
        GatewayMetrics metrics,
        GlobalOutboundBudget globalBudget,
        IOutboundQueue outboundQueue,
        TcpClientSession.DrainOperation? drainOperation = null)
    {
        var socket = new Socket(
            AddressFamily.InterNetwork,
            SocketType.Stream,
            ProtocolType.Tcp);

        return new TcpClientSession(
            socket: socket,
            connectionId: 2,
            outboundQueueCapacity: 16,
            maxOutboundQueuedBytes: 4096,
            sendTimeout: TimeSpan.FromSeconds(5),
            timeProvider: TimeProvider.System,
            metrics: metrics,
            logger: NullLogger<TcpClientSession>.Instance,
            globalOutboundBudget: globalBudget,
            usePerSessionDrain: true,
            outboundQueue: outboundQueue,
            drainOperation: drainOperation);
    }

    private sealed class PumpReadBarrierOutboundQueue : IOutboundQueue, IDisposable
    {
        private readonly object _lock = new();
        private readonly ManualResetEventSlim _releaseFirstRead = new(false);
        private OutboundWrite _item;
        private bool _hasItem;
        private int _firstReaderClaimed;
        private int _activeConsumers;
        private int _completed;
        private int _concurrentConsumerObserved;

        public ManualResetEventSlim FirstReadEntered { get; } = new(false);

        public bool ConcurrentConsumerObserved =>
            Volatile.Read(ref _concurrentConsumerObserved) != 0;

        public bool TryWrite(OutboundWrite item)
        {
            if (Volatile.Read(ref _completed) != 0)
                return false;

            lock (_lock)
            {
                if (_hasItem)
                    return false;
                _item = item;
                _hasItem = true;
                return true;
            }
        }

        public bool TryRead(out OutboundWrite item)
        {
            EnterConsumerOperation();

            try
            {
                if (Interlocked.CompareExchange(
                        ref _firstReaderClaimed,
                        1,
                        0) == 0)
                {
                    FirstReadEntered.Set();
                    _releaseFirstRead.Wait(
                        TimeSpan.FromSeconds(2),
                        TestContext.Current.CancellationToken);
                }

                lock (_lock)
                {
                    if (!_hasItem)
                    {
                        item = default;
                        return false;
                    }

                    item = _item;
                    _item = default;
                    _hasItem = false;
                    return true;
                }
            }
            finally
            {
                ExitConsumerOperation();
            }
        }

        public bool TryPeek(out OutboundWrite item)
        {
            EnterConsumerOperation();
            try
            {
                lock (_lock)
                {
                    item = _item;
                    return _hasItem;
                }
            }
            finally
            {
                ExitConsumerOperation();
            }
        }

        public ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken) =>
            new(_hasItem);

        public void TryComplete() => Volatile.Write(ref _completed, 1);

        public void ReleaseFirstRead() => _releaseFirstRead.Set();

        private void EnterConsumerOperation()
        {
            if (Interlocked.Increment(ref _activeConsumers) != 1)
                Volatile.Write(ref _concurrentConsumerObserved, 1);
        }

        private void ExitConsumerOperation() =>
            Interlocked.Decrement(ref _activeConsumers);

        public void Dispose()
        {
            _releaseFirstRead.Dispose();
            FirstReadEntered.Dispose();
        }
    }

    private sealed class BarrierDrainOperation :
        TcpClientSession.DrainOperation,
        IDisposable
    {
        private readonly bool _blockFirstReset;
        private readonly bool _blockFirstComplete;
        private readonly CancellationToken _cancellationToken;
        private readonly ManualResetEventSlim _releaseReset = new(false);
        private readonly ManualResetEventSlim _releaseComplete = new(false);
        private int _resetCallCount;
        private int _completeCallCount;

        public BarrierDrainOperation(
            bool blockFirstReset,
            bool blockFirstComplete,
            CancellationToken cancellationToken)
        {
            _blockFirstReset = blockFirstReset;
            _blockFirstComplete = blockFirstComplete;
            _cancellationToken = cancellationToken;
        }

        public ManualResetEventSlim FirstResetPublished { get; } = new(false);
        public ManualResetEventSlim FirstCompleteEntered { get; } = new(false);
        public ManualResetEventSlim SecondResetPublished { get; } = new(false);

        public int ResetCallCount => Volatile.Read(ref _resetCallCount);

        public override void Reset(int generation)
        {
            base.Reset(generation);
            var call = Interlocked.Increment(ref _resetCallCount);
            if (call == 1)
            {
                FirstResetPublished.Set();
                if (_blockFirstReset &&
                    !_releaseReset.Wait(
                        TimeSpan.FromSeconds(4),
                        _cancellationToken))
                {
                    throw new TimeoutException("Reset publication barrier timed out.");
                }
            }
            else if (call == 2)
            {
                SecondResetPublished.Set();
            }
        }

        public override void Complete(int generation)
        {
            var call = Interlocked.Increment(ref _completeCallCount);
            if (call == 1)
            {
                FirstCompleteEntered.Set();
                if (_blockFirstComplete &&
                    !_releaseComplete.Wait(
                        TimeSpan.FromSeconds(4),
                        _cancellationToken))
                {
                    throw new TimeoutException("Complete finalizing barrier timed out.");
                }
            }

            base.Complete(generation);
        }

        public void ReleaseReset() => _releaseReset.Set();
        public void ReleaseComplete() => _releaseComplete.Set();

        public void Dispose()
        {
            _releaseReset.Set();
            _releaseComplete.Set();
            _releaseReset.Dispose();
            _releaseComplete.Dispose();
            FirstResetPublished.Dispose();
            FirstCompleteEntered.Dispose();
            SecondResetPublished.Dispose();
        }
    }

    private sealed class FirstReadEmptyOutboundQueue : IOutboundQueue
    {
        private readonly object _lock = new();
        private OutboundWrite _item;
        private bool _hasItem;
        private int _firstReadClaimed;
        private int _activeConsumers;
        private int _completed;
        private int _concurrentConsumerObserved;

        public bool ConcurrentConsumerObserved =>
            Volatile.Read(ref _concurrentConsumerObserved) != 0;

        public bool TryWrite(OutboundWrite item)
        {
            if (Volatile.Read(ref _completed) != 0)
                return false;

            lock (_lock)
            {
                if (_hasItem)
                    return false;
                _item = item;
                _hasItem = true;
                return true;
            }
        }

        public bool TryRead(out OutboundWrite item)
        {
            EnterConsumerOperation();
            try
            {
                if (Interlocked.CompareExchange(
                        ref _firstReadClaimed,
                        1,
                        0) == 0)
                {
                    item = default;
                    return false;
                }

                lock (_lock)
                {
                    if (!_hasItem)
                    {
                        item = default;
                        return false;
                    }

                    item = _item;
                    _item = default;
                    _hasItem = false;
                    return true;
                }
            }
            finally
            {
                ExitConsumerOperation();
            }
        }

        public bool TryPeek(out OutboundWrite item)
        {
            EnterConsumerOperation();
            try
            {
                lock (_lock)
                {
                    item = _item;
                    return _hasItem;
                }
            }
            finally
            {
                ExitConsumerOperation();
            }
        }

        public ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken)
        {
            lock (_lock)
                return new ValueTask<bool>(_hasItem);
        }

        public void TryComplete() => Volatile.Write(ref _completed, 1);

        private void EnterConsumerOperation()
        {
            if (Interlocked.Increment(ref _activeConsumers) != 1)
                Volatile.Write(ref _concurrentConsumerObserved, 1);
        }

        private void ExitConsumerOperation() =>
            Interlocked.Decrement(ref _activeConsumers);
    }

    private sealed class FinalizingPeekBarrierOutboundQueue :
        IOutboundQueue,
        IDisposable
    {
        private readonly object _lock = new();
        private readonly ManualResetEventSlim _releaseFinalizingPeek = new(false);
        private OutboundWrite _item;
        private bool _hasItem;
        private int _firstReadClaimed;
        private int _firstPeekClaimed;
        private int _activeConsumers;
        private int _completed;
        private int _concurrentConsumerObserved;

        public ManualResetEventSlim FinalizingPeekEntered { get; } = new(false);

        public bool ConcurrentConsumerObserved =>
            Volatile.Read(ref _concurrentConsumerObserved) != 0;

        public bool TryWrite(OutboundWrite item)
        {
            if (Volatile.Read(ref _completed) != 0)
                return false;

            lock (_lock)
            {
                if (_hasItem)
                    return false;
                _item = item;
                _hasItem = true;
                return true;
            }
        }

        public bool TryRead(out OutboundWrite item)
        {
            EnterConsumerOperation();
            try
            {
                if (Interlocked.CompareExchange(
                        ref _firstReadClaimed,
                        1,
                        0) == 0)
                {
                    item = default;
                    return false;
                }

                lock (_lock)
                {
                    if (!_hasItem)
                    {
                        item = default;
                        return false;
                    }

                    item = _item;
                    _item = default;
                    _hasItem = false;
                    return true;
                }
            }
            finally
            {
                ExitConsumerOperation();
            }
        }

        public bool TryPeek(out OutboundWrite item)
        {
            EnterConsumerOperation();
            try
            {
                if (Interlocked.CompareExchange(
                        ref _firstPeekClaimed,
                        1,
                        0) == 0)
                {
                    FinalizingPeekEntered.Set();
                    _releaseFinalizingPeek.Wait(
                        TimeSpan.FromSeconds(4),
                        TestContext.Current.CancellationToken);
                }

                lock (_lock)
                {
                    item = _item;
                    return _hasItem;
                }
            }
            finally
            {
                ExitConsumerOperation();
            }
        }

        public ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken)
        {
            lock (_lock)
                return new ValueTask<bool>(_hasItem);
        }

        public void TryComplete() => Volatile.Write(ref _completed, 1);

        public void ReleaseFinalizingPeek() => _releaseFinalizingPeek.Set();

        private void EnterConsumerOperation()
        {
            if (Interlocked.Increment(ref _activeConsumers) != 1)
                Volatile.Write(ref _concurrentConsumerObserved, 1);
        }

        private void ExitConsumerOperation() =>
            Interlocked.Decrement(ref _activeConsumers);

        public void Dispose()
        {
            _releaseFinalizingPeek.Set();
            _releaseFinalizingPeek.Dispose();
            FinalizingPeekEntered.Dispose();
        }
    }
}
