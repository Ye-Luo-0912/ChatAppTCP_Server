using System.Buffers;
using System.Net.Sockets;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.TcpGateway.Tests.Networking;

public sealed class TcpClientSessionEphemeralCloseRaceTests
{
    [Fact(Timeout = 5_000)]
    public async Task TryQueueEphemeral_CloseDuringSentinelWrite_ReleasesExactEntryImmediately()
    {
        using var metrics = new GatewayMetrics();
        var globalBudget = new GlobalOutboundBudget(4096);
        using var queue = new ClosingBarrierOutboundQueue();
        var session = CreateSession(metrics, globalBudget, queue);
        var buffer = ArrayPool<byte>.Shared.Rent(128);
        var frame = new SharedOutboundFrame(buffer, 128);

        try
        {
            var enqueueTask = Task.Run(() =>
                session.TryQueueEphemeral(
                    frame,
                    EphemeralKey.Presence(userId: 42)),
                TestContext.Current.CancellationToken);

            Assert.True(
                queue.SentinelWriteEntered.Wait(
                    TimeSpan.FromSeconds(2),
                    TestContext.Current.CancellationToken),
                "TryQueueEphemeral did not reach the sentinel write barrier.");

            // Close 在线性化点完成队列关闭，但 Dispose/Drain 尚未运行。
            // 旧实现会把刚存入的 mailbox entry 一直滞留到未来某次 Dispose。
            session.Close(SessionCloseReason.ApplicationStopping);
            queue.ReleaseSentinelWrite();

            Assert.False(await enqueueTask);
            Assert.False(session.HasEphemeralEntries);
            Assert.Equal(0, session.OutboundQueuedBytes);
            Assert.Equal(0, globalBudget.CurrentBytes);

            // mailbox 持有的 retained reference 已恰好释放一次；原始 owner 仍可访问。
            Assert.Equal(128, frame.Memory.Length);
            frame.Dispose();
            Assert.Throws<ObjectDisposedException>(() => _ = frame.Memory);

            await session.DisposeAsync();
        }
        finally
        {
            queue.ReleaseSentinelWrite();
            if (session.IsConnected)
                await session.DisposeAsync();

            // 仅在断言提前失败、原始 owner 尚未释放时兜底；已释放时 Memory 会抛出。
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

    private static TcpClientSession CreateSession(
        GatewayMetrics metrics,
        GlobalOutboundBudget globalBudget,
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
            usePerSessionDrain: true,
            outboundQueue: outboundQueue);
    }

    /// <summary>
    /// 在 sentinel TryWrite 内建立确定性 barrier：测试线程可先完成 Close/TryComplete，
    /// 再让生产者观察写失败。无消费者，确保断言针对 TryQueueEphemeral 的回滚所有权。
    /// </summary>
    private sealed class ClosingBarrierOutboundQueue : IOutboundQueue, IDisposable
    {
        private readonly ManualResetEventSlim _release = new(false);
        private int _completed;

        public ManualResetEventSlim SentinelWriteEntered { get; } = new(false);

        public bool TryWrite(OutboundWrite item)
        {
            SentinelWriteEntered.Set();
            Assert.Null(item.Frame);
            _release.Wait(TimeSpan.FromSeconds(2));
            return Volatile.Read(ref _completed) == 0;
        }

        public bool TryRead(out OutboundWrite item)
        {
            item = default;
            return false;
        }

        public bool TryPeek(out OutboundWrite item)
        {
            item = default;
            return false;
        }

        public ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken) =>
            new(false);

        public void TryComplete() => Volatile.Write(ref _completed, 1);

        public void ReleaseSentinelWrite() => _release.Set();

        public void Dispose()
        {
            _release.Dispose();
            SentinelWriteEntered.Dispose();
        }
    }
}
