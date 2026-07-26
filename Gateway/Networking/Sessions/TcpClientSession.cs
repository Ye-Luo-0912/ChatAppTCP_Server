using System.Net.Sockets;
using System.Threading.Channels;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

internal sealed class TcpClientSession : IAsyncDisposable
{
    private readonly Socket _socket;
    private readonly Channel<OutboundWrite> _outbound;
    private readonly OutboundQueueBudget _outboundBudget;
    private readonly GlobalOutboundBudget? _globalOutboundBudget;
    private readonly CancellationTokenSource _lifetime = new();
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _sendTimeout;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger<TcpClientSession> _logger;
    private readonly Task _sendLoop;
    private readonly long _connectedTimestamp;

    // 每 Session 一个可复用发送超时 Timer，避免每次发送创建 LinkedCts + CancelAfter。
    private readonly ITimer _sendTimeoutTimer;
    private int _sendInProgress; // 0 = idle, 1 = sending
    // 当前发送的 deadline（UtcTicks）。仅在 _sendInProgress=1 时有效。
    // 跨发送代次的旧 Timer 回调通过比较此值识别自己已过期，避免误关后续发送。
    private long _sendDeadlineTicks;

    // 鉴权超时精确 Deadline，不依赖定时扫描。
    private readonly ITimer _authDeadlineTimer;

    private long _lastInboundTimestamp;
    // Token Bucket 替代固定一秒窗口。单线程读取循环访问，无需 Interlocked。
    private long _packetTokens;
    private long _byteTokens;
    private long _lastRefillTimestamp;
    private bool _bucketInitialized;
    private int _authenticated;
    private int _handshakeCompleted;
    private int _closeState;
    private int _closeReason;

    public TcpClientSession(
        Socket socket,
        uint connectionId,
        int outboundQueueCapacity,
        long maxOutboundQueuedBytes,
        TimeSpan sendTimeout,
        TimeProvider timeProvider,
        GatewayMetrics metrics,
        ILogger<TcpClientSession> logger,
        GlobalOutboundBudget? globalOutboundBudget = null,
        TimeSpan authenticationTimeout = default)
    {
        _socket = socket;
        ConnectionId = connectionId;
        _sendTimeout = sendTimeout;
        _timeProvider = timeProvider;
        _metrics = metrics;
        _logger = logger;
        _globalOutboundBudget = globalOutboundBudget;
        _outboundBudget = new OutboundQueueBudget(
            maxOutboundQueuedBytes);

        _connectedTimestamp = timeProvider.GetTimestamp();
        _lastInboundTimestamp = _connectedTimestamp;
        // Token Bucket 初始化时间戳，首次调用时补充满桶。
        _lastRefillTimestamp = _connectedTimestamp;

        _outbound = Channel.CreateBounded<OutboundWrite>(
            new BoundedChannelOptions(outboundQueueCapacity)
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
                FullMode = BoundedChannelFullMode.Wait
            });

        // 创建可复用发送超时 Timer。到期时双重校验：
        //   1) _sendInProgress 仍为 1（发送未完成）
        //   2) 当前单调时钟已过 _sendDeadlineTicks
        // deadline 校验防止跨发送代次的旧回调误判：
        // 旧回调触发时 _sendDeadlineTicks 已被后续发送覆盖为更晚的值，
        // now < deadline，不会误关连接。
        _sendTimeoutTimer = timeProvider.CreateTimer(
            static state =>
            {
                var session = (TcpClientSession)state!;
                if (Volatile.Read(ref session._sendInProgress) == 1
                    && session._timeProvider.GetUtcNow().UtcTicks
                       >= Volatile.Read(ref session._sendDeadlineTicks))
                {
                    if (Interlocked.CompareExchange(
                            ref session._sendInProgress, 0, 1) == 1)
                    {
                        session.Close(SessionCloseReason.SendTimedOut);
                    }
                }
            },
            this,
            Timeout.InfiniteTimeSpan,
            Timeout.InfiniteTimeSpan);

        // 鉴权超时精确 Deadline。连接建立时启动，认证成功后取消。
        // 到期时检查 _authenticated，若仍为 0 则 Close(AuthenticationTimedOut)。
        _authDeadlineTimer = authenticationTimeout > TimeSpan.Zero
            ? timeProvider.CreateTimer(
                static state =>
                {
                    var session = (TcpClientSession)state!;
                    if (Volatile.Read(ref session._authenticated) == 0)
                    {
                        session.Close(SessionCloseReason.AuthenticationTimedOut);
                    }
                },
                this,
                authenticationTimeout,
                Timeout.InfiniteTimeSpan)
            : timeProvider.CreateTimer(
                static _ => { },
                null,
                Timeout.InfiniteTimeSpan,
                Timeout.InfiniteTimeSpan);

        _sendLoop = SendLoopAsync();
    }

    public uint ConnectionId { get; }

    /// <summary>
    /// 每次 TCP 连接生成的唯一所有权令牌（GUID），用于设备租约的 compare-and-delete/refresh。
    /// 与 <see cref="SessionId"/> 分离：SessionId 是用户可见会话标识，ConnectionLeaseId 是内部所有权凭证。
    /// </summary>
    public string ConnectionLeaseId { get; } = Guid.NewGuid().ToString("N");

    public bool IsConnected => Volatile.Read(ref _closeState) == 0;

    public bool IsAuthenticated => Volatile.Read(ref _authenticated) != 0;

    /// <summary>
    /// 是否已完成 ClientHello 握手。RequireClientHello=true 时认证前必须为 true。
    /// </summary>
    public bool HasCompletedHandshake => Volatile.Read(ref _handshakeCompleted) != 0;

    /// <summary>标记 ClientHello 握手已完成。</summary>
    public void MarkHandshakeCompleted() =>
        Volatile.Write(ref _handshakeCompleted, 1);

    public long UserId { get; private set; }

    public string? SessionId { get; private set; }

    public ulong? DeviceIdHash { get; private set; }

    /// <summary>
    /// 来自 Token 的服务器签发设备标识（权威身份）。
    /// </summary>
    public string? DeviceId { get; private set; }

    public SessionCloseReason CloseReason =>
        (SessionCloseReason)Volatile.Read(ref _closeReason);

    public TimeSpan ConnectionAge =>
        _timeProvider.GetElapsedTime(_connectedTimestamp);

    public TimeSpan LastInboundAge =>
        _timeProvider.GetElapsedTime(
            Volatile.Read(ref _lastInboundTimestamp));

    /// <summary>
    /// 暴露 Session 生命周期 Token。连接关闭时取消。
    /// 业务调用应使用此 Token（或其与宿主 Token 的 linked CTS），避免连接关闭后仍占用后端资源。
    /// </summary>
    public CancellationToken LifetimeToken => _lifetime.Token;

    public ValueTask<int> ReceiveAsync(
        Memory<byte> destination,
        CancellationToken cancellationToken) =>
        _socket.ReceiveAsync(
            destination,
            SocketFlags.None,
            cancellationToken);

    public void Authenticate(
        long userId,
        string? sessionId,
        ulong? deviceIdHash,
        string? deviceId = null)
    {
        UserId = userId;
        SessionId = string.IsNullOrWhiteSpace(sessionId)
            ? $"tcp-{ConnectionId}"
            : sessionId;
        DeviceIdHash = deviceIdHash;
        DeviceId = deviceId;
        Volatile.Write(ref _authenticated, 1);
        // 认证成功，取消鉴权 deadline timer。
        _authDeadlineTimer.Change(
            Timeout.InfiniteTimeSpan,
            Timeout.InfiniteTimeSpan);
        MarkInboundActivity();
    }

    /// <summary>
    /// Token Bucket 限流，替代固定一秒窗口。
    /// 按时间比例补充令牌，避免边界处近两倍突发流量。
    /// 单线程读取循环调用，无需 Interlocked。
    /// </summary>
    /// <param name="maximumPacketsPerSecond">每秒包数上限（桶容量）。</param>
    /// <param name="maximumBytesPerSecond">每秒字节数上限（桶容量）。</param>
    /// <param name="frameByteCount">整帧字节数（包头 + payload）。</param>
    /// <param name="packetCost">命令级包令牌权重（默认 1）。昂贵命令消耗更多令牌。</param>
    public bool RecordInboundTraffic(
        int maximumPacketsPerSecond,
        long maximumBytesPerSecond,
        int frameByteCount,
        int packetCost = 1)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(frameByteCount);
        if (packetCost < 1)
            packetCost = 1;

        MarkInboundActivity();

        var now = _timeProvider.GetTimestamp();
        var frequency = _timeProvider.TimestampFrequency;

        if (!_bucketInitialized)
        {
            // 首次调用，初始化为满桶。
            _packetTokens = maximumPacketsPerSecond;
            _byteTokens = maximumBytesPerSecond;
            _lastRefillTimestamp = now;
            _bucketInitialized = true;
        }
        else
        {
            var elapsed = now - _lastRefillTimestamp;
            if (elapsed > 0)
            {
                // 按时间比例补充令牌，不超过桶容量。
                // 桶容量在 1 秒内即可完全补满，超过该量的 elapsed 会导致
                // 下方乘法在高分辨率计时器（如 Linux 上 1e9 ticks/s）下溢出，
                // 因此先夹紧到 1 秒等价的 tick 数。
                var clampedElapsed = Math.Min(elapsed, frequency);
                var packetRefill =
                    clampedElapsed * maximumPacketsPerSecond / frequency;
                var byteRefill =
                    clampedElapsed * maximumBytesPerSecond / frequency;

                if (packetRefill > 0)
                {
                    _packetTokens = Math.Min(
                        _packetTokens + packetRefill,
                        maximumPacketsPerSecond);
                }

                if (byteRefill > 0)
                {
                    _byteTokens = Math.Min(
                        _byteTokens + byteRefill,
                        maximumBytesPerSecond);
                }

                _lastRefillTimestamp = now;
            }
        }

        // 消费令牌：包令牌按命令权重消耗，字节令牌按实际帧大小消耗
        if (_packetTokens < packetCost || _byteTokens < frameByteCount)
        {
            return false;
        }

        _packetTokens -= packetCost;
        _byteTokens -= frameByteCount;
        return true;
    }

    public bool TryQueue(
        SharedOutboundFrame frame,
        SessionCloseReason? closeAfterSend = null)
    {
        if (!IsConnected)
        {
            return false;
        }

        var byteCount = frame.Length;
        if (!_outboundBudget.TryReserve(byteCount))
        {
            _metrics.OutboundRejected("byte-budget");
            Close(SessionCloseReason.OutboundQueueFull);
            return false;
        }

        // 全局出站字节预算检查。
        if (_globalOutboundBudget is not null &&
            !_globalOutboundBudget.TryReserve(byteCount))
        {
            _outboundBudget.Release(byteCount);
            _metrics.OutboundRejectedGlobalBudget();
            _metrics.OutboundRejected("global-byte-budget");
            Close(SessionCloseReason.OutboundQueueFull);
            return false;
        }

        _metrics.OutboundEnqueued(byteCount);

        if (!frame.TryRetain())
        {
            ReleaseQueuedWrite(byteCount);
            return false;
        }

        if (_outbound.Writer.TryWrite(
                new OutboundWrite(
                    frame,
                    byteCount,
                    closeAfterSend)))
        {
            return true;
        }

        frame.Dispose();
        ReleaseQueuedWrite(byteCount);
        _metrics.OutboundRejected("item-capacity-or-closed");
        Close(SessionCloseReason.OutboundQueueFull);
        return false;
    }

    /// <summary>
    /// Ephemeral 等级帧入队。Typing/Presence 等瞬态状态只保留最新，
    /// 队列满时直接丢弃，不关闭连接，避免慢消费者因瞬态帧被踢下线。
    /// <para>
    /// 与 <see cref="TryQueue"/> 的区别：
    /// <list type="bullet">
    /// <item>队列满（item-capacity）时仅丢弃帧，不 Close。</item>
    /// <item>字节预算超限时仅丢弃帧，不 Close。</item>
    /// </list>
    /// Critical（Auth/SessionRevoked/Error）和 Durable（Chat/Receipt/Edit）仍使用 <see cref="TryQueue"/>。
    /// </para>
    /// </summary>
    public bool TryQueueEphemeral(SharedOutboundFrame frame)
    {
        if (!IsConnected)
            return false;

        var byteCount = frame.Length;

        // 字节预算超限：丢弃帧，不断开连接。
        if (!_outboundBudget.TryReserve(byteCount))
        {
            _metrics.OutboundRejected("ephemeral-byte-budget");
            return false;
        }

        if (_globalOutboundBudget is not null &&
            !_globalOutboundBudget.TryReserve(byteCount))
        {
            _outboundBudget.Release(byteCount);
            _metrics.OutboundRejectedGlobalBudget();
            _metrics.OutboundRejected("ephemeral-global-byte-budget");
            return false;
        }

        _metrics.OutboundEnqueued(byteCount);

        if (!frame.TryRetain())
        {
            ReleaseQueuedWrite(byteCount);
            return false;
        }

        if (_outbound.Writer.TryWrite(
                new OutboundWrite(frame, byteCount, null)))
        {
            return true;
        }

        // 队列满或已关闭：丢弃帧，不断开连接（与 TryQueue 的关键差异）。
        frame.Dispose();
        ReleaseQueuedWrite(byteCount);
        _metrics.OutboundRejected("ephemeral-item-capacity");
        return false;
    }

    public void Close(SessionCloseReason reason)
    {
        if (Interlocked.CompareExchange(ref _closeState, 1, 0) != 0)
        {
            return;
        }

        Volatile.Write(ref _closeReason, (int)reason);
        _outbound.Writer.TryComplete();
        _lifetime.Cancel();

        try
        {
            _socket.Shutdown(SocketShutdown.Both);
        }
        catch (SocketException)
        {
            // The peer may already have closed the connection.
        }
        catch (ObjectDisposedException)
        {
            // Another close path already released the socket.
        }

        _socket.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        Close(SessionCloseReason.ApplicationStopping);

        try
        {
            await _sendLoop.ConfigureAwait(false);
        }
        finally
        {
            _sendTimeoutTimer.Dispose();
            _authDeadlineTimer.Dispose();
            _lifetime.Dispose();
        }
    }

    private void MarkInboundActivity() =>
        Interlocked.Exchange(
            ref _lastInboundTimestamp,
            _timeProvider.GetTimestamp());

    private async Task SendLoopAsync()
    {
        try
        {
            await foreach (var write in _outbound.Reader.ReadAllAsync(
                               _lifetime.Token).ConfigureAwait(false))
            {
                ReleaseQueuedWrite(write.ByteCount);

                try
                {
                    await SendFrameAsync(
                            write.Frame.Memory,
                            _lifetime.Token)
                        .ConfigureAwait(false);
                    _metrics.FrameSent();

                    if (write.CloseAfterSend is { } closeReason)
                    {
                        Close(closeReason);
                        return;
                    }
                }
                finally
                {
                    write.Frame.Dispose();
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal close path.
        }
        catch (SocketException)
        {
            Close(SessionCloseReason.TransportError);
        }
        catch (ObjectDisposedException)
        {
            Close(SessionCloseReason.TransportError);
        }
        catch (Exception exception)
        {
            _logger.TransportFailed(
                GatewayTransportOperation.SendLoop,
                ConnectionId,
                exception);
            Close(SessionCloseReason.TransportError);
        }
        finally
        {
            while (_outbound.Reader.TryRead(out var pending))
            {
                ReleaseQueuedWrite(pending.ByteCount);
                pending.Frame.Dispose();
            }
        }
    }

    private void ReleaseQueuedWrite(int byteCount)
    {
        _outboundBudget.Release(byteCount);
        _globalOutboundBudget?.Release(byteCount);
        _metrics.OutboundDequeued(byteCount);
    }

    private async ValueTask SendFrameAsync(
        ReadOnlyMemory<byte> frame,
        CancellationToken lifetimeToken)
    {
        // 使用可复用 Timer 替代每次创建 LinkedCts + CancelAfter。
        // 顺序：先设 deadline，再标记发送中，最后启动 Timer。
        // 先设 deadline 确保回调看到 _sendInProgress=1 时 deadline 已更新为本代次的值。
        Volatile.Write(ref _sendDeadlineTicks,
            _timeProvider.GetUtcNow().UtcTicks + _sendTimeout.Ticks);
        Interlocked.Exchange(ref _sendInProgress, 1);
        _sendTimeoutTimer.Change(_sendTimeout, Timeout.InfiniteTimeSpan);

        var sent = 0;
        try
        {
            while (sent < frame.Length)
            {
                var bytesSent = await _socket.SendAsync(
                        frame[sent..],
                        SocketFlags.None,
                        lifetimeToken)
                    .ConfigureAwait(false);

                if (bytesSent <= 0)
                {
                    throw new SocketException(
                        (int)SocketError.ConnectionReset);
                }

                sent += bytesSent;
            }
        }
        finally
        {
            // 标记发送完成，Timer 回调将忽略。
            Interlocked.Exchange(ref _sendInProgress, 0);
            _sendTimeoutTimer.Change(
                Timeout.InfiniteTimeSpan,
                Timeout.InfiniteTimeSpan);
        }
    }
}
