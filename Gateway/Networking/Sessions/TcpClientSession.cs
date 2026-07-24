using System.Net.Sockets;
using System.Threading.Channels;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Networking.Sessions;

internal sealed partial class TcpClientSession : IAsyncDisposable
{
    private readonly Socket _socket;
    private readonly Channel<OutboundWrite> _outbound;
    private readonly OutboundQueueBudget _outboundBudget;
    private readonly CancellationTokenSource _lifetime = new();
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _sendTimeout;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger<TcpClientSession> _logger;
    private readonly Task _sendLoop;
    private readonly long _connectedTimestamp;

    private long _lastInboundTimestamp;
    private long _rateWindowSecond;
    private int _rateWindowCount;
    private long _rateWindowBytes;
    private int _authenticated;
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
        ILogger<TcpClientSession> logger)
    {
        _socket = socket;
        ConnectionId = connectionId;
        _sendTimeout = sendTimeout;
        _timeProvider = timeProvider;
        _metrics = metrics;
        _logger = logger;
        _outboundBudget = new OutboundQueueBudget(
            maxOutboundQueuedBytes);

        _connectedTimestamp = timeProvider.GetTimestamp();
        _lastInboundTimestamp = _connectedTimestamp;
        _rateWindowSecond =
            _connectedTimestamp / timeProvider.TimestampFrequency;

        _outbound = Channel.CreateBounded<OutboundWrite>(
            new BoundedChannelOptions(outboundQueueCapacity)
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
                FullMode = BoundedChannelFullMode.Wait
            });

        _sendLoop = SendLoopAsync();
    }

    public uint ConnectionId { get; }

    public bool IsConnected => Volatile.Read(ref _closeState) == 0;

    public bool IsAuthenticated => Volatile.Read(ref _authenticated) != 0;

    public long UserId { get; private set; }

    public string? SessionId { get; private set; }

    public ulong? DeviceIdHash { get; private set; }

    public SessionCloseReason CloseReason =>
        (SessionCloseReason)Volatile.Read(ref _closeReason);

    public TimeSpan ConnectionAge =>
        _timeProvider.GetElapsedTime(_connectedTimestamp);

    public TimeSpan LastInboundAge =>
        _timeProvider.GetElapsedTime(
            Volatile.Read(ref _lastInboundTimestamp));

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
        ulong? deviceIdHash)
    {
        UserId = userId;
        SessionId = string.IsNullOrWhiteSpace(sessionId)
            ? $"tcp-{ConnectionId}"
            : sessionId;
        DeviceIdHash = deviceIdHash;
        Volatile.Write(ref _authenticated, 1);
        MarkInboundActivity();
    }

    /// <summary>
    /// 记录入站帧并按 1 秒窗口同时限制包数与字节数。
    /// </summary>
    /// <param name="maximumBytesPerSecond"></param>
    /// <param name="frameByteCount">整帧字节数（包头 + payload）。</param>
    /// <param name="maximumPacketsPerSecond"></param>
    public bool RecordInboundTraffic(
        int maximumPacketsPerSecond,
        long maximumBytesPerSecond,
        int frameByteCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(frameByteCount);

        MarkInboundActivity();

        var currentSecond =
            _timeProvider.GetTimestamp() / _timeProvider.TimestampFrequency;
        var observedSecond = Volatile.Read(ref _rateWindowSecond);

        if (observedSecond != currentSecond &&
            Interlocked.CompareExchange(
                ref _rateWindowSecond,
                currentSecond,
                observedSecond) == observedSecond)
        {
            Interlocked.Exchange(ref _rateWindowCount, 0);
            Interlocked.Exchange(ref _rateWindowBytes, 0);
        }

        var packetCount = Interlocked.Increment(ref _rateWindowCount);
        var byteCount = Interlocked.Add(ref _rateWindowBytes, frameByteCount);
        return packetCount <= maximumPacketsPerSecond &&
               byteCount <= maximumBytesPerSecond;
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
            LogSendLoopError(ConnectionId, exception);
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
        _metrics.OutboundDequeued(byteCount);
    }

    [LoggerMessage(
        EventId = 20,
        Level = LogLevel.Error,
        Message = "Unhandled send-loop error for connection {ConnectionId}.")]
    private partial void LogSendLoopError(
        uint connectionId,
        Exception exception);

    private async ValueTask SendFrameAsync(
        ReadOnlyMemory<byte> frame,
        CancellationToken lifetimeToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(
            lifetimeToken);
        timeout.CancelAfter(_sendTimeout);

        var sent = 0;
        try
        {
            while (sent < frame.Length)
            {
                var bytesSent = await _socket.SendAsync(
                        frame[sent..],
                        SocketFlags.None,
                        timeout.Token)
                    .ConfigureAwait(false);

                if (bytesSent <= 0)
                {
                    throw new SocketException(
                        (int)SocketError.ConnectionReset);
                }

                sent += bytesSent;
            }
        }
        catch (OperationCanceledException)
            when (!lifetimeToken.IsCancellationRequested)
        {
            Close(SessionCloseReason.SendTimedOut);
            throw;
        }
    }
}
