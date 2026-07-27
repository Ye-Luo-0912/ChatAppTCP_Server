using System.Net.Sockets;
using System.Threading.Channels;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Executor;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// TCP 客户端会话：封装单连接的状态机、限流、出站队列与生命周期。
/// <para>
/// 出站驱动、传输超时与关闭逻辑拆分至 partial 文件：
/// <list type="bullet">
/// <item><see cref="TcpClientSession.Outbound"/> — Durable FIFO + Ephemeral mailbox + SendLoop/Pump 驱动</item>
/// <item><see cref="TcpClientSession.Transport"/> — Close/Dispose、空闲/发送 deadline、SendFrameAsync</item>
/// </list>
/// </para>
/// </summary>
internal sealed partial class TcpClientSession : IAsyncDisposable
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
    // PersistentSendLoop 模式下的永久发送 Task；OnDemandSendPump 模式下为 null。
    private readonly Task? _sendLoop;
    private readonly long _connectedTimestamp;

    // OnDemandSendPump 模式专用：共享出站 pump 协调器。
    // PersistentSendLoop 模式下为 null，TryQueue/TryQueueEphemeral 不调用 TryScheduleSend。
    private readonly OutboundPumpCoordinator? _outboundPump;
    // OnDemandSendPump 调度标志：0 = 未调度，1 = 已入 ready queue 待 pump。
    // 入队时 CAS 0→1 成功才 TrySchedule；pump 结束时 CAS 1→0 + re-check 防止丢失唤醒。
    private int _sendScheduled;

    // Ephemeral latest-state mailbox：按 EphemeralKey 分槽，同 key 覆盖旧帧保留最新状态。
    // Typing key = (KindTyping, hash(conversationId))，Presence key = (KindPresence, userId)。
    // 与 _outbound FIFO 独立：flush sentinel 写入 FIFO 唤醒发送循环排空 mailbox。
    private readonly EphemeralMailbox _ephemeralMailbox = new();
    // 标记是否已有 flush sentinel 在 _outbound 队列中，避免重复入队。
    private volatile bool _ephemeralFlushPending;

    // 发送超时：通过全局 DeadlineWheel 注册 deadline，到期检查 _sendInProgress 后关闭会话。
    // 单调时钟 + generation（_sendInProgress CompareExchange）防止墙钟回拨与跨发送代次误关。
    private readonly DeadlineWheel? _deadlineWheel;
    private int _sendInProgress; // 0 = idle, 1 = sending
    // 当前发送开始时的单调时间戳（GetTimestamp()）。仅在 _sendInProgress=1 时有效，0 表示空闲。
    // Timer 回调用 GetElapsedTime(startedAt) >= _sendTimeout 判断超时，
    // 避免墙钟回拨导致 deadline 永不到达、Socket Send 永不被关闭。
    // 跨发送代次的旧 Timer 回调通过 _sendInProgress 的 CompareExchange 防止误关后续发送。
    private long _sendStartedAt;
    private DeadlineRegistration _sendDeadlineRegistration;

    // 鉴权超时：通过全局 DeadlineWheel 注册 deadline，认证成功后取消。
    private DeadlineRegistration _authDeadlineRegistration;

    // 空闲超时：通过全局 DeadlineWheel 注册 deadline。
    // 采用"check-on-fire"模式：deadline 到期时检查 LastInboundAge，
    // 若仍活跃则重新注册 (idleTimeout - lastInboundAge)，否则关闭连接。
    // 避免每包 re-register 的开销（仅 Volatile.Write 时间戳），deadline 至多每 idleTimeout 周期触发一次。
    private readonly TimeSpan _idleTimeout;
    private DeadlineRegistration _idleDeadlineRegistration;

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
        TimeSpan authenticationTimeout = default,
        DeadlineWheel? deadlineWheel = null,
        TimeSpan idleTimeout = default,
        OutboundPumpCoordinator? outboundPump = null)
    {
        _socket = socket;
        ConnectionId = connectionId;
        _sendTimeout = sendTimeout;
        _timeProvider = timeProvider;
        _metrics = metrics;
        _logger = logger;
        _globalOutboundBudget = globalOutboundBudget;
        _deadlineWheel = deadlineWheel;
        _idleTimeout = idleTimeout;
        _outboundPump = outboundPump;
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

        // 发送超时：通过全局 DeadlineWheel 注册。
        // _sendDeadlineRegistration 初始为 default(Id=0)，在 SendFrameAsync 中按需注册。
        if (_deadlineWheel is not null && sendTimeout > TimeSpan.Zero)
        {
            // 占位：实际注册在每次 SendFrameAsync 开始时。
            _sendDeadlineRegistration = default;
        }

        // 鉴权超时：通过全局 DeadlineWheel 注册一次性 deadline，认证成功后取消。
        // deadlineWheel=null 时（测试场景）退化为不启用 deadline，由 HeartbeatCoordinator 兜底扫描。
        if (_deadlineWheel is not null && authenticationTimeout > TimeSpan.Zero)
        {
            _authDeadlineRegistration = _deadlineWheel.Register(
                authenticationTimeout,
                () =>
                {
                    if (Volatile.Read(ref _authenticated) == 0)
                    {
                        Close(SessionCloseReason.AuthenticationTimedOut);
                    }
                });
        }

        // 空闲超时：通过全局 DeadlineWheel 注册 deadline，到期时检查活跃度。
        // check-on-fire 模式：到期时若仍活跃则按剩余时间 re-register，避免每包 re-register 开销。
        // deadlineWheel=null 时（测试场景）退化为不启用 deadline，由 HeartbeatCoordinator 兜底扫描。
        if (_deadlineWheel is not null && idleTimeout > TimeSpan.Zero)
        {
            _idleDeadlineRegistration = RegisterIdleDeadline();
        }

        // 出站驱动模型按模式分支：
        // - PersistentSendLoop（_outboundPump=null）：启动永久 SendLoop Task 消费 _outbound Channel。
        // - OnDemandSendPump（_outboundPump≠null）：不启动 Task，由 TryQueue/TryQueueEphemeral
        //   入队后 CAS 唤醒共享 worker 池（PumpOutboundAsync）。
        _sendLoop = _outboundPump is null ? SendLoopAsync() : null;
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

    /// <summary>
    /// 当前会话颁发的 ResumeToken。会话被吊销（同设备替换/SessionRevoked）时
    /// 据此调用 <see cref="Core.Authentication.IResumeTokenStore.RevokeAsync"/> 撤销，
    /// 防止被替换的旧会话在 Token TTL 窗口内凭此 Token 复活。
    /// </summary>
    public string? CurrentResumeToken { get; internal set; }

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
        // 认证成功，取消鉴权 deadline。
        if (_authDeadlineRegistration.Id != 0)
        {
            _deadlineWheel?.Cancel(_authDeadlineRegistration);
            _authDeadlineRegistration = default;
        }
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

    private void MarkInboundActivity() =>
        Interlocked.Exchange(
            ref _lastInboundTimestamp,
            _timeProvider.GetTimestamp());
}
