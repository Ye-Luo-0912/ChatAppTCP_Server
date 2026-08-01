using System.Net.Sockets;
using System.Threading.Channels;
using System.Threading.Tasks.Sources;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Protocol;
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
    // PersistentSendLoop 模式下的永久发送 Task；OnDemandSendPump/PerSessionDrain 模式下为 null。
    private readonly Task? _sendLoop;
    private readonly long _connectedTimestamp;

    // OnDemandSendPump 模式专用：共享出站 pump 协调器。
    // PersistentSendLoop/PerSessionDrain 模式下为 null。
    private readonly OutboundPumpCoordinator? _outboundPump;
    // 八.1：PerSessionDrain 状态机——packed state (bit 32) + generation (bits 0-31) 到单个 long，
    // 单次 CAS 原子完成 Idle→Running 转换 + 代次发布，消除每次 enqueue 的 DrainOperation 分配。
    // <para>
    // 状态：0=Idle，1=Running（bit 32）。代次：每次 Idle→Running 转换递增，区分不同 drain 实例。
    // drain 释放/重夺所有权时 CAS 完整 64 位值（含代次），防止 ABA（旧 drain 重夺已被新 drain 接管的代次）。
    // </para>
    // <para>
    // P1-6 不变量保留：状态转换与代次发布在单次 CAS 中原子完成。Dispose 读 _drainStateGen：
    // Running 则 <c>await _drainOp.WaitAsync()</c>；Idle 则无活跃 drain。
    // </para>
    // PersistentSendLoop/OnDemandSendPump 模式下永远为 0（Idle, gen 0）。
    private long _drainStateGen;
    private const long DrainStateRunningBit = 1L << 32;
    // 八.1：可复用 drain 句柄——惰性初始化一次，跨代次通过 <see cref="DrainOperation.Reset"/> 重用。
    // 旧实现每次 <c>TryScheduleSend</c> 都 <c>new DrainOperation(含 TCS)</c>，CAS 失败时立即成为垃圾。
    // PersistentSendLoop/OnDemandSendPump 模式下永远为 null。
    private DrainOperation? _drainOp;
    // PerSessionDrain 模式标志：构造时确定，影响 TryScheduleSend 分支。
    private readonly bool _usePerSessionDrain;

    /// <summary>
    /// 八.1：可复用的 PerSessionDrain 句柄，封装 <see cref="ManualResetValueTaskSourceCore{T}"/>
    /// 替代每次 drain 分配 <see cref="TaskCompletionSource"/>。
    /// <para>
    /// 旧实现每次 <c>TryScheduleSend</c> 都 <c>new DrainOperation(含 TCS)</c>，CAS 失败时立即成为垃圾。
    /// 现改为 Session 内复用单实例，MRVTSC 可 <c>Reset()</c> 跨代次重用——普通入队路径只做 CAS，零分配。
    /// </para>
    /// <para>
    /// Reset/Complete 通过 <see cref="SpinLock"/> 串行化，防止跨代次 SetResult 竞态
    /// （旧代次 Complete 的 SetResult 误完成新代次的 MRVTSC）。临界区极短（~10ns），
    /// 且仅在 drain 启动/退出时执行，不在每次 enqueue 热路径上。
    /// </para>
    /// </summary>
    internal sealed class DrainOperation : IValueTaskSource
    {
        private ManualResetValueTaskSourceCore<bool> _core;
        private int _completed;          // 1 if Complete() was called for the current generation
        private int _activeGeneration;   // the generation this handle is currently bound to
        private SpinLock _lock;          // serializes Reset/Complete (very short critical sections)

        public DrainOperation()
        {
            _core.RunContinuationsAsynchronously = true;
            _lock = new SpinLock(false);
        }

        /// <summary>绑定到新 drain 代次。重置 MRVTSC 以跨代次重用。</summary>
        public void Reset(int generation)
        {
            var lockTaken = false;
            _lock.Enter(ref lockTaken);
            try
            {
                _activeGeneration = generation;
                _core.Reset();
                Volatile.Write(ref _completed, 0);
            }
            finally
            {
                if (lockTaken) _lock.Exit();
            }
        }

        /// <summary>
        /// 通知等待方 drain 已完成。仅当代次匹配时生效——防止旧代次的 Complete
        /// 误完成新代次的 MRVTSC（跨代次 SetResult 竞态）。
        /// </summary>
        public void Complete(int generation)
        {
            var lockTaken = false;
            _lock.Enter(ref lockTaken);
            try
            {
                if (_activeGeneration != generation)
                    return; // 旧代次：新代次已 Reset，不操作 MRVTSC。
                if (Volatile.Read(ref _completed) == 1)
                    return; // 当前代次已完成（幂等）。
                Volatile.Write(ref _completed, 1);
                _core.SetResult(true);
            }
            finally
            {
                if (lockTaken) _lock.Exit();
            }
        }

        /// <summary>获取可等待的 <see cref="ValueTask"/>。Version 随 Reset 递增，使旧 token 失效。</summary>
        /// <summary>
    /// 当前绑定的代次。DisposeAsync 据此判断 op 是否已发布到当前 generation。
    /// </summary>
    public int ActiveGeneration => Volatile.Read(ref _activeGeneration);

    /// <summary>
    /// P0-2：Generation-aware 等待。代次不匹配时同步完成（让调用方重新检查状态），
    /// 避免读到上一代 op 的已完成 Version 导致 busy-loop 或 InvalidOperationException。
    /// </summary>
    public ValueTask WaitAsync(int expectedGeneration)
    {
        if (Volatile.Read(ref _activeGeneration) != expectedGeneration)
            return ValueTask.CompletedTask;
        return new(this, _core.Version);
    }

        ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) =>
            _core.GetStatus(token);

        void IValueTaskSource.OnCompleted(
            Action<object?> continuation,
            object? state,
            short token,
            ValueTaskSourceOnCompletedFlags flags) =>
            _core.OnCompleted(continuation, state, token, flags);

        void IValueTaskSource.GetResult(short token) => _core.GetResult(token);
    }

    // OnDemandSendPump 三态调度状态机，CAS 驱动：
    //   Idle(0)    → Queued(1)：enqueuer CAS 成功后 TrySchedule 入 ready queue
    //   Queued(1)   → Running(2)：worker 出队后 CAS 取得发送所有权
    //   Running(2)  → Queued(1)：pump 结束但仍有 pending work，重新入队（不经过 Idle）
    //   Running(2)  → Idle(0)：pump 结束且无 pending work，转空闲后重检防丢失唤醒
    // PerSessionDrain 二态调度状态机（复用 Idle/Running，跳过 Queued）：
    //   Idle(0)    → Running(2)：enqueuer CAS 成功后启动自有 drain Task
    //   Running(2)  → Idle(0)：drain 队列空后 CAS 退出，重检防丢失唤醒
    // 关键规则：任意时刻最多一个 worker/drain 持有发送所有权。
    private const int SendStateIdle = 0;
    private const int SendStateQueued = 1;
    private const int SendStateRunning = 2;
    private int _sendState;

    // Ephemeral latest-state mailbox：按 EphemeralKey 分槽，同 key 覆盖旧帧保留最新状态。
    // Typing key = (KindTyping, hash(conversationId))，Presence key = (KindPresence, userId)。
    // 与 _outbound FIFO 独立：flush sentinel 写入 FIFO 唤醒发送循环排空 mailbox。
    // 惰性创建：首次 TryQueueEphemeral 时才分配。Specialized Typing 模式下永远不创建，
    // 节省每连接一个 mailbox 对象（数组 + List）。
    private EphemeralMailbox? _ephemeralMailbox;
    // 标记是否已有 flush sentinel 在 _outbound 队列中，避免重复入队。
    private volatile bool _ephemeralFlushPending;

    // 发送超时：通过 SendTimeoutTracker 周期扫描管理（替代每帧 DeadlineWheel.Register）。
    // 单调时钟 + generation（_sendInProgress CompareExchange）防止墙钟回拨与跨发送代次误关。
    // Auth/Idle 超时仍由 DeadlineWheel 管理（低频，符合其设计假设）。
    private readonly DeadlineWheel? _deadlineWheel;
    private readonly SendTimeoutTracker? _sendTimeoutTracker;
    // P1-5：帧装配超时扫描器，替代 DeadlineWheel 管理高频 Header/Payload 装配超时。
    private readonly FrameAssemblyTimeoutTracker? _frameAssemblyTracker;
    private int _sendInProgress; // 0 = idle, 1 = sending
    // 当前发送开始时的单调时间戳（GetTimestamp()）。仅在 _sendInProgress=1 时有效，0 表示空闲。
    // 扫描线程用 GetElapsedTime(startedAt) >= _sendTimeout 判断超时，
    // 避免墙钟回拨导致 deadline 永不到达、Socket Send 永不被关闭。
    // 跨发送代次的旧扫描通过 _sendInProgress 的 CompareExchange 防止误关后续发送。
    private long _sendStartedAt;

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
    private int _negotiatedProtocolVersion;
    private int _negotiatedFeatureBits;
    private int _closeState;
    private int _closeReason;
    // P0-4 / 主线二子项2：admission 状态机，独立于 _authenticated 以避免 Resume Commit 失败时泄漏未认证计数。
    // 三态：Unauthenticated(0) → Promoted(1) → Released(2)，CAS 转换防止重复递减。
    private int _admissionState = (int)AdmissionState.Unauthenticated;

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
        OutboundPumpCoordinator? outboundPump = null,
        SendTimeoutTracker? sendTimeoutTracker = null,
        FrameAssemblyTimeoutTracker? frameAssemblyTracker = null,
        bool usePerSessionDrain = false)
    {
        _socket = socket;
        ConnectionId = connectionId;
        _sendTimeout = sendTimeout;
        _timeProvider = timeProvider;
        _metrics = metrics;
        _logger = logger;
        _globalOutboundBudget = globalOutboundBudget;
        _deadlineWheel = deadlineWheel;
        _sendTimeoutTracker = sendTimeoutTracker;
        _frameAssemblyTracker = frameAssemblyTracker;
        _idleTimeout = idleTimeout;
        _outboundPump = outboundPump;
        _usePerSessionDrain = usePerSessionDrain;
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
        // - PersistentSendLoop（_outboundPump=null, _usePerSessionDrain=false）：启动永久 SendLoop Task。
        // - OnDemandSendPump（_outboundPump≠null, _usePerSessionDrain=false）：不启动 Task，
        //   由 TryQueue/TryQueueEphemeral 入队后 CAS 唤醒共享 worker 池（PumpOutboundAsync）。
        // - PerSessionDrain（_outboundPump=null, _usePerSessionDrain=true）：不启动 Task，
        //   由 TryQueue/TryQueueEphemeral 入队后 CAS Idle→Running 启动自有 drain Task。
        _sendLoop = (_outboundPump is null && !_usePerSessionDrain) ? SendLoopAsync() : null;
    }

    public uint ConnectionId { get; }

    /// <summary>
    /// 每次 TCP 连接生成的公开路由标识（GUID），用于：
    /// <list type="bullet">
    /// <item>跨 Gateway 吊销路由匹配（<see cref="Core.Protocol.SessionRevokedPayload.TransportId"/>）；</item>
    /// <item>本机新旧连接区分（<see cref="UserSessionRegistry.TakeOverSameDevice"/>）；</item>
    /// <item>写入 <see cref="Core.Authentication.ResumeContext"/> 供客户端跟踪。</item>
    /// </list>
    /// <para>
    /// P1-A2：与 <see cref="LeaseOwnerToken"/> 分离。<c>ConnectionLeaseId</c> 是公开标识，
    /// 可出现在日志/NATS 事件中；<c>LeaseOwnerToken</c> 是私有所有权凭证，仅用于 Redis CAS。
    /// 内部存储为 <see cref="Guid"/>（16 字节），仅首次访问时格式化为 "N" 字符串。
    /// 未认证即断开的连接不产生字符串分配。
    /// </para>
    /// </summary>
    private readonly Guid _connectionLeaseId = Guid.NewGuid();
    private string? _connectionLeaseIdString;
    public string ConnectionLeaseId => _connectionLeaseIdString ??= _connectionLeaseId.ToString("N");

    /// <summary>
    /// 每次 TCP 连接生成的私有所有权凭证（GUID），仅用于 Redis 设备租约的
    /// compare-and-delete/refresh（<see cref="Core.Authentication.IDeviceSessionLeaseStore"/>）。
    /// <para>
    /// P1-A2：与 <see cref="ConnectionLeaseId"/> 分离，遵循最小权限原则——
    /// 即使 <c>ConnectionLeaseId</c>（公开 TransportId）泄漏到日志或客户端，
    /// 攻击者也无法据此构造伪造的 Release/Refresh 请求释放他人租约。
    /// </para>
    /// <para>
    /// 内部存储为 <see cref="Guid"/>（16 字节），仅首次访问时格式化为 "N" 字符串。
    /// </para>
    /// </summary>
    private readonly Guid _leaseOwnerToken = Guid.NewGuid();
    private string? _leaseOwnerTokenString;
    public string LeaseOwnerToken => _leaseOwnerTokenString ??= _leaseOwnerToken.ToString("N");

    public bool IsConnected => Volatile.Read(ref _closeState) == 0;

    public bool IsAuthenticated => Volatile.Read(ref _authenticated) != 0;

    /// <summary>
    /// P0-4 / 主线二子项2：admission 状态机。
    /// <para>
    /// 三态：<c>Unauthenticated</c> → <c>Promoted</c>（认证成功）→ <c>Released</c>（连接关闭）。
    /// 仅当 <c>Promoted</c> 时占用已认证槽位；<c>Released</c> 防止重复递减。
    /// </para>
    /// <para>
    /// 不能通过 <see cref="UserId"/> &gt; 0 推断——Resume Commit 失败时
    /// <see cref="Authenticate"/> 已设置 UserId 但状态仍为 <c>Unauthenticated</c>，
    /// 清理必须递减未认证计数否则泄漏 <c>MaxUnauthenticatedConnections</c> 槽位。
    /// </para>
    /// </summary>
    public AdmissionState AdmissionState => (AdmissionState)Volatile.Read(ref _admissionState);

    /// <summary>
    /// P0-4：admission 是否已被提升为已认证（向后兼容）。
    /// </summary>
    public bool AdmissionPromoted => AdmissionState == AdmissionState.Promoted;

    /// <summary>
    /// P0-4 / 主线二子项2：标记 admission 已提升为 Promoted。
    /// 仅由 SessionControlHandler 在 <c>_listenerHost.MarkAuthenticated()</c> 后调用。
    /// CAS 从 Unauthenticated → Promoted，已 Promoted/Released 时为 no-op。
    /// </summary>
    public void MarkAdmissionPromoted()
    {
        Interlocked.CompareExchange(
            ref _admissionState,
            (int)AdmissionState.Promoted,
            (int)AdmissionState.Unauthenticated);
    }

    /// <summary>
    /// 主线二子项2：标记 admission 已释放。仅由清理路径调用，CAS 从 Promoted → Released。
    /// 返回 true 表示本次调用完成了 Promoted → Released 转换（调用方据此递减已认证计数）。
    /// 已 Released 或 Unauthenticated 时返回 false（不重复递减）。
    /// </summary>
    public bool TryReleaseAdmission()
    {
        return Interlocked.CompareExchange(
            ref _admissionState,
            (int)AdmissionState.Released,
            (int)AdmissionState.Promoted) == (int)AdmissionState.Promoted;
    }

    /// <summary>
    /// 是否已完成 ClientHello 握手。RequireClientHello=true 时认证前必须为 true。
    /// </summary>
    public bool HasCompletedHandshake => Volatile.Read(ref _handshakeCompleted) != 0;

    /// <summary>握手选定的协议版本；未握手的兼容连接为 0。</summary>
    public ushort NegotiatedProtocolVersion =>
        (ushort)Volatile.Read(ref _negotiatedProtocolVersion);

    /// <summary>ClientHello 与 ServerHello 的能力位交集。</summary>
    public uint NegotiatedFeatureBits =>
        unchecked((uint)Volatile.Read(ref _negotiatedFeatureBits));

    /// <summary>
    /// 发布协议协商结果并标记握手完成。结果字段先写、完成标记最后写，
    /// 使异步命令 worker 读取到完成状态时一定能看到完整协商快照。
    /// </summary>
    public void CompleteHandshake(
        ushort protocolVersion,
        uint featureBits)
    {
        Volatile.Write(ref _negotiatedProtocolVersion, protocolVersion);
        Volatile.Write(
            ref _negotiatedFeatureBits,
            unchecked((int)featureBits));
        Volatile.Write(ref _handshakeCompleted, 1);
    }

    /// <summary>
    /// 判断扩展能力是否可用。未启用命令能力门控的旧客户端保持兼容；
    /// 启用后要求协商结果包含全部指定能力位。
    /// </summary>
    public bool AllowsFeature(GatewayFeature required)
    {
        var featureBits = NegotiatedFeatureBits;
        return !GatewayFeatureSet.ContainsAll(
                   featureBits,
                   GatewayFeature.CommandCapabilities) ||
               GatewayFeatureSet.ContainsAll(featureBits, required);
    }

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

    /// <summary>
    /// 获取或创建 EphemeralMailbox。首次 TryQueueEphemeral 时惰性创建。
    /// Specialized Typing 模式下永远不会被调用，节省每连接一个 mailbox 对象。
    /// 线程安全：CAS 发布，多线程首次调用时仅一个实例胜出。
    /// </summary>
    private EphemeralMailbox GetOrCreateEphemeralMailbox()
    {
        var mailbox = Volatile.Read(ref _ephemeralMailbox);
        if (mailbox is not null)
            return mailbox;

        var created = new EphemeralMailbox();
        return Interlocked.CompareExchange(ref _ephemeralMailbox, created, null) is null
            ? created
            : Volatile.Read(ref _ephemeralMailbox);
    }

    /// <summary>
    /// EphemeralMailbox 是否非空（有未排空的 ephemeral 条目）。
    /// mailbox 未创建（null）时返回 false，避免 null-check 散落。
    /// </summary>
    private bool HasEphemeralEntries => _ephemeralMailbox is not null && !_ephemeralMailbox.IsEmpty;
}
