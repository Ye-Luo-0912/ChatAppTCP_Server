using System.Buffers;
using ChatApp.ActorRuntime.Abstractions;
using ChatApp.ActorRuntime.Runtime;
using ChatApp.ActorRuntime.Scheduling;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Ephemeral;

/// <summary>
/// Typing 领域 Actor 管道：替代 EphemeralCommandPipeline 处理 TypingNotify。
/// <para>
/// TCP Read 路径直接解析 TypingNotify 并路由到 <see cref="ActorRuntime{TKey,TState,TMessage}"/>，
/// 不创建通用 <see cref="SessionCommand"/>、不复制 payload 到 ArrayPool、不携带 RemoteIp/Budget。
/// Actor 使用 <see cref="ActorMailboxMode.LatestOnly"/> 自动合并快速状态变更。
/// </para>
/// <para>
/// 授权 I/O 经 <see cref="TypingAuthorizationWork"/> 提交到 AsyncOperationExecutor，
/// Actor Suspend 期间 LatestOnly Mailbox 仅保留最新 Notify，实现 typing=true→false 授权前合并。
/// </para>
/// </summary>
internal sealed class TypingActorPipeline : IAsyncDisposable, ITypingAuthorizationInvalidator
{
    private readonly ActorRuntime<TypingActorKey, TypingActorState, TypingActorMessage> _actor;
    private readonly TypingActorBehavior _behavior;
    private readonly DomainWorkLane<TypingAuthorizationWork> _authLane;
    private readonly IPayloadCodec<TypingNotify> _typingNotifyCodec;
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _operationTimeout;
    private bool _disposed;

    public TypingActorPipeline(
        TcpGatewayOptions options,
        IPayloadCodec<TypingNotify> typingNotifyCodec,
        IDirectConversationAuthorizer? directConversationAuthorizer,
        TypingFanoutCoordinator typingFanout,
        GatewayMetrics metrics,
        TimeProvider timeProvider,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        _typingNotifyCodec = typingNotifyCodec;
        _timeProvider = timeProvider;
        _operationTimeout = options.EphemeralActorOperationTimeout;

        _authLane = new DomainWorkLane<TypingAuthorizationWork>(
            maxConcurrency: options.EphemeralActorAsyncConcurrency > 0
                ? options.EphemeralActorAsyncConcurrency
                : Math.Max(2, Environment.ProcessorCount * 2),
            queueCapacity: Math.Max(1024, options.EphemeralActorIngressCapacity),
            operationTimeout: options.EphemeralActorOperationTimeout);

        _behavior = new TypingActorBehavior(
            _authLane,
            directConversationAuthorizer,
            typingFanout,
            metrics,
            logger,
            timeProvider,
            // 授权 TTL 与 CachedDirectConversationAuthorizer 默认 allowTtl 对齐（30s）。
            // 关系变更后由 InvalidateAuthorizationAsync 主动失效，TTL 是兜底。
            TimeSpan.FromSeconds(30));
        var dropHandler = new TypingActorDropHandler(metrics);

        _actor = new ActorRuntime<TypingActorKey, TypingActorState, TypingActorMessage>(
            behavior: _behavior,
            mailboxMode: ActorMailboxMode.LatestOnly,
            options: new ActorRuntimeOptions
            {
                ShardCount = options.EphemeralActorShardCount > 0
                    ? options.EphemeralActorShardCount
                    : NextPowerOfTwo(Math.Max(2, Environment.ProcessorCount)),
                ShardIngressCapacity = options.EphemeralActorIngressCapacity,
                // LatestOnly 模式忽略此值，设为 1 即可。
                DefaultMailboxCapacity = 1,
                ShardBurstLimit = 64,
                // LatestOnly 每次只处理 1 条，设小值即可。
                MaxMessagesPerActorTurn = 4,
                ShardTickInterval = TimeSpan.FromMilliseconds(25),
                AsyncOperationConcurrency =
                    options.EphemeralActorAsyncConcurrency > 0
                        ? options.EphemeralActorAsyncConcurrency
                        : Math.Max(2, Environment.ProcessorCount * 2),
                AsyncOperationQueueCapacity =
                    Math.Max(1024, options.EphemeralActorIngressCapacity),
                AsyncOperationTimeout = options.EphemeralActorOperationTimeout,
                ActorIdleTimeout = options.EphemeralActorIdleTimeout
            },
            timeProvider: timeProvider,
            dropHandler: dropHandler);
        _behavior.Attach(_actor);
    }

    public ActorRuntimeSnapshot Snapshot => _actor.GetSnapshot();

    /// <summary>
    /// 授权 I/O DomainWorkLane 快照：用于注册 typing_auth.* 指标。
    /// </summary>
    public DomainWorkLaneSnapshot AuthLaneSnapshot => _authLane.GetSnapshot();

    /// <summary>
    /// 关系变更触发的授权失效：向对应 (sender, target) 双向 Actor 投递 AuthorizationInvalidated。
    /// 清空 Actor 内缓存的 Authorized=true，下一次 Notify 必须重新走授权 I/O。
    /// <para>
    /// 调用方（如 RelationshipListHandler）在拉黑/解除好友时对两个方向各调用一次。
    /// Ephemeral 语义：若 Actor 已被 IdleSweep 回收，TryTell 返回 ActorClosed，调用方无需处理。
    /// </para>
    /// </summary>
    public void InvalidateAuthorization(long senderUserId, long targetUserId)
    {
        if (senderUserId <= 0 || targetUserId <= 0)
            return;

        var forward = new TypingActorKey(senderUserId, targetUserId);
        var reverse = new TypingActorKey(targetUserId, senderUserId);
        var message = TypingActorMessage.AuthorizationInvalidated();
        // 丢弃语义：Actor 不存在或 Shard 满载时静默失败，关系变更后 TTL 兜底。
        _actor.TryTellEphemeral(in forward, in message);
        _actor.TryTellEphemeral(in reverse, in message);
    }

    /// <summary>
    /// 从原始帧解析 TypingNotify 并路由到 Typing Actor。
/// <para>
/// 在 TCP Read 路径调用，替代 EphemeralCommandPipeline 的 TryEnqueue。
/// payload 直接从 <see cref="PacketFrame.Payload"/> 解析，不复制到 ArrayPool。
/// </para>
    /// <returns>true 表示已处理（无论 Actor 是否接受）；false 表示协议错误需关闭连接。</returns>
    /// </summary>
    public bool TryHandleFrame(
        in PacketFrame frame,
        TcpClientSession session)
    {
        var notify = _typingNotifyCodec.Deserialize(frame.Payload);
        if (notify is null || string.IsNullOrWhiteSpace(notify.ConversationId))
            return true; // 静默丢弃无效 payload，不关闭连接

        if (!TryResolveDirectConversationTarget(
                notify.ConversationId,
                session.UserId,
                out _,
                out var targetUserId))
        {
            return true; // 非法 conversationId，静默丢弃
        }

        var key = new TypingActorKey(session.UserId, targetUserId);
        var message = TypingActorMessage.Notify(
            isTyping: notify.IsTyping,
            timestamp: _timeProvider.GetTimestamp(),
            sessionGeneration: 0);

        var status = _actor.TryTellEphemeral(in key, in message);
        return status == ActorPostStatus.Accepted ||
               status == ActorPostStatus.Replaced;
        // ShardOverloaded / ActorClosed / RuntimeStopped → 丢弃（Ephemeral 语义允许丢弃）
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _authLane.StartAsync(cancellationToken).ConfigureAwait(false);
        await _actor.StartAsync(cancellationToken).AsTask().ConfigureAwait(false);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        using var drainCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken);
        drainCts.CancelAfter(_operationTimeout + TimeSpan.FromSeconds(2));
        await _actor
            .StopAsync(ActorStopMode.Drain, drainCts.Token)
            .ConfigureAwait(false);
        await _authLane.StopAsync().ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await _actor.DisposeAsync().ConfigureAwait(false);
        await _authLane.DisposeAsync().ConfigureAwait(false);
    }

    private static int NextPowerOfTwo(int value)
    {
        value--;
        value |= value >> 1;
        value |= value >> 2;
        value |= value >> 4;
        value |= value >> 8;
        value |= value >> 16;
        return value + 1;
    }

    /// <summary>
    /// 从 conversationId 解析私聊会话的另一方用户 Id。
    /// 以 conversationId 为权威源，校验发送方必须是会话成员。
    /// </summary>
    private static bool TryResolveDirectConversationTarget(
        string? conversationId,
        long senderUserId,
        out string normalizedId,
        out long targetUserId)
    {
        normalizedId = string.Empty;
        targetUserId = 0;

        if (string.IsNullOrWhiteSpace(conversationId) || senderUserId <= 0)
            return false;

        var trimmed = conversationId.Trim();
        if (!ConversationId.TryParseDirect(trimmed, out var userLo, out var userHi))
            return false;

        if (senderUserId != userLo && senderUserId != userHi)
            return false;

        targetUserId = senderUserId == userLo ? userHi : userLo;
        normalizedId = trimmed;
        return true;
    }
}
