using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Push;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure.Push;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.TcpGateway.Gateway.Commands.Messaging;

/// <summary>
/// 直聊消息接受成功后的离线推送触发：接收方全局离线（所有 Gateway 实例都无在线会话）时，
/// 发布 <see cref="PushDeliveryCommand"/> 到 JetStream，由推送投递方按已注册 token 走
/// FCM/APNs/WebPush 送达。
/// <para>
/// 设计约束：
/// - 由 <see cref="PushOptions.Enabled"/> 门控，未启用时零开销返回（保持 JSON 默认路径不变）；
/// - 触发失败绝不影响消息主链路（消息已投递/持久化），所有异常内部吞掉并记录；
/// - 免打扰过滤（ACCOUNT-OPS-1）：推送前经 Realtime 查询成员"当前生效免打扰"状态，
///   静音成员跳过；<see cref="PushDeliveryCommand.IsMention"/> 的成员不受静音豁免照常发布
///   （Mention 推送优先级更高，不受静音影响）；
/// - 查询依赖以可选委托注入（<c>queryMutes == null</c> 表示无查询能力，行为不过滤）；
///   查询失败/超时按 fail-open 处理（不过滤、照常推送）——离线推送的可达性优先于静音精度，
///   宁可对静音成员多推一次，也不因查询故障让所有离线成员收不到推送。
/// </para>
/// </summary>
internal sealed partial class OfflinePushTrigger
{
    internal const int MaxPreviewChars = 120;
    internal const string AttachmentPlaceholder = "[附件]";
    private const string DefaultTitle = "新消息";

    private readonly IGlobalPresenceStore _presence;
    private readonly Func<PushDeliveryCommand, CancellationToken, Task> _publishPushDelivery;
    private readonly Func<string, CancellationToken, Task<long[]>> _resolveAudience;
    private readonly Func<ConversationMutesQuery, CancellationToken, Task<IReadOnlyList<long>>>? _queryMutes;
    private readonly PushOptions _options;
    private readonly ILogger<OfflinePushTrigger> _logger;

    public OfflinePushTrigger(
        IGlobalPresenceStore presence,
        Func<PushDeliveryCommand, CancellationToken, Task> publishPushDelivery,
        Func<string, CancellationToken, Task<long[]>> resolveAudience,
        IOptions<PushOptions> options,
        ILogger<OfflinePushTrigger> logger,
        Func<ConversationMutesQuery, CancellationToken, Task<IReadOnlyList<long>>>? queryMutes = null)
    {
        _presence = presence;
        _publishPushDelivery = publishPushDelivery;
        _resolveAudience = resolveAudience;
        _options = options.Value;
        _logger = logger;
        _queryMutes = queryMutes;
    }

    /// <summary>
    /// 消息接受成功后调用。接收方在线时不做任何事；离线时发布推送命令。
    /// 所有异常内部吞掉（仅记录），不向调用方传播。
    /// </summary>
    public async Task TryTriggerForDirectMessageAsync(
        long receiverUserId,
        string? conversationId,
        string messageId,
        string? content,
        bool hasAttachments,
        CancellationToken cancellationToken = default)
    {
        if (!_options.Enabled)
        {
            return;
        }

        if (receiverUserId <= 0)
        {
            return;
        }

        try
        {
            if (await _presence
                    .IsOnlineAsync(receiverUserId, cancellationToken)
                    .ConfigureAwait(false))
            {
                return;
            }

            // ACCOUNT-OPS-1：接收方对会话静音且生效 → 不推送。
            // conversationId 缺失（理论不应发生）或查询失败时 fail-open 照常推送。
            if (conversationId is not null)
            {
                var muted = await TryResolveMutedAsync(
                    conversationId,
                    [receiverUserId],
                    receiverUserId,
                    messageId,
                    cancellationToken).ConfigureAwait(false);
                if (muted is not null && muted.Contains(receiverUserId))
                {
                    return;
                }
            }

            await _publishPushDelivery(
                    new PushDeliveryCommand
                    {
                        TargetUserId = receiverUserId,
                        Title = DefaultTitle,
                        Body = BuildPreview(content, hasAttachments),
                        ConversationId = conversationId,
                        MessageId = messageId,
                        OccurredAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    },
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // 关停期间的取消属预期。
        }
        catch (Exception exception)
        {
            LogTriggerFailed(
                _logger,
                exception,
                receiverUserId,
                messageId);
        }
    }

    private static string BuildPreview(string? content, bool hasAttachments)
    {
        if (string.IsNullOrWhiteSpace(content))
        {
            return hasAttachments ? AttachmentPlaceholder : DefaultTitle;
        }

        var trimmed = content.Trim();
        return trimmed.Length <= MaxPreviewChars
            ? trimmed
            : trimmed[..MaxPreviewChars] + "…";
    }
    [LoggerMessage(
        EventId = 3001,
        Level = LogLevel.Error,
        Message = "离线推送触发失败 ReceiverUserId={ReceiverUserId} MessageId={MessageId}")]
    private static partial void LogTriggerFailed(
        ILogger logger,
        Exception exception,
        long receiverUserId,
        string messageId);

    /// <summary>
    /// 群聊消息接受成功后调用：受众成员批量离线判定，离线成员逐个发布
    /// （Collapse Key 按会话折叠）。提及成员优先且 IsMention 置位；
    /// 免打扰过滤（ACCOUNT-OPS-1）在 cap 之前执行：非提及的静音成员跳过，
    /// 被提及成员不受静音豁免照常发布（IsMention=true）；
    /// 过滤后的数量超过 <see cref="PushOptions.MaxGroupOfflinePushesPerMessage"/> 时
    /// 按提及优先截断并记日志。
    /// </summary>
    public async Task TryTriggerForGroupMessageAsync(
        long senderUserId,
        string conversationId,
        string messageId,
        string? content,
        bool hasAttachments,
        IReadOnlyList<long>? mentionedUserIds,
        CancellationToken cancellationToken = default)
    {
        if (!_options.Enabled)
        {
            return;
        }

        try
        {
            var members = await _resolveAudience(conversationId, cancellationToken)
                .ConfigureAwait(false);
            if (members.Length == 0)
            {
                return;
            }

            var onlineMap = await _presence
                .GetOnlineManyAsync(members, cancellationToken)
                .ConfigureAwait(false);

            var mentioned = mentionedUserIds is { Count: > 0 }
                ? new HashSet<long>(mentionedUserIds)
                : null;

            // 离线候选（发送者除外）。提及优先排序：先发被提及的离线成员，再发其余。
            var offlineCandidates = members
                .Where(m => m != senderUserId && !onlineMap.GetValueOrDefault(m, false))
                .OrderByDescending(m => mentioned != null && mentioned.Contains(m))
                .ToArray();
            if (offlineCandidates.Length == 0)
            {
                return;
            }

            // ACCOUNT-OPS-1：批量查询离线候选的生效免打扰状态。
            // 返回 null（无查询能力/查询失败 fail-open）= 不过滤，保持可用性。
            var muted = await TryResolveMutedAsync(
                conversationId,
                offlineCandidates,
                senderUserId,
                messageId,
                cancellationToken).ConfigureAwait(false);

            var body = BuildPreview(content, hasAttachments);
            var published = 0;
            var skippedByCap = 0;
            var skippedByMuted = 0;

            foreach (var member in offlineCandidates)
            {
                // 非提及的静音成员跳过（不占 cap 名额）；被提及成员不受静音豁免。
                if (muted is not null
                    && muted.Contains(member)
                    && mentioned?.Contains(member) != true)
                {
                    skippedByMuted++;
                    continue;
                }

                if (published >= _options.MaxGroupOfflinePushesPerMessage)
                {
                    skippedByCap++;
                    continue;
                }

                await _publishPushDelivery(
                        new PushDeliveryCommand
                        {
                            TargetUserId = member,
                            Title = DefaultTitle,
                            Body = body,
                            ConversationId = conversationId,
                            MessageId = messageId,
                            IsMention = mentioned != null && mentioned.Contains(member),
                            OccurredAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                        },
                        cancellationToken)
                    .ConfigureAwait(false);
                published++;
            }

            if (skippedByMuted > 0)
            {
                LogGroupPushMutedSkipped(
                    _logger,
                    skippedByMuted,
                    conversationId,
                    messageId);
            }

            if (skippedByCap > 0)
            {
                LogGroupPushCapped(
                    _logger,
                    skippedByCap,
                    conversationId,
                    messageId);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // 关停期间的取消属预期。
        }
        catch (Exception exception)
        {
            LogGroupTriggerFailed(
                _logger,
                exception,
                conversationId,
                messageId);
        }
    }

    /// <summary>
    /// 查询成员的"当前生效免打扰"集合。
    /// 返回 <c>null</c> 表示结果未知（<c>_queryMutes</c> 未注入或查询失败），
    /// 调用方按 fail-open 处理：不过滤、照常推送。
    /// <para>
    /// 权衡说明：免打扰是降噪体验，而离线推送是消息可达性底线。查询链路
    ///（NATS request/reply → Realtime → Postgres）任何一环故障时宁可多推，
    /// 也不放弃整批离线推送。
    /// </para>
    /// </summary>
    private async Task<HashSet<long>?> TryResolveMutedAsync(
        string conversationId,
        IReadOnlyList<long> userIds,
        long actorUserId,
        string messageId,
        CancellationToken cancellationToken)
    {
        if (_queryMutes is null)
        {
            return null;
        }

        try
        {
            var mutedUserIds = await _queryMutes(
                    new ConversationMutesQuery
                    {
                        RequestId = Guid.NewGuid().ToString("N")[..16],
                        ConversationId = conversationId,
                        MemberUserIds = userIds
                    },
                    cancellationToken)
                .ConfigureAwait(false);
            return new HashSet<long>(mutedUserIds);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            // fail-open：查询失败不过滤（保持离线推送可用性）。
            LogMutesQueryFailed(
                _logger,
                exception,
                conversationId,
                actorUserId,
                messageId);
            return null;
        }
    }

    [LoggerMessage(
        EventId = 3002,
        Level = LogLevel.Information,
        Message = "群聊离线推送超过单条上限已截断 Skipped={Skipped} ConversationId={ConversationId} MessageId={MessageId}")]
    private static partial void LogGroupPushCapped(
        ILogger logger,
        int skipped,
        string conversationId,
        string messageId);

    [LoggerMessage(
        EventId = 3003,
        Level = LogLevel.Error,
        Message = "群聊离线推送触发失败 ConversationId={ConversationId} MessageId={MessageId}")]
    private static partial void LogGroupTriggerFailed(
        ILogger logger,
        Exception exception,
        string conversationId,
        string messageId);

    [LoggerMessage(
        EventId = 3004,
        Level = LogLevel.Warning,
        Message = "会话免打扰查询失败，fail-open 不过滤 ConversationId={ConversationId} ActorUserId={ActorUserId} MessageId={MessageId}")]
    private static partial void LogMutesQueryFailed(
        ILogger logger,
        Exception exception,
        string conversationId,
        long actorUserId,
        string messageId);

    [LoggerMessage(
        EventId = 3005,
        Level = LogLevel.Information,
        Message = "群聊离线推送按免打扰过滤 Skipped={Skipped} ConversationId={ConversationId} MessageId={MessageId}")]
    private static partial void LogGroupPushMutedSkipped(
        ILogger logger,
        int skipped,
        string conversationId,
        string messageId);

}
