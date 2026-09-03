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
/// - v1 范围：仅单聊（群聊离线推送涉及成员批量判定与去重，单独立项）；
/// - 通知偏好/免打扰过滤属 ACCOUNT-OPS-1，待偏好存储落地后在此处接入过滤。
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
    private readonly PushOptions _options;
    private readonly ILogger<OfflinePushTrigger> _logger;

    public OfflinePushTrigger(
        IGlobalPresenceStore presence,
        Func<PushDeliveryCommand, CancellationToken, Task> publishPushDelivery,
        Func<string, CancellationToken, Task<long[]>> resolveAudience,
        IOptions<PushOptions> options,
        ILogger<OfflinePushTrigger> logger)
    {
        _presence = presence;
        _publishPushDelivery = publishPushDelivery;
        _resolveAudience = resolveAudience;
        _options = options.Value;
        _logger = logger;
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
    /// 数量超过 <see cref="PushOptions.MaxGroupOfflinePushesPerMessage"/> 时
    /// 按提及优先截断并记日志。v1 不做免打扰过滤（ACCOUNT-OPS-1 接入点）。
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
            var body = BuildPreview(content, hasAttachments);
            var published = 0;
            var skippedByCap = 0;

            // 提及优先排序：先发被提及的离线成员，再发其余。
            foreach (var member in members
                         .Where(m => m != senderUserId && !onlineMap.GetValueOrDefault(m, false))
                         .OrderByDescending(m => mentioned != null && mentioned.Contains(m)))
            {
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

}
