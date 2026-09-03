using ChatApp.Realtime.Integration.Push;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Commands.Messaging;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure.Push;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace ChatApp.TcpGateway.Tests.Messaging;

/// <summary>
/// 离线推送触发器：Push.Enabled 门控、全局在线判定、预览截断、
/// 附件占位与异常吞噬（触发失败绝不影响消息主链路）。
/// </summary>
public sealed class OfflinePushTriggerTests
{
    private const long ReceiverId = 20002;

    private static readonly Dictionary<string, long[]> ResolvedAudiences = new();

    private static OfflinePushTrigger CreateTrigger(
        FakePresence presence,
        List<PushDeliveryCommand> published,
        bool enabled = true,
        Action<PushOptions>? configureOptions = null)
    {
        published.Clear();
        presence.Reset();
        ResolvedAudiences.Clear();
        var options = new PushOptions { Enabled = enabled };
        configureOptions?.Invoke(options);
        return new OfflinePushTrigger(
            presence,
            (command, _) =>
            {
                published.Add(command);
                return Task.CompletedTask;
            },
            (conversationId, _) =>
            {
                ResolvedAudiences.TryGetValue(conversationId, out var members);
                return Task.FromResult(members ?? Array.Empty<long>());
            },
            Options.Create(options),
            NullLogger<OfflinePushTrigger>.Instance);
    }

    [Fact]
    public async Task Disabled_DoesNotQueryPresence_NorPublish()
    {
        var presence = new FakePresence(isOnline: false);
        var published = new List<PushDeliveryCommand>();
        var trigger = CreateTrigger(presence, published, enabled: false);

        await trigger.TryTriggerForDirectMessageAsync(ReceiverId, "conv", "m1", "hello", false, TestContext.Current.CancellationToken);

        Assert.Equal(0, presence.IsOnlineCalls);
        Assert.Empty(published);
    }

    [Fact]
    public async Task OnlineRecipient_DoesNotPublish()
    {
        var presence = new FakePresence(isOnline: true);
        var published = new List<PushDeliveryCommand>();
        var trigger = CreateTrigger(presence, published);

        await trigger.TryTriggerForDirectMessageAsync(ReceiverId, "conv", "m1", "hello", false, TestContext.Current.CancellationToken);

        Assert.Equal(1, presence.IsOnlineCalls);
        Assert.Empty(published);
    }

    [Fact]
    public async Task OfflineRecipient_PublishesCommand_WithTruncatedPreview()
    {
        var presence = new FakePresence(isOnline: false);
        var published = new List<PushDeliveryCommand>();
        var trigger = CreateTrigger(presence, published);

        var content = new string('x', 300);
        await trigger.TryTriggerForDirectMessageAsync(ReceiverId, "conv-1", "m1", content, false, TestContext.Current.CancellationToken);

        var push = Assert.Single(published);
        Assert.Equal(ReceiverId, push.TargetUserId);
        Assert.Equal("conv-1", push.ConversationId);
        Assert.Equal("m1", push.MessageId);
        Assert.Equal(OfflinePushTrigger.MaxPreviewChars + 1, push.Body.Length);
        Assert.EndsWith("\u2026", push.Body);
        Assert.True(push.OccurredAtMs > 0);
    }

    [Fact]
    public async Task AttachmentsOnlyMessage_UsesPlaceholderBody()
    {
        var presence = new FakePresence(isOnline: false);
        var published = new List<PushDeliveryCommand>();
        var trigger = CreateTrigger(presence, published);

        await trigger.TryTriggerForDirectMessageAsync(ReceiverId, null, "m1", content: null, hasAttachments: true, TestContext.Current.CancellationToken);

        var push = Assert.Single(published);
        Assert.Equal(OfflinePushTrigger.AttachmentPlaceholder, push.Body);
        Assert.Null(push.ConversationId);
    }

    [Fact]
    public async Task PresenceFailure_IsSwallowed_AndNothingPublished()
    {
        var presence = new FakePresence(isOnline: false)
        {
            ThrowOnIsOnline = new InvalidOperationException("redis down")
        };
        var published = new List<PushDeliveryCommand>();
        var trigger = CreateTrigger(presence, published);

        // 不抛出：触发失败绝不影响消息主链路。
        await trigger.TryTriggerForDirectMessageAsync(ReceiverId, "conv", "m1", "hello", false, TestContext.Current.CancellationToken);

        Assert.Empty(published);
    }

    [Fact]
    public async Task NonPositiveReceiver_IsIgnored()
    {
        var presence = new FakePresence(isOnline: false);
        var published = new List<PushDeliveryCommand>();
        var trigger = CreateTrigger(presence, published);

        await trigger.TryTriggerForDirectMessageAsync(0, "conv", "m1", "hello", false, TestContext.Current.CancellationToken);

        Assert.Equal(0, presence.IsOnlineCalls);
        Assert.Empty(published);
    }

    [Fact]
    public async Task GroupMessage_OfflineMembers_Pushed_WithMentionFlags()
    {
        var presence = new FakePresence(isOnline: false, onlineUsers: [43]);
        var published = new List<PushDeliveryCommand>();
        // 受众：41=发送者（排除）、42/44 离线、43 在线。
        var trigger = CreateTrigger(presence, published);
        ResolvedAudiences["conv-group"] = [41, 42, 43, 44];

        await trigger.TryTriggerForGroupMessageAsync(
            senderUserId: 41,
            conversationId: "conv-group",
            messageId: "gm-1",
            content: "group hello",
            hasAttachments: false,
            mentionedUserIds: [44],
            TestContext.Current.CancellationToken);

        Assert.Equal(2, published.Count);
        // 提及优先：44（被提及）先发且 IsMention=true。
        Assert.Equal(44, published[0].TargetUserId);
        Assert.True(published[0].IsMention);
        Assert.Equal(42, published[1].TargetUserId);
        Assert.False(published[1].IsMention);
        // 在线成员 43 未被推送；发送者 41 被排除。
        Assert.DoesNotContain(published, p => p.TargetUserId is 43 or 41);
        // Collapse Key：同一会话折叠。
        Assert.All(published, p => Assert.Equal("conv-group", p.ConversationId));
    }

    [Fact]
    public async Task GroupMessage_CapsPushes_MentionFirst()
    {
        var presence = new FakePresence(isOnline: false);
        var published = new List<PushDeliveryCommand>();
        ResolvedAudiences["conv-cap"] = [41, 42, 43, 44, 45, 46];
        var options = new PushOptions { Enabled = true, MaxGroupOfflinePushesPerMessage = 1 };
        var trigger = new OfflinePushTrigger(
            presence,
            (command, _) =>
            {
                published.Add(command);
                return Task.CompletedTask;
            },
            (conversationId, _) =>
            {
                ResolvedAudiences.TryGetValue(conversationId, out var members);
                var resolved = members ?? Array.Empty<long>();
                Console.WriteLine($"[PROBE] resolve conv={conversationId} count={resolved.Length} cap={options.MaxGroupOfflinePushesPerMessage} enabled={options.Enabled}");
                return Task.FromResult(resolved);
            },
            Microsoft.Extensions.Options.Options.Create(options),
            NullLogger<OfflinePushTrigger>.Instance);

        await trigger.TryTriggerForGroupMessageAsync(
            senderUserId: 41,
            conversationId: "conv-cap",
            messageId: "gm-2",
            content: "cap",
            hasAttachments: false,
            mentionedUserIds: [45],
            TestContext.Current.CancellationToken);

        // cap 1：仅提及的 45（提及优先）。
        Assert.True(published.Count == 1,
            $"DEBUG published={published.Count} cap={options.MaxGroupOfflinePushesPerMessage} enabled={options.Enabled} audience={ResolvedAudiences["conv-cap"].Length}");
        Assert.Equal(45, published[0].TargetUserId);
        Assert.True(published[0].IsMention);
    }

    [Fact]
    public async Task GroupMessage_AudienceResolveFailure_IsSwallowed()
    {
        var presence = new FakePresence(isOnline: false);
        var published = new List<PushDeliveryCommand>();
        var trigger = CreateTrigger(presence, published);
        // 未注册受众 → 解析返回空数组（仓内 fail-closed 惯例在缓存内部实现）。

        await trigger.TryTriggerForGroupMessageAsync(
            41, "conv-unknown", "gm-3", "hello", false, null,
            TestContext.Current.CancellationToken);

        Assert.Empty(published);
    }

    internal sealed class FakePresence : IGlobalPresenceStore
    {
        private readonly bool _isOnline;
        private readonly HashSet<long>? _onlineUsers;
        private int _isOnlineCalls;

        public FakePresence(bool isOnline, IEnumerable<long>? onlineUsers = null)
        {
            _isOnline = isOnline;
            _onlineUsers = onlineUsers is null ? null : new HashSet<long>(onlineUsers);
        }

        public int IsOnlineCalls => _isOnlineCalls;

        public InvalidOperationException? ThrowOnIsOnline { get; init; }

        public void Reset() => _isOnlineCalls = 0;

        public Task<PresenceTransition> SetOnlineAsync(long userId, string instanceId, CancellationToken ct = default) =>
            Task.FromResult(default(PresenceTransition));

        public Task<PresenceTransition> SetOfflineAsync(long userId, string instanceId, CancellationToken ct = default) =>
            Task.FromResult(default(PresenceTransition));

        public Task RefreshOnlineAsync(long userId, string instanceId, CancellationToken ct = default) => Task.CompletedTask;

        public Task<bool> IsOnlineAsync(long userId, CancellationToken ct = default)
        {
            _isOnlineCalls++;
            if (ThrowOnIsOnline is not null)
            {
                throw ThrowOnIsOnline;
            }

            return Task.FromResult(_isOnline);
        }

        public Task<IReadOnlyDictionary<long, bool>> GetOnlineManyAsync(
            IReadOnlyList<long> userIds, CancellationToken ct = default)
        {
            Console.WriteLine($"[PROBE] getonline n={userIds.Count} users={string.Join(",", userIds)}");
            return Task.FromResult<IReadOnlyDictionary<long, bool>>(
                userIds.ToDictionary(
                    id => id,
                    id => _onlineUsers is not null && _onlineUsers.Contains(id)));
        }

        public Task RunMaintenanceAsync(CancellationToken ct = default) => Task.CompletedTask;
    }
}
