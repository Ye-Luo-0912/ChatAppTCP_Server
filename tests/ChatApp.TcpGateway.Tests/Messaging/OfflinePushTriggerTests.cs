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

    private static OfflinePushTrigger CreateTrigger(
        FakePresence presence,
        List<PushDeliveryCommand> published,
        bool enabled = true)
    {
        published.Clear();
        presence.Reset();
        return new OfflinePushTrigger(
            presence,
            (command, _) =>
            {
                published.Add(command);
                return Task.CompletedTask;
            },
            Options.Create(new PushOptions { Enabled = enabled }),
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

    internal sealed class FakePresence : IGlobalPresenceStore
    {
        private readonly bool _isOnline;
        private int _isOnlineCalls;

        public FakePresence(bool isOnline) => _isOnline = isOnline;

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
            IReadOnlyList<long> userIds, CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyDictionary<long, bool>>(
                userIds.ToDictionary(id => id, _ => _isOnline));

        public Task RunMaintenanceAsync(CancellationToken ct = default) => Task.CompletedTask;
    }
}
