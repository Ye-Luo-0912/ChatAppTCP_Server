using ChatApp.Realtime.Integration.Push;
using ChatApp.TcpGateway.Core.Messaging.Push;
using ChatApp.TcpGateway.Core.Push;
using ChatApp.TcpGateway.Infrastructure.Push;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Tests.Infrastructure;

/// <summary>
/// <see cref="PushDispatcher"/> 单元测试（P3-Push Contract 基础编排）。
/// <para>
/// 验证：无令牌跳过、单平台投递、多平台并行、invalid_token 注销、Provider 异常容错。
/// </para>
/// </summary>
public sealed class PushDispatcherTests
{
    private static readonly ILogger<PushDispatcher> Logger =
        LoggerFactory.Create(b => b.AddDebug()).CreateLogger<PushDispatcher>();

    private static readonly ILogger<NoopPushProvider> NoopLogger =
        LoggerFactory.Create(b => b.AddDebug()).CreateLogger<NoopPushProvider>();

    [Fact]
    public async Task DispatchAsync_NoTokens_ReturnsSkipped()
    {
        var ct = TestContext.Current.CancellationToken;
        var tokenStore = new FakePushTokenStore();
        var dispatcher = new PushDispatcher(tokenStore, Array.Empty<IPushProvider>(), Logger);

        var result = await dispatcher.DispatchAsync(
            new PushDeliveryCommand
            {
                TargetUserId = 1001,
                Title = "t",
                Body = "b"
            }, ct);

        Assert.True(result.NoTokensRegistered);
        Assert.Equal(0, result.AttemptedCount);
    }

    [Fact]
    public async Task DispatchAsync_SinglePlatform_Succeeds()
    {
        var ct = TestContext.Current.CancellationToken;
        var tokenStore = new FakePushTokenStore
        {
            Tokens =
            [
                new() { Token = "fcm-token-1", Platform = PushPlatform.Fcm, DeviceIdHash = 0xAA }
            ]
        };
        var provider = new FakePushProvider(PushPlatform.Fcm);
        var dispatcher = new PushDispatcher(tokenStore, new[] { provider }, Logger);

        var result = await dispatcher.DispatchAsync(
            new PushDeliveryCommand
            {
                TargetUserId = 1001,
                Title = "Hello",
                Body = "World",
                ConversationId = "c1"
            }, ct);

        Assert.Equal(1, result.AttemptedCount);
        Assert.Equal(1, result.SucceededCount);
        Assert.Empty(result.InvalidTokenFingerprints);
        Assert.Single(provider.Sent);
        Assert.Equal("fcm-token-1", provider.Sent[0].Token);
        Assert.Equal("Hello", provider.Sent[0].Title);
    }

    [Fact]
    public async Task DispatchAsync_MultiPlatform_ParallelDispatch()
    {
        var ct = TestContext.Current.CancellationToken;
        var tokenStore = new FakePushTokenStore
        {
            Tokens =
            [
                new() { Token = "fcm-1", Platform = PushPlatform.Fcm, DeviceIdHash = 0x01 },
                new() { Token = "apns-1", Platform = PushPlatform.Apns, DeviceIdHash = 0x02 },
                new() { Token = "webpush-1", Platform = PushPlatform.WebPush, DeviceIdHash = 0x03 }
            ]
        };
        var fcmProvider = new FakePushProvider(PushPlatform.Fcm);
        var apnsProvider = new FakePushProvider(PushPlatform.Apns);
        var webProvider = new FakePushProvider(PushPlatform.WebPush);
        var dispatcher = new PushDispatcher(
            tokenStore, new[] { fcmProvider, apnsProvider, webProvider }, Logger);

        var result = await dispatcher.DispatchAsync(
            new PushDeliveryCommand
            {
                TargetUserId = 1001,
                Title = "t",
                Body = "b"
            }, ct);

        Assert.Equal(3, result.AttemptedCount);
        Assert.Equal(3, result.SucceededCount);
        Assert.Single(fcmProvider.Sent);
        Assert.Single(apnsProvider.Sent);
        Assert.Single(webProvider.Sent);
    }

    [Fact]
    public async Task DispatchAsync_InvalidToken_TriggersUnregister()
    {
        var ct = TestContext.Current.CancellationToken;
        var tokenStore = new FakePushTokenStore
        {
            Tokens =
            [
                new() { Token = "bad-token", Platform = PushPlatform.Fcm, DeviceIdHash = 0xAA },
                new() { Token = "good-token", Platform = PushPlatform.Fcm, DeviceIdHash = 0xBB }
            ]
        };
        // Provider 对 bad-token 返回 invalid_token，对 good-token 返回成功
        var provider = new FakePushProvider(PushPlatform.Fcm);
        provider.FailTokens["bad-token"] = ("invalid_token", null);
        var dispatcher = new PushDispatcher(tokenStore, new[] { provider }, Logger);

        var result = await dispatcher.DispatchAsync(
            new PushDeliveryCommand
            {
                TargetUserId = 1001,
                Title = "t",
                Body = "b"
            }, ct);

        Assert.Equal(2, result.AttemptedCount);
        Assert.Equal(1, result.SucceededCount);
        Assert.Single(result.InvalidTokenFingerprints);

        // 等待异步注销完成
        await Task.Delay(100, ct);
        Assert.Single(tokenStore.UnregisteredTokens);
        Assert.Equal("bad-token", tokenStore.UnregisteredTokens[0]);
    }

    [Fact]
    public async Task DispatchAsync_ProviderThrows_ReturnsUnknownError()
    {
        var ct = TestContext.Current.CancellationToken;
        var tokenStore = new FakePushTokenStore
        {
            Tokens =
            [
                new() { Token = "throw-token", Platform = PushPlatform.Fcm, DeviceIdHash = 0xAA }
            ]
        };
        var provider = new FakePushProvider(PushPlatform.Fcm);
        provider.ThrowOnToken = "throw-token";
        var dispatcher = new PushDispatcher(tokenStore, new[] { provider }, Logger);

        var result = await dispatcher.DispatchAsync(
            new PushDeliveryCommand
            {
                TargetUserId = 1001,
                Title = "t",
                Body = "b"
            }, ct);

        Assert.Equal(1, result.AttemptedCount);
        Assert.Equal(0, result.SucceededCount);
        Assert.Equal(0, result.RetryableFailureCount);
        // 异常不触发注销（不是 invalid_token）
        Assert.Empty(result.InvalidTokenFingerprints);
    }

    [Fact]
    public async Task DispatchAsync_NoProviderForPlatform_ReturnsProviderUnavailable()
    {
        var ct = TestContext.Current.CancellationToken;
        var tokenStore = new FakePushTokenStore
        {
            Tokens =
            [
                new() { Token = "fcm-1", Platform = PushPlatform.Fcm, DeviceIdHash = 0xAA }
            ]
        };
        // 不注册 Fcm Provider
        var dispatcher = new PushDispatcher(tokenStore, Array.Empty<IPushProvider>(), Logger);

        var result = await dispatcher.DispatchAsync(
            new PushDeliveryCommand
            {
                TargetUserId = 1001,
                Title = "t",
                Body = "b"
            }, ct);

        Assert.Equal(1, result.AttemptedCount);
        Assert.Equal(0, result.SucceededCount);
        // provider_unavailable 属于可重试失败（FromOutcomes 归入 retryable）
        Assert.Equal(1, result.RetryableFailureCount);
        // provider_unavailable 不是 invalid_token，不触发注销
        Assert.Empty(result.InvalidTokenFingerprints);
    }

    private sealed class FakePushTokenStore : IPushTokenStore
    {
        public IReadOnlyList<PushTokenRecord> Tokens { get; set; } = Array.Empty<PushTokenRecord>();
        public List<string> UnregisteredTokens { get; } = [];

        public ValueTask<int> RegisterAsync(
            long userId, ulong deviceIdHash, PushPlatform platform,
            string token, string? appDeviceLabel, CancellationToken cancellationToken)
            => ValueTask.FromResult(1);

        public ValueTask<int> UnregisterByDeviceAsync(
            long userId, ulong deviceIdHash, CancellationToken cancellationToken)
            => ValueTask.FromResult(0);

        public ValueTask<int> UnregisterByTokenAsync(
            long userId, string token, CancellationToken cancellationToken)
        {
            UnregisteredTokens.Add(token);
            return ValueTask.FromResult(0);
        }

        public ValueTask<IReadOnlyList<PushTokenRecord>> ListAsync(
            long userId, CancellationToken cancellationToken)
            => ValueTask.FromResult(Tokens);
    }

    private sealed class FakePushProvider : IPushProvider
    {
        private readonly PushPlatform _platform;
        public Dictionary<string, (string ErrorCode, TimeSpan? RetryAfter)> FailTokens { get; } = [];
        public string? ThrowOnToken { get; set; }
        public List<(string Token, string Title, string Body, string? CollapseKey)> Sent { get; } = [];

        public FakePushProvider(PushPlatform platform) => _platform = platform;

        public PushPlatform Platform => _platform;

        public Task<PushProviderResult> SendAsync(
            string token, string title, string body,
            string? collapseKey, IReadOnlyDictionary<string, string>? customData,
            CancellationToken cancellationToken = default)
        {
            if (ThrowOnToken == token)
                throw new InvalidOperationException("simulated provider crash");

            Sent.Add((token, title, body, collapseKey));

            if (FailTokens.TryGetValue(token, out var fail))
                return Task.FromResult(PushProviderResult.Fail(fail.ErrorCode, fail.RetryAfter));

            return Task.FromResult(PushProviderResult.Ok());
        }
    }
}
