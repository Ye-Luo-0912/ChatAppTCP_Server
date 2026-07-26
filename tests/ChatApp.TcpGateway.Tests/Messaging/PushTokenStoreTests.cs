using ChatApp.TcpGateway.Core.Messaging.Push;
using ChatApp.TcpGateway.Infrastructure.Push;

namespace ChatApp.TcpGateway.Tests.Messaging;

public sealed class PushTokenStoreTests
{
    private static InMemoryPushTokenStore CreateStore() => new();

    [Fact]
    public async Task RegisterAsync_NewDevice_ReturnsOne()
    {
        var store = CreateStore();

        var count = await store.RegisterAsync(
            userId: 1001,
            deviceIdHash: 0xAA,
            platform: PushPlatform.Fcm,
            token: "fcm-token-aaa",
            appDeviceLabel: "pixel-8",
            CancellationToken.None);

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task RegisterAsync_SameDeviceOverwritesToken()
    {
        var store = CreateStore();

        await store.RegisterAsync(1001, 0xAA, PushPlatform.Fcm, "token-1", null, CancellationToken.None);
        var count = await store.RegisterAsync(1001, 0xAA, PushPlatform.Fcm, "token-2", null, CancellationToken.None);

        Assert.Equal(1, count);

        var tokens = await store.ListAsync(1001, CancellationToken.None);
        Assert.Single(tokens);
        Assert.Equal("token-2", tokens[0].Token);
    }

    [Fact]
    public async Task RegisterAsync_MultiDevice_AllStored()
    {
        var store = CreateStore();

        await store.RegisterAsync(1001, 0xAA, PushPlatform.Fcm, "fcm-1", null, CancellationToken.None);
        await store.RegisterAsync(1001, 0xBB, PushPlatform.Apns, "apns-1", null, CancellationToken.None);

        var tokens = await store.ListAsync(1001, CancellationToken.None);
        Assert.Equal(2, tokens.Count);
        Assert.Contains(tokens, t => t.Token == "fcm-1" && t.Platform == PushPlatform.Fcm);
        Assert.Contains(tokens, t => t.Token == "apns-1" && t.Platform == PushPlatform.Apns);
    }

    [Fact]
    public async Task RegisterAsync_ExceedsMax_EvictsOldest()
    {
        var store = CreateStore();

        // PushTokenLimits.MaxTokensPerUser = 8
        for (ulong i = 1; i <= 8; i++)
        {
            await store.RegisterAsync(
                1001,
                i,
                PushPlatform.Fcm,
                $"token-{i}",
                null,
                CancellationToken.None);
        }

        // 第 9 个设备注册应淘汰 deviceIdHash=1 的最旧 token。
        await store.RegisterAsync(
            1001,
            9,
            PushPlatform.Fcm,
            "token-9",
            null,
            CancellationToken.None);

        var tokens = await store.ListAsync(1001, CancellationToken.None);
        Assert.Equal(8, tokens.Count);
        Assert.DoesNotContain(tokens, t => t.Token == "token-1");
        Assert.Contains(tokens, t => t.Token == "token-9");
    }

    [Fact]
    public async Task UnregisterByDeviceAsync_RemovesOnlyThatDevice()
    {
        var store = CreateStore();

        await store.RegisterAsync(1001, 0xAA, PushPlatform.Fcm, "fcm-1", null, CancellationToken.None);
        await store.RegisterAsync(1001, 0xBB, PushPlatform.Apns, "apns-1", null, CancellationToken.None);

        var remaining = await store.UnregisterByDeviceAsync(1001, 0xAA, CancellationToken.None);

        Assert.Equal(1, remaining);
        var tokens = await store.ListAsync(1001, CancellationToken.None);
        Assert.Single(tokens);
        Assert.Equal("apns-1", tokens[0].Token);
    }

    [Fact]
    public async Task UnregisterByTokenAsync_RemovesMatchingAcrossDevices()
    {
        var store = CreateStore();

        // 同一 token 字符串注册在两台设备上（罕见但理论可能）。
        await store.RegisterAsync(1001, 0xAA, PushPlatform.Fcm, "shared-token", null, CancellationToken.None);
        await store.RegisterAsync(1001, 0xBB, PushPlatform.Fcm, "shared-token", null, CancellationToken.None);
        await store.RegisterAsync(1001, 0xCC, PushPlatform.Fcm, "other-token", null, CancellationToken.None);

        var remaining = await store.UnregisterByTokenAsync(1001, "shared-token", CancellationToken.None);

        Assert.Equal(1, remaining);
        var tokens = await store.ListAsync(1001, CancellationToken.None);
        Assert.Single(tokens);
        Assert.Equal("other-token", tokens[0].Token);
    }

    [Fact]
    public async Task ListAsync_Empty_WhenNoTokens()
    {
        var store = CreateStore();

        var tokens = await store.ListAsync(9999, CancellationToken.None);

        Assert.Empty(tokens);
    }

    [Fact]
    public async Task UnregisterByDeviceAsync_OnNonExistentUser_ReturnsZero()
    {
        var store = CreateStore();

        var remaining = await store.UnregisterByDeviceAsync(9999, 0xAA, CancellationToken.None);

        Assert.Equal(0, remaining);
    }

    [Fact]
    public async Task RegisterAsync_PreservesAppDeviceLabel()
    {
        var store = CreateStore();

        await store.RegisterAsync(
            1001,
            0xAA,
            PushPlatform.Fcm,
            "fcm-1",
            "pixel-8-pro",
            CancellationToken.None);

        var tokens = await store.ListAsync(1001, CancellationToken.None);
        Assert.Single(tokens);
        Assert.Equal("pixel-8-pro", tokens[0].AppDeviceLabel);
    }
}
