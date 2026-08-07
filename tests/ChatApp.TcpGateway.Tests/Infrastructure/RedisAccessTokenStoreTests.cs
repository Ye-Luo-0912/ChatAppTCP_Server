using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using ChatApp.Auth.Contracts;
using ChatApp.TcpGateway.Infrastructure.Authentication;
using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Tests.Infrastructure;

public sealed class RedisAccessTokenStoreTests
{
    private const string Token = "abc";
    private const string ExpectedKey =
        "cache:AT:BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD";

    [Fact]
    public async Task FindAsync_ReadsCanonicalStringAndDeserializesCurrentServerPayload()
    {
        var expiresAtMs = DateTimeOffset.UtcNow.AddMinutes(5).ToUnixTimeMilliseconds();
        var database = CreateDatabase(
            $$"""
            {"u":42,"e":{{expiresAtMs}},"s":"session-42","d":123,"v":7,"a":1}
            """);
        var store = CreateStore(database);

        var record = await store.FindAsync(Token, CancellationToken.None);

        Assert.NotNull(record);
        Assert.Equal(42, record.UserId);
        Assert.Null(record.UserName);
        Assert.Equal("session-42", record.SessionId);
        Assert.Equal(123UL, record.DeviceIdHash);
        Assert.Equal(7, record.SecurityVersion);
        Assert.Equal(AccessTokenAccountState.DeletionPending, record.AccountState);
        Assert.Equal(ExpectedKey, database.LastStringGetKey.ToString());
        Assert.Equal(1, database.StringGetCalls);
    }

    [Fact]
    public async Task FindAsync_ExpiredCanonicalString_ReturnsNull()
    {
        var database = CreateDatabase(
            """
            {"u":42,"n":"legacy-name","e":0,"v":3,"a":0}
            """);
        var store = CreateStore(database);

        var record = await store.FindAsync(Token, CancellationToken.None);

        Assert.Null(record);
        Assert.Equal(ExpectedKey, database.LastStringGetKey.ToString());
        Assert.Equal(1, database.StringGetCalls);
    }

    [Fact]
    public async Task FindAsync_MalformedCanonicalString_FailsClosed()
    {
        var database = CreateDatabase("not-json");
        var store = CreateStore(database);

        var record = await store.FindAsync(Token, CancellationToken.None);

        Assert.Null(record);
        Assert.Equal(ExpectedKey, database.LastStringGetKey.ToString());
        Assert.Equal(1, database.StringGetCalls);
    }

    private static RedisAccessTokenStore CreateStore(RedisDatabaseStub database) =>
        new(
            () => database.Database,
            NullLogger<RedisAccessTokenStore>.Instance);

    private static RedisDatabaseStub CreateDatabase(string value)
    {
        var database = DispatchProxy.Create<IDatabase, RedisDatabaseProxy>();
        var proxy = (RedisDatabaseProxy)(object)database;
        proxy.StringValue = value;
        return new RedisDatabaseStub(database, proxy);
    }

    private sealed class RedisDatabaseStub(
        IDatabase database,
        RedisDatabaseProxy proxy)
    {
        public IDatabase Database { get; } = database;
        public RedisKey LastStringGetKey => proxy.LastStringGetKey;
        public int StringGetCalls => proxy.StringGetCalls;
    }

    [SuppressMessage(
        "Performance",
        "CA1852:Seal internal types",
        Justification = "DispatchProxy creates a runtime-derived implementation.")]
    private class RedisDatabaseProxy : DispatchProxy
    {
        public RedisValue StringValue { get; set; }
        public RedisKey LastStringGetKey { get; private set; }
        public int StringGetCalls { get; private set; }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            ArgumentNullException.ThrowIfNull(targetMethod);

            if (targetMethod.Name == nameof(IDatabase.StringGetAsync)
                && targetMethod.ReturnType == typeof(Task<RedisValue>)
                && args is [RedisKey key, CommandFlags])
            {
                LastStringGetKey = key;
                StringGetCalls++;
                return Task.FromResult(StringValue);
            }

            throw new NotSupportedException(
                $"Unexpected Redis operation: {targetMethod.Name}");
        }
    }
}
