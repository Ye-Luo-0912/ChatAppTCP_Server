using ChatApp.Realtime.Integration.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace ChatApp.TcpGateway.Tests.Configuration;

public sealed class RealtimeIntegrationOptionsTests
{
    [Fact]
    public void CommandLineUrlOverridesTheDefaultNatsAddress()
    {
        var configuration = new ConfigurationBuilder()
            .AddCommandLine(["--RealtimeIntegration:Url=nats://127.0.0.1:34222"])
            .Build();

        var options = configuration
            .GetRequiredSection("RealtimeIntegration")
            .Get<RealtimeIntegrationOptions>();

        Assert.NotNull(options);
        Assert.Equal("nats://127.0.0.1:34222", options.Url);
    }

    [Fact]
    public void HostApplicationBuilderUsesTheCommandLineNatsAddress()
    {
        var builder = Host.CreateApplicationBuilder(
            ["--RealtimeIntegration:Url=nats://127.0.0.1:34222"]);

        var options = builder.Configuration
            .GetRequiredSection("RealtimeIntegration")
            .Get<RealtimeIntegrationOptions>();

        Assert.NotNull(options);
        Assert.Equal("nats://127.0.0.1:34222", options.Url);
    }

    [Fact]
    public void HostApplicationBuilderUsesTheEnvironmentNatsAddress()
    {
        const string key = "RealtimeIntegration__Url";
        var original = Environment.GetEnvironmentVariable(key);
        try
        {
            Environment.SetEnvironmentVariable(key, "nats://127.0.0.1:34222");
            var builder = Host.CreateApplicationBuilder([]);
            var options = builder.Configuration
                .GetRequiredSection("RealtimeIntegration")
                .Get<RealtimeIntegrationOptions>();

            Assert.NotNull(options);
            Assert.Equal("nats://127.0.0.1:34222", options.Url);
        }
        finally
        {
            Environment.SetEnvironmentVariable(key, original);
        }
    }

    [Fact]
    public void HostApplicationBuilderLoadsGatewayAppSettingsFromTheContentRoot()
    {
        var repositoryRoot = FindRepositoryRoot(AppContext.BaseDirectory);
        var builder = Host.CreateApplicationBuilder(
            new HostApplicationBuilderSettings { ContentRootPath = repositoryRoot });

        var options = builder.Configuration
            .GetRequiredSection("RealtimeIntegration")
            .Get<RealtimeIntegrationOptions>();

        Assert.NotNull(options);
        Assert.Equal("chatapp-tcp-gateway", options.ClientName);
    }

    // ---- 第三阶段：分片路由配置 ----

    [Fact]
    public void RoutingMode_DefaultsToBroadcast_ForBackwardCompatibility()
    {
        var options = new RealtimeIntegrationOptions();

        Assert.Equal(EventRoutingMode.Broadcast, options.RoutingMode);
    }

    [Fact]
    public void ShardedSubjectPatterns_HaveExpectedDefaults()
    {
        var options = new RealtimeIntegrationOptions();

        Assert.Equal("chat.realtime-events.{0}", options.RealtimeEventsShardSubjectPattern);
        Assert.Equal("chat.ephemeral.typing.{0}", options.EphemeralTypingShardSubjectPattern);
        Assert.Equal("chat.ephemeral.presence.{0}", options.EphemeralPresenceShardSubjectPattern);
    }

    [Fact]
    public void RoutingMode_CanBeOverriddenToSharded_ViaConfiguration()
    {
        var configuration = new ConfigurationBuilder()
            .AddCommandLine(["--RealtimeIntegration:RoutingMode=Sharded"])
            .Build();

        var options = configuration
            .GetRequiredSection("RealtimeIntegration")
            .Get<RealtimeIntegrationOptions>();

        Assert.NotNull(options);
        Assert.Equal(EventRoutingMode.Sharded, options.RoutingMode);
    }

    [Fact]
    public void ShardedSubjectPatterns_CanBeOverridden_ViaConfiguration()
    {
        var configuration = new ConfigurationBuilder()
            .AddCommandLine([
                "--RealtimeIntegration:RealtimeEventsShardSubjectPattern=chat.rt.{0}",
                "--RealtimeIntegration:EphemeralTypingShardSubjectPattern=chat.tp.{0}"
            ])
            .Build();

        var options = configuration
            .GetRequiredSection("RealtimeIntegration")
            .Get<RealtimeIntegrationOptions>();

        Assert.NotNull(options);
        Assert.Equal("chat.rt.{0}", options.RealtimeEventsShardSubjectPattern);
        Assert.Equal("chat.tp.{0}", options.EphemeralTypingShardSubjectPattern);
    }

    private static string FindRepositoryRoot(string startDirectory)
    {
        var current = new DirectoryInfo(startDirectory);
        while (current is not null)
        {
            if (File.Exists(Path.Combine(current.FullName, "ChatApp.TcpGateway.csproj")))
                return current.FullName;
            current = current.Parent;
        }

        throw new DirectoryNotFoundException("Gateway repository root was not found.");
    }
}