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