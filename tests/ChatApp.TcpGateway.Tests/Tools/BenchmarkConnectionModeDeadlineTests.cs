using ChatApp.Performance.Orchestrator.Configuration;
using ChatApp.Performance.Orchestrator.Runtime;

namespace ChatApp.TcpGateway.Tests.Tools;

public sealed class BenchmarkConnectionModeDeadlineTests
{
    [Fact]
    public void ConnectionModeDeadlineCoversRampWarmupAndMeasurement()
    {
        var options = BenchmarkOptions.Parse(
        [
            "--tcp-mode", "connection",
            "--tcp-connections", "10000",
            "--tcp-connections-per-second", "1000",
            "--warmup-seconds", "60",
            "--duration-seconds", "120"
        ]);

        Assert.Equal(
            TimeSpan.FromSeconds(220),
            options.GetConnectionModeAuthenticationTimeout());

        var arguments = new List<string>();
        BenchmarkRunner.AddConnectionModeDeadlineArguments(arguments, options);

        Assert.Equal(
            [
                "--TcpGateway:AuthenticationTimeout=00:03:40",
                "--TcpGateway:IdleTimeout=00:04:10"
            ],
            arguments);
    }

    [Theory]
    [InlineData("heartbeat")]
    [InlineData("chat")]
    public void AuthenticatedModesKeepProductionAuthenticationDeadline(string mode)
    {
        var options = BenchmarkOptions.Parse(
        [
            "--tcp-mode", mode,
            "--tcp-token", "test-token-a",
            "--tcp-token", "test-token-b"
        ]);

        Assert.Null(options.GetConnectionModeAuthenticationTimeout());

        var arguments = new List<string>();
        BenchmarkRunner.AddConnectionModeDeadlineArguments(arguments, options);
        Assert.Empty(arguments);
    }
}
