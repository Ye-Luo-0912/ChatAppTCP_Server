using ChatApp.Performance.Orchestrator.Configuration;
using ChatApp.Performance.Orchestrator.Runtime;

namespace ChatApp.TcpGateway.Tests.Tools;

public sealed class BenchmarkConnectionModeDeadlineTests
{
    [Fact]
    public void ConnectionModeDeadlineCoversRampWarmupAndMeasurement()
    {
        using var roots = TestRepositoryRoots.Create();
        var args = new List<string>(roots.ValidationArguments)
        {
            "--tcp-mode", "connection",
            "--tcp-connections", "10000",
            "--tcp-connections-per-second", "1000",
            "--warmup-seconds", "60",
            "--duration-seconds", "120"
        };
        var options = BenchmarkOptions.Parse(
            args.ToArray());

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
        using var roots = TestRepositoryRoots.Create();
        var args = new List<string>(roots.ValidationArguments)
        {
            "--tcp-mode", mode,
            "--tcp-token", "test-token-a",
            "--tcp-token", "test-token-b"
        };
        var options = BenchmarkOptions.Parse(
            args.ToArray());

        Assert.Null(options.GetConnectionModeAuthenticationTimeout());

        var arguments = new List<string>();
        BenchmarkRunner.AddConnectionModeDeadlineArguments(arguments, options);
        Assert.Empty(arguments);
    }

    private sealed class TestRepositoryRoots : IDisposable
    {
        private readonly DirectoryInfo _root;

        private TestRepositoryRoots(DirectoryInfo root)
        {
            _root = root;
            GatewayRoot = root.FullName;
            RealtimeRoot = Directory.CreateDirectory(Path.Combine(root.FullName, "realtime")).FullName;
            File.WriteAllText(Path.Combine(GatewayRoot, "ChatApp.TcpGateway.csproj"), "<Project />");
            File.WriteAllText(Path.Combine(RealtimeRoot, "ChatApp.RealtimeServices.slnx"), "<Solution />");
        }

        public string GatewayRoot { get; }
        public string RealtimeRoot { get; }
        public IEnumerable<string> ValidationArguments =>
            ["--repository-root", GatewayRoot, "--realtime-root", RealtimeRoot];

        public static TestRepositoryRoots Create() =>
            new(Directory.CreateTempSubdirectory("chatapp-tcp-options-test-"));

        public void Dispose()
        {
            try
            {
                _root.Delete(recursive: true);
            }
            catch (IOException)
            {
                // Best effort cleanup on test hosts that still hold a file handle.
            }
        }
    }
}
