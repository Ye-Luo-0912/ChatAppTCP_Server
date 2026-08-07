using System.Security.Cryptography;
using ChatApp.Performance.Orchestrator.Diagnostics;

namespace ChatApp.TcpGateway.Tests.Tools;

public sealed class BenchmarkProvenanceCaptureTests
{
    [Fact]
    public void StrictSnapshotBindingHashesAndCapturesEveryImmutableInput()
    {
        using var fixture = new SnapshotFixture();

        var binding = BenchmarkProvenanceCapture.CaptureSnapshotBinding(
            fixture.Values.GetValueOrDefault,
            fixture.GatewayRepository,
            fixture.RealtimeRepository);

        Assert.True(binding.Required);
        Assert.True(binding.Complete);
        Assert.Equal(fixture.RunId, binding.RunId);
        Assert.Equal(fixture.RunRoot, binding.RunRoot);
        Assert.Equal(fixture.SourceArchive, binding.SourceArchivePath);
        Assert.Equal(fixture.Values[BenchmarkProvenanceCapture.SourceArchiveSha256Environment],
            binding.SourceArchiveSha256);
        Assert.Equal(fixture.CanonicalFeedArchive, binding.CanonicalFeedArchivePath);
        Assert.Equal(fixture.DotnetExecutable, binding.DotnetExecutablePath);
        Assert.Equal(fixture.Values[BenchmarkProvenanceCapture.DotnetSha256Environment],
            binding.DotnetExecutableSha256);
    }

    [Fact]
    public void StrictSnapshotBindingRejectsMissingInputs()
    {
        using var fixture = new SnapshotFixture();
        fixture.Values.Remove(BenchmarkProvenanceCapture.SourceArchiveSha256Environment);

        var exception = Assert.Throws<InvalidOperationException>(() =>
            BenchmarkProvenanceCapture.CaptureSnapshotBinding(
                fixture.Values.GetValueOrDefault,
                fixture.GatewayRepository,
                fixture.RealtimeRepository));

        Assert.Contains(
            BenchmarkProvenanceCapture.SourceArchiveSha256Environment,
            exception.Message,
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("abc")]
    [InlineData("g123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")]
    [InlineData("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0")]
    public void SnapshotBindingRejectsInvalidSha256(string invalidSha)
    {
        using var fixture = new SnapshotFixture();
        fixture.Values[BenchmarkProvenanceCapture.SourceArchiveSha256Environment] = invalidSha;

        var exception = Assert.Throws<InvalidOperationException>(() =>
            BenchmarkProvenanceCapture.CaptureSnapshotBinding(
                fixture.Values.GetValueOrDefault,
                fixture.GatewayRepository,
                fixture.RealtimeRepository));

        Assert.Contains("64 hexadecimal", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void StrictSnapshotBindingRejectsDigestThatDoesNotMatchFile()
    {
        using var fixture = new SnapshotFixture();
        fixture.Values[BenchmarkProvenanceCapture.SourceArchiveSha256Environment] =
            new string('0', 64);

        var exception = Assert.Throws<InvalidOperationException>(() =>
            BenchmarkProvenanceCapture.CaptureSnapshotBinding(
                fixture.Values.GetValueOrDefault,
                fixture.GatewayRepository,
                fixture.RealtimeRepository));

        Assert.Contains("does not match", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void StrictSnapshotBindingRejectsRepositoryOutsideFrozenSource()
    {
        using var fixture = new SnapshotFixture();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            BenchmarkProvenanceCapture.CaptureSnapshotBinding(
                fixture.Values.GetValueOrDefault,
                fixture.DotnetDirectory,
                fixture.RealtimeRepository));

        Assert.Contains("gateway repository", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void OptionalSnapshotBindingMayBeAbsentForAdHocRuns()
    {
        var binding = BenchmarkProvenanceCapture.CaptureSnapshotBinding(static _ => null);

        Assert.False(binding.Required);
        Assert.False(binding.Complete);
        Assert.Null(binding.SourceArchiveSha256);
    }

    private sealed class SnapshotFixture : IDisposable
    {
        public SnapshotFixture()
        {
            RunId = $"codex-tcp-soak-{Guid.NewGuid():N}";
            RunRoot = Path.GetFullPath(Path.Combine(Path.GetTempPath(), RunId));
            var sourceRoot = Directory.CreateDirectory(Path.Combine(RunRoot, "source")).FullName;
            GatewayRepository = Directory.CreateDirectory(
                Path.Combine(sourceRoot, "ChatAppTCP_Server")).FullName;
            RealtimeRepository = Directory.CreateDirectory(
                Path.Combine(sourceRoot, "ChatApp.RealtimeServices")).FullName;
            var archiveRoot = Directory.CreateDirectory(
                Path.Combine(RunRoot, "source-archives")).FullName;
            SourceArchive = Path.Combine(archiveRoot, "source.tar.gz");
            CanonicalFeedArchive = Path.Combine(archiveRoot, "canonical-feed.tar.gz");
            File.WriteAllText(SourceArchive, "frozen source");
            File.WriteAllText(CanonicalFeedArchive, "frozen canonical feed");
            DotnetDirectory = Directory.CreateDirectory(Path.Combine(RunRoot, "sdk")).FullName;
            DotnetExecutable = Path.Combine(DotnetDirectory, "dotnet");
            File.WriteAllText(DotnetExecutable, "frozen dotnet host");

            Values = new Dictionary<string, string?>
            {
                [BenchmarkProvenanceCapture.RequireSnapshotBindingEnvironment] = "true",
                [BenchmarkProvenanceCapture.RunIdEnvironment] = RunId,
                [BenchmarkProvenanceCapture.RunRootEnvironment] = RunRoot,
                [BenchmarkProvenanceCapture.SourceArchivePathEnvironment] = SourceArchive,
                [BenchmarkProvenanceCapture.SourceArchiveSha256Environment] = Hash(SourceArchive),
                [BenchmarkProvenanceCapture.CanonicalFeedArchivePathEnvironment] =
                    CanonicalFeedArchive,
                [BenchmarkProvenanceCapture.CanonicalFeedArchiveSha256Environment] =
                    Hash(CanonicalFeedArchive),
                [BenchmarkProvenanceCapture.DotnetPathEnvironment] = DotnetExecutable,
                [BenchmarkProvenanceCapture.DotnetSha256Environment] = Hash(DotnetExecutable),
            };
        }

        public string RunId { get; }
        public string RunRoot { get; }
        public string GatewayRepository { get; }
        public string RealtimeRepository { get; }
        public string SourceArchive { get; }
        public string CanonicalFeedArchive { get; }
        public string DotnetDirectory { get; }
        public string DotnetExecutable { get; }
        public Dictionary<string, string?> Values { get; }

        public void Dispose() => Directory.Delete(RunRoot, recursive: true);

        private static string Hash(string path)
        {
            using var stream = File.OpenRead(path);
            return Convert.ToHexString(SHA256.HashData(stream));
        }
    }
}
