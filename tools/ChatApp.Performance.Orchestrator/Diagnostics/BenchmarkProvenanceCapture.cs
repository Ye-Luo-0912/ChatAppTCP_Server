using System.Reflection;
using System.Security.Cryptography;
using ChatApp.Performance.Orchestrator.Runtime;

namespace ChatApp.Performance.Orchestrator.Diagnostics;

internal static class BenchmarkProvenanceCapture
{
    internal const string RequireSnapshotBindingEnvironment =
        "CHATAPP_BENCHMARK_REQUIRE_SNAPSHOT_BINDING";
    internal const string RunIdEnvironment = "CHATAPP_BENCHMARK_RUN_ID";
    internal const string RunRootEnvironment = "CHATAPP_BENCHMARK_RUN_ROOT";
    internal const string SourceArchivePathEnvironment =
        "CHATAPP_BENCHMARK_SOURCE_ARCHIVE_PATH";
    internal const string SourceArchiveSha256Environment =
        "CHATAPP_BENCHMARK_SOURCE_ARCHIVE_SHA256";
    internal const string CanonicalFeedArchivePathEnvironment =
        "CHATAPP_BENCHMARK_CANONICAL_FEED_ARCHIVE_PATH";
    internal const string CanonicalFeedArchiveSha256Environment =
        "CHATAPP_BENCHMARK_CANONICAL_FEED_ARCHIVE_SHA256";
    internal const string DotnetPathEnvironment = "CHATAPP_BENCHMARK_DOTNET_PATH";
    internal const string DotnetSha256Environment = "CHATAPP_BENCHMARK_DOTNET_SHA256";

    public static async Task<BenchmarkProvenance> CaptureAsync(
        string gatewayRepository,
        string realtimeRepository,
        CancellationToken cancellationToken)
    {
        var gateway = CaptureGitAsync(gatewayRepository, cancellationToken);
        var realtime = CaptureGitAsync(realtimeRepository, cancellationToken);
        await Task.WhenAll(gateway, realtime).ConfigureAwait(false);

        return new BenchmarkProvenance
        {
            OrchestratorVersion = typeof(BenchmarkProvenanceCapture).Assembly
                .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
                .InformationalVersion ?? "unknown",
            GatewayRepository = await gateway.ConfigureAwait(false),
            RealtimeRepository = await realtime.ConfigureAwait(false),
            SnapshotBinding = CaptureSnapshotBinding(
                Environment.GetEnvironmentVariable,
                gatewayRepository,
                realtimeRepository),
        };
    }

    internal static BenchmarkSnapshotBinding CaptureSnapshotBinding(
        Func<string, string?> getEnvironmentVariable,
        string? gatewayRepository = null,
        string? realtimeRepository = null)
    {
        ArgumentNullException.ThrowIfNull(getEnvironmentVariable);

        var required = ReadRequiredFlag(getEnvironmentVariable);
        var runId = ReadOptional(getEnvironmentVariable, RunIdEnvironment);
        var runRoot = ReadOptional(getEnvironmentVariable, RunRootEnvironment);
        var sourceArchivePath = ReadOptional(
            getEnvironmentVariable,
            SourceArchivePathEnvironment);
        var sourceArchiveSha256 = ReadSha256(
            getEnvironmentVariable,
            SourceArchiveSha256Environment);
        var canonicalFeedArchivePath = ReadOptional(
            getEnvironmentVariable,
            CanonicalFeedArchivePathEnvironment);
        var canonicalFeedArchiveSha256 = ReadSha256(
            getEnvironmentVariable,
            CanonicalFeedArchiveSha256Environment);
        var dotnetPath = ReadOptional(getEnvironmentVariable, DotnetPathEnvironment);
        var dotnetSha256 = ReadSha256(getEnvironmentVariable, DotnetSha256Environment);

        if (runId is not null &&
            (runId.Length > 128 || runId.Any(static character =>
                !(char.IsAsciiLetterOrDigit(character) || character is '-' or '_' or '.'))))
        {
            throw new InvalidOperationException(
                $"{RunIdEnvironment} must contain at most 128 ASCII letters, digits, '.', '_' or '-'.");
        }

        if (runRoot is not null &&
            (runRoot.Length > 1024 || runRoot.Any(char.IsControl)))
        {
            throw new InvalidOperationException(
                $"{RunRootEnvironment} must be a non-control path of at most 1024 characters.");
        }

        var complete = runId is not null
                       && runRoot is not null
                       && sourceArchivePath is not null
                       && sourceArchiveSha256 is not null
                       && canonicalFeedArchivePath is not null
                       && canonicalFeedArchiveSha256 is not null
                       && dotnetPath is not null
                       && dotnetSha256 is not null;
        if (required && !complete)
        {
            var missing = new List<string>();
            AddMissing(missing, runId, RunIdEnvironment);
            AddMissing(missing, runRoot, RunRootEnvironment);
            AddMissing(missing, sourceArchivePath, SourceArchivePathEnvironment);
            AddMissing(missing, sourceArchiveSha256, SourceArchiveSha256Environment);
            AddMissing(
                missing,
                canonicalFeedArchivePath,
                CanonicalFeedArchivePathEnvironment);
            AddMissing(
                missing,
                canonicalFeedArchiveSha256,
                CanonicalFeedArchiveSha256Environment);
            AddMissing(missing, dotnetPath, DotnetPathEnvironment);
            AddMissing(missing, dotnetSha256, DotnetSha256Environment);
            throw new InvalidOperationException(
                "Strict benchmark snapshot binding is enabled, but these environment " +
                $"variables are missing: {string.Join(", ", missing)}.");
        }

        string? normalizedRunRoot = null;
        if (runRoot is not null)
        {
            normalizedRunRoot = NormalizeAbsolutePath(runRoot, RunRootEnvironment);
            if (!Directory.Exists(normalizedRunRoot))
                throw new InvalidOperationException($"{RunRootEnvironment} does not exist.");
            if (!string.Equals(
                    runId,
                    Path.GetFileName(normalizedRunRoot.TrimEnd(
                        Path.DirectorySeparatorChar,
                        Path.AltDirectorySeparatorChar)),
                    PathComparison))
            {
                throw new InvalidOperationException(
                    $"{RunIdEnvironment} must equal the final directory name of {RunRootEnvironment}.");
            }
        }

        var sourceArchive = VerifyBoundFile(
            sourceArchivePath,
            sourceArchiveSha256,
            SourceArchivePathEnvironment,
            SourceArchiveSha256Environment);
        var canonicalFeedArchive = VerifyBoundFile(
            canonicalFeedArchivePath,
            canonicalFeedArchiveSha256,
            CanonicalFeedArchivePathEnvironment,
            CanonicalFeedArchiveSha256Environment);
        var dotnetExecutable = VerifyBoundFile(
            dotnetPath,
            dotnetSha256,
            DotnetPathEnvironment,
            DotnetSha256Environment);

        if (normalizedRunRoot is not null)
        {
            var archiveRoot = Path.Combine(normalizedRunRoot, "source-archives");
            EnsureDescendant(sourceArchive?.Path, archiveRoot, SourceArchivePathEnvironment);
            EnsureDescendant(
                canonicalFeedArchive?.Path,
                archiveRoot,
                CanonicalFeedArchivePathEnvironment);
            var sourceRoot = Path.Combine(normalizedRunRoot, "source");
            EnsureDescendant(gatewayRepository, sourceRoot, "gateway repository");
            EnsureDescendant(realtimeRepository, sourceRoot, "realtime repository");
        }

        return new BenchmarkSnapshotBinding
        {
            Required = required,
            Complete = complete,
            RunId = runId,
            RunRoot = normalizedRunRoot,
            SourceArchivePath = sourceArchive?.Path,
            SourceArchiveSha256 = sourceArchive?.Sha256,
            CanonicalFeedArchivePath = canonicalFeedArchive?.Path,
            CanonicalFeedArchiveSha256 = canonicalFeedArchive?.Sha256,
            DotnetExecutablePath = dotnetExecutable?.Path,
            DotnetExecutableSha256 = dotnetExecutable?.Sha256,
        };
    }

    private static string NormalizeAbsolutePath(string path, string name)
    {
        if (!Path.IsPathFullyQualified(path))
            throw new InvalidOperationException($"{name} must be an absolute path.");
        return Path.GetFullPath(path);
    }

    private static BoundFile? VerifyBoundFile(
        string? path,
        string? expectedSha256,
        string pathEnvironment,
        string shaEnvironment)
    {
        if (path is null && expectedSha256 is null)
            return null;
        if (path is null || expectedSha256 is null)
        {
            throw new InvalidOperationException(
                $"{pathEnvironment} and {shaEnvironment} must be supplied together.");
        }

        var normalizedPath = NormalizeAbsolutePath(path, pathEnvironment);
        if (!File.Exists(normalizedPath))
            throw new InvalidOperationException($"{pathEnvironment} does not exist or is not a file.");

        using var stream = File.OpenRead(normalizedPath);
        var actualSha256 = Convert.ToHexString(SHA256.HashData(stream));
        if (!string.Equals(actualSha256, expectedSha256, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"{shaEnvironment} does not match the file referenced by {pathEnvironment}.");
        }

        return new BoundFile(normalizedPath, actualSha256);
    }

    private static void EnsureDescendant(string? path, string root, string label)
    {
        if (path is null)
            return;
        var normalizedPath = NormalizeAbsolutePath(path, label);
        var normalizedRoot = Path.GetFullPath(root);
        var relative = Path.GetRelativePath(normalizedRoot, normalizedPath);
        if (Path.IsPathRooted(relative) ||
            relative.Equals("..", PathComparison) ||
            relative.StartsWith($"..{Path.DirectorySeparatorChar}", PathComparison) ||
            relative.StartsWith($"..{Path.AltDirectorySeparatorChar}", PathComparison))
        {
            throw new InvalidOperationException(
                $"{label} must be located below {normalizedRoot}.");
        }
    }

    private static StringComparison PathComparison => OperatingSystem.IsWindows()
        ? StringComparison.OrdinalIgnoreCase
        : StringComparison.Ordinal;

    private static bool ReadRequiredFlag(Func<string, string?> getEnvironmentVariable)
    {
        var value = ReadOptional(getEnvironmentVariable, RequireSnapshotBindingEnvironment);
        if (value is null)
            return false;
        if (value == "1")
            return true;
        if (value == "0")
            return false;
        if (bool.TryParse(value, out var parsed))
            return parsed;

        throw new InvalidOperationException(
            $"{RequireSnapshotBindingEnvironment} must be true, false, 1 or 0.");
    }

    private static string? ReadSha256(
        Func<string, string?> getEnvironmentVariable,
        string name)
    {
        var value = ReadOptional(getEnvironmentVariable, name);
        if (value is null)
            return null;
        if (value.Length != 64 || value.Any(static character => !Uri.IsHexDigit(character)))
            throw new InvalidOperationException($"{name} must be exactly 64 hexadecimal characters.");
        return value.ToUpperInvariant();
    }

    private static string? ReadOptional(
        Func<string, string?> getEnvironmentVariable,
        string name)
    {
        var value = getEnvironmentVariable(name)?.Trim();
        return string.IsNullOrWhiteSpace(value) ? null : value;
    }

    private static void AddMissing(List<string> missing, string? value, string name)
    {
        if (value is null)
            missing.Add(name);
    }

    private static async Task<GitRepositorySnapshot> CaptureGitAsync(
        string repository,
        CancellationToken cancellationToken)
    {
        try
        {
            var revision = await CommandRunner.RunAsync(
                    "git",
                    ["rev-parse", "HEAD"],
                    repository,
                    cancellationToken)
                .ConfigureAwait(false);
            var branch = await CommandRunner.RunAsync(
                    "git",
                    ["rev-parse", "--abbrev-ref", "HEAD"],
                    repository,
                    cancellationToken)
                .ConfigureAwait(false);
            var status = await CommandRunner.RunAsync(
                    "git",
                    ["status", "--porcelain", "--untracked-files=normal"],
                    repository,
                    cancellationToken)
                .ConfigureAwait(false);

            if (revision.ExitCode != 0 || branch.ExitCode != 0 || status.ExitCode != 0)
            {
                var error = string.Join(
                    " | ",
                    new[] { revision.StandardError, branch.StandardError, status.StandardError }
                        .Where(static value => !string.IsNullOrWhiteSpace(value))
                        .Select(static value => value.Trim()));
                return Unknown(error.Length == 0 ? "git returned a non-zero exit code" : error);
            }

            return new GitRepositorySnapshot
            {
                CommitSha = revision.StandardOutput.Trim(),
                Branch = branch.StandardOutput.Trim(),
                WorkingTreeDirty = !string.IsNullOrWhiteSpace(status.StandardOutput),
            };
        }
        catch (Exception exception)
            when (exception is InvalidOperationException
                  or System.ComponentModel.Win32Exception
                  or IOException)
        {
            return Unknown(exception.Message);
        }
    }

    private static GitRepositorySnapshot Unknown(string error) => new()
    {
        CommitSha = "unknown",
        Branch = "unknown",
        WorkingTreeDirty = true,
        CaptureError = error,
    };

    private sealed record BoundFile(string Path, string Sha256);
}
