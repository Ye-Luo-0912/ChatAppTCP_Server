using System.Net;
using System.Text.Json;
using System.Net.Sockets;
using ChatApp.Performance.Orchestrator.Configuration;
using ChatApp.Performance.Orchestrator.Diagnostics;

namespace ChatApp.Performance.Orchestrator.Runtime;

internal sealed class BenchmarkRunner(BenchmarkOptions options)
{
    public async Task<BenchmarkRunResult> RunAsync(CancellationToken ct)
    {
        var startedAt = DateTimeOffset.UtcNow;
        var sessionDirectory = CreateSessionDirectory(options.ReportDirectory, startedAt);
        var logDirectory = Path.Combine(sessionDirectory, "logs");
        var managedProcesses = new List<ManagedProcess>();
        var errors = new List<string>();
        var metricsBefore = new Dictionary<string, double>(StringComparer.Ordinal);
        var metricsAfter = new Dictionary<string, double>(StringComparer.Ordinal);
        var resourceSampler = new ResourceSampler();
        var prometheusTrendSampler = new PrometheusTrendSampler();
        TcpAuthenticationBootstrap? tcpAuthenticationBootstrap = null;
        IReadOnlyList<string> tcpTokens = options.TcpTokens;
        string? tcpTokenFilePath = null;
        using var samplerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task? samplerTask = null;
        Task? prometheusTrendTask = null;

        try
        {
            Console.WriteLine($"Benchmark session: {sessionDirectory}");
            if (options.BuildBeforeRun)
            {
                Console.WriteLine("Building benchmark targets...");
                await BuildTargetsAsync(ct).ConfigureAwait(false);
            }

            var binaries = ResolveBinaries();
            EnsurePortAvailable(options.RealtimePort);
            for (var index = 0; index < options.GatewayCount; index++)
                EnsurePortAvailable(options.GatewayBasePort + index);
Console.WriteLine("Starting RealtimeServices...");
            var realtime = StartRealtimeService(binaries.RealtimeService, logDirectory);
            managedProcesses.Add(realtime);
            resourceSampler.AddProcess(realtime.Label, realtime.Process);
            var realtimeBaseUri = new Uri($"http://127.0.0.1:{options.RealtimePort}");
            await EndpointProbe.WaitForHttpSuccessAsync(
                    new Uri(
                        realtimeBaseUri,
                        options.SmokeNoopStorage ? "/live" : "/ready"),
                    realtime,
                    options.StartupTimeout,
                    ct)
                .ConfigureAwait(false);
            if (options.SmokeNoopStorage)
            {
                await EndpointProbe.WaitForPrometheusValueAsync(
                        new Uri(realtimeBaseUri, "/metrics"),
                        "chatapp_nats_connection_connected",
                        expectedValue: 1,
                        realtime,
                        options.StartupTimeout,
                        ct)
                    .ConfigureAwait(false);
            }

            samplerTask = resourceSampler.RunAsync(
                options.SampleInterval,
                options.DockerContainers,
                samplerCts.Token);
            prometheusTrendTask = prometheusTrendSampler.RunAsync(
                new Uri(realtimeBaseUri, "/metrics"),
                options.SampleInterval,
                samplerCts.Token);

            if (options.TcpBootstrapAuthentication)
            {
                tcpAuthenticationBootstrap = await TcpAuthenticationBootstrap
                    .CreateAsync(
                        Environment.GetEnvironmentVariable(options.GarnetEnvironmentVariable!)!,
                        options.TcpBootstrapUserId,
                        ct)
                    .ConfigureAwait(false);
                tcpTokens = [tcpAuthenticationBootstrap.Token];
                Console.WriteLine(
                    $"Seeded temporary TCP benchmark authentication for user {options.TcpBootstrapUserId}; the token is not written to reports.");
            }

            Console.WriteLine($"Starting {options.GatewayCount} Gateway instance(s)...");
            var gateways = new List<ManagedProcess>(options.GatewayCount);
            for (var index = 0; index < options.GatewayCount; index++)
            {
                var gateway = StartGateway(
                    binaries.Gateway,
                    index,
                    logDirectory);
                managedProcesses.Add(gateway);
                gateways.Add(gateway);
                resourceSampler.AddProcess(gateway.Label, gateway.Process);
                await EndpointProbe.WaitForTcpAsync(
                        "127.0.0.1",
                        options.GatewayBasePort + index,
                        gateway,
                        options.StartupTimeout,
                        ct)
                    .ConfigureAwait(false);
            }

            if (options.Warmup > TimeSpan.Zero)
            {
                Console.WriteLine($"Service warmup: {options.Warmup.TotalSeconds:F0}s...");
                await Task.Delay(options.Warmup, ct).ConfigureAwait(false);
            }

            metricsBefore = await EndpointProbe.CapturePrometheusAsync(
                    new Uri(realtimeBaseUri, "/metrics"),
                    ct)
                .ConfigureAwait(false);

            Console.WriteLine("Starting load generators...");
            if (tcpTokens.Count > 0)
            {
                tcpTokenFilePath = WriteTcpTokenFile(sessionDirectory, tcpTokens);
                Console.WriteLine("Prepared temporary TCP benchmark credentials.");
            }

            var loads = new List<ManagedProcess>();
            if (options.PipelineEnabled)
            {
                var pipeline = StartPipelineLoad(
                    binaries.PipelineLoadGenerator,
                    sessionDirectory,
                    logDirectory);
                managedProcesses.Add(pipeline);
                loads.Add(pipeline);
                resourceSampler.AddProcess(pipeline.Label, pipeline.Process);
            }

            for (var index = 0; index < gateways.Count; index++)
            {
                var tcpLoad = StartTcpLoad(
                    binaries.TcpLoadGenerator,
                    index,
                    sessionDirectory,
                    logDirectory,
                    tcpTokens,
                    tcpTokenFilePath);
                managedProcesses.Add(tcpLoad);
                loads.Add(tcpLoad);
                resourceSampler.AddProcess(tcpLoad.Label, tcpLoad.Process);
            }

            using var loadTimeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            loadTimeout.CancelAfter(
                options.Duration + options.PipelineOperationTimeout + TimeSpan.FromSeconds(30));
            var exitCodes = await Task.WhenAll(
                    loads.Select(load => load.WaitForExitAsync(loadTimeout.Token)))
                .ConfigureAwait(false);
            for (var index = 0; index < loads.Count; index++)
            {
                if (exitCodes[index] != 0)
                    errors.Add($"{loads[index].Label} exited with code {exitCodes[index]}.");
            }

            foreach (var service in managedProcesses.Where(
                         static process => process.Kind is "gateway" or "realtime"))
            {
                if (service.HasExited)
                    errors.Add($"{service.Label} exited during the benchmark.");
            }

            metricsAfter = await EndpointProbe.CapturePrometheusAsync(
                    new Uri(realtimeBaseUri, "/metrics"),
                    ct)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            errors.Add("Benchmark was canceled.");
        }
        catch (Exception exception)
        {
            errors.Add(exception.ToString());
        }
        finally
        {
            samplerCts.Cancel();
            if (samplerTask is not null)
            {
                try
                {
                    await samplerTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (samplerCts.IsCancellationRequested)
                {
                }
                catch (Exception exception)
                {
                    errors.Add($"Resource sampler failed: {exception.Message}");
                }
            }
        }

        errors.AddRange(resourceSampler.Errors);
        if (prometheusTrendTask is not null)
        {
            try
            {
                await prometheusTrendTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (samplerCts.IsCancellationRequested)
            {
            }
            catch (Exception exception)
            {
                errors.Add($"Prometheus trend sampler failed: {exception.Message}");
            }
        }
        errors.AddRange(prometheusTrendSampler.Errors);
        var processResources = resourceSampler.GetProcessSummaries();
        var dockerResources = resourceSampler.GetDockerSummaries();
        var processResults = new List<BenchmarkProcessResult>(managedProcesses.Count);
        for (var index = managedProcesses.Count - 1; index >= 0; index--)
        {
            var process = managedProcesses[index];
            try
            {
                await process.StopAsync().ConfigureAwait(false);
                processResults.Add(process.CreateResult());
            }
            catch (Exception exception)
            {
                errors.Add($"Failed to stop {process.Label}: {exception.Message}");
            }
            finally
            {
                await process.DisposeAsync().ConfigureAwait(false);
            }
        }
        processResults.Reverse();
        if (tcpAuthenticationBootstrap is not null)
        {
            try
            {
                await tcpAuthenticationBootstrap.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                errors.Add($"Failed to remove temporary TCP benchmark authentication: {exception.Message}");
            }
        }
        if (tcpTokenFilePath is not null)
        {
            try
            {
                File.Delete(tcpTokenFilePath);
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
                errors.Add($"Failed to remove temporary TCP benchmark credentials: {exception.Message}");
            }
        }

        var artifacts = Directory.Exists(sessionDirectory)
            ? Directory.EnumerateFiles(sessionDirectory, "*.*", SearchOption.AllDirectories)
                .Where(static file =>
                    Path.GetExtension(file) is ".json" or ".md" &&
                    (Path.GetFileName(file).StartsWith("tcp-load-", StringComparison.Ordinal) ||
                     Path.GetFileName(file).StartsWith("pipeline-load-", StringComparison.Ordinal)))
                .Order(StringComparer.Ordinal)
                .Select(Path.GetFullPath)
                .ToArray()
            : [];
        var loadReadResult = ReadLoadResults(
            artifacts.Where(static path => Path.GetExtension(path) == ".json"));
        errors.AddRange(loadReadResult.Errors);
        var expectedLoadReports = options.GatewayCount + (options.PipelineEnabled ? 1 : 0);
        if (loadReadResult.Summaries.Count != expectedLoadReports)
        {
            errors.Add(
                $"Expected {expectedLoadReports} load JSON reports but found {loadReadResult.Summaries.Count}.");
        }
        var metricDeltas = metricsAfter.ToDictionary(
            static pair => pair.Key,
            pair => pair.Value - metricsBefore.GetValueOrDefault(pair.Key),
            StringComparer.Ordinal);
        var distinctErrors = errors
            .Where(static error => !string.IsNullOrWhiteSpace(error))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        var report = new BenchmarkReport
        {
            StartedAtUtc = startedAt,
            CompletedAtUtc = DateTimeOffset.UtcNow,
            Succeeded = distinctErrors.Length == 0,
            Configuration = BenchmarkConfiguration.Create(options),
            Environment = BenchmarkEnvironment.Create(),
            Processes = processResults,
            LoadResults = loadReadResult.Summaries,
            ProcessResources = processResources,
            DockerResources = dockerResources,
            MetricsBefore = metricsBefore,
            MetricsAfter = metricsAfter,
            MetricDeltas = metricDeltas,
            MetricTrends = prometheusTrendSampler.GetTrends(),
            Artifacts = artifacts,
            Errors = distinctErrors
        };
        return new BenchmarkRunResult(report, sessionDirectory);
    }

    private async Task BuildTargetsAsync(CancellationToken ct)
    {
        var projects = new List<string>
        {
            Path.Combine(options.RepositoryRoot, "ChatApp.TcpGateway.csproj"),
            Path.Combine(
                options.RealtimeRepositoryRoot,
                "ChatApp.RealtimeServices",
                "ChatApp.RealtimeServices.csproj"),
            Path.Combine(
                options.RepositoryRoot,
                "tools",
                "ChatApp.TcpGateway.LoadGenerator",
                "ChatApp.TcpGateway.LoadGenerator.csproj")
        };
        if (options.PipelineEnabled)
        {
            projects.Add(Path.Combine(
                options.RepositoryRoot,
                "tools",
                "ChatApp.Realtime.PipelineLoadGenerator",
                "ChatApp.Realtime.PipelineLoadGenerator.csproj"));
        }

        foreach (var project in projects)
        {
            Console.WriteLine($"  build {Path.GetFileNameWithoutExtension(project)}");
            await CommandRunner.EnsureSuccessAsync(
                    "dotnet",
                    ["build", project, "-c", options.BuildConfiguration],
                    options.RepositoryRoot,
                    ct)
                .ConfigureAwait(false);
        }
    }

    private BenchmarkBinaries ResolveBinaries() => new(
        FindAssembly(
            Path.Combine(options.RepositoryRoot, "ChatApp.TcpGateway.csproj"),
            "ChatApp.TcpGateway.dll"),
        FindAssembly(
            Path.Combine(
                options.RealtimeRepositoryRoot,
                "ChatApp.RealtimeServices",
                "ChatApp.RealtimeServices.csproj"),
            "ChatApp.RealtimeServices.dll"),
        FindAssembly(
            Path.Combine(
                options.RepositoryRoot,
                "tools",
                "ChatApp.TcpGateway.LoadGenerator",
                "ChatApp.TcpGateway.LoadGenerator.csproj"),
            "ChatApp.TcpGateway.LoadGenerator.dll"),
        options.PipelineEnabled
            ? FindAssembly(
                Path.Combine(
                    options.RepositoryRoot,
                    "tools",
                    "ChatApp.Realtime.PipelineLoadGenerator",
                    "ChatApp.Realtime.PipelineLoadGenerator.csproj"),
                "ChatApp.Realtime.PipelineLoadGenerator.dll")
            : null);

    private string FindAssembly(string projectPath, string assemblyName)
    {
        var binDirectory = Path.Combine(
            Path.GetDirectoryName(projectPath)!,
            "bin",
            options.BuildConfiguration);
        if (!Directory.Exists(binDirectory))
            throw new DirectoryNotFoundException($"Build output directory was not found: {binDirectory}");

        var assembly = Directory
            .EnumerateFiles(binDirectory, assemblyName, SearchOption.AllDirectories)
            .Where(static path =>
                !path.Contains($"{Path.DirectorySeparatorChar}ref{Path.DirectorySeparatorChar}", StringComparison.OrdinalIgnoreCase) &&
                !path.Contains($"{Path.DirectorySeparatorChar}publish{Path.DirectorySeparatorChar}", StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(File.GetLastWriteTimeUtc)
            .FirstOrDefault();
        return assembly is null
            ? throw new FileNotFoundException($"Assembly was not found under {binDirectory}: {assemblyName}")
            : Path.GetFullPath(assembly);
    }

    private ManagedProcess StartRealtimeService(string assemblyPath, string logDirectory)
    {
        var arguments = new List<string>
        {
            assemblyPath,
            "--environment=Development",
            $"--urls=http://127.0.0.1:{options.RealtimePort}",
            $"--Nats:Url={options.NatsUrl}",
            "--Nats:Mode=JetStream",
            $"--RealtimeIntegration:Replicas={options.JetStreamReplicas}",
            "--Observability:OtlpEnabled=false",
            "--Logging:LogLevel:Default=Warning"
        };
        if (options.SmokeNoopStorage)
        {
            arguments.Add("--RealtimeDatabase:MessageStoreProvider=Noop");
            arguments.Add("--RealtimeDatabase:InitializeSchemaOnStart=false");
            arguments.Add("--ConnectionStrings:RealtimeDatabase=");
            arguments.Add("--ConnectionStrings:DefaultConnection=");
            arguments.Add("--ConnectionStrings:Garnet=");
        }

        var environment = new Dictionary<string, string?>
        {
            ["DOTNET_ENVIRONMENT"] = "Development"
        };
        if (options.RealtimeDatabaseEnvironmentVariable is not null)
        {
            environment["ConnectionStrings__RealtimeDatabase"] =
                Environment.GetEnvironmentVariable(options.RealtimeDatabaseEnvironmentVariable);
        }
        if (options.GarnetEnvironmentVariable is not null)
        {
            environment["ConnectionStrings__Garnet"] =
                Environment.GetEnvironmentVariable(options.GarnetEnvironmentVariable);
        }

        return ManagedProcess.Start(
            "realtime-1",
            "realtime",
            "dotnet",
            arguments,
            Path.Combine(options.RealtimeRepositoryRoot, "ChatApp.RealtimeServices"),
            logDirectory,
            environment);
    }

    private ManagedProcess StartGateway(
        string assemblyPath,
        int index,
        string logDirectory)
    {
        var environment = new Dictionary<string, string?>
        {
            ["DOTNET_ENVIRONMENT"] = "Development",
            ["RealtimeIntegration__Url"] = options.NatsUrl,
            ["RealtimeIntegration__InstanceId"] = $"benchmark-gateway-{index + 1}"
        };
        if (options.GarnetEnvironmentVariable is not null)
        {
            environment["Redis__ConnectionString"] =
                Environment.GetEnvironmentVariable(options.GarnetEnvironmentVariable);
        }

        return ManagedProcess.Start(
            $"gateway-{index + 1}",
            "gateway",
            "dotnet",
            [
                assemblyPath,
                "--TcpGateway:ListenAddress=127.0.0.1",
                $"--TcpGateway:Port={options.GatewayBasePort + index}",
                "--Observability:OtlpEnabled=false",
                "--Logging:LogLevel:Default=Warning"
            ],
            options.RepositoryRoot,
            logDirectory,
            environment);
    }

    private ManagedProcess StartPipelineLoad(
        string? assemblyPath,
        string sessionDirectory,
        string logDirectory)
    {
        if (assemblyPath is null)
            throw new InvalidOperationException("Pipeline load generator assembly is unavailable.");
        var reportDirectory = Path.Combine(sessionDirectory, "pipeline");
        return ManagedProcess.Start(
            "pipeline-load",
            "load",
            "dotnet",
            [
                assemblyPath,
                "--nats-url", options.NatsUrl,
                "--warmup-seconds", "0",
                "--duration-seconds", FormatSeconds(options.Duration),
                "--concurrency", options.PipelineConcurrency.ToString(System.Globalization.CultureInfo.InvariantCulture),
                "--operations-per-second", options.PipelineOperationsPerSecond.ToString(System.Globalization.CultureInfo.InvariantCulture),
                "--payload-bytes", options.PipelinePayloadBytes.ToString(System.Globalization.CultureInfo.InvariantCulture),
                "--operation-timeout-seconds", FormatSeconds(options.PipelineOperationTimeout),
                "--base-user-id", options.PipelineBaseUserId.ToString(System.Globalization.CultureInfo.InvariantCulture),
                "--report-directory", reportDirectory
            ],
            options.RepositoryRoot,
            logDirectory);
    }

    private ManagedProcess StartTcpLoad(
        string assemblyPath,
        int gatewayIndex,
        string sessionDirectory,
        string logDirectory,
        IReadOnlyList<string> tcpTokens,
        string? tcpTokenFilePath)
    {
        var connections = Divide(options.TcpConnections, options.GatewayCount, gatewayIndex);
        var slowReaders = Divide(options.TcpSlowReaders, options.GatewayCount, gatewayIndex);
        var arguments = new List<string>
        {
            assemblyPath,
            "--mode", options.TcpMode,
            "--host", "127.0.0.1",
            "--port", (options.GatewayBasePort + gatewayIndex).ToString(System.Globalization.CultureInfo.InvariantCulture),
            "--connections", connections.ToString(System.Globalization.CultureInfo.InvariantCulture),
            "--duration-seconds", FormatSeconds(options.Duration),
            "--messages-per-second", options.TcpMessagesPerSecond.ToString(System.Globalization.CultureInfo.InvariantCulture),
            "--payload-bytes", options.TcpPayloadBytes.ToString(System.Globalization.CultureInfo.InvariantCulture),
            "--slow-readers", slowReaders.ToString(System.Globalization.CultureInfo.InvariantCulture),
            "--report-directory", Path.Combine(sessionDirectory, $"tcp-gateway-{gatewayIndex + 1}")
        };
        if (tcpTokenFilePath is not null)
        {
            arguments.Add("--token-file");
            arguments.Add(tcpTokenFilePath);
        }
        else
        {
            foreach (var token in tcpTokens)
            {
                arguments.Add("--token");
                arguments.Add(token);
            }
        }
        if (options.TcpTargetUserId is not null)
        {
            arguments.Add("--target-user-id");
            arguments.Add(options.TcpTargetUserId.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        return ManagedProcess.Start(
            $"tcp-load-{gatewayIndex + 1}",
            "load",
            "dotnet",
            arguments,
            options.RepositoryRoot,
            logDirectory);
    }

    private static string WriteTcpTokenFile(
        string sessionDirectory,
        IReadOnlyList<string> tcpTokens)
    {
        var path = Path.Combine(sessionDirectory, $".tcp-tokens-{Guid.NewGuid():N}");
        File.WriteAllLines(path, tcpTokens);
        if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            File.SetUnixFileMode(
                path,
                UnixFileMode.UserRead | UnixFileMode.UserWrite);
        }

        return path;
    }

    private static LoadSummaryReadResult ReadLoadResults(IEnumerable<string> jsonReports)
    {
        var summaries = new List<LoadResultSummary>();
        var readErrors = new List<string>();
        foreach (var reportPath in jsonReports)
        {
            try
            {
                using var document = JsonDocument.Parse(File.ReadAllText(reportPath));
                var root = document.RootElement;
                var fileName = Path.GetFileName(reportPath);
                if (fileName.StartsWith("pipeline-load-", StringComparison.Ordinal))
                {
                    var latency = root
                        .GetProperty("Latencies")
                        .GetProperty("complete_pipeline");
                    summaries.Add(new LoadResultSummary
                    {
                        Name = "pipeline",
                        Kind = "pipeline",
                        Succeeded = root.GetProperty("Succeeded").GetInt64(),
                        Failed = root.GetProperty("Failed").GetInt64(),
                        ErrorRatePercent = root.GetProperty("ErrorRatePercent").GetDouble(),
                        ThroughputPerSecond = root.GetProperty("CompletedPipelinesPerSecond").GetDouble(),
                        P50Milliseconds = latency.GetProperty("P50Ms").GetDouble(),
                        P95Milliseconds = latency.GetProperty("P95Ms").GetDouble(),
                        P99Milliseconds = latency.GetProperty("P99Ms").GetDouble(),
                        SourceReport = Path.GetFullPath(reportPath)
                    });
                    continue;
                }

                if (fileName.StartsWith("tcp-load-", StringComparison.Ordinal))
                {
                    var configuration = root.GetProperty("Configuration");
                    var latency = root.GetProperty("Latency");
                    var elapsed = root.GetProperty("ElapsedSeconds").GetDouble();
                    var mode = configuration.GetProperty("Mode").GetString() ?? "unknown";
                    var latencyCount = latency.GetProperty("Count").GetInt32();
                    var succeeded = root.GetProperty("SuccessfulConnections").GetInt32();
                    var failed = root.GetProperty("FailedConnections").GetInt32();
                    var throughput = mode.Equals("Chat", StringComparison.Ordinal)
                        ? root.GetProperty("SentPerSecond").GetDouble()
                        : latencyCount > 0 && elapsed > 0
                            ? latencyCount / elapsed
                            : elapsed > 0
                                ? succeeded / elapsed
                                : 0;
                    summaries.Add(new LoadResultSummary
                    {
                        Name = $"tcp:{configuration.GetProperty("Host").GetString()}:{configuration.GetProperty("Port").GetInt32()}",
                        Kind = $"tcp-{mode.ToLowerInvariant()}",
                        Succeeded = succeeded,
                        Failed = failed,
                        ErrorRatePercent = succeeded + failed == 0
                            ? 0
                            : failed * 100d / (succeeded + failed),
                        ThroughputPerSecond = throughput,
                        P50Milliseconds = latency.GetProperty("P50Ms").GetDouble(),
                        P95Milliseconds = latency.GetProperty("P95Ms").GetDouble(),
                        P99Milliseconds = latency.GetProperty("P99Ms").GetDouble(),
                        SourceReport = Path.GetFullPath(reportPath)
                    });
                }
            }
            catch (Exception exception)
                when (exception is IOException or JsonException or KeyNotFoundException)
            {
                readErrors.Add($"Failed to read child report {reportPath}: {exception.Message}");
            }
        }

        return new LoadSummaryReadResult(summaries, readErrors);
    }

    private static int Divide(int total, int partitions, int index) =>
        total / partitions + (index < total % partitions ? 1 : 0);

    private static string FormatSeconds(TimeSpan value) =>
        Math.Max(1, (int)Math.Ceiling(value.TotalSeconds))
            .ToString(System.Globalization.CultureInfo.InvariantCulture);

    private static void EnsurePortAvailable(int port)
    {
        var listener = new TcpListener(IPAddress.Loopback, port);
        try
        {
            listener.Start();
        }
        catch (SocketException exception)
        {
            throw new InvalidOperationException($"Port {port} is already in use.", exception);
        }
        finally
        {
            listener.Stop();
        }
    }

    private static string CreateSessionDirectory(
        string reportDirectory,
        DateTimeOffset startedAt)
    {
        var name = startedAt.ToString(
            "'benchmark-'yyyyMMdd-HHmmss'Z'",
            System.Globalization.CultureInfo.InvariantCulture);
        var path = Path.GetFullPath(Path.Combine(reportDirectory, name));
        if (Directory.Exists(path))
            path += $"-{Guid.NewGuid():N}"[..9];
        Directory.CreateDirectory(path);
        return path;
    }

    private sealed record LoadSummaryReadResult(
        IReadOnlyList<LoadResultSummary> Summaries,
        IReadOnlyList<string> Errors);

    private sealed record BenchmarkBinaries(
        string Gateway,
        string RealtimeService,
        string TcpLoadGenerator,
        string? PipelineLoadGenerator);
}

internal sealed record BenchmarkRunResult(
    BenchmarkReport Report,
    string SessionDirectory);
