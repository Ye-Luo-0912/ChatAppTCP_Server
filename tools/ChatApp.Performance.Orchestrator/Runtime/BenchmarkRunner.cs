using System.Diagnostics;
using System.Net;
using System.Text.Json;
using System.Net.Sockets;
using ChatApp.Performance.Orchestrator.Configuration;
using ChatApp.Performance.Orchestrator.Diagnostics;

namespace ChatApp.Performance.Orchestrator.Runtime;

internal sealed class BenchmarkRunner(BenchmarkOptions options)
{
    private string _dotnetExecutable = "dotnet";

    public async Task<BenchmarkRunResult> RunAsync(CancellationToken ct)
    {
        var startedAt = DateTimeOffset.UtcNow;
        var provenance = await BenchmarkProvenanceCapture.CaptureAsync(
                options.RepositoryRoot,
                options.RealtimeRepositoryRoot,
                CancellationToken.None)
            .ConfigureAwait(false);
        _dotnetExecutable = provenance.SnapshotBinding.DotnetExecutablePath ?? "dotnet";
        var sessionDirectory = CreateSessionDirectory(options.ReportDirectory, startedAt);
        var logDirectory = Path.Combine(sessionDirectory, "logs");
        var managedProcesses = new List<ManagedProcess>();
        var errors = new List<string>();
        var metricsBefore = new Dictionary<string, double>(StringComparer.Ordinal);
        var metricsAfter = new Dictionary<string, double>(StringComparer.Ordinal);
        var resourceSampler = new ResourceSampler();
        var prometheusTrendSampler = new PrometheusTrendSampler();
        TcpAuthenticationBootstrap? tcpAuthenticationBootstrap = null;
        PostgresIdentityBootstrap? postgresIdentityBootstrap = null;
        string[][] tcpTokenPartitions =
            Enumerable.Range(0, options.GatewayCount)
                .Select(_ => options.TcpTokens.ToArray())
                .ToArray();
        var tcpTokenFilePaths = new string?[options.GatewayCount];
        var tcpTargetRingFilePaths = new string?[options.GatewayCount];
        TimeSpan? bootstrapTokenLifetime = null;
        DateTimeOffset? loadStartedAtUtc = null;
        DateTimeOffset? measurementStartedAtUtc = null;
        DateTimeOffset? measurementCompletedAtUtc = null;
        long measurementStartedTimestamp = 0;
        long measurementCompletedTimestamp = 0;
        var expectedLoadProcesses = options.GatewayCount + (options.PipelineEnabled ? 1 : 0);
        var completedLoadProcesses = 0;
        var servicesAliveThroughMeasurement = true;
        var prometheusPollsAtMeasurementStart = 0;
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
                bootstrapTokenLifetime = options.GetBootstrapTokenLifetime();
                tcpAuthenticationBootstrap = await TcpAuthenticationBootstrap
                    .CreateAsync(
                        Environment.GetEnvironmentVariable(options.GarnetEnvironmentVariable!)!,
                        options.TcpBootstrapUserId,
                        options.GetTcpBootstrapIdentityCount(),
                        bootstrapTokenLifetime.Value,
                        ct)
                    .ConfigureAwait(false);
                var identityPlan = TcpBootstrapIdentityPlanner.Create(
                    tcpAuthenticationBootstrap.Identities,
                    Enumerable.Range(0, options.GatewayCount)
                        .Select(index => new TcpBootstrapPartitionShape(
                            options.GetTcpConnections(index),
                            options.GetTcpSlowReaders(index),
                            options.GetTcpActiveSenders(index)))
                        .ToArray());
                tcpTokenPartitions = identityPlan.ConnectionPartitions
                    .Select(static partition =>
                        partition.Select(static identity => identity.Token).ToArray())
                    .ToArray();
                if (options.TcpMode == "chat")
                {
                    if (options.TcpCrossGateway)
                    {
                        var crossGatewayPlan = BuildCrossGatewayPlan(
                            identityPlan.ConnectionPartitions);
                        for (var gatewayIndex = 0;
                             gatewayIndex < crossGatewayPlan.TargetRings.Count;
                             gatewayIndex++)
                        {
                            tcpTargetRingFilePaths[gatewayIndex] = WriteTargetRingFile(
                                sessionDirectory,
                                gatewayIndex,
                                crossGatewayPlan.TargetRings[gatewayIndex]);
                        }

                        postgresIdentityBootstrap =
                            await PostgresIdentityBootstrap.CreateCrossGatewayAsync(
                                    Environment.GetEnvironmentVariable(
                                        options.RealtimeDatabaseEnvironmentVariable!)!,
                                    identityPlan.HealthyPartitions
                                        .SelectMany(static partition =>
                                            partition.Select(static identity => identity.UserId))
                                        .ToArray(),
                                    crossGatewayPlan.FriendshipEdges,
                                    ct)
                                .ConfigureAwait(false);
                    }
                    else
                    {
                        postgresIdentityBootstrap = await PostgresIdentityBootstrap.CreateAsync(
                                Environment.GetEnvironmentVariable(options.RealtimeDatabaseEnvironmentVariable!)!,
                                identityPlan.HealthyPartitions
                                    .Select(static partition =>
                                        (IReadOnlyList<long>)partition.Select(static identity => identity.UserId).ToArray())
                                    .ToArray(),
                                ct)
                            .ConfigureAwait(false);
                    }
                }
                var bootstrapIdentityLayout = options.TcpSlowReaders == 0
                    ? "one unique identity per connection"
                    : $"{options.TcpSlowReaders} slow readers reuse actively targeted healthy identities";
                Console.WriteLine(
                    $"Seeded {tcpAuthenticationBootstrap.Identities.Count} temporary healthy TCP benchmark users " +
                    $"for {options.TcpConnections} connections with a " +
                    $"{bootstrapTokenLifetime.Value.TotalMinutes:F0}-minute token lifetime; " +
                    $"{bootstrapIdentityLayout}; tokens are not written to reports.");
                if (options.TcpCrossGateway)
                {
                    Console.WriteLine(
                        "Cross-gateway pairing: each Gateway's connection targets the " +
                        "counterpart connection on the next Gateway, so every chat message " +
                        "crosses the Gateway boundary through JetStream.");
                }
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

            Console.WriteLine("Starting load generators...");
            for (var index = 0; index < tcpTokenPartitions.Length; index++)
            {
                if (tcpTokenPartitions[index].Length == 0)
                    continue;
                tcpTokenFilePaths[index] = WriteTcpTokenFile(
                    sessionDirectory,
                    index,
                    tcpTokenPartitions[index]);
            }
            if (tcpTokenFilePaths.Any(static path => path is not null))
                Console.WriteLine("Prepared partitioned temporary TCP benchmark credentials.");

            var loads = new List<ManagedProcess>();
            var expectedLoadRuntimes = new List<TimeSpan>();
            if (options.PipelineEnabled)
            {
                var pipeline = StartPipelineLoad(
                    binaries.PipelineLoadGenerator,
                    sessionDirectory,
                    logDirectory);
                managedProcesses.Add(pipeline);
                loads.Add(pipeline);
                expectedLoadRuntimes.Add(
                    options.GetEstimatedTcpRamp() + options.Warmup + options.Duration);
                resourceSampler.AddProcess(pipeline.Label, pipeline.Process);
            }

            for (var index = 0; index < gateways.Count; index++)
            {
                var tcpLoad = StartTcpLoad(
                    binaries.TcpLoadGenerator,
                    index,
                    sessionDirectory,
                    logDirectory,
                    tcpTokenPartitions[index],
                    tcpTokenFilePaths[index],
                    tcpTargetRingFilePaths[index]);
                managedProcesses.Add(tcpLoad);
                loads.Add(tcpLoad);
                expectedLoadRuntimes.Add(GetEstimatedTcpRamp(index) + options.Warmup + options.Duration);
                resourceSampler.AddProcess(tcpLoad.Label, tcpLoad.Process);
            }

            loadStartedAtUtc = DateTimeOffset.UtcNow;
            var loadStartedTimestamp = Stopwatch.GetTimestamp();
            using var loadTimeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            loadTimeout.CancelAfter(BenchmarkTiming.CalculateLoadTimeout(
                options.GetEstimatedTcpRamp(),
                options.Warmup,
                options.Duration,
                options.TcpMode == "chat",
                options.TcpDeliveryDrain,
                options.PipelineEnabled,
                options.PipelineOperationTimeout));
            var exitTasks = loads
                .Select((load, index) => ObserveLoadExitAsync(
                    load,
                    loadStartedTimestamp,
                    expectedLoadRuntimes[index],
                    loadTimeout.Token))
                .ToArray();

            var monitoredServices = gateways.Prepend(realtime).ToArray();
            await WaitForMeasurementStartAsync(
                    loads,
                    monitoredServices,
                    options.GetEstimatedTcpRamp() + options.Warmup,
                    loadTimeout.Token)
                .ConfigureAwait(false);
            measurementStartedAtUtc = DateTimeOffset.UtcNow;
            measurementStartedTimestamp = Stopwatch.GetTimestamp();
            prometheusPollsAtMeasurementStart = prometheusTrendSampler.SuccessfulPolls;
            metricsBefore = await EndpointProbe.CapturePrometheusAsync(
                    new Uri(realtimeBaseUri, "/metrics"),
                    ct)
                .ConfigureAwait(false);

            var monitoring = await LoadMonitoringCoordinator.WaitForCompletionAsync(
                    exitTasks,
                    () => monitoredServices
                        .FirstOrDefault(static process => process.HasExited)
                        ?.Label,
                    TimeSpan.FromMilliseconds(250),
                    loadTimeout.Token)
                .ConfigureAwait(false);
            servicesAliveThroughMeasurement = monitoring.ServicesAlive;
            if (!monitoring.ServicesAlive)
                errors.Add("A Gateway or Realtime service exited during the measurement window.");

            foreach (var observation in monitoring.Loads)
            {
                completedLoadProcesses++;
                var observationFailure =
                    LoadMonitoringCoordinator.GetFailureReason(observation);
                if (observationFailure is not null)
                    errors.Add(observationFailure);
            }

            if (monitoring.FailFastReason is not null)
            {
                errors.Add(monitoring.FailFastReason);
                samplerCts.Cancel();
                await StopLoadsAfterFailFastAsync(loads, errors)
                    .ConfigureAwait(false);
            }

            if (monitoredServices.Any(static process => process.HasExited))
            {
                servicesAliveThroughMeasurement = false;
                errors.Add(
                    "A Gateway or Realtime service exited during the measurement window.");
            }

            measurementCompletedAtUtc = DateTimeOffset.UtcNow;
            measurementCompletedTimestamp = Stopwatch.GetTimestamp();
            if (!realtime.HasExited)
            {
                metricsAfter = await EndpointProbe.CapturePrometheusAsync(
                        new Uri(realtimeBaseUri, "/metrics"),
                        ct)
                    .ConfigureAwait(false);
            }
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
        var processTimelines = resourceSampler.GetProcessTimelines();
        var dockerResources = resourceSampler.GetDockerSummaries();
        var dockerTimelines = resourceSampler.GetDockerTimelines();
        var processResults = new List<BenchmarkProcessResult>(managedProcesses.Count);
        var processResourceByLabel = processResources.ToDictionary(
            static resource => resource.Label,
            static resource => resource,
            StringComparer.Ordinal);
        for (var index = managedProcesses.Count - 1; index >= 0; index--)
        {
            var process = managedProcesses[index];
            try
            {
                await process.StopAsync().ConfigureAwait(false);
                // item 八：把同进程采集到的 cgroup oom/oom_kill 计数传给退出归因。
                var resource = processResourceByLabel.GetValueOrDefault(process.Label);
                processResults.Add(process.CreateResult(
                    resource?.CgroupOomEvents ?? 0,
                    resource?.CgroupOomKillEvents ?? 0));
            }
            catch (Exception exception)
            {
                errors.Add($"Failed to stop {process.Label}: {exception.Message}");
            }
            finally
            {
                try
                {
                    await process.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    errors.Add($"Failed to dispose {process.Label}: {exception.Message}");
                }
            }
        }
        processResults.Reverse();
        if (postgresIdentityBootstrap is not null)
        {
            try
            {
                await postgresIdentityBootstrap.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                errors.Add($"Failed to remove temporary PostgreSQL benchmark identities: {exception.Message}");
            }
        }
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
        foreach (var tcpTokenFilePath in tcpTokenFilePaths.Where(static path => path is not null))
        {
            try
            {
                File.Delete(tcpTokenFilePath!);
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
                errors.Add($"Failed to remove temporary TCP benchmark credentials: {exception.Message}");
            }
        }
        foreach (var tcpTargetRingFilePath in tcpTargetRingFilePaths.Where(static path => path is not null))
        {
            try
            {
                File.Delete(tcpTargetRingFilePath!);
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
                errors.Add($"Failed to remove temporary TCP benchmark target ring: {exception.Message}");
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
        if (options.TcpCrossGateway)
        {
            var crossGatewayLoads = loadReadResult.Summaries
                .Where(static summary => summary.Kind == "tcp-chat")
                .ToArray();
            var sent = crossGatewayLoads.Sum(static summary => summary.MessagesSent);
            var received = crossGatewayLoads.Sum(static summary => summary.MessagesReceived);
            var deliveryRatio = sent == 0 ? 0d : received / (double)sent;
            if (deliveryRatio < options.TcpMinimumDeliveryRatio)
            {
                errors.Add(
                    $"Cross-Gateway delivery ratio {deliveryRatio:F6} is below " +
                    $"the required {options.TcpMinimumDeliveryRatio:F6} " +
                    $"({received}/{sent}).");
            }
            else if (received > sent)
            {
                errors.Add(
                    $"Cross-Gateway receivers observed more unique deliveries than " +
                    $"messages sent ({received}/{sent}).");
            }
        }
        var expectedLoadReports = options.GatewayCount + (options.PipelineEnabled ? 1 : 0);
        if (loadReadResult.Summaries.Count != expectedLoadReports)
        {
            errors.Add(
                $"Expected {expectedLoadReports} load JSON reports but found {loadReadResult.Summaries.Count}.");
        }
        var validity = CreateValidity(
            loadStartedAtUtc,
            measurementStartedAtUtc,
            measurementCompletedAtUtc,
            measurementStartedTimestamp,
            measurementCompletedTimestamp,
            expectedLoadProcesses,
            completedLoadProcesses,
            servicesAliveThroughMeasurement,
            processTimelines,
            dockerTimelines,
            prometheusPollsAtMeasurementStart,
            prometheusTrendSampler.SuccessfulPolls,
            loadReadResult.Summaries);
        errors.AddRange(validity.InvalidReasons);
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
            Validity = validity,
            Configuration = BenchmarkConfiguration.Create(
                options,
                tcpAuthenticationBootstrap?.Identities.Count ?? options.TcpTokens.Count,
                bootstrapTokenLifetime),
            Environment = BenchmarkEnvironment.Create(),
            Provenance = provenance,
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
        return new BenchmarkRunResult(report, processTimelines, sessionDirectory);
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
                    _dotnetExecutable,
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
            $"--Realtime:ProcessingConcurrency={options.RealtimeProcessingConcurrency}",
            $"--Realtime:ProcessingQueueCapacity={options.GetRealtimeProcessingQueueCapacity()}",
            $"--Nats:JetStream:Consumer:PrefetchMaxMsgs={options.GetRealtimePrefetchMaxMessages()}",
            $"--Nats:JetStream:Consumer:MaxAckPending={options.GetRealtimeMaxAckPending()}",
            $"--RealtimeIntegration:Replicas={options.JetStreamReplicas}",
            "--Observability:OtlpEnabled=false",
            "--Logging:LogLevel:Default=Warning"
        };
        if (options.ShouldUseShardedRealtimeRouting())
        {
            arguments.Add("--Nats:Routing:Mode=Sharded");
            arguments.Add("--Nats:Routing:RealtimeEventsShardSubjectPattern=chat.realtime-events.shards.{0}");
        }
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
            _dotnetExecutable,
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
        var benchmarkConnections = Divide(
            options.TcpConnections,
            options.GatewayCount,
            index);
        var benchmarkAdmissionLimit = checked(
            benchmarkConnections + 16);
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

        var arguments = new List<string>
        {
            assemblyPath,
            "--TcpGateway:ListenAddress=127.0.0.1",
            $"--TcpGateway:Port={options.GatewayBasePort + index}",
            // 本机负载全部来自 loopback。显式放宽基准专用 admission，
            // 避免默认单 IP 上限把连接规模误判为传输性能回退。
            $"--TcpGateway:MaxConnections={benchmarkAdmissionLimit}",
            $"--TcpGateway:MaxConnectionsPerIp={benchmarkAdmissionLimit}",
            $"--TcpGateway:MaxUnauthenticatedConnections={benchmarkAdmissionLimit}",
            $"--TcpGateway:InboundTransportMode={options.InboundTransportMode}",
            // 出站发送模式 A/B：注入 OutboundSendMode 与 OnDemandSendPump 相关参数。
            // PersistentSendLoop 模式下 OnDemandSendWorkerCount/BurstLimit 被忽略，不影响行为。
            $"--TcpGateway:OutboundSendMode={options.OutboundSendMode}",
            // 出站队列模式 A/B：LazySegmented 为自定义 MPSC 队列，仅作对照，默认 BoundedChannel。
            $"--TcpGateway:OutboundQueueMode={options.OutboundQueueMode}",
            $"--TcpGateway:OnDemandSendWorkerCount={options.OnDemandSendWorkerCount}",
            $"--TcpGateway:OnDemandSendBurstLimit={options.OnDemandSendBurstLimit}",
            // 负载生成器直接发 AuthenticationRequest，不做 ClientHello 握手；
            // 关闭 RequireClientHello 以避免握手前置导致的 ProtocolViolation 关闭连接。
            "--TcpGateway:RequireClientHello=false",
            "--Observability:OtlpEnabled=false",
            "--Logging:LogLevel:Default=Warning"
        };
        if (options.ShouldUseShardedRealtimeRouting())
        {
            arguments.Add("--RealtimeIntegration:RoutingMode=Sharded");
            arguments.Add("--RealtimeIntegration:RealtimeEventsShardSubjectPattern=chat.realtime-events.shards.{0}");
        }
        // Inbound 预算耗尽场景：收窄全局入站缓冲字节预算，验证 GlobalInboundBudget
        // 能在超限时对连接背压/关闭，避免配置上限形同虚设。
        if (options.TcpInboundBudgetBytes is long inboundBudget)
        {
            arguments.Add($"--TcpGateway:GlobalMaxInboundBufferedBytes={inboundBudget}");
        }

        return ManagedProcess.Start(
            $"gateway-{index + 1}",
            "gateway",
            _dotnetExecutable,
            arguments,
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
            _dotnetExecutable,
            [
                assemblyPath,
                "--nats-url", options.NatsUrl,
                "--warmup-seconds", FormatNonNegativeSeconds(options.GetEstimatedTcpRamp() + options.Warmup),
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
        string? tcpTokenFilePath,
        string? tcpTargetRingFilePath)
    {
        var connections = options.GetTcpConnections(gatewayIndex);
        var slowReaders = options.GetTcpSlowReaders(gatewayIndex);
        var activeSenders = options.GetTcpActiveSenders(gatewayIndex);
        var arguments = new List<string>
        {
            assemblyPath,
            "--mode", options.TcpMode,
            "--host", "127.0.0.1",
            "--port", (options.GatewayBasePort + gatewayIndex).ToString(System.Globalization.CultureInfo.InvariantCulture),
            "--connections", connections.ToString(System.Globalization.CultureInfo.InvariantCulture),
            "--duration-seconds", FormatSeconds(options.Duration),
            "--stabilization-seconds", FormatNonNegativeSeconds(options.Warmup),
            "--messages-per-second", options.TcpMessagesPerSecond.ToString("G17", System.Globalization.CultureInfo.InvariantCulture),
            "--delivery-drain-seconds", FormatNonNegativeSeconds(options.TcpDeliveryDrain),
            "--inactive-heartbeat-seconds", FormatNonNegativeSeconds(options.TcpInactiveHeartbeatInterval),
            "--min-ack-ratio", options.TcpMinimumAcknowledgementRatio.ToString("G17", System.Globalization.CultureInfo.InvariantCulture),
            "--min-delivery-ratio", options.TcpMinimumDeliveryRatio.ToString("G17", System.Globalization.CultureInfo.InvariantCulture),
            "--payload-bytes", options.TcpPayloadBytes.ToString(System.Globalization.CultureInfo.InvariantCulture),
            "--slow-readers", slowReaders.ToString(System.Globalization.CultureInfo.InvariantCulture),
            "--connections-per-second", options.GetTcpConnectionsPerSecond(gatewayIndex).ToString(System.Globalization.CultureInfo.InvariantCulture),
            "--report-directory", Path.Combine(sessionDirectory, $"tcp-gateway-{gatewayIndex + 1}")
        };
        if (activeSenders > 0)
        {
            arguments.Add("--active-senders");
            arguments.Add(activeSenders.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }
        if (options.TcpSlowlorisPhase is not null)
        {
            arguments.Add("--slowloris-phase");
            arguments.Add(options.TcpSlowlorisPhase);
            arguments.Add("--slowloris-delay-ms");
            arguments.Add(options.TcpSlowlorisDelayMs.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }
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
        if (tcpTargetRingFilePath is not null)
        {
            arguments.Add("--target-ring-file");
            arguments.Add(tcpTargetRingFilePath);
        }

        return ManagedProcess.Start(
            $"tcp-load-{gatewayIndex + 1}",
            "load",
            _dotnetExecutable,
            arguments,
            options.RepositoryRoot,
            logDirectory);
    }

    private static string WriteTcpTokenFile(
        string sessionDirectory,
        int gatewayIndex,
        IReadOnlyList<string> tcpTokens)
    {
        var path = Path.Combine(
            sessionDirectory,
            $".tcp-tokens-gateway-{gatewayIndex + 1}-{Guid.NewGuid():N}");
        File.WriteAllLines(path, tcpTokens);
        if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            File.SetUnixFileMode(
                path,
                UnixFileMode.UserRead | UnixFileMode.UserWrite);
        }

        return path;
    }

    /// <summary>
    /// item 五：为单个 Gateway 写跨 Gateway 目标环文件。第 i 行 = 连接 i 的
    /// 目标用户 id（LoadGenerator 按连接序号读取，未写行号注释）。
    /// </summary>
    private static string WriteTargetRingFile(
        string sessionDirectory,
        int gatewayIndex,
        IReadOnlyList<long> targetUserIds)
    {
        var path = Path.Combine(
            sessionDirectory,
            $".tcp-target-ring-gateway-{gatewayIndex + 1}-{Guid.NewGuid():N}");
        File.WriteAllLines(
            path,
            targetUserIds.Select(userId => userId.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            File.SetUnixFileMode(
                path,
                UnixFileMode.UserRead | UnixFileMode.UserWrite);
        }

        return path;
    }

    /// <summary>
    /// item 五：跨 Gateway 目标环。Gateway g 的连接槽 i 的目标是下一个 Gateway
    /// (g+1)%N 的连接槽 i（按下一个 Gateway 的连接数取模）。每个 sender 与目标
    /// 用户建立双向 friendship，保证消息真实跨 Gateway 且被接收侧 LoadGenerator
    /// 观测到。
    /// </summary>
    private static TcpCrossGatewayPlan BuildCrossGatewayPlan(
        IReadOnlyList<TcpBootstrapIdentity>[] connectionPartitions)
    {
        var gatewayCount = connectionPartitions.Length;
        var targetRings = new List<long[]>(gatewayCount);
        var friendshipEdges = new HashSet<(long UserId, long FriendId)>();
        for (var gatewayIndex = 0; gatewayIndex < gatewayCount; gatewayIndex++)
        {
            var next = (gatewayIndex + 1) % gatewayCount;
            var nextCount = connectionPartitions[next].Count;
            var ring = new long[connectionPartitions[gatewayIndex].Count];
            for (var connectionIndex = 0;
                 connectionIndex < ring.Length;
                 connectionIndex++)
            {
                var sender = connectionPartitions[gatewayIndex][connectionIndex];
                var target = connectionPartitions[next][connectionIndex % nextCount];
                ring[connectionIndex] = target.UserId;
                friendshipEdges.Add((sender.UserId, target.UserId));
                friendshipEdges.Add((target.UserId, sender.UserId));
            }

            targetRings.Add(ring);
        }

        return new TcpCrossGatewayPlan(targetRings, friendshipEdges);
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
                    var configuration = root.GetProperty("Configuration");
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
                        StabilizationSeconds = configuration.GetProperty("WarmupSeconds").GetDouble(),
                        MeasurementSeconds = root.GetProperty("ElapsedSeconds").GetDouble(),
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
                    var latencyCount = latency.GetProperty("Count").GetInt64();
                    var succeeded = root.GetProperty("SuccessfulConnections").GetInt32();
                    var failed = root.GetProperty("FailedConnections").GetInt32();
                    var peakActive = root.TryGetProperty("PeakActiveConnections", out var peakProp)
                        ? peakProp.GetInt32()
                        : succeeded;
                    var hasHealthy = root.TryGetProperty("Healthy", out var healthy);
                    var hasSlow = root.TryGetProperty("Slow", out var slow);
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
                        TcpConnectFailed = GetInt64OrDefault(root, "TcpConnectFailed"),
                        AuthInvalidToken = GetInt64OrDefault(root, "AuthInvalidToken"),
                        AuthDependencyUnavailable = GetInt64OrDefault(root, "AuthDependencyUnavailable"),
                        AuthOtherFailure = GetInt64OrDefault(root, "AuthOtherFailure"),
                        AuthSucceededWithoutResumeToken = GetInt64OrDefault(root, "AuthSucceededWithoutResumeToken"),
                        ServerClosed = GetInt64OrDefault(root, "ServerClosed"),
                        ProtocolRejected = GetInt64OrDefault(root, "ProtocolRejected"),
                        ErrorRatePercent = succeeded + failed == 0
                            ? 0
                            : failed * 100d / (succeeded + failed),
                        ThroughputPerSecond = throughput,
                        P50Milliseconds = latency.GetProperty("P50Ms").GetDouble(),
                        P95Milliseconds = latency.GetProperty("P95Ms").GetDouble(),
                        P99Milliseconds = latency.GetProperty("P99Ms").GetDouble(),
                        PeakActiveConnections = peakActive,
                        HealthyP95Milliseconds = hasHealthy
                            ? healthy.GetProperty("P95Ms").GetDouble()
                            : 0,
                        SlowP95Milliseconds = hasSlow
                            ? slow.GetProperty("P95Ms").GetDouble()
                            : 0,
                        MessagesSent = GetInt64OrDefault(root, "Sent"),
                        MessagesExpectedDeliveries = root.TryGetProperty(
                            "ExpectedDeliveries",
                            out var expectedDeliveries)
                                ? expectedDeliveries.GetInt64()
                                : GetInt64OrDefault(root, "Sent"),
                        MessagesAcknowledged = GetInt64OrDefault(root, "Acknowledged"),
                        MessagesRejected = GetInt64OrDefault(root, "Rejected"),
                        MessagesReceived = GetInt64OrDefault(root, "Received"),
                        RampSeconds = GetDoubleOrDefault(root, "RampSeconds"),
                        StabilizationSeconds = GetDoubleOrDefault(root, "StabilizationSeconds"),
                        MeasurementSeconds = root.TryGetProperty("MeasurementSeconds", out var measurement)
                            ? measurement.GetDouble()
                            : elapsed,
                        TargetStrategy = root.TryGetProperty("TargetStrategy", out var targetStrategy)
                            ? targetStrategy.GetString()
                            : null,
                        UniqueAuthenticatedUsers = root.TryGetProperty("UniqueAuthenticatedUsers", out var uniqueUsers)
                            ? uniqueUsers.GetInt32()
                            : 0,
                        ActiveSenders = configuration.TryGetProperty("ActiveSenders", out var activeSenders)
                            ? activeSenders.GetInt32()
                            : succeeded,
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

    private static long GetInt64OrDefault(JsonElement element, string propertyName) =>
        element.TryGetProperty(propertyName, out var property) && property.TryGetInt64(out var value)
            ? value
            : 0;

    private static double GetDoubleOrDefault(JsonElement element, string propertyName) =>
        element.TryGetProperty(propertyName, out var property) && property.TryGetDouble(out var value)
            ? value
            : 0;

    private TimeSpan GetEstimatedTcpRamp(int gatewayIndex)
    {
        var rate = options.GetTcpConnectionsPerSecond(gatewayIndex);
        return rate == 0
            ? TimeSpan.Zero
            : TimeSpan.FromSeconds(Math.Ceiling(
                options.GetTcpConnections(gatewayIndex) / (double)rate));
    }

    private static async Task<LoadExitObservation> ObserveLoadExitAsync(
        ManagedProcess load,
        long phaseStartedTimestamp,
        TimeSpan expectedMinimumRuntime,
        CancellationToken cancellationToken)
    {
        var exitCode = await load.WaitForExitAsync(cancellationToken).ConfigureAwait(false);
        return new LoadExitObservation(
            load.Label,
            exitCode,
            Stopwatch.GetElapsedTime(phaseStartedTimestamp),
            expectedMinimumRuntime);
    }

    private static async Task WaitForMeasurementStartAsync(
        IReadOnlyList<ManagedProcess> loads,
        IReadOnlyList<ManagedProcess> services,
        TimeSpan delay,
        CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        while (Stopwatch.GetElapsedTime(started) < delay)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var exitedLoad = loads.FirstOrDefault(static process => process.HasExited);
            if (exitedLoad is not null)
                throw new InvalidOperationException($"{exitedLoad.Label} exited before measurement started.");
            var exitedService = services.FirstOrDefault(static process => process.HasExited);
            if (exitedService is not null)
                throw new InvalidOperationException($"{exitedService.Label} exited before measurement started.");

            var remaining = delay - Stopwatch.GetElapsedTime(started);
            await Task.Delay(
                    remaining < TimeSpan.FromMilliseconds(250)
                        ? remaining
                        : TimeSpan.FromMilliseconds(250),
                    cancellationToken)
                .ConfigureAwait(false);
        }

        var finalExited = loads.Concat(services).FirstOrDefault(static process => process.HasExited);
        if (finalExited is not null)
            throw new InvalidOperationException($"{finalExited.Label} exited before measurement started.");
    }

    private static async Task StopLoadsAfterFailFastAsync(
        IReadOnlyList<ManagedProcess> loads,
        List<string> errors)
    {
        foreach (var load in loads)
        {
            if (load.HasExited)
                continue;

            try
            {
                await load.StopAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                errors.Add(
                    $"Fail-fast could not stop {load.Label}: {exception.Message}");
            }
        }
    }

    private BenchmarkRunValidity CreateValidity(
        DateTimeOffset? loadStartedAtUtc,
        DateTimeOffset? measurementStartedAtUtc,
        DateTimeOffset? measurementCompletedAtUtc,
        long measurementStartedTimestamp,
        long measurementCompletedTimestamp,
        int expectedLoadProcesses,
        int completedLoadProcesses,
        bool servicesAlive,
        IReadOnlyList<ProcessTimeline> timelines,
        IReadOnlyList<DockerTimeline> dockerTimelines,
        int prometheusPollsAtStart,
        int prometheusPollsAtEnd,
        IReadOnlyList<LoadResultSummary> loadSummaries)
    {
        const double minimumCoveragePercent = 80;
        var reasons = new List<string>();
        var hasMeasurementWindow = measurementStartedAtUtc is not null
                                   && measurementCompletedAtUtc is not null
                                   && measurementStartedTimestamp > 0
                                   && measurementCompletedTimestamp >= measurementStartedTimestamp;
        var observed = hasMeasurementWindow
            ? Stopwatch.GetElapsedTime(measurementStartedTimestamp, measurementCompletedTimestamp)
            : TimeSpan.Zero;
        var expectedSamples = Math.Max(
            1,
            (int)Math.Floor(options.Duration.TotalMilliseconds / options.SampleInterval.TotalMilliseconds));
        var resourceSamplingSeriesCoverage = ResourceSamplingCoverage.Calculate(
            timelines,
            dockerTimelines,
            options.DockerContainers,
            measurementStartedTimestamp,
            measurementCompletedTimestamp,
            options.Duration,
            options.SampleInterval);
        var processSeriesCoverage = resourceSamplingSeriesCoverage
            .Where(static series => series.Kind == "process")
            .ToArray();
        var minimumProcessSamples = processSeriesCoverage.Length == 0
            ? 0
            : processSeriesCoverage.Min(static series => series.SamplesInMeasurement);
        var processCoverage = processSeriesCoverage.Length == 0
            ? 0
            : processSeriesCoverage.Min(static series => series.CoveragePercent);
        var prometheusSamples = Math.Max(0, prometheusPollsAtEnd - prometheusPollsAtStart);
        var prometheusCoverage = Math.Min(100, prometheusSamples * 100d / expectedSamples);

        if (!hasMeasurementWindow)
            reasons.Add("The coordinated measurement window was not completed.");
        else if (observed + TimeSpan.FromSeconds(1) < options.Duration)
            reasons.Add("The coordinated measurement window ended early.");
        if (completedLoadProcesses != expectedLoadProcesses)
            reasons.Add($"Only {completedLoadProcesses} of {expectedLoadProcesses} load processes completed.");
        if (!servicesAlive)
            reasons.Add("A service process exited during measurement.");
        if (loadSummaries.Count != expectedLoadProcesses)
            reasons.Add($"Only {loadSummaries.Count} of {expectedLoadProcesses} child reports were readable.");
        foreach (var summary in loadSummaries.Where(summary =>
                     summary.MeasurementSeconds + 1 < options.Duration.TotalSeconds))
        {
            reasons.Add(
                $"{summary.Name} reported only {summary.MeasurementSeconds:F2}s of measurement data.");
        }
        if (options.TcpBootstrapAuthentication && options.TcpMode is "heartbeat" or "chat")
        {
            var authenticatedUsers = loadSummaries
                .Where(static summary => summary.Name.StartsWith("tcp:", StringComparison.Ordinal))
                .Sum(static summary => summary.UniqueAuthenticatedUsers);
            var expectedAuthenticatedUsers = options.GetTcpBootstrapIdentityCount();
            if (authenticatedUsers != expectedAuthenticatedUsers)
            {
                reasons.Add(
                    $"TCP children authenticated {authenticatedUsers} distinct users; " +
                    $"{expectedAuthenticatedUsers} were required for healthy connections. " +
                    "Slow readers intentionally reuse healthy identities.");
            }
        }
        if (options.TcpBootstrapAuthentication && options.TcpMode == "chat" &&
            loadSummaries.Any(static summary =>
                summary.Name.StartsWith("tcp:", StringComparison.Ordinal) &&
                !string.Equals(summary.TargetStrategy, "peer-ring", StringComparison.Ordinal)))
        {
            reasons.Add("A TCP chat child did not report peer-ring non-self targeting.");
        }
        if (processCoverage < minimumCoveragePercent)
            reasons.Add($"Process sampling coverage was {processCoverage:F1}%, below {minimumCoveragePercent:F0}%.");
        if (prometheusCoverage < minimumCoveragePercent)
            reasons.Add($"Prometheus sampling coverage was {prometheusCoverage:F1}%, below {minimumCoveragePercent:F0}%.");

        return new BenchmarkRunValidity
        {
            IsValid = reasons.Count == 0,
            LoadStartedAtUtc = loadStartedAtUtc,
            MeasurementStartedAtUtc = measurementStartedAtUtc,
            MeasurementCompletedAtUtc = measurementCompletedAtUtc,
            ExpectedMeasurementSeconds = options.Duration.TotalSeconds,
            ObservedMeasurementSeconds = observed.TotalSeconds,
            MeasurementBoundarySource =
                "orchestrator estimated ramp + stabilization; validity uses child MeasurementSeconds",
            ExpectedLoadProcesses = expectedLoadProcesses,
            CompletedLoadProcesses = completedLoadProcesses,
            ServicesAliveThroughMeasurement = servicesAlive,
            ExpectedProcessSamplesPerProcess = expectedSamples,
            MinimumProcessSamples = minimumProcessSamples,
            ProcessSamplingCoveragePercent = processCoverage,
            ExpectedPrometheusSamples = expectedSamples,
            PrometheusSamples = prometheusSamples,
            PrometheusSamplingCoveragePercent = prometheusCoverage,
            ResourceSamplingSeriesCoverage = resourceSamplingSeriesCoverage,
            InvalidReasons = reasons,
        };
    }

    private static int Divide(int total, int partitions, int index) =>
        total / partitions + (index < total % partitions ? 1 : 0);

    private static string FormatSeconds(TimeSpan value) =>
        Math.Max(1, (int)Math.Ceiling(value.TotalSeconds))
            .ToString(System.Globalization.CultureInfo.InvariantCulture);

    private static string FormatNonNegativeSeconds(TimeSpan value) =>
        Math.Max(0, (int)Math.Ceiling(value.TotalSeconds))
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

    private sealed record TcpCrossGatewayPlan(
        IReadOnlyList<long[]> TargetRings,
        IReadOnlySet<(long UserId, long FriendId)> FriendshipEdges);

    private sealed record BenchmarkBinaries(
        string Gateway,
        string RealtimeService,
        string TcpLoadGenerator,
        string? PipelineLoadGenerator);
}

internal sealed record BenchmarkRunResult(
    BenchmarkReport Report,
    IReadOnlyList<ProcessTimeline> ProcessTimelines,
    string SessionDirectory);
