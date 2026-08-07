param(
    [int[]] $Rates = @(40, 80, 120, 160, 200),
    [ValidateRange(1, 86400)] [int] $DurationSeconds = 60,
    [ValidateRange(0, 3600)] [int] $WarmupSeconds = 10,
    [ValidateRange(1, 1024)] [int] $PipelineConcurrency = 32,
    [ValidateRange(1, 1048576)] [int] $PipelinePayloadBytes = 512,
    [ValidateRange(1, 100000)] [int] $TcpConnections = 1000,
    [ValidateSet('connection','heartbeat','chat')] [string] $TcpMode = 'connection',
    [ValidateRange(0, 100000)] [int] $TcpActiveSenders = 0,
    [switch] $TcpCrossGateway,
    [ValidateRange(0.001, 100000)] [double] $TcpMessagesPerSecond = 10,
    [ValidateRange(0, 3600)] [int] $TcpDeliveryDrainSeconds = 30,
    [ValidateRange(0, 3600)] [int] $TcpInactiveHeartbeatSeconds = 30,
    [ValidateRange(1, 1048576)] [int] $TcpPayloadBytes = 128,
    [int] $TcpSlowReaders = 0,
    [ValidateRange(0, 100000)] [int] $TcpConnectionsPerSecond = 500,
    [ValidateSet('header','payload')] [string] $TcpSlowlorisPhase = '',
    [ValidateRange(1, 60000)] [int] $TcpSlowlorisDelayMs = 1000,
    [ValidateRange(1, 1099511627776)] [long] $TcpInboundBudgetBytes = 0,
    [int] $GatewayBasePort = 18888,
    [int] $RealtimePort = 18080,
    [ValidateRange(1, 1024)] [int] $RealtimeProcessingConcurrency = 4,
    [int] $NatsPort = 4222,
    [int] $NatsMonitorPort = 18222,
    [int] $PostgresPort = 15432,
    [int] $GarnetPort = 16379,
    [string] $NatsImage = 'nats:2.10.26-alpine',
    [string] $PostgresImage = 'postgres:16.8',
    [string] $GarnetImage = 'ghcr.io/microsoft/garnet:1.0.84',
    [ValidateSet('Pipelines','DirectSocket')] [string] $InboundTransportMode = 'DirectSocket',
    [ValidateSet('PersistentSendLoop','OnDemandSendPump','PerSessionDrain')] [string] $OutboundSendMode = 'PersistentSendLoop',
    [ValidateSet('BoundedChannel','LazySegmented')] [string] $OutboundQueueMode = 'BoundedChannel',
    [int] $OnDemandSendWorkerCount = 0,
    [int] $OnDemandSendBurstLimit = 16,
    [ValidateRange(1, 300)] [int] $DependencyStartupTimeoutSeconds = 60,
    [ValidateRange(0, 100)] [double] $MinimumConnectionSuccessPercent = 99,
    [ValidateRange(0, 100)] [double] $MinimumPeakConnectionPercent = 99,
    [ValidateRange(0, 100)] [double] $MinimumThroughputAttainmentPercent = 90,
    [ValidateRange(0, 100)] [double] $MinimumAcknowledgementPercent = 99,
    [ValidateRange(0, 100)] [double] $MinimumDeliveryPercent = 95,
    [ValidateRange(0, 100)] [double] $MinimumResourceSampleCoveragePercent = 90,
    [ValidateRange(0, 9223372036854775807)] [long] $MaximumDeadLetters = 0,
    [string] $ReportDirectory,
    [string] $InvocationManifestPath,
    [switch] $NoPipeline,
    [switch] $UseTcpMessagesPerSecond,
    [switch] $SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$dotnetCommand = 'dotnet'
$strictSnapshotBinding = [Environment]::GetEnvironmentVariable(
    'CHATAPP_BENCHMARK_REQUIRE_SNAPSHOT_BINDING') -in @('1', 'true', 'True', 'TRUE')
$boundDotnetPath = [Environment]::GetEnvironmentVariable('CHATAPP_BENCHMARK_DOTNET_PATH')
$boundDotnetSha256 = [Environment]::GetEnvironmentVariable('CHATAPP_BENCHMARK_DOTNET_SHA256')
if (-not [string]::IsNullOrWhiteSpace($boundDotnetPath)) {
    if ([string]::IsNullOrWhiteSpace($boundDotnetSha256)) {
        throw 'CHATAPP_BENCHMARK_DOTNET_PATH requires CHATAPP_BENCHMARK_DOTNET_SHA256.'
    }
    if (-not [IO.Path]::IsPathFullyQualified($boundDotnetPath)) {
        throw 'CHATAPP_BENCHMARK_DOTNET_PATH must be an absolute path.'
    }
    $dotnetCommand = [IO.Path]::GetFullPath($boundDotnetPath)
    if (-not (Test-Path -LiteralPath $dotnetCommand -PathType Leaf)) {
        throw 'CHATAPP_BENCHMARK_DOTNET_PATH does not exist or is not a file.'
    }
    $actualDotnetSha256 = (Get-FileHash -LiteralPath $dotnetCommand -Algorithm SHA256).Hash
    if (-not $actualDotnetSha256.Equals($boundDotnetSha256, [StringComparison]::OrdinalIgnoreCase)) {
        throw 'CHATAPP_BENCHMARK_DOTNET_SHA256 does not match CHATAPP_BENCHMARK_DOTNET_PATH.'
    }
}
elseif ($strictSnapshotBinding) {
    throw 'Strict benchmark snapshot binding requires CHATAPP_BENCHMARK_DOTNET_PATH.'
}
if ($Rates.Count -eq 0 -or $Rates.Where({ $_ -le 0 }).Count -ne 0) {
    throw 'Rates must contain one or more positive integers.'
}
$effectiveTcpActiveSenders = if ($TcpMode -in @('heartbeat','chat')) {
    if ($TcpActiveSenders -eq 0) { $TcpConnections - $TcpSlowReaders } else { $TcpActiveSenders }
} else { 0 }
if ($TcpActiveSenders -gt $TcpConnections - $TcpSlowReaders) {
    throw 'TcpActiveSenders cannot exceed non-slow-reader TCP connections.'
}
if ($TcpMode -in @('heartbeat','chat') -and $effectiveTcpActiveSenders -le 0) {
    throw 'Heartbeat/chat mode requires at least one active sender.'
}
if ($TcpCrossGateway -and $TcpMode -ne 'chat') {
    throw 'TcpCrossGateway requires TcpMode=chat.'
}
$tcpRateDrivenByRates = $NoPipeline `
    -and $TcpMode -in @('heartbeat','chat') `
    -and -not $UseTcpMessagesPerSecond

$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..\..'))
$realtimeRoot = [IO.Path]::GetFullPath((Join-Path $repositoryRoot '..\ChatApp.RealtimeServices'))
$orchestratorProject = Join-Path $repositoryRoot 'tools\ChatApp.Performance.Orchestrator'
if (-not (Test-Path -LiteralPath $orchestratorProject -PathType Container)) {
    throw "Performance orchestrator was not found: $orchestratorProject"
}
if (-not (Test-Path -LiteralPath $realtimeRoot -PathType Container)) {
    throw "Realtime repository was not found: $realtimeRoot"
}
if ([string]::IsNullOrWhiteSpace($ReportDirectory)) {
    $ReportDirectory = Join-Path $repositoryRoot '.artifacts\performance'
}
$ReportDirectory = [IO.Path]::GetFullPath($ReportDirectory)
$startedAt = [DateTimeOffset]::UtcNow
$stamp = $startedAt.ToString("yyyyMMdd-HHmmss'Z'")
$curveDirectory = Join-Path $ReportDirectory "capacity-curve-$stamp"
[IO.Directory]::CreateDirectory($curveDirectory) | Out-Null

$dependencyHelpers = Join-Path $PSScriptRoot 'Performance-DependencyPreflight.ps1'
if (-not (Test-Path -LiteralPath $dependencyHelpers -PathType Leaf)) {
    throw "Dependency preflight helpers were not found: $dependencyHelpers"
}
. $dependencyHelpers
$dockerLifecycleHelpers = Join-Path $PSScriptRoot 'Performance-DockerLifecycle.ps1'
if (-not (Test-Path -LiteralPath $dockerLifecycleHelpers -PathType Leaf)) {
    throw "Docker lifecycle helpers were not found: $dockerLifecycleHelpers"
}
. $dockerLifecycleHelpers

$manifestPath = Join-Path $curveDirectory 'run-manifest.json'
if (-not [string]::IsNullOrWhiteSpace($InvocationManifestPath)) {
    $InvocationManifestPath = [IO.Path]::GetFullPath($InvocationManifestPath)
}

function Write-CapacityManifest {
    param(
        [Parameter(Mandatory)] [string] $State,
        [AllowNull()] [Nullable[int]] $ExitCode,
        [AllowNull()] [object] $RunValid,
        [AllowNull()] [object] $RunResults,
        [AllowNull()] [object] $RunErrors
    )

    $manifest = [pscustomobject]@{
        SchemaVersion = 1
        RunId = $stamp
        Kind = 'capacity-curve'
        State = $State
        StartedAtUtc = $startedAt
        CompletedAtUtc = if ($State -eq 'running') { $null } else { [DateTimeOffset]::UtcNow }
        RunDirectory = $curveDirectory
        ReportDirectory = $ReportDirectory
        ExitCode = $ExitCode
        RunValid = $RunValid
        Configuration = [pscustomobject]@{
            Rates = $Rates
            DurationSeconds = $DurationSeconds
            WarmupSeconds = $WarmupSeconds
            StabilizationSeconds = $WarmupSeconds
            TcpMode = $TcpMode
            TcpConnections = $TcpConnections
            TcpActiveSenders = $effectiveTcpActiveSenders
            TcpCrossGateway = [bool]$TcpCrossGateway
            TcpConnectionsPerSecond = $TcpConnectionsPerSecond
            TcpMessagesPerSecond = $TcpMessagesPerSecond
            TcpRateDrivenByRates = [bool]$tcpRateDrivenByRates
            TcpDeliveryDrainSeconds = $TcpDeliveryDrainSeconds
            TcpInactiveHeartbeatSeconds = $TcpInactiveHeartbeatSeconds
            RealtimeProcessingConcurrency = $RealtimeProcessingConcurrency
            TcpPayloadBytes = $TcpPayloadBytes
            DependencyStartupTimeoutSeconds = $DependencyStartupTimeoutSeconds
        }
        Environment = [pscustomobject]@{
            OpenFiles = $openFileLimitPreflight
        }
        Results = $RunResults
        Errors = $RunErrors
        Artifacts = [pscustomobject]@{
            Manifest = $manifestPath
            CapacityJson = Join-Path $curveDirectory 'capacity-curve-report.json'
            CapacityMarkdown = Join-Path $curveDirectory 'capacity-curve-report.md'
        }
    }
    Write-PerformanceJsonArtifact -Path $manifestPath -Value $manifest
    if (-not [string]::IsNullOrWhiteSpace($InvocationManifestPath)) {
        Write-PerformanceJsonArtifact -Path $InvocationManifestPath -Value $manifest
    }
}

function Get-OptionalProperty {
    param(
        [AllowNull()] [object] $InputObject,
        [Parameter(Mandatory)] [string] $Name,
        [AllowNull()] [object] $DefaultValue = $null
    )
    if ($null -eq $InputObject) { return $DefaultValue }
    $property = $InputObject.psobject.Properties[$Name]
    if ($null -eq $property -or $null -eq $property.Value) { return $DefaultValue }
    return $property.Value
}

function New-ValidityGate {
    param(
        [Parameter(Mandatory)] [string] $Name,
        [Parameter(Mandatory)] [bool] $Passed,
        [AllowNull()] [object] $Actual,
        [AllowNull()] [object] $Expected,
        [Parameter(Mandatory)] [string] $Details
    )
    return [pscustomobject]@{
        Name = $Name
        Passed = $Passed
        Actual = $Actual
        Expected = $Expected
        Details = $Details
    }
}

$results = [Collections.Generic.List[object]]::new()
$runErrors = [Collections.Generic.List[string]]::new()
$abortCurve = $false
$openFileLimitPreflight = Get-PerformanceOpenFileLimitPreflight `
    -TcpConnections $TcpConnections `
    -GatewayCount 2 `
    -SafetyMargin 1024

function Stop-CapacityRunEarly {
    param([Parameter(Mandatory)] [string] $Message)

    $runErrors.Add($Message)
    $failure = [pscustomobject]@{
        TargetPerSecond = if ($Rates.Count -gt 0) { $Rates[0] } else { 0 }
        Passed = $false
        RunValid = $false
        Error = $Message
    }
    $results.Add($failure)
    $earlySummary = [pscustomobject]@{
        StartedAtUtc = $startedAt
        CompletedAtUtc = [DateTimeOffset]::UtcNow
        RunValid = $false
        AbortedEarly = $true
        Configuration = [pscustomobject]@{
            Rates = $Rates
            DurationSeconds = $DurationSeconds
            WarmupSeconds = $WarmupSeconds
            StabilizationSeconds = $WarmupSeconds
            TcpConnections = $TcpConnections
            TcpActiveSenders = $effectiveTcpActiveSenders
            TcpCrossGateway = [bool]$TcpCrossGateway
            TcpConnectionsPerSecond = $TcpConnectionsPerSecond
            TcpMode = $TcpMode
        }
        Results = $results.ToArray()
        Errors = $runErrors.ToArray()
    }
    $earlyJson = Join-Path $curveDirectory 'capacity-curve-report.json'
    $earlyMarkdown = Join-Path $curveDirectory 'capacity-curve-report.md'
    Write-PerformanceJsonArtifact -Path $earlyJson -Value $earlySummary
    [IO.File]::WriteAllLines(
        $earlyMarkdown,
        @('# TCP capacity run', '', 'Run validity: **INVALID**', '', "- $Message"),
        [Text.UTF8Encoding]::new($false))
    Write-CapacityManifest `
        -State 'completed' `
        -ExitCode 1 `
        -RunValid $false `
        -RunResults $results.ToArray() `
        -RunErrors $runErrors.ToArray()
    Write-Host "Capacity curve aborted: $Message" -ForegroundColor Red
    Write-Output $curveDirectory
    exit 1
}

# Publish the run location before build/container work starts. Callers use the
# explicit invocation manifest, not the last stdout line, even if this run fails.
Write-CapacityManifest -State 'running' -ExitCode $null -RunValid $null -RunResults @() -RunErrors @()
Write-Output $curveDirectory

$environmentPreflightPath = Join-Path $curveDirectory 'environment-preflight.json'
Write-PerformanceJsonArtifact -Path $environmentPreflightPath -Value ([pscustomobject]@{
    OpenFiles = $openFileLimitPreflight
})
if (-not $openFileLimitPreflight.Passed) {
    $message = "Linux open-file soft limit is insufficient: actual=$($openFileLimitPreflight.SoftLimit), required>=$($openFileLimitPreflight.RequiredSoftLimit). $($openFileLimitPreflight.Recommendation)"
    Stop-CapacityRunEarly -Message $message
}

if (-not $SkipBuild) {
    & $dotnetCommand build (Join-Path $repositoryRoot 'ChatApp.TcpGateway.sln') -c Release --no-restore
    if ($LASTEXITCODE -ne 0) { Stop-CapacityRunEarly -Message 'Gateway solution build failed.' }
    & $dotnetCommand build (Join-Path $realtimeRoot 'ChatApp.RealtimeServices.slnx') -c Release --no-restore
    if ($LASTEXITCODE -ne 0) { Stop-CapacityRunEarly -Message 'Realtime solution build failed.' }
}

$dbEnvName = 'CHATAPP_CAPACITY_CURVE_DB'
$garnetEnvName = 'CHATAPP_CAPACITY_CURVE_GARNET'
$oldDbEnv = [Environment]::GetEnvironmentVariable($dbEnvName, 'Process')
$oldGarnetEnv = [Environment]::GetEnvironmentVariable($garnetEnvName, 'Process')
$password = 'capacity-' + [Guid]::NewGuid().ToString('N')
[Environment]::SetEnvironmentVariable(
    $dbEnvName,
    "Host=127.0.0.1;Port=$PostgresPort;Database=ChatAppDatabase;Username=postgres;Password=$password;Pooling=true;Minimum Pool Size=0;Maximum Pool Size=100;Connection Idle Lifetime=300;Timeout=5;Command Timeout=5;",
    'Process')
[Environment]::SetEnvironmentVariable(
    $garnetEnvName, "127.0.0.1:$GarnetPort,abortConnect=false", 'Process')

function Wait-Postgres([string] $Container) {
    for ($attempt = 1; $attempt -le 30; $attempt++) {
        & docker exec $Container pg_isready -U postgres -d ChatAppDatabase *> $null
        if ($LASTEXITCODE -eq 0) { return }
        Start-Sleep -Seconds 1
    }
    throw "PostgreSQL did not become ready: $Container"
}

function Get-MetricSum($Metrics, [string] $Prefix) {
    $sum = 0.0
    foreach ($property in $Metrics.psobject.Properties) {
        if ($property.Name.StartsWith($Prefix, [StringComparison]::Ordinal)) {
            $sum += [double]$property.Value
        }
    }
    return $sum
}

try {
    foreach ($rate in $Rates) {
        # In TCP-only heartbeat/chat curves, Rates is the aggregate message target.
        # Distribute it evenly across active senders so every curve point changes
        # the offered TCP load instead of repeating TcpMessagesPerSecond unchanged.
        $rateTcpMessagesPerSecond = if ($tcpRateDrivenByRates) {
            [double]$rate / [double]$effectiveTcpActiveSenders
        }
        else {
            $TcpMessagesPerSecond
        }
        $tag = "$($stamp.Replace('-','').ToLowerInvariant())-$rate"
        $nats = "codex-chatapp-capacity-nats-$tag"
        $postgres = "codex-chatapp-capacity-postgres-$tag"
        $garnet = "codex-chatapp-capacity-garnet-$tag"
        $dockerRunId = "capacity-$tag"
        $created = [Collections.Generic.List[string]]::new()
        $rateDirectory = Join-Path $curveDirectory "rate-$rate"
        [IO.Directory]::CreateDirectory($rateDirectory) | Out-Null

        Write-Host "Starting target $rate pipeline/s..."
        try {
            Start-PerformanceDockerContainer `
                -Name $nats -RunId $dockerRunId -CreatedContainers $created `
                -CreateArguments @(
                '-p',"127.0.0.1:$($NatsPort):4222",
                '-p',"127.0.0.1:$($NatsMonitorPort):8222",
                $NatsImage,'-js','-m','8222')
            Start-PerformanceDockerContainer `
                -Name $postgres -RunId $dockerRunId -CreatedContainers $created `
                -CreateArguments @(
                '-e',"POSTGRES_PASSWORD=$password",
                '-e','POSTGRES_DB=ChatAppDatabase',
                '-p',"127.0.0.1:$($PostgresPort):5432",$PostgresImage)
            Start-PerformanceDockerContainer `
                -Name $garnet -RunId $dockerRunId -CreatedContainers $created `
                -CreateArguments @(
                '-p',"127.0.0.1:$($GarnetPort):6379",$GarnetImage,'--lua')
            Wait-Postgres $postgres
            try {
                $preflightPath = Join-Path $rateDirectory 'dependency-preflight.json'
                [void](Wait-PerformanceDependencies `
                    -NatsMonitorPort $NatsMonitorPort `
                    -GarnetPort $GarnetPort `
                    -ArtifactPath $preflightPath `
                    -TimeoutSeconds $DependencyStartupTimeoutSeconds)
                Write-Host "Dependency preflight passed: NATS health/JetStream and Garnet PING/write-read/EVAL."
            }
            catch {
                $abortCurve = $true
                throw "Dependency preflight failed for rate $rate; benchmark was not started. $($_.Exception.Message)"
            }

            $orchestratorArgs = @(
                'run','--project',$orchestratorProject,'-c','Release','--no-build','--',
                '--no-build','--gateway-count','2',
                '--gateway-base-port',"$GatewayBasePort",
                '--realtime-port',"$RealtimePort",
                '--realtime-processing-concurrency',"$RealtimeProcessingConcurrency",
                '--nats-url',"nats://127.0.0.1:$NatsPort",
                '--warmup-seconds',"$WarmupSeconds",
                '--duration-seconds',"$DurationSeconds",
                '--sample-interval-ms','2000',
                '--tcp-mode',"$TcpMode",
                '--tcp-connections',"$TcpConnections",
                '--tcp-active-senders',"$TcpActiveSenders",
                '--tcp-messages-per-second',$rateTcpMessagesPerSecond.ToString('G17', [Globalization.CultureInfo]::InvariantCulture),
                '--tcp-delivery-drain-seconds',"$TcpDeliveryDrainSeconds",
                '--tcp-inactive-heartbeat-seconds',"$TcpInactiveHeartbeatSeconds",
                '--tcp-min-ack-ratio',($MinimumAcknowledgementPercent / 100.0).ToString('G17', [Globalization.CultureInfo]::InvariantCulture),
                '--tcp-min-delivery-ratio',($MinimumDeliveryPercent / 100.0).ToString('G17', [Globalization.CultureInfo]::InvariantCulture),
                '--tcp-payload-bytes',"$TcpPayloadBytes",
                '--tcp-slow-readers',"$TcpSlowReaders",
                '--tcp-connections-per-second',"$TcpConnectionsPerSecond",
                '--pipeline-concurrency',"$PipelineConcurrency",
                '--pipeline-operations-per-second',"$rate",
                '--pipeline-payload-bytes',"$PipelinePayloadBytes",
                '--inbound-transport-mode',"$InboundTransportMode",
                '--outbound-send-mode',"$OutboundSendMode",
                '--outbound-queue-mode',"$OutboundQueueMode",
                '--on-demand-send-worker-count',"$OnDemandSendWorkerCount",
                '--on-demand-send-burst-limit',"$OnDemandSendBurstLimit",
                '--realtime-database-environment',$dbEnvName,
                '--garnet-environment',$garnetEnvName,
                '--docker-container',$nats,
                '--docker-container',$postgres,
                '--docker-container',$garnet,
                '--report-directory',$rateDirectory)
            if ($TcpMode -in @('heartbeat','chat')) {
                $orchestratorArgs += '--tcp-bootstrap-auth'
            }
            if ($TcpCrossGateway) {
                $orchestratorArgs += '--tcp-cross-gateway'
            }
            if ($TcpSlowlorisPhase) {
                $orchestratorArgs += @('--tcp-slowloris-phase',"$TcpSlowlorisPhase",
                    '--tcp-slowloris-delay-ms',"$TcpSlowlorisDelayMs")
            }
            if ($TcpInboundBudgetBytes -gt 0) {
                $orchestratorArgs += @('--tcp-inbound-budget-bytes',"$TcpInboundBudgetBytes")
            }
            if ($NoPipeline) {
                $orchestratorArgs += '--no-pipeline'
            }
            & $dotnetCommand @orchestratorArgs
            $orchestratorExit = $LASTEXITCODE

            $reportFile = Get-ChildItem -LiteralPath $rateDirectory -Filter 'benchmark-report.json' -Recurse |
                Sort-Object LastWriteTimeUtc -Descending |
                Select-Object -First 1
            if ($null -eq $reportFile) {
                throw "Benchmark report was not created for rate $rate."
            }
            $report = Get-Content -LiteralPath $reportFile.FullName -Raw | ConvertFrom-Json
            $validityGates = [Collections.Generic.List[object]]::new()
            $measurementWindowSeconds = 0.0
            $measurementWindowSource = $null
            $messageTotal = 0L
            $messagesSent = 0L
            $messagesAcknowledged = 0L
            $messagesRejected = 0L
            $messagesDelivered = 0L
            $connectionSuccessPercent = 100.0
            $peakConnectionPercent = 100.0
            $acknowledgementPercent = 100.0
            $deliveryPercent = 100.0
            $rateUnit = 'operations/s'
            $childMeasurements = @()
            $maximumRampSeconds = 0.0

            if ($NoPipeline) {
                $tcpLoads = @($report.LoadResults |
                    Where-Object Kind -like 'tcp-*')
                if ($tcpLoads.Count -eq 0) {
                    $errors = $report.Errors -join '; '
                    throw "TCP result was not created for rate $rate. $errors"
                }

                $measurementCandidates = [Collections.Generic.List[double]]::new()
                $childMeasurements = [Collections.Generic.List[object]]::new()
                $childSemanticGateFailures = [Collections.Generic.List[string]]::new()
                $maximumRampSeconds = 0.0
                $latencySampleTotal = 0L
                foreach ($tcpLoad in $tcpLoads) {
                    $loadMeasurement = [double](Get-OptionalProperty $tcpLoad 'MeasurementSeconds' 0)
                    if ($loadMeasurement -le 0) {
                        $loadMeasurement = [double](Get-OptionalProperty $tcpLoad 'MeasurementElapsedSeconds' 0)
                    }
                    $loadMeasurementSource = if ($loadMeasurement -gt 0) {
                        'benchmark LoadResults.MeasurementSeconds/MeasurementElapsedSeconds'
                    } else { $null }
                    $rampSeconds = 0.0
                    $childGatePassed = $false
                    $childGateFailureText = 'child report missing'
                    if (Test-Path -LiteralPath $tcpLoad.SourceReport -PathType Leaf) {
                        $loadDetail = Get-Content -LiteralPath $tcpLoad.SourceReport -Raw | ConvertFrom-Json
                        if ($loadMeasurement -le 0) {
                            $loadMeasurement = [double](Get-OptionalProperty $loadDetail 'MeasurementSeconds' 0)
                            if ($loadMeasurement -gt 0) {
                                $loadMeasurementSource = 'tcp load report MeasurementSeconds'
                            }
                        }
                        $rampSeconds = [double](Get-OptionalProperty $loadDetail 'RampSeconds' 0)
                        $maximumRampSeconds = [Math]::Max($maximumRampSeconds, $rampSeconds)
                        $latency = Get-OptionalProperty $loadDetail 'Latency' $null
                        $latencySampleTotal += [long](Get-OptionalProperty $latency 'Count' 0)
                        $childGate = Get-OptionalProperty $loadDetail 'Gate' $null
                        $childGatePassed = $null -ne $childGate -and
                            [bool](Get-OptionalProperty $childGate 'Passed' $false)
                        $childGateFailureText = if ($null -ne $childGate) {
                            @(Get-OptionalProperty $childGate 'Failures' @()) -join '; '
                        } else { 'Gate section missing from child report' }
                    }
                    if ($loadMeasurement -gt 0) {
                        $measurementCandidates.Add($loadMeasurement)
                        $measurementWindowSource = $loadMeasurementSource
                    }
                    $childMeasurements.Add([pscustomobject]@{
                        Name = $tcpLoad.Name
                        MeasurementSeconds = $loadMeasurement
                        RampSeconds = $rampSeconds
                        Source = $loadMeasurementSource
                        SemanticGatePassed = $childGatePassed
                        SemanticGateFailures = $childGateFailureText
                    })
                    if (-not $childGatePassed) {
                        $childSemanticGateFailures.Add("$($tcpLoad.Name): $childGateFailureText")
                    }
                }

                if ($measurementCandidates.Count -gt 0) {
                    # All generators share one measurement phase. Using the maximum
                    # reported window is conservative and prevents a short-lived
                    # generator from inflating aggregate throughput.
                    $measurementWindowSeconds = [double](($measurementCandidates | Measure-Object -Maximum).Maximum)
                }
                else {
                    $configuredMeasurement = Get-OptionalProperty $report.Configuration 'MeasurementSeconds' $null
                    if ($null -ne $configuredMeasurement -and [double]$configuredMeasurement -gt 0) {
                        $measurementWindowSeconds = [double]$configuredMeasurement
                        $measurementWindowSource = 'benchmark Configuration.MeasurementSeconds'
                    }
                    else {
                        $measurementWindowSeconds = [double]$DurationSeconds
                        $measurementWindowSource = 'configured fallback (not independently reported)'
                    }
                }

                $measurementReported = $measurementCandidates.Count -eq $tcpLoads.Count -and
                    $measurementWindowSource -notlike 'configured fallback*'
                $validityGates.Add((New-ValidityGate `
                    -Name 'measurement-window-reported' `
                    -Passed $measurementReported `
                    -Actual "$($measurementCandidates.Count)/$($tcpLoads.Count) child windows" `
                    -Expected 'positive actual MeasurementSeconds for every TCP load process' `
                    -Details "Source: $measurementWindowSource"))
                $minimumMeasurementSeconds = if ($measurementCandidates.Count -gt 0) {
                    [double](($measurementCandidates | Measure-Object -Minimum).Minimum)
                } else { 0.0 }
                $maximumMeasurementSeconds = if ($measurementCandidates.Count -gt 0) {
                    [double](($measurementCandidates | Measure-Object -Maximum).Maximum)
                } else { 0.0 }
                $minimumExpectedMeasurement = 0.995 * $DurationSeconds
                $maximumExpectedMeasurement = 1.01 * $DurationSeconds
                $measurementDurationPassed = $measurementCandidates.Count -eq $tcpLoads.Count -and
                    $minimumMeasurementSeconds -ge $minimumExpectedMeasurement -and
                    $maximumMeasurementSeconds -le $maximumExpectedMeasurement
                $validityGates.Add((New-ValidityGate `
                    -Name 'measurement-duration' `
                    -Passed $measurementDurationPassed `
                    -Actual "min=$minimumMeasurementSeconds; max=$maximumMeasurementSeconds" `
                    -Expected "$minimumExpectedMeasurement <= every child MeasurementSeconds <= $maximumExpectedMeasurement" `
                    -Details 'Any child that ends early invalidates the run; aggregate throughput still uses the conservative maximum window'))
                $validityGates.Add((New-ValidityGate `
                    -Name 'child-semantic-gates' `
                    -Passed ($childSemanticGateFailures.Count -eq 0) `
                    -Actual $childSemanticGateFailures.ToArray() `
                    -Expected 'every TCP load child Gate.Passed=true' `
                    -Details 'Child gates include runtime failure, connection, bounded tracking, acknowledgement and delivery semantics'))

                $succeeded = [long](($tcpLoads | Measure-Object Succeeded -Sum).Sum)
                $failed = [long](($tcpLoads | Measure-Object Failed -Sum).Sum)
                $peakConnections = [long](($tcpLoads | Measure-Object PeakActiveConnections -Sum).Sum)
                $activeSenders = [long](($tcpLoads | Measure-Object ActiveSenders -Sum).Sum)
                $messagesSent = [long](($tcpLoads | Measure-Object MessagesSent -Sum).Sum)
                $messagesExpectedDeliveries = [long](($tcpLoads | ForEach-Object {
                    [long](Get-OptionalProperty `
                        -InputObject $_ `
                        -Name 'MessagesExpectedDeliveries' `
                        -Default (Get-OptionalProperty -InputObject $_ -Name 'MessagesSent' -Default 0))
                } | Measure-Object -Sum).Sum)
                $messagesAcknowledged = [long](($tcpLoads | Measure-Object MessagesAcknowledged -Sum).Sum)
                $messagesRejected = [long](($tcpLoads | Measure-Object MessagesRejected -Sum).Sum)
                $messagesDelivered = [long](($tcpLoads | Measure-Object MessagesReceived -Sum).Sum)
                $errorRatePercent = if ($succeeded + $failed -eq 0) {
                    100.0
                } else {
                    100.0 * $failed / ($succeeded + $failed)
                }
                $connectionSuccessPercent = if ($TcpConnections -gt 0) {
                    100.0 * $succeeded / $TcpConnections
                } else { 0.0 }
                $peakConnectionPercent = if ($TcpConnections -gt 0) {
                    100.0 * $peakConnections / $TcpConnections
                } else { 0.0 }

                $validityGates.Add((New-ValidityGate `
                    -Name 'connection-success' `
                    -Passed ($connectionSuccessPercent -ge $MinimumConnectionSuccessPercent) `
                    -Actual $connectionSuccessPercent `
                    -Expected ">= $MinimumConnectionSuccessPercent percent" `
                    -Details "$succeeded of $TcpConnections requested connections succeeded"))
                $validityGates.Add((New-ValidityGate `
                    -Name 'peak-active-connections' `
                    -Passed ($peakConnectionPercent -ge $MinimumPeakConnectionPercent) `
                    -Actual $peakConnectionPercent `
                    -Expected ">= $MinimumPeakConnectionPercent percent" `
                    -Details "Peak active $peakConnections of $TcpConnections requested connections"))
                if ($TcpMode -in @('heartbeat','chat')) {
                    $validityGates.Add((New-ValidityGate `
                        -Name 'active-sender-count' `
                        -Passed ($activeSenders -eq $effectiveTcpActiveSenders) `
                        -Actual $activeSenders `
                        -Expected $effectiveTcpActiveSenders `
                        -Details 'Every configured sender must be represented in a child load report'))
                }

                if ($TcpMode -eq 'chat') {
                    $messageTotal = $messagesSent
                    $targetPerSecond = [double]$effectiveTcpActiveSenders * $rateTcpMessagesPerSecond
                    $achievedPerSecond = if ($measurementWindowSeconds -gt 0) {
                        $messageTotal / $measurementWindowSeconds
                    } else { 0.0 }
                    $acknowledgementPercent = if ($messagesSent -gt 0) {
                        100.0 * $messagesAcknowledged / $messagesSent
                    } else { 0.0 }
                    $deliveryPercent = if ($messagesExpectedDeliveries -gt 0) {
                        100.0 * $messagesDelivered / $messagesExpectedDeliveries
                    } else { 0.0 }
                    $validityGates.Add((New-ValidityGate `
                        -Name 'message-acknowledgement' `
                        -Passed ($acknowledgementPercent -ge $MinimumAcknowledgementPercent) `
                        -Actual $acknowledgementPercent `
                        -Expected ">= $MinimumAcknowledgementPercent percent" `
                        -Details "$messagesAcknowledged acknowledged of $messagesSent sent"))
                    $validityGates.Add((New-ValidityGate `
                        -Name 'message-delivery' `
                        -Passed ($deliveryPercent -ge $MinimumDeliveryPercent) `
                        -Actual $deliveryPercent `
                        -Expected ">= $MinimumDeliveryPercent percent" `
                        -Details "$messagesDelivered delivered of $messagesExpectedDeliveries expected recipient deliveries"))
                    $validityGates.Add((New-ValidityGate `
                        -Name 'message-rejections' `
                        -Passed ($messagesRejected -eq 0) `
                        -Actual $messagesRejected `
                        -Expected 0 `
                        -Details 'No chat message may be rejected in a baseline capacity/soak run'))
                }
                elseif ($TcpMode -eq 'heartbeat') {
                    $messageTotal = $latencySampleTotal
                    $targetPerSecond = [double]$effectiveTcpActiveSenders * $rateTcpMessagesPerSecond
                    $achievedPerSecond = if ($measurementWindowSeconds -gt 0) {
                        $messageTotal / $measurementWindowSeconds
                    } else { 0.0 }
                }
                else {
                    $messageTotal = $succeeded
                    $targetPerSecond = $TcpConnections
                    $achievedPerSecond = $succeeded
                    $rateUnit = 'connections'
                }

                $completeP50Ms = [double](($tcpLoads |
                    Measure-Object P50Milliseconds -Maximum).Maximum)
                $completeP95Ms = [double](($tcpLoads |
                    Measure-Object P95Milliseconds -Maximum).Maximum)
                $completeP99Ms = [double](($tcpLoads |
                    Measure-Object P99Milliseconds -Maximum).Maximum)
                $historyP95Ms = 0
                $messageOutboxP95Ms = 0
                $receiptOutboxP95Ms = 0
            } else {
                $pipeline = $report.LoadResults |
                    Where-Object Name -eq 'pipeline' |
                    Select-Object -First 1
                if ($null -eq $pipeline) {
                    $errors = $report.Errors -join '; '
                    throw "Pipeline result was not created for rate $rate. $errors"
                }
                $detail = Get-Content -LiteralPath $pipeline.SourceReport -Raw | ConvertFrom-Json
                $measurementWindowSeconds = [double](Get-OptionalProperty $detail 'MeasurementSeconds' 0)
                if ($measurementWindowSeconds -le 0) {
                    $measurementWindowSeconds = [double](Get-OptionalProperty $detail 'ElapsedSeconds' $DurationSeconds)
                    $measurementWindowSource = 'pipeline report ElapsedSeconds'
                } else {
                    $measurementWindowSource = 'pipeline report MeasurementSeconds'
                }
                $targetPerSecond = $rate
                $succeeded = [long]$pipeline.Succeeded
                $failed = [long]$pipeline.Failed
                $messageTotal = $succeeded
                $errorRatePercent = [double]$pipeline.ErrorRatePercent
                $achievedPerSecond = if ($measurementWindowSeconds -gt 0) {
                    $succeeded / $measurementWindowSeconds
                } else { 0.0 }
                $completeP50Ms = [double]$pipeline.P50Milliseconds
                $completeP95Ms = [double]$pipeline.P95Milliseconds
                $completeP99Ms = [double]$pipeline.P99Milliseconds
                $historyP95Ms = [double]$detail.Latencies.history_query.P95Ms
                $messageOutboxP95Ms = [double]$detail.Latencies.message_persisted_outbox.P95Ms
                $receiptOutboxP95Ms = [double]$detail.Latencies.receipt_persisted_outbox.P95Ms
            }
            $pgResource = $report.DockerResources |
                Where-Object Container -eq $postgres |
                Select-Object -First 1
            $natsResource = $report.DockerResources |
                Where-Object Container -eq $nats |
                Select-Object -First 1
            $rtResource = $report.ProcessResources |
                Where-Object Label -eq 'realtime-1' |
                Select-Object -First 1
            if ($null -eq $pgResource -or $null -eq $natsResource -or $null -eq $rtResource) {
                throw "Resource summary was incomplete for rate $rate."
            }

            $unexpectedServiceExits = @($report.Processes | Where-Object {
                $_.Kind -in @('gateway','realtime') -and
                -not [bool](Get-OptionalProperty $_ 'StoppedByOrchestrator' $false) -and
                $null -ne $_.ExitCode
            })
            $validityGates.Add((New-ValidityGate `
                -Name 'service-lifetime' `
                -Passed ($unexpectedServiceExits.Count -eq 0) `
                -Actual ($unexpectedServiceExits | ForEach-Object { "$($_.Label):$($_.ExitCode)" }) `
                -Expected 'no service exits during measurement' `
                -Details 'Orchestrator cleanup exits (StoppedByOrchestrator=true), including Linux 137, are ignored'))

            $orchestratorDetails = if (@($report.Errors).Count -eq 0) {
                'No orchestrator errors were reported.'
            } else {
                $report.Errors -join '; '
            }
            $validityGates.Add((New-ValidityGate `
                -Name 'orchestrator-result' `
                -Passed ([bool]$report.Succeeded -and $orchestratorExit -eq 0) `
                -Actual "reportSucceeded=$($report.Succeeded); exitCode=$orchestratorExit" `
                -Expected 'reportSucceeded=true and exitCode=0' `
                -Details $orchestratorDetails))
            $benchmarkValidity = Get-OptionalProperty $report 'Validity' $null
            $benchmarkValidityPassed = $null -ne $benchmarkValidity -and
                [bool](Get-OptionalProperty $benchmarkValidity 'IsValid' $false)
            $invalidReasons = if ($null -ne $benchmarkValidity) {
                $reportedInvalidReasons = @(Get-OptionalProperty $benchmarkValidity 'InvalidReasons' @())
                if ($reportedInvalidReasons.Count -eq 0) {
                    'No benchmark invalid reasons were reported.'
                } else {
                    $reportedInvalidReasons -join '; '
                }
            } else { 'Benchmark report did not contain the Validity section.' }
            $validityGates.Add((New-ValidityGate `
                -Name 'benchmark-run-validity' `
                -Passed $benchmarkValidityPassed `
                -Actual $(if ($null -eq $benchmarkValidity) { 'missing' } else { $benchmarkValidity.IsValid }) `
                -Expected 'Validity.IsValid=true' `
                -Details $invalidReasons))
            $processMeasurementCoverage = if ($null -ne $benchmarkValidity) {
                [double](Get-OptionalProperty $benchmarkValidity 'ProcessSamplingCoveragePercent' 0)
            } else { 0.0 }
            $prometheusMeasurementCoverage = if ($null -ne $benchmarkValidity) {
                [double](Get-OptionalProperty $benchmarkValidity 'PrometheusSamplingCoveragePercent' 0)
            } else { 0.0 }
            $measurementSampleCoverage = [Math]::Min(
                $processMeasurementCoverage,
                $prometheusMeasurementCoverage)
            $validityGates.Add((New-ValidityGate `
                -Name 'measurement-sample-coverage' `
                -Passed ($measurementSampleCoverage -ge $MinimumResourceSampleCoveragePercent) `
                -Actual $measurementSampleCoverage `
                -Expected ">= $MinimumResourceSampleCoveragePercent percent" `
                -Details "Process=$processMeasurementCoverage%; Prometheus=$prometheusMeasurementCoverage%; uses the coordinated measurement boundary"))

            $targetAttainmentPercent = if ($targetPerSecond -gt 0) {
                100.0 * $achievedPerSecond / $targetPerSecond
            } else { 0.0 }
            if ($TcpMode -ne 'connection' -or -not $NoPipeline) {
                $validityGates.Add((New-ValidityGate `
                    -Name 'throughput-attainment' `
                    -Passed ($targetAttainmentPercent -ge $MinimumThroughputAttainmentPercent) `
                    -Actual $targetAttainmentPercent `
                    -Expected ">= $MinimumThroughputAttainmentPercent percent" `
                    -Details "Aggregate total $messageTotal over one $measurementWindowSeconds-second measurement window"))
            }

            $deadLetters = Get-MetricSum $report.MetricDeltas 'realtime_messages_dead_letters_total{'
            $validityGates.Add((New-ValidityGate `
                -Name 'dead-letters' `
                -Passed ($deadLetters -le $MaximumDeadLetters) `
                -Actual $deadLetters `
                -Expected "<= $MaximumDeadLetters" `
                -Details 'Sum of realtime_messages_dead_letters_total deltas across reasons'))

            $reportedResourceCoverage = @(
                Get-OptionalProperty $benchmarkValidity 'ResourceSamplingSeriesCoverage' @())
            $criticalResourceCoverage = @($reportedResourceCoverage | Where-Object {
                $_.Kind -eq 'docker' -or
                ($_.Kind -eq 'process' -and (
                    $_.Series -like 'gateway-*' -or
                    $_.Series -like 'realtime-*' -or
                    $_.Series -like 'tcp-*' -or
                    $_.Series -like 'pipeline-*'))
            })
            $expectedProcessSeriesCount = $report.Configuration.GatewayCount + 1 +
                $report.Configuration.GatewayCount +
                $(if ([bool]$report.Configuration.PipelineEnabled) { 1 } else { 0 })
            $expectedDockerSeriesCount = @($report.Configuration.DockerContainers).Count
            $expectedSeriesCount = $expectedProcessSeriesCount + $expectedDockerSeriesCount
            $minimumCoverage = if ($criticalResourceCoverage.Count -gt 0) {
                [double](($criticalResourceCoverage |
                    Measure-Object CoveragePercent -Minimum).Minimum)
            } else { 0.0 }
            $sampleCoveragePassed = $criticalResourceCoverage.Count -ge $expectedSeriesCount -and
                $minimumCoverage -ge $MinimumResourceSampleCoveragePercent
            $validityGates.Add((New-ValidityGate `
                -Name 'resource-sample-coverage' `
                -Passed $sampleCoveragePassed `
                -Actual $minimumCoverage `
                -Expected ">= $MinimumResourceSampleCoveragePercent percent for >= $expectedSeriesCount process/container series" `
                -Details "Observed $($criticalResourceCoverage.Count) series; each percentage is capped at 100 and counts only samples inside the coordinated measurement phase"))

            $runValid = @($validityGates | Where-Object -Property Passed -eq $false).Count -eq 0
            $results.Add([pscustomobject]@{
                TargetPerSecond = $targetPerSecond
                RateUnit = $rateUnit
                Passed = $runValid
                RunValid = $runValid
                Succeeded = $succeeded
                Failed = $failed
                ErrorRatePercent = $errorRatePercent
                AchievedPerSecond = $achievedPerSecond
                TargetAttainmentPercent = $targetAttainmentPercent
                MeasurementSeconds = $measurementWindowSeconds
                MeasurementWindowSource = $measurementWindowSource
                ChildMeasurements = @($childMeasurements)
                MaximumRampSeconds = $maximumRampSeconds
                MessageTotal = $messageTotal
                MessagesSent = $messagesSent
                MessagesExpectedDeliveries = $messagesExpectedDeliveries
                MessagesAcknowledged = $messagesAcknowledged
                MessagesRejected = $messagesRejected
                MessagesDelivered = $messagesDelivered
                ConnectionSuccessPercent = $connectionSuccessPercent
                PeakConnectionPercent = $peakConnectionPercent
                ActiveSenders = $activeSenders
                MessagesPerSecondPerActiveSender = $rateTcpMessagesPerSecond
                AcknowledgementPercent = $acknowledgementPercent
                DeliveryPercent = $deliveryPercent
                DeadLetters = $deadLetters
                MinimumMeasurementResourceSampleCoveragePercent = $minimumCoverage
                ValidityGates = $validityGates.ToArray()
                CompleteP50Ms = $completeP50Ms
                CompleteP95Ms = $completeP95Ms
                CompleteP99Ms = $completeP99Ms
                HistoryP95Ms = $historyP95Ms
                MessageOutboxP95Ms = $messageOutboxP95Ms
                ReceiptOutboxP95Ms = $receiptOutboxP95Ms
                JetStreamPendingFinal = Get-MetricSum $report.MetricsAfter 'chatapp_jetstream_pending{'
                OutboxPendingFinal = Get-MetricSum $report.MetricsAfter 'realtime_outbox_pending{'
                OutboxOldestAgeSecondsFinal = Get-MetricSum $report.MetricsAfter 'realtime_outbox_oldest_age_seconds{'
                PostgresAverageCpuPercent = [double]$pgResource.AverageCpuPercent
                PostgresMaximumCpuPercent = [double]$pgResource.MaximumCpuPercent
                NatsAverageCpuPercent = [double]$natsResource.AverageCpuPercent
                RealtimeAverageCpuPercent = [double]$rtResource.AverageCpuPercent
                Report = $reportFile.FullName
            })
        }
        catch {
            $runErrors.Add($_.Exception.Message)
            $results.Add([pscustomobject]@{
                TargetPerSecond = $rate
                Passed = $false
                RunValid = $false
                Error = $_.Exception.Message
            })
            Write-Warning "Rate $rate failed: $($_.Exception.Message)"
        }
        finally {
            if ($created.Count -gt 0) {
                $containerDiagnostics = Join-Path $rateDirectory 'container-diagnostics'
                foreach ($container in $created) {
                    if (Test-PerformanceDockerContainerOwnership `
                            -Name $container -RunId $dockerRunId) {
                        Save-PerformanceContainerDiagnostics `
                            -Container $container `
                            -Directory $containerDiagnostics
                    }
                }
                # These containers use anonymous data volumes. Remove only the
                # containers created by this rate and their attached anonymous
                # volumes; otherwise every soak leaks the PostgreSQL/NATS/Garnet
                # data set even though the containers themselves are gone.
                Remove-PerformanceDockerContainers `
                    -Names @($created) -RunId $dockerRunId -RemoveVolumes
            }
        }
        if ($abortCurve) {
            Write-Warning 'Capacity curve aborted after dependency preflight failure; no later rates will be attempted.'
            break
        }
    }
}
finally {
    [Environment]::SetEnvironmentVariable($dbEnvName, $oldDbEnv, 'Process')
    [Environment]::SetEnvironmentVariable($garnetEnvName, $oldGarnetEnv, 'Process')
}

$completedAt = [DateTimeOffset]::UtcNow
$runValid = $results.Count -eq $Rates.Count -and
    @($results | Where-Object { -not [bool]$_.RunValid }).Count -eq 0
$summary = [pscustomobject]@{
    StartedAtUtc = $startedAt
    CompletedAtUtc = $completedAt
    RunValid = $runValid
    AbortedAfterDependencyPreflight = $abortCurve
    Configuration = [pscustomobject]@{
        Rates = $Rates
        DurationSeconds = $DurationSeconds
        WarmupSeconds = $WarmupSeconds
        StabilizationSeconds = $WarmupSeconds
        PipelineConcurrency = $PipelineConcurrency
        PipelinePayloadBytes = $PipelinePayloadBytes
        RealtimeProcessingConcurrency = $RealtimeProcessingConcurrency
        TcpConnections = $TcpConnections
        TcpActiveSenders = $effectiveTcpActiveSenders
        TcpCrossGateway = [bool]$TcpCrossGateway
        TcpMode = $TcpMode
        TcpMessagesPerSecond = $TcpMessagesPerSecond
        TcpRateDrivenByRates = [bool]$tcpRateDrivenByRates
        TcpDeliveryDrainSeconds = $TcpDeliveryDrainSeconds
        TcpInactiveHeartbeatSeconds = $TcpInactiveHeartbeatSeconds
        TcpPayloadBytes = $TcpPayloadBytes
        TcpConnectionsPerSecond = $TcpConnectionsPerSecond
        NoPipeline = [bool]$NoPipeline
        RateModel = if ($tcpRateDrivenByRates) {
            'aggregate Rates target distributed across active TCP senders; bounded periodic pacing'
        } else {
            'bounded closed-loop pacing'
        }
        InboundTransportMode = $InboundTransportMode
        OutboundSendMode = $OutboundSendMode
        OnDemandSendWorkerCount = $OnDemandSendWorkerCount
        OnDemandSendBurstLimit = $OnDemandSendBurstLimit
        NatsImage = $NatsImage
        PostgresImage = $PostgresImage
        GarnetImage = $GarnetImage
        GarnetLuaEnabled = $true
        DependencyStartupTimeoutSeconds = $DependencyStartupTimeoutSeconds
        Gates = [pscustomobject]@{
            MinimumConnectionSuccessPercent = $MinimumConnectionSuccessPercent
            MinimumPeakConnectionPercent = $MinimumPeakConnectionPercent
            MinimumThroughputAttainmentPercent = $MinimumThroughputAttainmentPercent
            MinimumAcknowledgementPercent = $MinimumAcknowledgementPercent
            MinimumDeliveryPercent = $MinimumDeliveryPercent
            MinimumResourceSampleCoveragePercent = $MinimumResourceSampleCoveragePercent
            MaximumDeadLetters = $MaximumDeadLetters
        }
    }
    Results = $results
    Errors = $runErrors.ToArray()
}
$jsonPath = Join-Path $curveDirectory 'capacity-curve-report.json'
$markdownPath = Join-Path $curveDirectory 'capacity-curve-report.md'
[IO.File]::WriteAllText($jsonPath, ($summary | ConvertTo-Json -Depth 12),
    [Text.UTF8Encoding]::new($false))

$lines = [Collections.Generic.List[string]]::new()
$lines.Add($(if ($NoPipeline) { '# TCP capacity run' } else { '# Pipeline capacity curve' }))
$lines.Add('')
$lines.Add("Window: $($startedAt.ToString('O')) - $($completedAt.ToString('O'))")
$lines.Add('')
$lines.Add("Run validity: **$(if ($runValid) { 'VALID' } else { 'INVALID' })**")
$lines.Add('')
$peerRouting = if ($TcpCrossGateway) { 'cross-gateway' } else { 'same-gateway' }
$rateModelText = if ($tcpRateDrivenByRates) {
    'aggregate Rates target distributed across active TCP senders; bounded periodic pacing'
} else {
    'bounded closed-loop pacing'
}
$lines.Add("Rate model: $rateModelText; concurrency=$PipelineConcurrency; stabilization=$($WarmupSeconds)s; requested measurement=$($DurationSeconds)s; TCP connection ramp=$TcpConnectionsPerSecond/s; active senders=$effectiveTcpActiveSenders/$TcpConnections; peer routing=$peerRouting.")
$lines.Add('')
$lines.Add('| Target | Unit | Achieved | Attainment | Conn success | Peak conns | Ack | Delivery | DLQ | Samples | Valid |')
$lines.Add('|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|')
foreach ($result in $results) {
    if ($null -ne $result.psobject.Properties['Error']) {
        $lines.Add("| $($result.TargetPerSecond) | - | - | - | - | - | - | - | - | - | INVALID |")
        $lines.Add('')
        $lines.Add("- Error: $($result.Error)")
        continue
    }
    $lines.Add([string]::Format(
        [Globalization.CultureInfo]::InvariantCulture,
        '| {0} | {1} | {2:F2} | {3:F1}% | {4:F1}% | {5:F1}% | {6:F1}% | {7:F1}% | {8:F0} | {9:F1}% | {10} |',
        $result.TargetPerSecond,$result.RateUnit,$result.AchievedPerSecond,
        $result.TargetAttainmentPercent,$result.ConnectionSuccessPercent,
        $result.PeakConnectionPercent,$result.AcknowledgementPercent,
        $result.DeliveryPercent,$result.DeadLetters,
        $result.MinimumMeasurementResourceSampleCoveragePercent,
        $(if ($result.RunValid) { 'VALID' } else { 'INVALID' })))
    $failedGates = @($result.ValidityGates | Where-Object -Property Passed -eq $false)
    foreach ($gate in $failedGates) {
        $lines.Add("- Failed gate ``$($gate.Name)``: actual=$($gate.Actual); expected=$($gate.Expected). $($gate.Details)")
    }
}
$lines.Add('')
$lines.Add('Each rate uses fresh temporary NATS, PostgreSQL, and Garnet (`--lua`) containers. NATS JetStream and Garnet PING/write-read/EVAL are verified before the benchmark starts. Bounded container diagnostics are retained before cleanup.')
[IO.File]::WriteAllLines($markdownPath, $lines, [Text.UTF8Encoding]::new($false))
Write-Host "Capacity curve JSON: $jsonPath"
Write-Host "Capacity curve Markdown: $markdownPath"
$capacityExitCode = if ($runValid) { 0 } else { 1 }
Write-CapacityManifest `
    -State 'completed' `
    -ExitCode $capacityExitCode `
    -RunValid $runValid `
    -RunResults $results.ToArray() `
    -RunErrors $runErrors.ToArray()
# Retained for interactive callers. Run-Soak uses InvocationManifestPath and
# never infers the run from stdout ordering.
Write-Output $curveDirectory
exit $capacityExitCode
