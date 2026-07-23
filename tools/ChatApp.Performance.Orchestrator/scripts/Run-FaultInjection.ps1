param(
    [ValidateSet('Nats', 'Postgres', 'Garnet')]
    [string[]] $Targets = @('Nats', 'Postgres', 'Garnet'),
    [ValidateRange(1, 3600)] [int] $FaultAfterSeconds = 20,
    [ValidateRange(1, 300)] [int] $FaultDurationSeconds = 10,
    [ValidateRange(5, 3600)] [int] $RecoveryWindowSeconds = 60,
    [ValidateRange(1, 10000)] [int] $PipelineOperationsPerSecond = 80,
    [ValidateRange(1, 1024)] [int] $PipelineConcurrency = 32,
    [ValidateRange(1, 1048576)] [int] $PipelinePayloadBytes = 512,
    [ValidateRange(1, 100000)] [int] $TcpConnections = 1000,
    [int] $GatewayBasePort = 18888,
    [int] $RealtimePort = 18080,
    [int] $NatsPort = 4222,
    [int] $NatsMonitorPort = 18222,
    [int] $PostgresPort = 15432,
    [int] $GarnetPort = 16379,
    [string] $NatsImage = 'nats:2.10.26-alpine',
    [string] $PostgresImage = 'postgres:16.8',
    [string] $GarnetImage = 'ghcr.io/microsoft/garnet:1.0.84',
    [string] $ReportDirectory,
    [switch] $SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

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
$runDirectory = Join-Path $ReportDirectory "fault-injection-$stamp"
[IO.Directory]::CreateDirectory($runDirectory) | Out-Null

if (-not $SkipBuild) {
    & dotnet build (Join-Path $repositoryRoot 'ChatApp.TcpGateway.sln') -c Release --no-restore
    if ($LASTEXITCODE -ne 0) { throw 'Gateway solution build failed.' }
    & dotnet build (Join-Path $realtimeRoot 'ChatApp.RealtimeServices.slnx') -c Release --no-restore
    if ($LASTEXITCODE -ne 0) { throw 'Realtime solution build failed.' }
}

$dbEnvName = 'CHATAPP_FAULT_INJECTION_DB'
$garnetEnvName = 'CHATAPP_FAULT_INJECTION_GARNET'
$oldDbEnv = [Environment]::GetEnvironmentVariable($dbEnvName, 'Process')
$oldGarnetEnv = [Environment]::GetEnvironmentVariable($garnetEnvName, 'Process')
$password = 'fault-' + [Guid]::NewGuid().ToString('N')
[Environment]::SetEnvironmentVariable(
    $dbEnvName,
    "Host=127.0.0.1;Port=$PostgresPort;Database=ChatAppDatabase;Username=postgres;Password=$password;Pooling=true;Minimum Pool Size=0;Maximum Pool Size=100;Connection Idle Lifetime=300;Timeout=3;Command Timeout=5;",
    'Process')
[Environment]::SetEnvironmentVariable(
    $garnetEnvName, "127.0.0.1:$GarnetPort,abortConnect=false", 'Process')

$results = [Collections.Generic.List[object]]::new()

function Invoke-Docker([string[]] $Arguments) {
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker $($Arguments[0]) failed with exit code $LASTEXITCODE."
    }
}

function Wait-Postgres([string] $Container) {
    for ($attempt = 1; $attempt -le 30; $attempt++) {
        & docker exec $Container pg_isready -U postgres -d ChatAppDatabase *> $null
        if ($LASTEXITCODE -eq 0) { return }
        Start-Sleep -Seconds 1
    }
    throw "PostgreSQL did not become ready: $Container"
}

function Wait-LoadStarted([Diagnostics.Process] $Process, [string] $OutputPath) {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(120)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        if ($Process.HasExited) {
            throw "Orchestrator exited before load started (exit $($Process.ExitCode))."
        }
        if ((Test-Path -LiteralPath $OutputPath) -and
            (Select-String -LiteralPath $OutputPath -Pattern 'Starting load generators...' -SimpleMatch -Quiet)) {
            return
        }
        Start-Sleep -Milliseconds 500
    }
    throw 'Timed out waiting for the load phase.'
}

function Get-ReadySample([string] $Url) {
    $sampledAt = [DateTimeOffset]::UtcNow
    try {
        $response = Invoke-WebRequest -Uri $Url -TimeoutSec 4 -SkipHttpErrorCheck
        $body = $null
        if (-not [string]::IsNullOrWhiteSpace($response.Content)) {
            $body = $response.Content | ConvertFrom-Json
        }
        return [pscustomobject]@{
            SampledAtUtc = $sampledAt
            StatusCode = [int]$response.StatusCode
            IsReady = [int]$response.StatusCode -eq 200
            Dependencies = $body.dependencies
            Error = $null
        }
    }
    catch {
        return [pscustomobject]@{
            SampledAtUtc = $sampledAt
            StatusCode = 0
            IsReady = $false
            Dependencies = $null
            Error = $_.Exception.Message
        }
    }
}

function Get-MetricValue([string] $Metrics, [string] $Prefix) {
    $sum = 0.0
    foreach ($line in ($Metrics -split "`n")) {
        if (-not $line.StartsWith($Prefix, [StringComparison]::Ordinal)) { continue }
        $parts = $line.Trim() -split '\s+'
        if ($parts.Count -ge 2) {
            $parsed = 0.0
            if ([double]::TryParse(
                    $parts[-1],
                    [Globalization.NumberStyles]::Float,
                    [Globalization.CultureInfo]::InvariantCulture,
                    [ref]$parsed)) {
                $sum += $parsed
            }
        }
    }
    return $sum
}

function Get-BacklogSample([string] $MetricsUrl) {
    try {
        $metrics = (Invoke-WebRequest -Uri $MetricsUrl -TimeoutSec 4).Content
        return [pscustomobject]@{
            JetStreamPending = Get-MetricValue $metrics 'chatapp_jetstream_pending{'
            OutboxPending = Get-MetricValue $metrics 'realtime_outbox_pending{'
            OutboxOldestAgeSeconds = Get-MetricValue $metrics 'realtime_outbox_oldest_age_seconds{'
        }
    }
    catch {
        return [pscustomobject]@{
            JetStreamPending = [double]::NaN
            OutboxPending = [double]::NaN
            OutboxOldestAgeSeconds = [double]::NaN
        }
    }
}

try {
    foreach ($target in $Targets) {
        $tag = "$($stamp.Replace('-','').ToLowerInvariant())-$($target.ToLowerInvariant())"
        $nats = "codex-chatapp-fault-nats-$tag"
        $postgres = "codex-chatapp-fault-postgres-$tag"
        $garnet = "codex-chatapp-fault-garnet-$tag"
        $containers = [ordered]@{ Nats = $nats; Postgres = $postgres; Garnet = $garnet }
        $created = [Collections.Generic.List[string]]::new()
        $scenarioDirectory = Join-Path $runDirectory $target.ToLowerInvariant()
        [IO.Directory]::CreateDirectory($scenarioDirectory) | Out-Null
        $stdoutPath = Join-Path $scenarioDirectory 'orchestrator.stdout.log'
        $stderrPath = Join-Path $scenarioDirectory 'orchestrator.stderr.log'
        $timeline = [Collections.Generic.List[object]]::new()
        $process = $null

        Write-Host "Starting $target fault scenario..."
        try {
            Invoke-Docker @('run','-d','--name',$nats,
                '-p',"127.0.0.1:$($NatsPort):4222",
                '-p',"127.0.0.1:$($NatsMonitorPort):8222",
                $NatsImage,'-js','-m','8222')
            $created.Add($nats)
            Invoke-Docker @('run','-d','--name',$postgres,
                '-e',"POSTGRES_PASSWORD=$password",
                '-e','POSTGRES_DB=ChatAppDatabase',
                '-p',"127.0.0.1:$($PostgresPort):5432",$PostgresImage)
            $created.Add($postgres)
            Invoke-Docker @('run','-d','--name',$garnet,
                '-p',"127.0.0.1:$($GarnetPort):6379",$GarnetImage)
            $created.Add($garnet)
            Wait-Postgres $postgres

            $durationSeconds = $FaultAfterSeconds + $FaultDurationSeconds + $RecoveryWindowSeconds
            $arguments = @(
                'run','--project',$orchestratorProject,'-c','Release','--no-build','--',
                '--no-build','--gateway-count','2',
                '--gateway-base-port',"$GatewayBasePort",
                '--realtime-port',"$RealtimePort",
                '--nats-url',"nats://127.0.0.1:$NatsPort",
                '--warmup-seconds','5',
                '--duration-seconds',"$durationSeconds",
                '--sample-interval-ms','1000',
                '--tcp-mode','connection','--tcp-connections',"$TcpConnections",
                '--pipeline-concurrency',"$PipelineConcurrency",
                '--pipeline-operations-per-second',"$PipelineOperationsPerSecond",
                '--pipeline-payload-bytes',"$PipelinePayloadBytes",
                '--pipeline-operation-timeout-seconds',"$([Math]::Max(30, $FaultDurationSeconds + 30))",
                '--pipeline-base-user-id',"$(9300000000 + ($results.Count * 10000000))",
                '--realtime-database-environment',$dbEnvName,
                '--garnet-environment',$garnetEnvName,
                '--docker-container',$nats,
                '--docker-container',$postgres,
                '--docker-container',$garnet,
                '--report-directory',$scenarioDirectory)
            $process = Start-Process -FilePath 'dotnet' -ArgumentList $arguments -NoNewWindow -PassThru `
                -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
            Wait-LoadStarted $process $stdoutPath
            Start-Sleep -Seconds $FaultAfterSeconds

            $readyUrl = "http://127.0.0.1:$RealtimePort/ready"
            $metricsUrl = "http://127.0.0.1:$RealtimePort/metrics"
            $before = Get-BacklogSample $metricsUrl
            $faultStartedAt = [DateTimeOffset]::UtcNow
            $faultAction = if ($target -eq 'Nats') { 'pause/unpause' } else { 'stop/start' }
            if ($target -eq 'Nats') {
                Invoke-Docker @('pause',$containers[$target])
            }
            else {
                Invoke-Docker @('stop','--timeout','1',$containers[$target])
            }
            $faultDeadline = $faultStartedAt.AddSeconds($FaultDurationSeconds)
            while ([DateTimeOffset]::UtcNow -lt $faultDeadline) {
                $timeline.Add((Get-ReadySample $readyUrl))
                $remainingMs = ($faultDeadline - [DateTimeOffset]::UtcNow).TotalMilliseconds
                if ($remainingMs -gt 0) {
                    Start-Sleep -Milliseconds ([Math]::Min(1000, [int]$remainingMs))
                }
            }
            $restartStartedAt = [DateTimeOffset]::UtcNow
            if ($target -eq 'Nats') {
                Invoke-Docker @('unpause',$containers[$target])
            }
            else {
                Invoke-Docker @('start',$containers[$target])
            }
            if ($target -eq 'Postgres') { Wait-Postgres $postgres }

            $readyRecoveredAt = $null
            $convergedAt = $null
            $after = $null
            $deadline = [DateTimeOffset]::UtcNow.AddSeconds($RecoveryWindowSeconds)
            while ([DateTimeOffset]::UtcNow -lt $deadline -and -not $process.HasExited) {
                $ready = Get-ReadySample $readyUrl
                $timeline.Add($ready)
                if ($null -eq $readyRecoveredAt -and $ready.IsReady) {
                    $readyRecoveredAt = $ready.SampledAtUtc
                }
                $after = Get-BacklogSample $metricsUrl
                if ($ready.IsReady -and
                    -not [double]::IsNaN($after.JetStreamPending) -and
                    $after.JetStreamPending -le $before.JetStreamPending -and
                    $after.OutboxPending -le [Math]::Max(16, $before.OutboxPending)) {
                    $convergedAt = [DateTimeOffset]::UtcNow
                    break
                }
                Start-Sleep -Seconds 1
            }

            $process.WaitForExit()
            $reportFile = Get-ChildItem -LiteralPath $scenarioDirectory -Filter 'benchmark-report.json' -Recurse |
                Sort-Object LastWriteTimeUtc -Descending | Select-Object -First 1
            $report = if ($null -eq $reportFile) { $null } else {
                Get-Content -LiteralPath $reportFile.FullName -Raw | ConvertFrom-Json
            }
            $pipeline = if ($null -eq $report) { $null } else {
                $report.LoadResults | Where-Object Name -eq 'pipeline' | Select-Object -First 1
            }
            if ($null -eq $after) { $after = Get-BacklogSample $metricsUrl }
            $passed = $null -ne $pipeline -and [long]$pipeline.Failed -eq 0 -and
                $null -ne $readyRecoveredAt -and $null -ne $convergedAt
            $results.Add([pscustomobject]@{
                Target = $target
                FaultAction = $faultAction
                Passed = $passed
                FaultStartedAtUtc = $faultStartedAt
                RestartStartedAtUtc = $restartStartedAt
                ReadyRecoverySeconds = if ($null -eq $readyRecoveredAt) { $null } else {
                    ($readyRecoveredAt - $restartStartedAt).TotalSeconds
                }
                ConvergenceSeconds = if ($null -eq $convergedAt) { $null } else {
                    ($convergedAt - $restartStartedAt).TotalSeconds
                }
                Succeeded = if ($null -eq $pipeline) { 0 } else { [long]$pipeline.Succeeded }
                Failed = if ($null -eq $pipeline) { 0 } else { [long]$pipeline.Failed }
                ThroughputPerSecond = if ($null -eq $pipeline) { 0 } else { [double]$pipeline.ThroughputPerSecond }
                P95Milliseconds = if ($null -eq $pipeline) { 0 } else { [double]$pipeline.P95Milliseconds }
                P99Milliseconds = if ($null -eq $pipeline) { 0 } else { [double]$pipeline.P99Milliseconds }
                JetStreamPendingBefore = $before.JetStreamPending
                JetStreamPendingFinal = $after.JetStreamPending
                OutboxPendingBefore = $before.OutboxPending
                OutboxPendingFinal = $after.OutboxPending
                Timeline = $timeline
                Report = if ($null -eq $reportFile) { $null } else { $reportFile.FullName }
                OrchestratorExitCode = $process.ExitCode
            })
        }
        catch {
            $results.Add([pscustomobject]@{
                Target = $target
                Passed = $false
                Error = $_.Exception.Message
            })
            Write-Warning "$target scenario failed: $($_.Exception.Message)"
        }
        finally {
            if ($null -ne $process -and -not $process.HasExited) {
                Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
                $process.WaitForExit()
            }
            if ($created.Count -gt 0) {
                & docker rm -f @($created) | Out-Null
                if ($LASTEXITCODE -ne 0) {
                    Write-Warning "Temporary container cleanup failed for $target."
                }
            }
        }
    }
}
finally {
    [Environment]::SetEnvironmentVariable($dbEnvName, $oldDbEnv, 'Process')
    [Environment]::SetEnvironmentVariable($garnetEnvName, $oldGarnetEnv, 'Process')
}

$completedAt = [DateTimeOffset]::UtcNow
$summary = [pscustomobject]@{
    StartedAtUtc = $startedAt
    CompletedAtUtc = $completedAt
    Configuration = [pscustomobject]@{
        Targets = $Targets
        FaultAfterSeconds = $FaultAfterSeconds
        FaultDurationSeconds = $FaultDurationSeconds
        RecoveryWindowSeconds = $RecoveryWindowSeconds
        PipelineOperationsPerSecond = $PipelineOperationsPerSecond
        PipelineConcurrency = $PipelineConcurrency
        PipelinePayloadBytes = $PipelinePayloadBytes
        TcpConnections = $TcpConnections
        NatsImage = $NatsImage
        PostgresImage = $PostgresImage
        GarnetImage = $GarnetImage
    }
    Results = $results
}
$jsonPath = Join-Path $runDirectory 'fault-injection-report.json'
$markdownPath = Join-Path $runDirectory 'fault-injection-report.md'
[IO.File]::WriteAllText($jsonPath, ($summary | ConvertTo-Json -Depth 12),
    [Text.UTF8Encoding]::new($false))

$lines = [Collections.Generic.List[string]]::new()
$lines.Add('# Dependency fault injection')
$lines.Add('')
$lines.Add("Window: $($startedAt.ToString('O')) - $($completedAt.ToString('O'))")
$lines.Add('')
$lines.Add('| Target | Passed | Ready recovery s | Convergence s | Success | Failed | Throughput/s | p95 ms | p99 ms | JS pending final | Outbox final |')
$lines.Add('|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|')
foreach ($result in $results) {
    if ($null -ne $result.psobject.Properties['Error']) {
        $lines.Add("| $($result.Target) | false | - | - | - | - | - | - | - | - | - |")
        continue
    }
    $lines.Add([string]::Format(
        [Globalization.CultureInfo]::InvariantCulture,
        '| {0} | {1} | {2} | {3} | {4} | {5} | {6:F2} | {7:F1} | {8:F1} | {9:F0} | {10:F0} |',
        $result.Target,$result.Passed,$result.ReadyRecoverySeconds,$result.ConvergenceSeconds,
        $result.Succeeded,$result.Failed,$result.ThroughputPerSecond,
        $result.P95Milliseconds,$result.P99Milliseconds,
        $result.JetStreamPendingFinal,$result.OutboxPendingFinal))
}
$lines.Add('')
$lines.Add('Each scenario uses fresh temporary dependency containers. The selected container is stopped during active load and restarted before convergence is measured.')
[IO.File]::WriteAllLines($markdownPath, $lines, [Text.UTF8Encoding]::new($false))
Write-Host "Fault injection JSON: $jsonPath"
Write-Host "Fault injection Markdown: $markdownPath"
if ($results.Where({ -not $_.Passed }).Count -ne 0) { exit 1 }
