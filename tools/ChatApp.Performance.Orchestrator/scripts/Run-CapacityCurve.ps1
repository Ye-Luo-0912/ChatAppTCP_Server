param(
    [int[]] $Rates = @(40, 80, 120, 160, 200),
    [ValidateRange(1, 86400)] [int] $DurationSeconds = 60,
    [ValidateRange(0, 3600)] [int] $WarmupSeconds = 10,
    [ValidateRange(1, 1024)] [int] $PipelineConcurrency = 32,
    [ValidateRange(1, 1048576)] [int] $PipelinePayloadBytes = 512,
    [ValidateRange(1, 100000)] [int] $TcpConnections = 1000,
    [ValidateSet('connection','heartbeat','chat')] [string] $TcpMode = 'connection',
    [ValidateRange(1, 100000)] [int] $TcpMessagesPerSecond = 10,
    [ValidateRange(1, 1048576)] [int] $TcpPayloadBytes = 128,
    [int] $TcpSlowReaders = 0,
    [ValidateSet('header','payload')] [string] $TcpSlowlorisPhase = '',
    [ValidateRange(1, 60000)] [int] $TcpSlowlorisDelayMs = 1000,
    [ValidateRange(1, 1099511627776)] [long] $TcpInboundBudgetBytes = 0,
    [int] $GatewayBasePort = 18888,
    [int] $RealtimePort = 18080,
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
    [string] $ReportDirectory,
    [switch] $NoPipeline,
    [switch] $SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if ($Rates.Count -eq 0 -or $Rates.Where({ $_ -le 0 }).Count -ne 0) {
    throw 'Rates must contain one or more positive integers.'
}

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

if (-not $SkipBuild) {
    & dotnet build (Join-Path $repositoryRoot 'ChatApp.TcpGateway.sln') -c Release --no-restore
    if ($LASTEXITCODE -ne 0) { throw 'Gateway solution build failed.' }
    & dotnet build (Join-Path $realtimeRoot 'ChatApp.RealtimeServices.slnx') -c Release --no-restore
    if ($LASTEXITCODE -ne 0) { throw 'Realtime solution build failed.' }
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
        $tag = "$($stamp.Replace('-','').ToLowerInvariant())-$rate"
        $nats = "codex-chatapp-capacity-nats-$tag"
        $postgres = "codex-chatapp-capacity-postgres-$tag"
        $garnet = "codex-chatapp-capacity-garnet-$tag"
        $created = [Collections.Generic.List[string]]::new()
        $rateDirectory = Join-Path $curveDirectory "rate-$rate"
        [IO.Directory]::CreateDirectory($rateDirectory) | Out-Null

        Write-Host "Starting target $rate pipeline/s..."
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

            $orchestratorArgs = @(
                'run','--project',$orchestratorProject,'-c','Release','--no-build','--',
                '--no-build','--gateway-count','2',
                '--gateway-base-port',"$GatewayBasePort",
                '--realtime-port',"$RealtimePort",
                '--nats-url',"nats://127.0.0.1:$NatsPort",
                '--warmup-seconds',"$WarmupSeconds",
                '--duration-seconds',"$DurationSeconds",
                '--sample-interval-ms','2000',
                '--tcp-mode',"$TcpMode",
                '--tcp-connections',"$TcpConnections",
                '--tcp-messages-per-second',"$TcpMessagesPerSecond",
                '--tcp-payload-bytes',"$TcpPayloadBytes",
                '--tcp-slow-readers',"$TcpSlowReaders",
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
            & dotnet @orchestratorArgs
            $orchestratorExit = $LASTEXITCODE

            $reportFile = Get-ChildItem -LiteralPath $rateDirectory -Filter 'benchmark-report.json' -Recurse |
                Sort-Object LastWriteTimeUtc -Descending |
                Select-Object -First 1
            if ($null -eq $reportFile) {
                throw "Benchmark report was not created for rate $rate."
            }
            $report = Get-Content -LiteralPath $reportFile.FullName -Raw | ConvertFrom-Json
            if ($NoPipeline) {
                $tcpLoads = @($report.LoadResults |
                    Where-Object Kind -like 'tcp-*')
                if ($tcpLoads.Count -eq 0) {
                    $errors = $report.Errors -join '; '
                    throw "TCP result was not created for rate $rate. $errors"
                }
                $targetPerSecond = $TcpConnections * $TcpMessagesPerSecond
                $succeeded = [long](($tcpLoads | Measure-Object Succeeded -Sum).Sum)
                $failed = [long](($tcpLoads | Measure-Object Failed -Sum).Sum)
                $errorRatePercent = if ($succeeded + $failed -eq 0) {
                    0
                } else {
                    100.0 * $failed / ($succeeded + $failed)
                }
                $achievedPerSecond = [double](($tcpLoads |
                    Measure-Object ThroughputPerSecond -Sum).Sum)
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
                $targetPerSecond = $rate
                $succeeded = [long]$pipeline.Succeeded
                $failed = [long]$pipeline.Failed
                $errorRatePercent = [double]$pipeline.ErrorRatePercent
                $achievedPerSecond = [double]$pipeline.ThroughputPerSecond
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

            $results.Add([pscustomobject]@{
                TargetPerSecond = $targetPerSecond
                Passed = [bool]$report.Succeeded -and $orchestratorExit -eq 0
                Succeeded = $succeeded
                Failed = $failed
                ErrorRatePercent = $errorRatePercent
                AchievedPerSecond = $achievedPerSecond
                TargetAttainmentPercent = 100.0 * $achievedPerSecond / $targetPerSecond
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
            $results.Add([pscustomobject]@{
                TargetPerSecond = $rate
                Passed = $false
                Error = $_.Exception.Message
            })
            Write-Warning "Rate $rate failed: $($_.Exception.Message)"
        }
        finally {
            if ($created.Count -gt 0) {
                & docker rm -f @($created) | Out-Null
                if ($LASTEXITCODE -ne 0) {
                    Write-Warning "Temporary container cleanup failed for rate $rate."
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
        Rates = $Rates
        DurationSeconds = $DurationSeconds
        WarmupSeconds = $WarmupSeconds
        PipelineConcurrency = $PipelineConcurrency
        PipelinePayloadBytes = $PipelinePayloadBytes
        TcpConnections = $TcpConnections
        TcpMode = $TcpMode
        TcpMessagesPerSecond = $TcpMessagesPerSecond
        TcpPayloadBytes = $TcpPayloadBytes
        NoPipeline = [bool]$NoPipeline
        RateModel = 'bounded closed-loop pacing'
        InboundTransportMode = $InboundTransportMode
        OutboundSendMode = $OutboundSendMode
        OnDemandSendWorkerCount = $OnDemandSendWorkerCount
        OnDemandSendBurstLimit = $OnDemandSendBurstLimit
        NatsImage = $NatsImage
        PostgresImage = $PostgresImage
        GarnetImage = $GarnetImage
    }
    Results = $results
}
$jsonPath = Join-Path $curveDirectory 'capacity-curve-report.json'
$markdownPath = Join-Path $curveDirectory 'capacity-curve-report.md'
[IO.File]::WriteAllText($jsonPath, ($summary | ConvertTo-Json -Depth 8),
    [Text.UTF8Encoding]::new($false))

$lines = [Collections.Generic.List[string]]::new()
$lines.Add($(if ($NoPipeline) { '# TCP capacity run' } else { '# Pipeline capacity curve' }))
$lines.Add('')
$lines.Add("Window: $($startedAt.ToString('O')) - $($completedAt.ToString('O'))")
$lines.Add('')
$lines.Add("Rate model: bounded closed-loop pacing; concurrency=$PipelineConcurrency; warmup=$($WarmupSeconds)s; measurement=$($DurationSeconds)s.")
$lines.Add('')
$lines.Add('| Target/s | Achieved/s | Attainment | Success | Failed | p95 ms | p99 ms | History p95 | PostgreSQL avg CPU | JS pending | Outbox pending |')
$lines.Add('|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|')
foreach ($result in $results) {
    if ($null -ne $result.psobject.Properties['Error']) {
        $lines.Add("| $($result.TargetPerSecond) | - | - | - | - | - | - | - | - | - | - |")
        continue
    }
    $lines.Add([string]::Format(
        [Globalization.CultureInfo]::InvariantCulture,
        '| {0} | {1:F2} | {2:F1}% | {3} | {4} | {5:F1} | {6:F1} | {7:F1} | {8:F1}% | {9:F0} | {10:F0} |',
        $result.TargetPerSecond,$result.AchievedPerSecond,
        $result.TargetAttainmentPercent,$result.Succeeded,$result.Failed,
        $result.CompleteP95Ms,$result.CompleteP99Ms,$result.HistoryP95Ms,
        $result.PostgresAverageCpuPercent,$result.JetStreamPendingFinal,
        $result.OutboxPendingFinal))
}
$lines.Add('')
$lines.Add('Each rate uses fresh temporary NATS, PostgreSQL, and Garnet containers. Containers are removed in a finally block.')
[IO.File]::WriteAllLines($markdownPath, $lines, [Text.UTF8Encoding]::new($false))
Write-Host "Capacity curve JSON: $jsonPath"
Write-Host "Capacity curve Markdown: $markdownPath"
if ($results.Where({ -not $_.Passed }).Count -ne 0) { exit 1 }
