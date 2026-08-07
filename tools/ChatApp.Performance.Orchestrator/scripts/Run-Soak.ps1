<#
.SYNOPSIS
    8~24 小时 Soak 测试：长时间运行 + 内存稳定性监控。
    委托 Run-CapacityCurve.ps1 执行长时负载，随后对报告中的 Gateway 进程
    Working Set 做稳定性分析，验证「24h 后内存进入稳定平台」这一门禁3 检查点。

.DESCRIPTION
    默认 DirectSocket + PersistentSendLoop + BoundedChannel（当前默认组合）。
    可通过 -TcpMode heartbeat/chat + -TcpPayloadBytes 做真实聊天负载浸泡。

    内存稳定性判定（门禁3）：
      - 每个 Gateway 进程单独判定，不聚合（避免某一进程异常被整体掩盖）。
      - 排除连接爬坡和 post-ramp stabilization 后建立基线。
      - 对每个进程的 Working Set 时序做分窗分析：
        基线中位数 = 实际测量窗口前段中位数；最终窗口 = 末尾分位窗口。
        最终窗口斜率 = 末尾窗口线性回归斜率（MiB/小时）。
      - 每个进程须满足：最终窗口斜率 <= -FinalSlopeMiBPerHour 且
        最终窗口 vs 基线中位数增长占比 <= -MemoryGrowthThresholdPercent，才判为 STABLE。
      - 同时检查 PID 稳定性与进程是否中途退出（编排器主动清理除外）。
      - 分析仅限当前 Run 生成并上报的报告目录，绝不递归扫描整个 Artifact 目录取"最新文件"。
      - RunValid、MemoryConclusive、MemoryStable 分开输出；业务门禁失败时内存只能是 INCONCLUSIVE。
      - StoppedByOrchestrator=true 的清理退出（Linux 常见 137）不算测量期崩溃。

.PARAMETER DurationSeconds
    浸泡时长（秒）。默认 28800（8 小时）。上限 86400（24 小时）。

.PARAMETER WarmupSeconds
    全部连接建立后的稳定期（秒）。默认 300；不计入正式测量。

.PARAMETER TcpMode
    TCP 负载模式：connection / heartbeat / chat。默认 connection。

.PARAMETER TcpConnections
    TCP 连接数。默认 1000。

.PARAMETER TcpActiveSenders
    heartbeat/chat 下主动发送的连接总数。0 表示所有非慢读连接；可用它将长连接规模与持久消息速率解耦。

.PARAMETER TcpMessagesPerSecond
    chat/heartbeat 下每个主动发送连接每秒消息数。默认 5。

.PARAMETER TcpDeliveryDrainSeconds
    chat 测量停止发送后继续接收 ACK/投递的最长时间；不计入吞吐测量窗口。默认 30 秒。

.PARAMETER TcpInactiveHeartbeatSeconds
    chat 下非主动发送连接的 keepalive 间隔；仅维持认证会话，不计入 durable chat 吞吐。默认 30 秒。

.PARAMETER TcpPayloadBytes
    chat 负载字节数。默认 512。

.PARAMETER TcpSlowReaders
    慢消费者数（chat 下）。默认 0。

.PARAMETER MemoryGrowthThresholdPercent
    最终窗口 vs 基线中位数增长占比阈值（%）。默认 20。

.PARAMETER FinalSlopeMiBPerHour
    最终窗口斜率阈值（MiB/小时）。默认 30，即最终窗口内存增长速率须 <= 30 MiB/小时才算进入平台。

.PARAMETER FinalWindowFraction
    分位窗口占正式测量样本比例。默认 0.2（首尾各取 20%）。

.PARAMETER ReportDirectory
    报告输出目录。默认 .artifacts\performance。

.EXAMPLE
    .\Run-Soak.ps1 -DurationSeconds 28800 -TcpMode chat -TcpPayloadBytes 512
    .\Run-Soak.ps1 -DurationSeconds 86400 -MemoryGrowthThresholdPercent 15 -FinalSlopeMiBPerHour 20
#>
param(
    [ValidateRange(300, 86400)] [int] $DurationSeconds = 28800,
    [ValidateRange(0, 3600)] [int] $WarmupSeconds = 300,
    [ValidateSet('connection', 'heartbeat', 'chat')] [string] $TcpMode = 'connection',
    [ValidateRange(1, 100000)] [int] $TcpConnections = 1000,
    [ValidateRange(0, 100000)] [int] $TcpActiveSenders = 0,
    [ValidateRange(0.001, 100000)] [double] $TcpMessagesPerSecond = 5,
    [ValidateRange(0, 3600)] [int] $TcpDeliveryDrainSeconds = 30,
    [ValidateRange(0, 3600)] [int] $TcpInactiveHeartbeatSeconds = 30,
    [ValidateRange(1, 1048576)] [int] $TcpPayloadBytes = 512,
    [ValidateRange(0, 10000)] [int] $TcpSlowReaders = 0,
    [ValidateRange(0, 100000)] [int] $TcpConnectionsPerSecond = 500,
    [ValidateRange(1, 1024)] [int] $RealtimeProcessingConcurrency = 4,
    [ValidateRange(1, 100)] [int] $MemoryGrowthThresholdPercent = 20,
    [ValidateRange(0, 1000)] [double] $FinalSlopeMiBPerHour = 30,
    [ValidateRange(0.05, 0.5)] [double] $FinalWindowFraction = 0.2,
    [ValidateRange(0, 100)] [double] $MinimumConnectionSuccessPercent = 99,
    [ValidateRange(0, 100)] [double] $MinimumPeakConnectionPercent = 99,
    [ValidateRange(0, 100)] [double] $MinimumThroughputAttainmentPercent = 90,
    [ValidateRange(0, 100)] [double] $MinimumAcknowledgementPercent = 99,
    [ValidateRange(0, 100)] [double] $MinimumDeliveryPercent = 95,
    [ValidateRange(0, 100)] [double] $MinimumResourceSampleCoveragePercent = 90,
    [ValidateRange(0, 9223372036854775807)] [long] $MaximumDeadLetters = 0,
    [ValidateSet('Pipelines','DirectSocket')] [string] $InboundTransportMode = 'DirectSocket',
    [ValidateSet('PersistentSendLoop','OnDemandSendPump','PerSessionDrain')] [string] $OutboundSendMode = 'PersistentSendLoop',
    [ValidateSet('BoundedChannel','LazySegmented')] [string] $OutboundQueueMode = 'BoundedChannel',
    [int] $OnDemandSendWorkerCount = 0,
    [int] $OnDemandSendBurstLimit = 16,
    [string] $ReportDirectory,
    [switch] $SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$snapshotBindingRequired = [Environment]::GetEnvironmentVariable(
    'CHATAPP_BENCHMARK_REQUIRE_SNAPSHOT_BINDING')
if ($snapshotBindingRequired -notin @('1', 'true', 'True', 'TRUE')) {
    throw 'Formal soak requires CHATAPP_BENCHMARK_REQUIRE_SNAPSHOT_BINDING=true.'
}
$requiredSnapshotVariables = @(
    'CHATAPP_BENCHMARK_RUN_ID',
    'CHATAPP_BENCHMARK_RUN_ROOT',
    'CHATAPP_BENCHMARK_SOURCE_ARCHIVE_PATH',
    'CHATAPP_BENCHMARK_SOURCE_ARCHIVE_SHA256',
    'CHATAPP_BENCHMARK_CANONICAL_FEED_ARCHIVE_PATH',
    'CHATAPP_BENCHMARK_CANONICAL_FEED_ARCHIVE_SHA256',
    'CHATAPP_BENCHMARK_DOTNET_PATH',
    'CHATAPP_BENCHMARK_DOTNET_SHA256'
)
$missingSnapshotVariables = @($requiredSnapshotVariables.Where({
    [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($_))
}))
if ($missingSnapshotVariables.Count -ne 0) {
    throw "Formal soak snapshot binding is incomplete: $($missingSnapshotVariables -join ', ')."
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

$capacityScript = Join-Path $PSScriptRoot 'Run-CapacityCurve.ps1'
if (-not (Test-Path -LiteralPath $capacityScript -PathType Leaf)) {
    throw "Capacity-curve runner was not found: $capacityScript"
}

$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..\..'))
if ([string]::IsNullOrWhiteSpace($ReportDirectory)) {
    $ReportDirectory = Join-Path $repositoryRoot '.artifacts\performance'
}
$ReportDirectory = [IO.Path]::GetFullPath($ReportDirectory)
[IO.Directory]::CreateDirectory($ReportDirectory) | Out-Null

$dependencyHelpers = Join-Path $PSScriptRoot 'Performance-DependencyPreflight.ps1'
if (-not (Test-Path -LiteralPath $dependencyHelpers -PathType Leaf)) {
    throw "Dependency/environment preflight helpers were not found: $dependencyHelpers"
}
. $dependencyHelpers
$openFileLimitPreflight = Get-PerformanceOpenFileLimitPreflight `
    -TcpConnections $TcpConnections `
    -GatewayCount 2 `
    -SafetyMargin 1024
$soakEnvironmentPreflightPath = Join-Path $ReportDirectory "soak-environment-preflight-$([DateTimeOffset]::UtcNow.ToString("yyyyMMdd-HHmmss'Z'")).json"
Write-PerformanceJsonArtifact -Path $soakEnvironmentPreflightPath -Value ([pscustomobject]@{
    TcpConnections = $TcpConnections
    GatewayCount = 2
    OpenFiles = $openFileLimitPreflight
})
if (-not $openFileLimitPreflight.Passed) {
    throw "Linux open-file soft limit is insufficient: actual=$($openFileLimitPreflight.SoftLimit), required>=$($openFileLimitPreflight.RequiredSoftLimit). $($openFileLimitPreflight.Recommendation) Preflight: $soakEnvironmentPreflightPath"
}

Write-Host "Starting soak run: duration=$DurationSeconds s; mode=$TcpMode; conns=$TcpConnections; activeSenders=$TcpActiveSenders (0=all eligible); rate=$TcpMessagesPerSecond/s per sender; drain=$TcpDeliveryDrainSeconds s; inactiveHeartbeat=$TcpInactiveHeartbeatSeconds s; realtimeConcurrency=$RealtimeProcessingConcurrency; payload=$TcpPayloadBytes B; send=$InboundTransportMode+$OutboundSendMode+$OutboundQueueMode."
Write-Host 'The generated benchmark report includes process, Docker, GC/heap, Npgsql connection, JetStream and Outbox trends.'

$arguments = @{
    Rates = @(1)
    DurationSeconds = $DurationSeconds
    WarmupSeconds = $WarmupSeconds
    TcpConnections = $TcpConnections
    TcpActiveSenders = $effectiveTcpActiveSenders
    TcpMode = $TcpMode
    TcpMessagesPerSecond = $TcpMessagesPerSecond
    TcpDeliveryDrainSeconds = $TcpDeliveryDrainSeconds
    TcpInactiveHeartbeatSeconds = $TcpInactiveHeartbeatSeconds
    TcpPayloadBytes = $TcpPayloadBytes
    TcpSlowReaders = $TcpSlowReaders
    TcpConnectionsPerSecond = $TcpConnectionsPerSecond
    RealtimeProcessingConcurrency = $RealtimeProcessingConcurrency
    MinimumConnectionSuccessPercent = $MinimumConnectionSuccessPercent
    MinimumPeakConnectionPercent = $MinimumPeakConnectionPercent
    MinimumThroughputAttainmentPercent = $MinimumThroughputAttainmentPercent
    MinimumAcknowledgementPercent = $MinimumAcknowledgementPercent
    MinimumDeliveryPercent = $MinimumDeliveryPercent
    MinimumResourceSampleCoveragePercent = $MinimumResourceSampleCoveragePercent
    MaximumDeadLetters = $MaximumDeadLetters
    InboundTransportMode = $InboundTransportMode
    OutboundSendMode = $OutboundSendMode
    OutboundQueueMode = $OutboundQueueMode
    OnDemandSendWorkerCount = $OnDemandSendWorkerCount
    OnDemandSendBurstLimit = $OnDemandSendBurstLimit
    ReportDirectory = $ReportDirectory
    NoPipeline = $true
}
$invocationDirectory = Join-Path $ReportDirectory 'run-invocations'
[IO.Directory]::CreateDirectory($invocationDirectory) | Out-Null
$invocationManifestPath = Join-Path $invocationDirectory "soak-$([Guid]::NewGuid().ToString('N')).json"
$arguments.InvocationManifestPath = $invocationManifestPath
if ($SkipBuild) {
    $arguments.SkipBuild = $true
}

$capacityInvocationError = $null
try {
    & $capacityScript @arguments | Out-Host
    $curveExit = $LASTEXITCODE
}
catch {
    $capacityInvocationError = $_.Exception.Message
    $curveExit = 1
}

# Capacity writes this caller-selected manifest before build/container work.
# This remains deterministic even when the child exits early or emits native stdout.
if (-not (Test-Path -LiteralPath $invocationManifestPath -PathType Leaf)) {
    Write-Error "Run-CapacityCurve.ps1 did not create its invocation manifest: $invocationManifestPath"
    exit 1
}
$invocationManifest = Get-Content -LiteralPath $invocationManifestPath -Raw | ConvertFrom-Json
$curveDirectory = [IO.Path]::GetFullPath([string]$invocationManifest.RunDirectory)
if (-not (Test-Path -LiteralPath $curveDirectory -PathType Container)) {
    Write-Error "Current run output directory does not exist: $curveDirectory"
    exit 1
}

$capacityReportFile = Join-Path $curveDirectory 'capacity-curve-report.json'
$capacityReport = if (Test-Path -LiteralPath $capacityReportFile -PathType Leaf) {
    Get-Content -LiteralPath $capacityReportFile -Raw | ConvertFrom-Json
} else { $null }
$capacityResult = if ($null -ne $capacityReport) {
    @($capacityReport.Results) | Select-Object -First 1
} else { $null }
$runValid = $null -ne $capacityReport -and
    [bool](Get-OptionalProperty $capacityReport 'RunValid' $false) -and
    $null -ne $capacityResult -and
    [bool](Get-OptionalProperty $capacityResult 'RunValid' $false) -and
    $curveExit -eq 0
$runValidityGates = if ($null -ne $capacityResult) {
    @(Get-OptionalProperty $capacityResult 'ValidityGates' @())
} else { @() }
$measurementSeconds = if ($null -ne $capacityResult) {
    [double](Get-OptionalProperty $capacityResult 'MeasurementSeconds' 0)
} else { 0.0 }
$maximumRampSeconds = if ($null -ne $capacityResult) {
    [double](Get-OptionalProperty $capacityResult 'MaximumRampSeconds' 0)
} else { 0.0 }
$memoryAnalysisOffsetSeconds = $maximumRampSeconds + $WarmupSeconds
$memoryAnalysisOffsetSource = 'max(child RampSeconds) + configured post-ramp stabilization; additionally tail-bounded by actual MeasurementSeconds'

$reportFile = Get-ChildItem -LiteralPath $curveDirectory -Filter 'benchmark-report.json' -Recurse |
    Sort-Object LastWriteTimeUtc -Descending |
    Select-Object -First 1
$report = if ($null -ne $reportFile) {
    Get-Content -LiteralPath $reportFile.FullName -Raw | ConvertFrom-Json
} else { $null }
$gateways = if ($null -ne $report) {
    @($report.ProcessResources | Where-Object Label -like 'gateway-*')
} else { @() }

$stopwatchFrequency = [Diagnostics.Stopwatch]::Frequency
$sampleIntervalSeconds = if ($null -ne $report) {
    [double](Get-OptionalProperty $report.Configuration 'SampleIntervalMilliseconds' 2000) / 1000.0
} else { 2.0 }
if ($measurementSeconds -le 0 -and $null -ne $report) {
    $measurementSeconds = [double](Get-OptionalProperty $report.Configuration 'MeasurementSeconds' 0)
}

function Get-Median([double[]] $Values) {
    if ($Values.Count -eq 0) { return 0.0 }
    $sorted = @($Values | Sort-Object)
    $count = $sorted.Count
    $mid = [int][Math]::Floor($count / 2)
    if ($count % 2 -eq 1) { return [double]$sorted[$mid] }
    return ([double]$sorted[$mid - 1] + [double]$sorted[$mid]) / 2.0
}

function Get-SlopeBytesPerSecond([object[]] $Samples) {
    $sorted = @($Samples | Sort-Object TimestampTicks)
    $n = $sorted.Count
    if ($n -lt 2) { return 0.0 }
    $t0 = [long]$sorted[0].TimestampTicks
    $x = [double[]]($sorted | ForEach-Object { ([long]$_.TimestampTicks - $t0) / [double]$stopwatchFrequency })
    $y = [double[]]($sorted | ForEach-Object { [long]$_.WorkingSetBytes })
    $xbar = ($x | Measure-Object -Average).Average
    $ybar = ($y | Measure-Object -Average).Average
    $num = 0.0; $den = 0.0
    for ($i = 0; $i -lt $n; $i++) {
        $dx = $x[$i] - $xbar
        $num += $dx * ($y[$i] - $ybar)
        $den += $dx * $dx
    }
    if ($den -eq 0) { return 0.0 }
    return $num / $den
}

# Read only this run's process timeline. The last actual measurement window is
# selected by Stopwatch ticks, so ramp and post-ramp stabilization are excluded.
$timelineCsv = Get-ChildItem -LiteralPath $curveDirectory -Filter 'process-resource-timeline.csv' -Recurse |
    Sort-Object LastWriteTimeUtc -Descending |
    Select-Object -First 1
$timelineSamples = [Collections.Generic.List[object]]::new()
if ($null -ne $timelineCsv) {
    $csvLines = Get-Content -LiteralPath $timelineCsv.FullName | Select-Object -Skip 1
    foreach ($line in $csvLines) {
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        $parts = $line.Split(',')
        if ($parts.Count -lt 4) { continue }
        $timelineSamples.Add([pscustomobject]@{
            Label = $parts[0]
            Pid = [int]$parts[1]
            TimestampTicks = [long]$parts[2]
            WorkingSetBytes = [long]$parts[3]
        })
    }
}

$perProcess = [Collections.Generic.List[object]]::new()
$restartDetected = $false
$restartReasons = [Collections.Generic.List[string]]::new()
$reportedMeasurementResourceCoverage = if ($null -ne $report) {
    @(Get-OptionalProperty $report.Validity 'ResourceSamplingSeriesCoverage' @())
} else { @() }

# A service exit is unexpected only when it was not initiated by the
# orchestrator. Managed cleanup exit 137 on Linux is therefore not a restart.
if ($null -ne $report) {
    foreach ($process in $report.Processes) {
        if ($process.Label -notlike 'gateway-*' -and $process.Label -notlike 'realtime-*') { continue }
        $stoppedByOrchestrator = [bool](Get-OptionalProperty $process 'StoppedByOrchestrator' $false)
        if ($null -ne $process.ExitCode -and -not $stoppedByOrchestrator) {
            $restartDetected = $true
            $restartReasons.Add("$($process.Label) exited during the run with code $($process.ExitCode)")
        }
    }
}

foreach ($label in ($gateways | Select-Object -ExpandProperty Label)) {
    $summary = $gateways | Where-Object Label -eq $label | Select-Object -First 1
    $samples = @($timelineSamples | Where-Object Label -eq $label | Sort-Object TimestampTicks)

    $measurementSamples = @()
    if ($samples.Count -gt 0 -and $measurementSeconds -gt 0) {
        $lastTick = [long]$samples[-1].TimestampTicks
        $tailMeasurementStartTick = $lastTick - [long]($measurementSeconds * $stopwatchFrequency)
        $phaseMeasurementStartTick = [long]$samples[0].TimestampTicks +
            [long]($memoryAnalysisOffsetSeconds * $stopwatchFrequency)
        $measurementStartTick = [Math]::Max($tailMeasurementStartTick, $phaseMeasurementStartTick)
        $measurementSamples = @($samples | Where-Object {
            [long]$_.TimestampTicks -ge $measurementStartTick
        })
    }
    $expectedSamples = if ($sampleIntervalSeconds -gt 0 -and $measurementSeconds -gt 0) {
        [Math]::Max(1.0, $measurementSeconds / $sampleIntervalSeconds)
    } else { 0.0 }
    $reportedSeriesCoverage = $reportedMeasurementResourceCoverage |
        Where-Object { $_.Kind -eq 'process' -and $_.Series -eq $label } |
        Select-Object -First 1
    $sampleCoveragePercent = if ($null -ne $reportedSeriesCoverage) {
        [Math]::Clamp([double]$reportedSeriesCoverage.CoveragePercent, 0.0, 100.0)
    } elseif ($expectedSamples -gt 0) {
        [Math]::Min(100.0, 100.0 * $measurementSamples.Count / $expectedSamples)
    } else { 0.0 }
    $dataComplete = $measurementSamples.Count -ge 4 -and
        $sampleCoveragePercent -ge $MinimumResourceSampleCoveragePercent

    $baselineMedian = $null
    $finalMedian = $null
    $growthBytes = $null
    $growthFraction = $null
    $slopeMiBPerHour = $null
    $growthOk = $null
    $slopeOk = $null
    $stableCandidate = $null
    if ($dataComplete) {
        $n = $measurementSamples.Count
        $windowSize = [Math]::Max(4, [int][Math]::Floor($n * $FinalWindowFraction))
        $baseline = @($measurementSamples[0..([Math]::Min($windowSize - 1, $n - 1))])
        $final = @($measurementSamples[([Math]::Max(0, $n - $windowSize))..($n - 1)])

        $baselineMedian = Get-Median ([double[]]($baseline.WorkingSetBytes))
        $finalMedian = Get-Median ([double[]]($final.WorkingSetBytes))
        $growthBytes = $finalMedian - $baselineMedian
        $growthFraction = if ($baselineMedian -gt 0) { 100.0 * $growthBytes / $baselineMedian } else { 100.0 }
        $slopeBps = Get-SlopeBytesPerSecond $final
        $slopeMiBPerHour = $slopeBps * 3600.0 / 1MB
        $growthOk = $growthFraction -le $MemoryGrowthThresholdPercent
        $slopeOk = $slopeMiBPerHour -le $FinalSlopeMiBPerHour
        $stableCandidate = $growthOk -and $slopeOk
    }

    $processConclusive = $runValid -and $dataComplete -and -not $restartDetected
    $stableProcess = if ($processConclusive) { [bool]$stableCandidate } else { $null }

    $perProcess.Add([pscustomobject]@{
        Label = $label
        Pid = [int]$summary.ProcessId
        Samples = $samples.Count
        MeasurementSamples = $measurementSamples.Count
        SampleCoveragePercent = $sampleCoveragePercent
        DataComplete = $dataComplete
        Conclusive = $processConclusive
        BaselineMedianMiB = if ($null -eq $baselineMedian) { $null } else { $baselineMedian / 1MB }
        FinalMedianMiB = if ($null -eq $finalMedian) { $null } else { $finalMedian / 1MB }
        GrowthMiB = if ($null -eq $growthBytes) { $null } else { $growthBytes / 1MB }
        GrowthPercent = $growthFraction
        FinalSlopeMiBPerHour = $slopeMiBPerHour
        GrowthOk = $growthOk
        SlopeOk = $slopeOk
        Stable = $stableProcess
    })
}

$expectedGatewayCount = if ($null -ne $report) {
    [int](Get-OptionalProperty $report.Configuration 'GatewayCount' 2)
} else { 2 }
$memoryDataComplete = $perProcess.Count -eq $expectedGatewayCount -and
    @($perProcess | Where-Object -Property DataComplete -eq $false).Count -eq 0
$memoryConclusive = $runValid -and $memoryDataComplete -and -not $restartDetected
$memoryStable = if ($memoryConclusive) {
    @($perProcess | Where-Object -Property Stable -eq $false).Count -eq 0
} else { $null }
$overallSucceeded = $runValid -and $memoryConclusive -and [bool]$memoryStable
$memoryStatus = if (-not $memoryConclusive) { 'INCONCLUSIVE' } elseif ($memoryStable) { 'STABLE' } else { 'UNSTABLE' }
$overallStatus = if (-not $runValid) { 'INVALID' } elseif ($overallSucceeded) { 'PASSED' } else { 'FAILED' }

$verdict = Join-Path $ReportDirectory "soak-verdict-$([DateTimeOffset]::UtcNow.ToString("yyyyMMdd-HHmmss'Z'")).json"
$verdictObj = [pscustomobject]@{
    StartedAtUtc = if ($null -ne $report) { $report.StartedAtUtc } else { $invocationManifest.StartedAtUtc }
    CompletedAtUtc = if ($null -ne $report) { $report.CompletedAtUtc } else { [DateTimeOffset]::UtcNow }
    OverallStatus = $overallStatus
    OverallSucceeded = $overallSucceeded
    RunValid = $runValid
    RunValidityGates = $runValidityGates
    MemoryConclusive = $memoryConclusive
    MemoryStatus = $memoryStatus
    MemoryStable = $memoryStable
    DurationSeconds = $DurationSeconds
    MeasurementSeconds = $measurementSeconds
    StabilizationSeconds = $WarmupSeconds
    MaximumChildRampSeconds = $maximumRampSeconds
    MemoryAnalysisOffsetSeconds = $memoryAnalysisOffsetSeconds
    MemoryAnalysisOffsetSource = $memoryAnalysisOffsetSource
    MeasurementStartedAtUtc = if ($null -ne $report) {
        Get-OptionalProperty (Get-OptionalProperty $report 'Validity' $null) 'MeasurementStartedAtUtc' $null
    } else { $null }
    TcpMode = $TcpMode
    TcpConnections = $TcpConnections
    TcpActiveSenders = $effectiveTcpActiveSenders
    TcpMessagesPerSecond = $TcpMessagesPerSecond
    TcpDeliveryDrainSeconds = $TcpDeliveryDrainSeconds
    TcpInactiveHeartbeatSeconds = $TcpInactiveHeartbeatSeconds
    TcpConnectionsPerSecond = $TcpConnectionsPerSecond
    RealtimeProcessingConcurrency = $RealtimeProcessingConcurrency
    RestartDetected = $restartDetected
    RestartReasons = $restartReasons.ToArray()
    GrowthThresholdPercent = $MemoryGrowthThresholdPercent
    FinalSlopeMiBPerHourThreshold = $FinalSlopeMiBPerHour
    FinalWindowFraction = $FinalWindowFraction
    ProcessVerdicts = $perProcess.ToArray()
    CurveDirectory = $curveDirectory
    InvocationManifest = $invocationManifestPath
    CapacityReport = if ($null -ne $capacityReport) { $capacityReportFile } else { $null }
    BenchmarkReport = if ($null -ne $reportFile) { $reportFile.FullName } else { $null }
    Timeline = if ($null -ne $timelineCsv) { $timelineCsv.FullName } else { $null }
    CapacityExitCode = $curveExit
    CapacityInvocationError = $capacityInvocationError
    EnvironmentPreflight = $soakEnvironmentPreflightPath
}
Write-PerformanceJsonArtifact -Path $verdict -Value $verdictObj -Depth 12

$tee = [Collections.Generic.List[string]]::new()
$tee.Add('# TCP soak verdict')
$tee.Add('')
$tee.Add("Overall: **$overallStatus**; run validity: **$(if ($runValid) { 'VALID' } else { 'INVALID' })**; memory: **$memoryStatus**.")
$tee.Add('')
$tee.Add("Requested measurement=$DurationSeconds s; actual measurement=$measurementSeconds s; post-ramp stabilization=$WarmupSeconds s; delivery drain=$TcpDeliveryDrainSeconds s; inactive heartbeat=$TcpInactiveHeartbeatSeconds s; mode=$TcpMode; active senders=$effectiveTcpActiveSenders/$TcpConnections at $TcpMessagesPerSecond msg/s each; connection ramp=$TcpConnectionsPerSecond/s.")
$tee.Add('')
$tee.Add("Memory analysis offset=$memoryAnalysisOffsetSeconds s ($memoryAnalysisOffsetSource).")
$tee.Add('')
$tee.Add('RunValid、MemoryConclusive、MemoryStable 独立判定。负载或正确性门禁失败时，内存结论固定为 INCONCLUSIVE。')
$tee.Add('')
$failedRunGates = @($runValidityGates | Where-Object -Property Passed -eq $false)
if ($failedRunGates.Count -gt 0) {
    $tee.Add('## Failed run-validity gates')
    $tee.Add('')
    foreach ($gate in $failedRunGates) {
        $tee.Add("- ``$($gate.Name)``: actual=$($gate.Actual); expected=$($gate.Expected). $($gate.Details)")
    }
    $tee.Add('')
}
$tee.Add('每个 Gateway 使用统一实际测量窗口内的样本单独判定；连接爬坡和稳定期不进入内存斜率。')
$tee.Add('')
$tee.Add('| Process | Coverage | Base Median MiB | Final Median MiB | Growth % | Final Slope MiB/h | Conclusion |')
$tee.Add('|---|---:|---:|---:|---:|---:|---|')
foreach ($p in $perProcess) {
    $baseText = if ($null -eq $p.BaselineMedianMiB) { 'n/a' } else { $p.BaselineMedianMiB.ToString('F2', [Globalization.CultureInfo]::InvariantCulture) }
    $finalText = if ($null -eq $p.FinalMedianMiB) { 'n/a' } else { $p.FinalMedianMiB.ToString('F2', [Globalization.CultureInfo]::InvariantCulture) }
    $growthText = if ($null -eq $p.GrowthPercent) { 'n/a' } else { $p.GrowthPercent.ToString('F1', [Globalization.CultureInfo]::InvariantCulture) + '%' }
    $slopeText = if ($null -eq $p.FinalSlopeMiBPerHour) { 'n/a' } else { $p.FinalSlopeMiBPerHour.ToString('F2', [Globalization.CultureInfo]::InvariantCulture) }
    $processStatus = if (-not $p.Conclusive) { 'INCONCLUSIVE' } elseif ($p.Stable) { 'STABLE' } else { 'UNSTABLE' }
    $tee.Add("| $($p.Label) | $($p.SampleCoveragePercent.ToString('F1', [Globalization.CultureInfo]::InvariantCulture))% | $baseText | $finalText | $growthText | $slopeText | $processStatus |")
}
$tee.Add('')
$tee.Add("- Minimum resource sample coverage: $MinimumResourceSampleCoveragePercent% (data complete: $memoryDataComplete)")
$tee.Add("- Growth threshold: <= $MemoryGrowthThresholdPercent%; final-window slope threshold: <= $FinalSlopeMiBPerHour MiB/h")
$tee.Add("- Service lifetime: $($(if ($restartDetected) { $restartReasons -join '; ' } else { 'OK; orchestrator-managed cleanup exits ignored' }))")
$tee.Add('')
$verdictMd = [IO.Path]::ChangeExtension($verdict, '.md')
[IO.File]::WriteAllLines($verdictMd, $tee, [Text.UTF8Encoding]::new($false))

Write-Host "Soak verdict: $($verdictMd)"
foreach ($p in $perProcess) {
    $status = if (-not $p.Conclusive) { 'INCONCLUSIVE' } elseif ($p.Stable) { 'STABLE' } else { 'UNSTABLE' }
    Write-Host ("  {0}: measurement samples={1}, coverage={2:F1}% -> {3}" -f $p.Label, $p.MeasurementSamples, $p.SampleCoveragePercent, $status)
}
if ($restartDetected) {
    Write-Host "Process restart detected: $($restartReasons -join '; ')" -ForegroundColor Red
}
if ($overallSucceeded) {
    Write-Host 'Soak: PASSED (run valid; memory conclusive and stable).' -ForegroundColor Green
    exit 0
}
if (-not $runValid) {
    Write-Host 'Soak: INVALID; memory result is INCONCLUSIVE.' -ForegroundColor Red
} elseif (-not $memoryConclusive) {
    Write-Host 'Soak: FAILED; memory result is INCONCLUSIVE because diagnostic coverage is incomplete.' -ForegroundColor Red
} else {
    Write-Host 'Soak: FAILED; memory is conclusively UNSTABLE.' -ForegroundColor Red
}
exit 1
