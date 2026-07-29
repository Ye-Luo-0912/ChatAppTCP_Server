<#
.SYNOPSIS
    三出站发送模式 A/B 对比：PersistentSendLoop / OnDemandSendPump / PerSessionDrain。
    复用 Run-CapacityCurve.ps1 执行单速率基准，对比吞吐/p95/p99/WorkingSet/GC。

.PARAMETER DurationSeconds
    每个模式运行时长（秒）。默认 60。

.PARAMETER WarmupSeconds
    预热时长（秒）。默认 10。

.PARAMETER TcpConnections
    TCP 连接数。默认 1000。

.PARAMETER TcpMessagesPerSecond
    每连接消息速率。默认 20。

.PARAMETER TcpPayloadBytes
    负载字节。默认 512。

.PARAMETER TcpMode
    TCP 负载模式：heartbeat / chat / connection。默认 heartbeat。

.PARAMETER SkipBuild
    跳过构建（首次运行仍构建）。

.PARAMETER Scenario
    场景预设：default / idle-10k / large-payload / slow-consumer / activity-mix。
    预设覆盖默认参数，便于门禁矩阵快速执行。

.EXAMPLE
    .\Run-OutboundSendModeAB.ps1 -Scenario default
    .\Run-OutboundSendModeAB.ps1 -Scenario idle-10k -DurationSeconds 120
    .\Run-OutboundSendModeAB.ps1 -Scenario large-payload
    .\Run-OutboundSendModeAB.ps1 -Scenario slow-consumer
    .\Run-OutboundSendModeAB.ps1 -Scenario activity-mix
#>
param(
    [ValidateRange(15, 7200)] [int] $DurationSeconds = 60,
    [ValidateRange(0, 3600)] [int] $WarmupSeconds = 10,
    [ValidateRange(1, 100000)] [int] $TcpConnections = 1000,
    [ValidateRange(1, 100000)] [int] $TcpMessagesPerSecond = 20,
    [ValidateRange(1, 1048576)] [int] $TcpPayloadBytes = 512,
    [ValidateSet('heartbeat','chat','connection')] [string] $TcpMode = 'heartbeat',
    [int] $TcpSlowReaders = 0,
    [string] $ReportDirectory,
    [switch] $SkipBuild,
    [ValidateSet('default','idle-10k','large-payload','slow-consumer','activity-mix')]
    [string] $Scenario = 'default'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# 场景预设：覆盖默认参数，对应性能门禁矩阵。
# idle-10k: 10k 空闲连接，零消息，验证 per-connection 内存与空闲开销。
# large-payload: 64KiB 大包，验证大帧编解码与出站预算。
# slow-consumer: 慢消费者，验证背压与连接保护。
# activity-mix: 10k 连接 + 10% 活跃（1000 连接 × 20 msg/s），验证混合负载。
switch ($Scenario) {
    'idle-10k' {
        if ($TcpConnections -eq 1000) { $TcpConnections = 10000 }
        if ($TcpMessagesPerSecond -eq 20) { $TcpMessagesPerSecond = 1 }
        if ($DurationSeconds -eq 60) { $DurationSeconds = 120 }
    }
    'large-payload' {
        if ($TcpPayloadBytes -eq 512) { $TcpPayloadBytes = 65536 }
        if ($TcpConnections -eq 1000) { $TcpConnections = 500 }
        if ($TcpMessagesPerSecond -eq 20) { $TcpMessagesPerSecond = 5 }
    }
    'slow-consumer' {
        if ($TcpSlowReaders -eq 0) { $TcpSlowReaders = 5 }
        if ($DurationSeconds -eq 60) { $DurationSeconds = 90 }
    }
    'activity-mix' {
        if ($TcpConnections -eq 1000) { $TcpConnections = 10000 }
        if ($TcpMessagesPerSecond -eq 20) { $TcpMessagesPerSecond = 2 }
        if ($DurationSeconds -eq 60) { $DurationSeconds = 180 }
    }
}

$capacityScript = Join-Path $PSScriptRoot 'Run-CapacityCurve.ps1'
if (-not (Test-Path -LiteralPath $capacityScript -PathType Leaf)) {
    throw "Capacity-curve runner was not found: $capacityScript"
}

$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..\..'))
if ([string]::IsNullOrWhiteSpace($ReportDirectory)) {
    $ReportDirectory = Join-Path $repositoryRoot '.artifacts\performance'
}
$startedAt = [DateTimeOffset]::UtcNow
$stamp = $startedAt.ToString("yyyyMMdd-HHmmss'Z'")
$abDirectory = Join-Path ([IO.Path]::GetFullPath($ReportDirectory)) "outbound-ab-${Scenario}-$stamp"
[IO.Directory]::CreateDirectory($abDirectory) | Out-Null

$results = [Collections.Generic.List[object]]::new()
$modes = @('PersistentSendLoop', 'OnDemandSendPump', 'PerSessionDrain')

for ($index = 0; $index -lt $modes.Count; $index++) {
    $mode = $modes[$index]
    $modeDirectory = Join-Path $abDirectory $mode
    Write-Host "Running outbound send mode $mode (scenario: $Scenario)..."

    $arguments = @{
        Rates = @(1)
        DurationSeconds = $DurationSeconds
        WarmupSeconds = $WarmupSeconds
        TcpConnections = $TcpConnections
        TcpMode = $TcpMode
        TcpMessagesPerSecond = $TcpMessagesPerSecond
        TcpPayloadBytes = $TcpPayloadBytes
        OutboundSendMode = $mode
        ReportDirectory = $modeDirectory
        NoPipeline = $true
    }
    if ($TcpSlowReaders -gt 0) {
        $arguments.TcpSlowReaders = $TcpSlowReaders
    }
    if ($SkipBuild -or $index -gt 0) {
        $arguments.SkipBuild = $true
    }

    & $capacityScript @arguments
    $runnerExitCode = $LASTEXITCODE
    $reportFile = Get-ChildItem -LiteralPath $modeDirectory -Filter 'benchmark-report.json' -Recurse |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1
    if ($null -eq $reportFile) {
        throw "Benchmark report was not created for outbound mode $mode."
    }

    $report = Get-Content -LiteralPath $reportFile.FullName -Raw | ConvertFrom-Json
    $tcpLoads = @($report.LoadResults | Where-Object { $_.Kind -like 'tcp-*' })
    $gateways = @($report.ProcessResources | Where-Object Label -like 'gateway-*')
    if ($tcpLoads.Count -ne [int]$report.Configuration.GatewayCount) {
        throw "Expected $($report.Configuration.GatewayCount) TCP reports for $mode, found $($tcpLoads.Count)."
    }

    # 提取 GC 增量指标（从 MetricDeltas）。
    $gen2Delta = 0.0
    $allocDelta = 0.0
    if ($report.MetricDeltas) {
        $report.MetricDeltas.PSObject.Properties |
            Where-Object { $_.Name -like 'dotnet_gc_collections_total*' -and $_.Name -like '*gen2*' } |
            ForEach-Object { $gen2Delta += [double]$_.Value }
        $report.MetricDeltas.PSObject.Properties |
            Where-Object { $_.Name -like 'dotnet_gc_heap_total_allocated_bytes_total*' } |
            ForEach-Object { $allocDelta += [double]$_.Value }
    }

    $results.Add([pscustomobject]@{
        Mode = $mode
        Passed = [bool]$report.Succeeded -and $runnerExitCode -eq 0
        RunnerExitCode = $runnerExitCode
        SuccessfulConnections = [long](($tcpLoads | Measure-Object Succeeded -Sum).Sum)
        FailedConnections = [long](($tcpLoads | Measure-Object Failed -Sum).Sum)
        ThroughputPerSecond = [double](($tcpLoads | Measure-Object ThroughputPerSecond -Sum).Sum)
        P95Milliseconds = [double](($tcpLoads | Measure-Object P95Milliseconds -Maximum).Maximum)
        P99Milliseconds = [double](($tcpLoads | Measure-Object P99Milliseconds -Maximum).Maximum)
        GatewayAverageCpuPercent = [double](($gateways | Measure-Object AverageCpuPercent -Sum).Sum)
        GatewayMaximumWorkingSetBytes = [long](($gateways | Measure-Object MaximumWorkingSetBytes -Sum).Sum)
        Gen2Collections = $gen2Delta
        AllocatedBytes = $allocDelta
        Report = $reportFile.FullName
        Errors = @($report.Errors)
    })
}

# 以 PersistentSendLoop 为基线，计算其他模式的回归门限。
$baseline = $results | Where-Object Mode -eq 'PersistentSendLoop' | Select-Object -First 1
$checks = [ordered]@{
    BaselineRunPassed = [bool]$baseline.Passed
    OnDemandSendPumpRunPassed = [bool]($results | Where-Object Mode -eq 'OnDemandSendPump' | Select-Object -First 1).Passed
    PerSessionDrainRunPassed = [bool]($results | Where-Object Mode -eq 'PerSessionDrain' | Select-Object -First 1).Passed
    NoFailedConnections = ($results | ForEach-Object { $_.FailedConnections } | Measure-Object -Sum).Sum -eq 0
}

# 回归检查：吞吐不低于基线 90%，p95 不超过基线 +50% 或 +10ms。
if ($baseline -and $baseline.ThroughputPerSecond -gt 0) {
    $minThroughput = $baseline.ThroughputPerSecond * 0.9
    foreach ($r in $results) {
        if ($r.Mode -ne 'PersistentSendLoop') {
            $checks["$($r.Mode)ThroughputGate"] = $r.ThroughputPerSecond -ge $minThroughput
        }
    }
    $maxP95 = [Math]::Max($baseline.P95Milliseconds * 1.5, $baseline.P95Milliseconds + 10)
    foreach ($r in $results) {
        if ($r.Mode -ne 'PersistentSendLoop') {
            $checks["$($r.Mode)P95Gate"] = $r.P95Milliseconds -le $maxP95
        }
    }
}

$passed = @($checks.Values | Where-Object { -not $_ }).Count -eq 0
$completedAt = [DateTimeOffset]::UtcNow
$summary = [pscustomobject]@{
    StartedAtUtc = $startedAt
    CompletedAtUtc = $completedAt
    Passed = $passed
    Scenario = $Scenario
    Configuration = [pscustomobject]@{
        DurationSeconds = $DurationSeconds
        WarmupSeconds = $WarmupSeconds
        TcpConnections = $TcpConnections
        TcpMessagesPerSecond = $TcpMessagesPerSecond
        TcpPayloadBytes = $TcpPayloadBytes
        TcpMode = $TcpMode
        TcpSlowReaders = $TcpSlowReaders
    }
    Checks = $checks
    Results = $results
}

$jsonPath = Join-Path $abDirectory "outbound-ab-${Scenario}-report.json"
$markdownPath = Join-Path $abDirectory "outbound-ab-${Scenario}-report.md"
[IO.File]::WriteAllText(
    $jsonPath,
    ($summary | ConvertTo-Json -Depth 10),
    [Text.UTF8Encoding]::new($false))

$lines = [Collections.Generic.List[string]]::new()
$lines.Add("# Outbound send mode A/B - $Scenario")
$lines.Add('')
$lines.Add("Result: **$(if ($passed) { 'PASSED' } else { 'FAILED' })**")
$lines.Add('')
$lines.Add("Window: $($startedAt.ToString('O')) - $($completedAt.ToString('O'))")
$lines.Add('')
$lines.Add("Scenario: $Scenario | Connections: $TcpConnections | Payload: $TcpPayloadBytes B | Mode: $TcpMode | Duration: ${DurationSeconds}s")
$lines.Add('')
$lines.Add("| Mode | Passed | Connections | Failed | Throughput/s | p95 ms | p99 ms | Gateway CPU | Gateway WS | Gen2 | Alloc MB |")
$lines.Add('|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|')
foreach ($r in $results) {
    $lines.Add([string]::Format(
        [Globalization.CultureInfo]::InvariantCulture,
        '| {0} | {1} | {2} | {3} | {4:F2} | {5:F2} | {6:F2} | {7:F2}% | {8:F2} MiB | {9:F0} | {10:F1} |',
        $r.Mode, $r.Passed, $r.SuccessfulConnections, $r.FailedConnections,
        $r.ThroughputPerSecond, $r.P95Milliseconds, $r.P99Milliseconds,
        $r.GatewayAverageCpuPercent, $r.GatewayMaximumWorkingSetBytes / 1MB,
        $r.Gen2Collections, $r.AllocatedBytes / 1MB))
}
$lines.Add('')
$lines.Add('## Gates')
$lines.Add('')
foreach ($check in $checks.GetEnumerator()) {
    $status = if ($check.Value) { 'PASS' } else { 'FAIL' }
    $lines.Add("- $status $($check.Key)")
}
[IO.File]::WriteAllLines($markdownPath, $lines, [Text.UTF8Encoding]::new($false))

Write-Host "Outbound A/B JSON: $jsonPath"
Write-Host "Outbound A/B Markdown: $markdownPath"
if (-not $passed) { exit 1 }
