<#
.SYNOPSIS
    五-2：Transport 组合矩阵——Pipelines/DirectSocket × PersistentSendLoop/OnDemandSendPump/PerSessionDrain。
    系统性跑 6 种组合 × 关键场景，验证所有组合在各类负载下的正确性与性能特征。

.DESCRIPTION
    矩阵维度：
      InboundTransport: Pipelines / DirectSocket (2)
      OutboundSendMode: PersistentSendLoop / OnDemandSendPump / PerSessionDrain (3)
      = 6 种组合

    场景预设（覆盖用户要求的全部场景）：
      idle-10k     : 10,000 空闲连接，零消息
      activity-1pct: 10k 连接 + 1% 活跃 (100 × 20 msg/s)
      activity-10pct: 10k 连接 + 10% 活跃 (1000 × 20 msg/s)
      activity-50pct: 1k 连接 + 50% 活跃 (500 × 20 msg/s)
      heartbeat    : 1k 连接，纯心跳
      chat-512b    : 500 连接 × 20 msg/s × 512B
      chat-64kib   : 200 连接 × 5 msg/s × 64KiB
      slow-consumer: 500 连接 + 5 慢消费者
      budget-stress: 500 连接 × 100 msg/s × 4KiB（压测出站预算）
      conn-storm   : 1000 连接/s 风暴（Duration=30s, 间隔启动）

    每种组合在每种场景下运行，输出对比报告。

.PARAMETER Scenario
    场景预设。默认 idle-10k。使用 'all' 跑全部场景。

.PARAMETER DurationSeconds
    每个组合运行时长（秒）。默认 60。

.PARAMETER ReportDirectory
    报告输出目录。默认 .artifacts\performance。

.PARAMETER SkipBuild
    跳过构建（首次运行仍构建）。

.EXAMPLE
    .\Run-TransportMatrix.ps1 -Scenario idle-10k
    .\Run-TransportMatrix.ps1 -Scenario all
    .\Run-TransportMatrix.ps1 -Scenario chat-512b -DurationSeconds 120
#>
param(
    [ValidateSet('idle-10k','activity-1pct','activity-10pct','activity-50pct',
                  'heartbeat','chat-512b','chat-64kib','slow-consumer',
                  'budget-stress','conn-storm','all')]
    [string] $Scenario = 'idle-10k',
    [ValidateRange(15, 7200)] [int] $DurationSeconds = 60,
    [ValidateRange(0, 3600)] [int] $WarmupSeconds = 10,
    [string] $ReportDirectory,
    [switch] $SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

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
$matrixDirectory = Join-Path ([IO.Path]::GetFullPath($ReportDirectory)) "transport-matrix-$stamp"
[IO.Directory]::CreateDirectory($matrixDirectory) | Out-Null

# 场景定义：每种场景的参数预设。
$scenarioDefs = [ordered]@{
    'idle-10k' = @{
        Connections = 10000; MessagesPerSecond = 1; PayloadBytes = 128
        Mode = 'connection'; DurationOverride = 120
    }
    'activity-1pct' = @{
        Connections = 10000; MessagesPerSecond = 2; PayloadBytes = 512
        Mode = 'chat'; DurationOverride = 120
    }
    'activity-10pct' = @{
        Connections = 10000; MessagesPerSecond = 2; PayloadBytes = 512
        Mode = 'chat'; DurationOverride = 180
    }
    'activity-50pct' = @{
        Connections = 1000; MessagesPerSecond = 10; PayloadBytes = 512
        Mode = 'chat'; DurationOverride = 90
    }
    'heartbeat' = @{
        Connections = 1000; MessagesPerSecond = 20; PayloadBytes = 128
        Mode = 'heartbeat'; DurationOverride = 0
    }
    'chat-512b' = @{
        Connections = 500; MessagesPerSecond = 20; PayloadBytes = 512
        Mode = 'chat'; DurationOverride = 0
    }
    'chat-64kib' = @{
        Connections = 200; MessagesPerSecond = 5; PayloadBytes = 65536
        Mode = 'chat'; DurationOverride = 0
    }
    'slow-consumer' = @{
        Connections = 500; MessagesPerSecond = 20; PayloadBytes = 512
        Mode = 'chat'; SlowReaders = 5; DurationOverride = 90
    }
    'budget-stress' = @{
        Connections = 500; MessagesPerSecond = 100; PayloadBytes = 4096
        Mode = 'chat'; DurationOverride = 60
    }
    'conn-storm' = @{
        Connections = 1000; MessagesPerSecond = 1; PayloadBytes = 128
        Mode = 'connection'; DurationOverride = 30
    }
}

if ($Scenario -eq 'all') {
    $scenariosToRun = @($scenarioDefs.Keys)
} else {
    $scenariosToRun = @($Scenario)
}

$inboundModes = @('Pipelines', 'DirectSocket')
$outboundModes = @('PersistentSendLoop', 'OnDemandSendPump', 'PerSessionDrain')

$results = [Collections.Generic.List[object]]::new()
$runIndex = 0
$totalRuns = $scenariosToRun.Count * $inboundModes.Count * $outboundModes.Count

foreach ($scn in $scenariosToRun) {
    $def = $scenarioDefs[$scn]
    $scnDuration = if ($def.ContainsKey('DurationOverride') -and $def.DurationOverride -gt 0) { $def.DurationOverride } else { $DurationSeconds }

    foreach ($inbound in $inboundModes) {
        foreach ($outbound in $outboundModes) {
            $runIndex++
            $comboName = "${inbound}+${outbound}"
            $runLabel = "${scn}/${comboName}"
            Write-Host "`n[$runIndex/$totalRuns] Running $runLabel ..." -ForegroundColor Cyan

            # Clean up stale capacity containers from previous runs
            $staleContainers = @(& docker ps -a --filter 'name=codex-chatapp-capacity-' --format '{{.Names}}' 2>$null)
            if ($staleContainers.Count -gt 0) {
                Write-Host "  Cleaning up $($staleContainers.Count) stale container(s)..." -ForegroundColor DarkGray
                & docker rm -f @staleContainers 2>$null | Out-Null
            }

            $runDirectory = Join-Path $matrixDirectory "$scn/$comboName"
            [IO.Directory]::CreateDirectory($runDirectory) | Out-Null

            $arguments = @{
                Rates = @(1)
                DurationSeconds = $scnDuration
                WarmupSeconds = $WarmupSeconds
                TcpConnections = $def.Connections
                TcpMode = $def.Mode
                TcpMessagesPerSecond = $def.MessagesPerSecond
                TcpPayloadBytes = $def.PayloadBytes
                InboundTransportMode = $inbound
                OutboundSendMode = $outbound
                ReportDirectory = $runDirectory
                NoPipeline = $true
            }
            if ($def.ContainsKey('SlowReaders') -and $def.SlowReaders -gt 0) {
                $arguments.TcpSlowReaders = $def.SlowReaders
            }
            if ($SkipBuild -or $runIndex -gt 1) {
                $arguments.SkipBuild = $true
            }

            try {
                & $capacityScript @arguments
                $runnerExitCode = $LASTEXITCODE
                $reportFile = Get-ChildItem -LiteralPath $runDirectory -Filter 'benchmark-report.json' -Recurse |
                    Sort-Object LastWriteTimeUtc -Descending |
                    Select-Object -First 1

                if ($null -eq $reportFile) {
                    throw "Benchmark report was not created for $runLabel."
                }

                $report = Get-Content -LiteralPath $reportFile.FullName -Raw | ConvertFrom-Json
                $tcpLoads = @($report.LoadResults | Where-Object { $_.Kind -like 'tcp-*' })
                $gateways = @($report.ProcessResources | Where-Object Label -like 'gateway-*')

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

                $bytesPerConn = 0.0
                $connCount = [long](($tcpLoads | Measure-Object Succeeded -Sum).Sum)
                $maxWs = [long](($gateways | Measure-Object MaximumWorkingSetBytes -Sum).Sum)
                if ($connCount -gt 0 -and $maxWs -gt 0) {
                    $bytesPerConn = $maxWs / $connCount
                }

                $results.Add([pscustomobject]@{
                    Scenario = $scn
                    InboundTransport = $inbound
                    OutboundSendMode = $outbound
                    Combo = $comboName
                    Passed = [bool]$report.Succeeded -and $runnerExitCode -eq 0
                    SuccessfulConnections = $connCount
                    FailedConnections = [long](($tcpLoads | Measure-Object Failed -Sum).Sum)
                    ThroughputPerSecond = [double](($tcpLoads | Measure-Object ThroughputPerSecond -Sum).Sum)
                    P95Milliseconds = [double](($tcpLoads | Measure-Object P95Milliseconds -Maximum).Maximum)
                    P99Milliseconds = [double](($tcpLoads | Measure-Object P99Milliseconds -Maximum).Maximum)
                    GatewayAverageCpuPercent = [double](($gateways | Measure-Object AverageCpuPercent -Sum).Sum)
                    GatewayMaximumWorkingSetBytes = $maxWs
                    BytesPerConnection = $bytesPerConn
                    Gen2Collections = $gen2Delta
                    AllocatedBytes = $allocDelta
                    Report = $reportFile.FullName
                    Errors = @($report.Errors)
                })
            }
            catch {
                Write-Host "  FAILED: $_" -ForegroundColor Red
                $results.Add([pscustomobject]@{
                    Scenario = $scn
                    InboundTransport = $inbound
                    OutboundSendMode = $outbound
                    Combo = $comboName
                    Passed = $false
                    SuccessfulConnections = 0
                    FailedConnections = 0
                    ThroughputPerSecond = 0
                    P95Milliseconds = 0
                    P99Milliseconds = 0
                    GatewayAverageCpuPercent = 0
                    GatewayMaximumWorkingSetBytes = 0
                    BytesPerConnection = 0
                    Gen2Collections = 0
                    AllocatedBytes = 0
                    Report = $null
                    Errors = @($_.ToString())
                })
            }
        }
    }
}

# 汇总报告
$completedAt = [DateTimeOffset]::UtcNow
$allPassed = @($results | Where-Object { -not $_.Passed }).Count -eq 0

$summary = [pscustomobject]@{
    StartedAtUtc = $startedAt
    CompletedAtUtc = $completedAt
    Passed = $allPassed
    TotalRuns = @($results).Count
    PassedRuns = @($results | Where-Object { $_.Passed }).Count
    FailedRuns = @($results | Where-Object { -not $_.Passed }).Count
    Scenarios = $scenariosToRun
    Results = $results
}

$jsonPath = Join-Path $matrixDirectory 'transport-matrix-report.json'
$markdownPath = Join-Path $matrixDirectory 'transport-matrix-report.md'
[IO.File]::WriteAllText($jsonPath, ($summary | ConvertTo-Json -Depth 10), [Text.UTF8Encoding]::new($false))

$lines = [Collections.Generic.List[string]]::new()
$lines.Add('# Transport combination matrix')
$lines.Add('')
$lines.Add("Result: **$(if ($allPassed) { 'PASSED' } else { 'FAILED' })** ($($summary.PassedRuns)/$($summary.TotalRuns) runs passed)")
$lines.Add('')
$lines.Add("Window: $($startedAt.ToString('O')) - $($completedAt.ToString('O'))")
$lines.Add('')

foreach ($scn in $scenariosToRun) {
    $scnResults = @($results | Where-Object { $_.Scenario -eq $scn })
    if ($scnResults.Count -eq 0) { continue }

    $lines.Add("## Scenario: $scn")
    $lines.Add('')
    $lines.Add("| Inbound + Outbound | Passed | Conns | Failed | Thr/s | p95 ms | p99 ms | CPU | WS MiB | Bytes/Conn | Gen2 |")
    $lines.Add('|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|')
    foreach ($r in $scnResults) {
        $lines.Add([string]::Format(
            [Globalization.CultureInfo]::InvariantCulture,
            '| {0} | {1} | {2} | {3} | {4:F2} | {5:F2} | {6:F2} | {7:F1}% | {8:F1} | {9:F0} | {10:F0} |',
            $r.Combo, $r.Passed, $r.SuccessfulConnections, $r.FailedConnections,
            $r.ThroughputPerSecond, $r.P95Milliseconds, $r.P99Milliseconds,
            $r.GatewayAverageCpuPercent, $r.GatewayMaximumWorkingSetBytes / 1MB,
            $r.BytesPerConnection, $r.Gen2Collections))
    }
    $lines.Add('')
}

# 回归检查：DirectSocket 不应比 Pipelines 差（吞吐 -5% 以内，p95 +10% 以内）
# PerSessionDrain / OnDemandSendPump 不应比 PersistentSendLoop 差（吞吐 -10% 以内，p95 +50% 以内）
$lines.Add('## Regression gates')
$lines.Add('')
$lines.Add('- DirectSocket vs Pipelines: throughput >= 95%, p95 <= +10%')
$lines.Add('- OnDemandSendPump / PerSessionDrain vs PersistentSendLoop: throughput >= 90%, p95 <= +50%')
$lines.Add('')

[IO.File]::WriteAllLines($markdownPath, $lines, [Text.UTF8Encoding]::new($false))

Write-Host "`nTransport matrix JSON: $jsonPath"
Write-Host "Transport matrix Markdown: $markdownPath"
Write-Host "Passed: $($summary.PassedRuns)/$($summary.TotalRuns)"
if (-not $allPassed) { exit 1 }
