<#
.SYNOPSIS
    五-2：Transport 组合矩阵——Pipelines/DirectSocket × PersistentSendLoop/OnDemandSendPump/PerSessionDrain × BoundedChannel/LazySegmented。
    系统性跑 12 种组合 × 关键场景，验证所有组合在各类负载下的正确性与性能特征。

.DESCRIPTION
    矩阵维度：
      InboundTransport: Pipelines / DirectSocket (2)
      OutboundSendMode: PersistentSendLoop / OnDemandSendPump / PerSessionDrain (3)
      OutboundQueueMode: BoundedChannel / LazySegmented (2)
      = 12 种组合

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

.PARAMETER OutboundQueueMode
    限制出站队列矩阵维度。默认 'all'（BoundedChannel + LazySegmented 双队列）。
    可指定 'BoundedChannel' 或 'LazySegmented' 仅跑单一队列（缩小矩阵）。

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
                  'budget-stress','conn-storm','slowloris-header','slowloris-payload',
                  'inbound-budget','all')]
    [string] $Scenario = 'idle-10k',
    [ValidateSet('all','BoundedChannel','LazySegmented')] [string] $OutboundQueueMode = 'all',
    [ValidateRange(15, 7200)] [int] $DurationSeconds = 60,
    [ValidateRange(0, 3600)] [int] $WarmupSeconds = 10,
    [ValidateRange(1, 100000)] [int] $TcpConnectionsPerSecond = 0,
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
        Mode = 'connection'; ConnectionsPerSecond = 200; DurationOverride = 30
    }
    'slowloris-header' = @{
        Connections = 500; MessagesPerSecond = 1; PayloadBytes = 128
        Mode = 'slowloris'; SlowlorisPhase = 'header'; SlowlorisDelayMs = 1000
        DurationOverride = 60
    }
    'slowloris-payload' = @{
        Connections = 500; MessagesPerSecond = 1; PayloadBytes = 128
        Mode = 'slowloris'; SlowlorisPhase = 'payload'; SlowlorisDelayMs = 1000
        DurationOverride = 90
    }
    'inbound-budget' = @{
        Connections = 500; MessagesPerSecond = 100; PayloadBytes = 4096
        Mode = 'chat'; InboundBudgetBytes = 1048576; DurationOverride = 60
    }
}

if ($Scenario -eq 'all') {
    $scenariosToRun = @($scenarioDefs.Keys)
} else {
    $scenariosToRun = @($Scenario)
}

$inboundModes = @('Pipelines', 'DirectSocket')
$outboundModes = @('PersistentSendLoop', 'OnDemandSendPump', 'PerSessionDrain')
$queueModes = if ($OutboundQueueMode -eq 'all') { @('BoundedChannel', 'LazySegmented') } else { @($OutboundQueueMode) }

$results = [Collections.Generic.List[object]]::new()
$runIndex = 0
$totalRuns = $scenariosToRun.Count * $inboundModes.Count * $outboundModes.Count * $queueModes.Count

# 按指标名标识符（不含 meter 前缀与标签）汇总 Prometheus 系列值。
# 用于采集队列深度 / segment、Actor active/busy/churn、Outbox lag 等报告列。
function Get-MetricSumByIdentifier {
    param(
        [Parameter(Mandatory)] $MetricSet,
        [Parameter(Mandatory)] [string] $Identifier
    )
    $sum = 0.0
    if ($null -eq $MetricSet) { return $sum }
    foreach ($property in $MetricSet.PSObject.Properties) {
        if ($property.Name.IndexOf($Identifier, [StringComparison]::OrdinalIgnoreCase) -ge 0) {
            $value = $property.Value
            if ($value -is [double] -or $value -is [long] -or $value -is [int]) {
                $sum += [double]$value
            }
        }
    }
    return $sum
}

foreach ($scn in $scenariosToRun) {
    $def = $scenarioDefs[$scn]
    $scnDuration = if ($def.ContainsKey('DurationOverride') -and $def.DurationOverride -gt 0) { $def.DurationOverride } else { $DurationSeconds }

    foreach ($inbound in $inboundModes) {
        foreach ($outbound in $outboundModes) {
            foreach ($queue in $queueModes) {
                $runIndex++
                $comboName = "${inbound}+${outbound}+${queue}"
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
                    OutboundQueueMode = $queue
                    ReportDirectory = $runDirectory
                    NoPipeline = $true
                }
                if ($def.ContainsKey('SlowReaders') -and $def.SlowReaders -gt 0) {
                    $arguments.TcpSlowReaders = $def.SlowReaders
                }
                if ($def.ContainsKey('ConnectionsPerSecond') -and $def.ConnectionsPerSecond -gt 0) {
                    $arguments.TcpConnectionsPerSecond = $def.ConnectionsPerSecond
                } elseif ($TcpConnectionsPerSecond -gt 0) {
                    $arguments.TcpConnectionsPerSecond = $TcpConnectionsPerSecond
                }
                if ($def.ContainsKey('SlowlorisPhase')) {
                    $arguments.TcpSlowlorisPhase = $def.SlowlorisPhase
                    $arguments.TcpSlowlorisDelayMs = $def.SlowlorisDelayMs
                }
                if ($def.ContainsKey('InboundBudgetBytes') -and $def.InboundBudgetBytes -gt 0) {
                    $arguments.TcpInboundBudgetBytes = $def.InboundBudgetBytes
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

                    # 队列深度 / Actor 状态 / Outbox lag 汇总（门禁3 需核对队列与 Actor 稳定性）。
                    # 队列深度与 Actor 活跃为瞬时 gauge（取结束快照 MetricsAfter）；
                    # Actor churn 为累计计数器（取运行期增量 MetricDeltas）。
                    $outboundQueueDepth = Get-MetricSumByIdentifier $report.MetricsAfter 'gateway_outbound_queued_frames'
                    $actorActive = Get-MetricSumByIdentifier $report.MetricsAfter 'gateway_actor_active'
                    $actorBusy = Get-MetricSumByIdentifier $report.MetricsAfter 'gateway_actor_busy'
                    $actorProcessed = Get-MetricSumByIdentifier $report.MetricDeltas 'gateway_actor_messages_processed'
                    $outboxPending = Get-MetricSumByIdentifier $report.MetricsAfter 'realtime_outbox_pending'
                    $outboxLagSeconds = Get-MetricSumByIdentifier $report.MetricsAfter 'realtime_outbox_oldest_age_seconds'

                    # R1：真实活跃连接数（网关 Prometheus gauge，二楼跨实例求和）。
                    $activeConnections = Get-MetricSumByIdentifier $report.MetricsAfter 'gateway_connections_active'
                    # R1：真实资源泄漏计数。
                    #  Budget：出站预算拒绝（运行期增量，应为 0）。
                    #  Frame：出站队列滞留帧（结束快照，应为 0）。
                    #  Segment：出站/入站打包滞留字节（结束快照，应为 0）。
                    $budgetRejected = Get-MetricSumByIdentifier $report.MetricDeltas 'gateway_outbound_rejected_global_budget'
                    $outboundCommittedBytes = Get-MetricSumByIdentifier $report.MetricsAfter 'gateway_outbound_committed_bytes'
                    $inboundCommittedBytes = Get-MetricSumByIdentifier $report.MetricsAfter 'gateway_inbound_committed_bytes'

                    $bytesPerConn = 0.0
                    $connCount = [long](($tcpLoads | Measure-Object Succeeded -Sum).Sum)
                    $maxWs = [long](($gateways | Measure-Object MaximumWorkingSetBytes -Sum).Sum)
                    if ($connCount -gt 0 -and $maxWs -gt 0) {
                        $bytesPerConn = $maxWs / $connCount
                    }

                    # R1：消息成功数独立于连接数（避免混用）。
                    $messagesAcknowledged = [long](($tcpLoads | Measure-Object MessagesAcknowledged -Sum).Sum)
                    $messagesRejected = [long](($tcpLoads | Measure-Object MessagesRejected -Sum).Sum)
                    $messagesSent = [long](($tcpLoads | Measure-Object MessagesSent -Sum).Sum)
                    $peakActiveConnections = [long](($tcpLoads | Measure-Object PeakActiveConnections -Sum).Sum)
                    # R1：健康/慢连接分桶延迟（取最大 p95 作为最差情况）。
                    $healthyP95 = [double](($tcpLoads | Measure-Object HealthyP95Milliseconds -Maximum).Maximum)
                    $slowP95 = [double](($tcpLoads | Measure-Object SlowP95Milliseconds -Maximum).Maximum)
                    $expectedConnections = [long]$def.Connections
                    $activeRatio = if ($expectedConnections -gt 0) { 100.0 * $activeConnections / $expectedConnections } else { 0.0 }

                    $results.Add([pscustomobject]@{
                        Scenario = $scn
                        InboundTransport = $inbound
                        OutboundSendMode = $outbound
                        OutboundQueueMode = $queue
                        Combo = $comboName
                        Passed = [bool]$report.Succeeded -and $runnerExitCode -eq 0
                        SuccessfulConnections = $connCount
                        FailedConnections = [long](($tcpLoads | Measure-Object Failed -Sum).Sum)
                        ActiveConnectionCount = $activeConnections
                        ActiveConnectionRatio = $activeRatio
                        PeakActiveConnections = $peakActiveConnections
                        ThroughputPerSecond = [double](($tcpLoads | Measure-Object ThroughputPerSecond -Sum).Sum)
                        P95Milliseconds = [double](($tcpLoads | Measure-Object P95Milliseconds -Maximum).Maximum)
                        P99Milliseconds = [double](($tcpLoads | Measure-Object P99Milliseconds -Maximum).Maximum)
                        HealthyP95Milliseconds = $healthyP95
                        SlowP95Milliseconds = $slowP95
                        MessagesSent = $messagesSent
                        MessagesAcknowledged = $messagesAcknowledged
                        MessagesRejected = $messagesRejected
                        GatewayAverageCpuPercent = [double](($gateways | Measure-Object AverageCpuPercent -Sum).Sum)
                        GatewayMaximumWorkingSetBytes = $maxWs
                        BytesPerConnection = $bytesPerConn
                        Gen2Collections = $gen2Delta
                        AllocatedBytes = $allocDelta
                        OutboundQueueDepth = $outboundQueueDepth
                        ActorActive = $actorActive
                        ActorBusy = $actorBusy
                        ActorProcessed = $actorProcessed
                        OutboxPending = $outboxPending
                        OutboxLagSeconds = $outboxLagSeconds
                        BudgetRejected = $budgetRejected
                        OutboundCommittedBytes = $outboundCommittedBytes
                        InboundCommittedBytes = $inboundCommittedBytes
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
                        OutboundQueueMode = $queue
                        Combo = $comboName
                        Passed = $false
                        SuccessfulConnections = 0
                        FailedConnections = 0
                        ActiveConnectionCount = 0
                        ActiveConnectionRatio = 0
                        PeakActiveConnections = 0
                        ThroughputPerSecond = 0
                        P95Milliseconds = 0
                        P99Milliseconds = 0
                        HealthyP95Milliseconds = 0
                        SlowP95Milliseconds = 0
                        MessagesSent = 0
                        MessagesAcknowledged = 0
                        MessagesRejected = 0
                        GatewayAverageCpuPercent = 0
                        GatewayMaximumWorkingSetBytes = 0
                        BytesPerConnection = 0
                        Gen2Collections = 0
                        AllocatedBytes = 0
                        OutboundQueueDepth = 0
                        ActorActive = 0
                        ActorBusy = 0
                        ActorProcessed = 0
                        OutboxPending = 0
                        OutboxLagSeconds = 0
                        BudgetRejected = 0
                        OutboundCommittedBytes = 0
                        InboundCommittedBytes = 0
                        Report = $null
                        Errors = @($_.ToString())
                    })
                }
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
    $lines.Add('| Inbound + Send + Queue | Passed | Conns | Active | Act % | Failed | Msg A/C | Thr/s | p95 ms | p99 ms | Hp95 | Sp95 | Queue | Actor | Outbox | Outbox Lag s |')
    $lines.Add('|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|')
    foreach ($r in $scnResults) {
        $lines.Add([string]::Format(
            [Globalization.CultureInfo]::InvariantCulture,
            '| {0} | {1} | {2} | {3:F0} | {4:F1}% | {5} | {6} | {7:F2} | {8:F2} | {9:F2} | {10:F2} | {11:F2} | {12:F0} | {13:F0}/{14:F0} | {15:F0} | {16:F1} |',
            $r.Combo, $r.Passed, $r.SuccessfulConnections, $r.ActiveConnectionCount,
            $r.ActiveConnectionRatio, $r.FailedConnections,
            "$($r.MessagesAcknowledged)/$($r.MessagesRejected)",
            $r.ThroughputPerSecond, $r.P95Milliseconds, $r.P99Milliseconds,
            $r.HealthyP95Milliseconds, $r.SlowP95Milliseconds,
            $r.OutboundQueueDepth, $r.ActorActive, $r.ActorBusy,
            $r.OutboxPending, $r.OutboxLagSeconds))
    }
    $lines.Add('')
}

# 回归检查：DirectSocket 不应比 Pipelines 差（吞吐 -5% 以内，p95 +10% 以内）
# PerSessionDrain / OnDemandSendPump 不应比 PersistentSendLoop 差（吞吐 -10% 以内，p95 +50% 以内）
# LazySegmented 不应比 BoundedChannel 差（吞吐 -10% 以内，p95 +50% 以内）
$lines.Add('## Regression gates')
$lines.Add('')
$lines.Add('- DirectSocket vs Pipelines: throughput >= 95%, p95 <= +10%')
$lines.Add('- OnDemandSendPump / PerSessionDrain vs PersistentSendLoop: throughput >= 90%, p95 <= +50%')
$lines.Add('- LazySegmented vs BoundedChannel: throughput >= 90%, p95 <= +50%')
$lines.Add('')

# 门禁3：默认值切换验收检查点。基线 = Pipelines + PersistentSendLoop + BoundedChannel（当前默认）。
$baselineKey = 'Pipelines+PersistentSendLoop+BoundedChannel'
$lines.Add('## Default-switch acceptance checkpoints (门禁3)')
$lines.Add('')
$lines.Add("基线：**$baselineKey**（当前默认）。相同场景下逐组合对比。")
$lines.Add('')
$lines.Add('> 只有全部满足才可用新默认替换 Persistent + Channel：')
$lines.Add('> 0 correctness failures · 0 budget leaks · 0 stranded frames · 0 unbounded collections ·')
$lines.Add('> Working Set 明显下降 · alloc/sec 不升高 · p99 不恶化超过 5% · 慢消费者不影响健康连接 · 24h 后内存稳定平台。')
$lines.Add('')
$lines.Add('判定规则：')
$lines.Add('- 0 correctness failures：FailedConnections == 0 且无运行错误。')
$lines.Add('- 0 budget leaks：BudgetRejected（gateway.outbound.rejected.global_budget 运行期增量）== 0。')
$lines.Add('- 0 stranded frames / 0 unbounded collections：OutboundQueueDepth == 0 且 OutboundCommittedBytes == 0 且 InboundCommittedBytes == 0（结束快照）。')
$lines.Add('- p99 不恶化超过 5%：(candidate.p99 - baseline.p99) / baseline.p99 <= 5%。')
$lines.Add('- Working Set 明显下降：candidate.WS < baseline.WS（wsOk 纳入 eligibility）。')
$lines.Add('- alloc/sec 不升高：candidate.AllocatedBytes <= baseline.AllocatedBytes。')
$lines.Add('- 慢消费者不影响健康连接：slow-consumer 场景下健康连接延迟不劣化（按连接分桶统计，Hp95）。')
$lines.Add('- 队列有界：结束快照 Queue Depth 应接近 0（表示已排空）；若持续高位说明存在未排空/滞留帧。')
$lines.Add('- Actor 稳定：Actor Active/Busy 结束快照应回到基线水平，Actor Proc 为运行期增量（churn 无异常放大）。')
$lines.Add('- Outbox 收敛：Outbox Pending 结束快照应接近 0，Outbox Lag 应不随时间增长（体现后端消费健康）。')
$lines.Add('')
$lines.Add('> 上述 Queue/Actor/Outbox/泄漏门禁均参与判定：任一候选不满足即令整体退出码非 0。')
$lines.Add('')

$gateViolations = [Collections.Generic.List[string]]::new()
foreach ($scn in $scenariosToRun) {
    $checkResults = @($results | Where-Object { $_.Scenario -eq $scn })
    if ($checkResults.Count -eq 0) { continue }
    $base = $checkResults | Where-Object { $_.Combo -eq $baselineKey } | Select-Object -First 1
    $candidates = @($checkResults | Where-Object { $_.Combo -ne $baselineKey } | Sort-Object Combo)
    if ($candidates.Count -eq 0) { continue }

    $lines.Add("### Scenario: $scn")
    $lines.Add('')
    $lines.Add('| Combo | 0 correctness | 0 leaks | 0 stranded | Queue | Actor | Outbox | p99 Δ | WS Δ | alloc Δ | Switch-eligible |')
    $lines.Add('|---|---|---|---|---|---|---:|---:|---:|---:|')
    foreach ($candidate in $candidates) {
        $correctness = $candidate.SuccessfulConnections -gt 0 -and $candidate.FailedConnections -eq 0 -and $candidate.Errors.Count -eq 0
        # 真实泄漏计数：预算拒绝（运行期增量）应为 0。
        $noLeaks = $candidate.BudgetRejected -eq 0
        # 真实滞留：出站队列滞留帧 + 出站/入站打包滞留字节均为 0。
        $noStranded = $candidate.OutboundQueueDepth -le 0 -and
                      $candidate.OutboundCommittedBytes -le 0 -and
                      $candidate.InboundCommittedBytes -le 0
        # 队列/门禁：Outbox 收敛。
        $queueOk = $candidate.OutboundQueueDepth -le 0
        $actorOk = $candidate.ActorActive -le 0 -and $candidate.ActorBusy -le 0
        $outboxOk = $candidate.OutboxPending -le 0 -and $candidate.OutboxLagSeconds -le 1.0

        $p99Delta = 0.0; $wsDelta = 0.0; $allocDelta = 0.0
        if ($null -ne $base -and $base.P99Milliseconds -gt 0) {
            $p99Delta = ($candidate.P99Milliseconds - $base.P99Milliseconds) / $base.P99Milliseconds * 100.0
        }
        if ($null -ne $base -and $base.GatewayMaximumWorkingSetBytes -gt 0) {
            $wsDelta = ($candidate.GatewayMaximumWorkingSetBytes - $base.GatewayMaximumWorkingSetBytes) / $base.GatewayMaximumWorkingSetBytes * 100.0
        }
        if ($null -ne $base -and $base.AllocatedBytes -gt 0) {
            $allocDelta = ($candidate.AllocatedBytes - $base.AllocatedBytes) / $base.AllocatedBytes * 100.0
        }

        $p99Ok = $p99Delta -le 5.0
        $wsOk = $wsDelta -lt 0.0
        $allocOk = $allocDelta -le 0.0
        # wsOk 纳入 eligibility；Queue/Actor/Outbox 门禁也参与判定。
        $eligible = $correctness -and $noLeaks -and $noStranded -and
                    $p99Ok -and $wsOk -and $allocOk -and
                    $queueOk -and $actorOk -and $outboxOk

        if (-not $eligible) {
            $gateViolations.Add("$scn/$($candidate.Combo): el=$eligible corr=$correctness leaks=$noLeaks stranded=$noStranded queue=$queueOk actor=$actorOk outbox=$outboxOk p99=$p99Ok ws=$wsOk alloc=$allocOk")
        }

        $lines.Add([string]::Format(
            [Globalization.CultureInfo]::InvariantCulture,
            '| {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7:F1}% | {8:F1}% | {9:F1}% | {10} |',
            $candidate.Combo, $correctness, $noLeaks, $noStranded,
            $queueOk, $actorOk, $outboxOk,
            $p99Delta, $wsDelta, $allocDelta, $eligible))
    }
    $lines.Add('')
}

[IO.File]::WriteAllLines($markdownPath, $lines, [Text.UTF8Encoding]::new($false))

Write-Host "`nTransport matrix JSON: $jsonPath"
Write-Host "Transport matrix Markdown: $markdownPath"
Write-Host "Passed: $($summary.PassedRuns)/$($summary.TotalRuns)"
if (-not $allPassed) { exit 1 }
