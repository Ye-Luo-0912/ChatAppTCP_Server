<#
.SYNOPSIS
    8~24 小时 Soak 测试：长时间运行 + 内存稳定性监控。
    委托 Run-CapacityCurve.ps1 执行长时负载，随后对报告中的 Gateway 进程
    Working Set 做稳定性分析，验证「24h 后内存进入稳定平台」这一门禁3 检查点。

.DESCRIPTION
    默认 DirectSocket + PersistentSendLoop + BoundedChannel（当前默认组合）。
    可通过 -TcpMode heartbeat/chat + -TcpPayloadBytes 做真实聊天负载浸泡。

    内存稳定性判定（门禁3）：
      - 汇总全部 gateway 进程 Working Set 的首/末/均值/最大值。
      - 增长占比 = (last - first) / first，须 <= -MemoryGrowthThresholdPercent。
      - 末值须 <= 均值 * (1 + -MemoryDeparturePercent)，确保无持续增长（已进入平台）。
    满足则输出 STABLE 判定；否则 FAILED 并返回非零退出码。

.PARAMETER DurationSeconds
    浸泡时长（秒）。默认 28800（8 小时）。上限 86400（24 小时）。

.PARAMETER WarmupSeconds
    预热时长（秒）。默认 300。

.PARAMETER TcpMode
    TCP 负载模式：connection / heartbeat / chat。默认 connection。

.PARAMETER TcpConnections
    TCP 连接数。默认 1000。

.PARAMETER TcpMessagesPerSecond
    chat/heartbeat 下每连接每秒消息数。默认 5。

.PARAMETER TcpPayloadBytes
    chat 负载字节数。默认 512。

.PARAMETER TcpSlowReaders
    慢消费者数（chat 下）。默认 0。

.PARAMETER MemoryGrowthThresholdPercent
    内存增长占比阈值（%）。默认 20，即浸泡后 Working Set 增长须 <= 20%。

.PARAMETER MemoryDeparturePercent
    末值相对均值允许偏离百分比（%）。默认 15。

.PARAMETER ReportDirectory
    报告输出目录。默认 .artifacts\performance。

.EXAMPLE
    .\Run-Soak.ps1 -DurationSeconds 28800 -TcpMode chat -TcpPayloadBytes 512
    .\Run-Soak.ps1 -DurationSeconds 86400 -MemoryGrowthThresholdPercent 15
#>
param(
    [ValidateRange(300, 86400)] [int] $DurationSeconds = 28800,
    [ValidateRange(0, 3600)] [int] $WarmupSeconds = 300,
    [ValidateSet('connection', 'heartbeat', 'chat')] [string] $TcpMode = 'connection',
    [ValidateRange(1, 100000)] [int] $TcpConnections = 1000,
    [ValidateRange(1, 100000)] [int] $TcpMessagesPerSecond = 5,
    [ValidateRange(1, 1048576)] [int] $TcpPayloadBytes = 512,
    [ValidateRange(0, 10000)] [int] $TcpSlowReaders = 0,
    [ValidateRange(1, 100)] [int] $MemoryGrowthThresholdPercent = 20,
    [ValidateRange(1, 100)] [int] $MemoryDeparturePercent = 15,
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

$capacityScript = Join-Path $PSScriptRoot 'Run-CapacityCurve.ps1'
if (-not (Test-Path -LiteralPath $capacityScript -PathType Leaf)) {
    throw "Capacity-curve runner was not found: $capacityScript"
}

$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..\..'))
if ([string]::IsNullOrWhiteSpace($ReportDirectory)) {
    $ReportDirectory = Join-Path $repositoryRoot '.artifacts\performance'
}
$ReportDirectory = [IO.Path]::GetFullPath($ReportDirectory)

Write-Host "Starting soak run: duration=$DurationSeconds s; mode=$TcpMode; conns=$TcpConnections; payload=$TcpPayloadBytes B; send=$InboundTransportMode+$OutboundSendMode+$OutboundQueueMode."
Write-Host 'The generated benchmark report includes process, Docker, GC/heap, Npgsql connection, JetStream and Outbox trends.'

$arguments = @{
    Rates = @(1)
    DurationSeconds = $DurationSeconds
    WarmupSeconds = $WarmupSeconds
    TcpConnections = $TcpConnections
    TcpMode = $TcpMode
    TcpMessagesPerSecond = $TcpMessagesPerSecond
    TcpPayloadBytes = $TcpPayloadBytes
    TcpSlowReaders = $TcpSlowReaders
    InboundTransportMode = $InboundTransportMode
    OutboundSendMode = $OutboundSendMode
    OutboundQueueMode = $OutboundQueueMode
    OnDemandSendWorkerCount = $OnDemandSendWorkerCount
    OnDemandSendBurstLimit = $OnDemandSendBurstLimit
    ReportDirectory = $ReportDirectory
    NoPipeline = $true
}
if ($SkipBuild) {
    $arguments.SkipBuild = $true
}

& $capacityScript @arguments
$curveExit = $LASTEXITCODE

# 定位最新一次 capacity-curve 的 per-rate benchmark-report.json（含 ProcessResources）。
$reportFile = Get-ChildItem -LiteralPath $ReportDirectory -Filter 'benchmark-report.json' -Recurse |
    Sort-Object LastWriteTimeUtc -Descending |
    Select-Object -First 1

if ($null -eq $reportFile) {
    Write-Error "Soak run completed but no benchmark-report.json was found under $ReportDirectory."
    exit 1
}

$report = Get-Content -LiteralPath $reportFile.FullName -Raw | ConvertFrom-Json
$gateways = @($report.ProcessResources | Where-Object Label -like 'gateway-*')
if ($gateways.Count -eq 0) {
    Write-Error "Soak report has no gateway process resources: $($reportFile.FullName)"
    exit 1
}

$first = [long](($gateways | Measure-Object FirstWorkingSetBytes -Sum).Sum)
$last = [long](($gateways | Measure-Object LastWorkingSetBytes -Sum).Sum)
$avg = [long](($gateways | Measure-Object AverageWorkingSetBytes -Sum).Sum)
$max = [long](($gateways | Measure-Object MaximumWorkingSetBytes -Sum).Sum)
$growth = $last - $first
$growthFraction = if ($first -gt 0) { 100.0 * $growth / $first } else { 100.0 }
$lastVsAvg = if ($avg -gt 0) { 100.0 * ($last - $avg) / $avg } else { 100.0 }

$growthOk = $growthFraction -le $MemoryGrowthThresholdPercent
$plateauOk = $lastVsAvg -le $MemoryDeparturePercent
$stable = $growthOk -and $plateauOk -and $curveExit -eq 0

$verdict = Join-Path $ReportDirectory "soak-verdict-$([DateTimeOffset]::UtcNow.ToString("yyyyMMdd-HHmmss'Z'")).json"
$verdictObj = [pscustomobject]@{
    StartedAtUtc = $report.StartedAtUtc
    CompletedAtUtc = $report.CompletedAtUtc
    DurationSeconds = $DurationSeconds
    TcpMode = $TcpMode
    TcpConnections = $TcpConnections
    MemoryStable = $stable
    FirstWorkingSetMiB = $first / 1MB
    LastWorkingSetMiB = $last / 1MB
    AverageWorkingSetMiB = $avg / 1MB
    MaxWorkingSetMiB = $max / 1MB
    GrowthMiB = $growth / 1MB
    GrowthPercent = $growthFraction
    LastVsAveragePercent = $lastVsAvg
    GrowthThresholdPercent = $MemoryGrowthThresholdPercent
    DepartureThresholdPercent = $MemoryDeparturePercent
    Report = $reportFile.FullName
}
[IO.File]::WriteAllText($verdict, ($verdictObj | ConvertTo-Json -Depth 6), [Text.UTF8Encoding]::new($false))

$tee = [Collections.Generic.List[string]]::new()
$tee.Add('# Soak memory stability')
$tee.Add('')
$tee.Add("Window: $($report.StartedAtUtc) - $($report.CompletedAtUtc); duration=$DurationSeconds s; mode=$TcpMode.")
$tee.Add('')
$tee.Add("Verdict: **$(if ($stable) { 'STABLE' } else { 'FAILED' })**")
$tee.Add('')
$tee.Add('| First MiB | Last MiB | Avg MiB | Max MiB | Growth MiB | Growth % | Last vs Avg % | Plateau |')
$tee.Add('|---|---:|---:|---:|---:|---:|---:|---:|')
$tee.Add([string]::Format(
    [Globalization.CultureInfo]::InvariantCulture,
    '| {0:F1} | {1:F1} | {2:F1} | {3:F1} | {4:F1} | {5:F1}% | {6:F1}% | {7} |',
    $first / 1MB, $last / 1MB, $avg / 1MB, $max / 1MB,
    $growth / 1MB, $growthFraction, $lastVsAvg, $stable))
$tee.Add('')
$tee.Add("- Growth threshold: <= $MemoryGrowthThresholdPercent% (OK: $growthOk)")
$tee.Add("- Last vs Avg departure threshold: <= $MemoryDeparturePercent% (OK: $plateauOk)")
$tee.Add('- Rule: 24h 后内存进入稳定平台 = 增长占比 <= 阈值 且 末值不显著偏离均值（无持续增长）。')
$tee.Add('')
$verdictMd = [IO.Path]::ChangeExtension($verdict, '.md')
[IO.File]::WriteAllLines($verdictMd, $tee, [Text.UTF8Encoding]::new($false))

Write-Host "Soak verdict: $($verdictMd)"
Write-Host ("Memory: first={0:F1} MiB, last={1:F1} MiB, avg={2:F1} MiB, growth={3:F1}% (threshold {4}%)." -f ($first / 1MB), ($last / 1MB), ($avg / 1MB), $growthFraction, $MemoryGrowthThresholdPercent)
if ($stable) {
    Write-Host 'Memory stability: STABLE (working set entered a stable plateau).' -ForegroundColor Green
    exit 0
}
Write-Host 'Memory stability: FAILED (working set may still be growing).' -ForegroundColor Red
if ($curveExit -ne 0) { exit $curveExit }
exit 1