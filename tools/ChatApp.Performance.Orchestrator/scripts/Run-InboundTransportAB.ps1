param(
    [ValidateRange(15, 3600)] [int] $DurationSeconds = 60,
    [ValidateRange(0, 3600)] [int] $WarmupSeconds = 10,
    [ValidateRange(1, 100000)] [int] $TcpConnections = 1000,
    [ValidateRange(1, 100000)] [int] $TcpMessagesPerSecond = 20,
    [ValidateRange(0, 50)] [double] $MaximumThroughputRegressionPercent = 5,
    [ValidateRange(0, 100)] [double] $MaximumP95RegressionPercent = 10,
    [ValidateRange(0, 100)] [double] $MaximumP99RegressionPercent = 15,
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
$abDirectory = Join-Path ([IO.Path]::GetFullPath($ReportDirectory)) "inbound-transport-ab-$stamp"
[IO.Directory]::CreateDirectory($abDirectory) | Out-Null

$results = [Collections.Generic.List[object]]::new()
$modes = @('Pipelines', 'DirectSocket')

for ($index = 0; $index -lt $modes.Count; $index++) {
    $mode = $modes[$index]
    $modeDirectory = Join-Path $abDirectory $mode.ToLowerInvariant()
    Write-Host "Running inbound transport $mode..."

    $arguments = @{
        Rates = @(1)
        DurationSeconds = $DurationSeconds
        WarmupSeconds = $WarmupSeconds
        TcpConnections = $TcpConnections
        TcpMode = 'heartbeat'
        TcpMessagesPerSecond = $TcpMessagesPerSecond
        InboundTransportMode = $mode
        ReportDirectory = $modeDirectory
        NoPipeline = $true
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
        throw "Benchmark report was not created for inbound mode $mode."
    }

    $report = Get-Content -LiteralPath $reportFile.FullName -Raw | ConvertFrom-Json
    $tcpLoads = @($report.LoadResults | Where-Object Kind -eq 'tcp-heartbeat')
    $gateways = @($report.ProcessResources | Where-Object Label -like 'gateway-*')
    if ($tcpLoads.Count -ne [int]$report.Configuration.GatewayCount) {
        throw "Expected $($report.Configuration.GatewayCount) TCP reports for $mode, found $($tcpLoads.Count)."
    }
    if ($gateways.Count -ne [int]$report.Configuration.GatewayCount) {
        throw "Expected $($report.Configuration.GatewayCount) Gateway resource summaries for $mode, found $($gateways.Count)."
    }

    $results.Add([pscustomobject]@{
        Mode = $mode
        Passed = [bool]$report.Succeeded -and $runnerExitCode -eq 0
        RunnerExitCode = $runnerExitCode
        SuccessfulConnections = [long](($tcpLoads | Measure-Object Succeeded -Sum).Sum)
        FailedConnections = [long](($tcpLoads | Measure-Object Failed -Sum).Sum)
        HeartbeatsPerSecond = [double](($tcpLoads | Measure-Object ThroughputPerSecond -Sum).Sum)
        P95Milliseconds = [double](($tcpLoads | Measure-Object P95Milliseconds -Maximum).Maximum)
        P99Milliseconds = [double](($tcpLoads | Measure-Object P99Milliseconds -Maximum).Maximum)
        GatewayAverageCpuPercent = [double](($gateways | Measure-Object AverageCpuPercent -Sum).Sum)
        GatewayMaximumWorkingSetBytes = [long](($gateways | Measure-Object MaximumWorkingSetBytes -Sum).Sum)
        Report = $reportFile.FullName
        Errors = @($report.Errors)
    })
}

$pipelines = $results | Where-Object Mode -eq 'Pipelines' | Select-Object -First 1
$direct = $results | Where-Object Mode -eq 'DirectSocket' | Select-Object -First 1

$minimumThroughput = $pipelines.HeartbeatsPerSecond *
    (1 - $MaximumThroughputRegressionPercent / 100)
$maximumP95 = [Math]::Max(
    $pipelines.P95Milliseconds * (1 + $MaximumP95RegressionPercent / 100),
    $pipelines.P95Milliseconds + 1)
$maximumP99 = [Math]::Max(
    $pipelines.P99Milliseconds * (1 + $MaximumP99RegressionPercent / 100),
    $pipelines.P99Milliseconds + 2)

$checks = [ordered]@{
    PipelinesRunPassed = [bool]$pipelines.Passed
    DirectSocketRunPassed = [bool]$direct.Passed
    NoFailedConnections = $pipelines.FailedConnections -eq 0 -and $direct.FailedConnections -eq 0
    ThroughputWithinGate = $direct.HeartbeatsPerSecond -ge $minimumThroughput
    P95WithinGate = $direct.P95Milliseconds -le $maximumP95
    P99WithinGate = $direct.P99Milliseconds -le $maximumP99
}
$passed = @($checks.Values | Where-Object { -not $_ }).Count -eq 0
$completedAt = [DateTimeOffset]::UtcNow
$summary = [pscustomobject]@{
    StartedAtUtc = $startedAt
    CompletedAtUtc = $completedAt
    Passed = $passed
    Configuration = [pscustomobject]@{
        DurationSeconds = $DurationSeconds
        WarmupSeconds = $WarmupSeconds
        TcpConnections = $TcpConnections
        TcpMessagesPerSecond = $TcpMessagesPerSecond
        MaximumThroughputRegressionPercent = $MaximumThroughputRegressionPercent
        MaximumP95RegressionPercent = $MaximumP95RegressionPercent
        MaximumP99RegressionPercent = $MaximumP99RegressionPercent
    }
    Checks = $checks
    Results = $results
}

$jsonPath = Join-Path $abDirectory 'inbound-transport-ab-report.json'
$markdownPath = Join-Path $abDirectory 'inbound-transport-ab-report.md'
[IO.File]::WriteAllText(
    $jsonPath,
    ($summary | ConvertTo-Json -Depth 10),
    [Text.UTF8Encoding]::new($false))

$lines = [Collections.Generic.List[string]]::new()
$lines.Add('# Inbound transport short A/B')
$lines.Add('')
$lines.Add("Result: **$(if ($passed) { 'PASSED' } else { 'FAILED' })**")
$lines.Add('')
$lines.Add("Window: $($startedAt.ToString('O')) - $($completedAt.ToString('O'))")
$lines.Add('')
$lines.Add("| Mode | Passed | Connections | Failed | Heartbeats/s | p95 ms | p99 ms | Gateway avg CPU | Gateway max WS |")
$lines.Add('|---|---|---:|---:|---:|---:|---:|---:|---:|')
foreach ($result in $results) {
    $lines.Add([string]::Format(
        [Globalization.CultureInfo]::InvariantCulture,
        '| {0} | {1} | {2} | {3} | {4:F2} | {5:F2} | {6:F2} | {7:F2}% | {8:F2} MiB |',
        $result.Mode,$result.Passed,$result.SuccessfulConnections,$result.FailedConnections,
        $result.HeartbeatsPerSecond,$result.P95Milliseconds,$result.P99Milliseconds,
        $result.GatewayAverageCpuPercent,$result.GatewayMaximumWorkingSetBytes / 1MB))
}
$lines.Add('')
$lines.Add('## Gates')
$lines.Add('')
foreach ($check in $checks.GetEnumerator()) {
    $lines.Add("- $($check.Key): $($check.Value)")
}
$lines.Add('')
$lines.Add("DirectSocket minimum throughput: $($minimumThroughput.ToString('F2', [Globalization.CultureInfo]::InvariantCulture))/s")
$lines.Add("DirectSocket maximum p95: $($maximumP95.ToString('F2', [Globalization.CultureInfo]::InvariantCulture)) ms")
$lines.Add("DirectSocket maximum p99: $($maximumP99.ToString('F2', [Globalization.CultureInfo]::InvariantCulture)) ms")
[IO.File]::WriteAllLines($markdownPath, $lines, [Text.UTF8Encoding]::new($false))

Write-Host "Inbound A/B JSON: $jsonPath"
Write-Host "Inbound A/B Markdown: $markdownPath"
if (-not $passed) { exit 1 }
