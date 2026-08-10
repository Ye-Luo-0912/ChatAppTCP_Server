<#
.SYNOPSIS
    TCP-MEM-1 内存归因画像：对 10k authenticated 连接的静默、heartbeat-only、
    1% active + 1% slow-reader 三类画像分别做 10–15 分钟测量，采 gcdump/PSS/fd/
    ss -tinm/cgroup sock 证据并汇总 Linux 内存归因。

.DESCRIPTION
    本脚本只产证据，不夹带任何功能或默认值改动。它委托 Run-CapacityCurve.ps1
    执行真实负载（固定 10k 连接、Release 二进制、2 个 Gateway），并在测量中段
    并行采集每例 Gateway 的 dotnet-gcdump 与 socket 证据（ss -tinm、/proc/net/sockstat）。

    三类画像的映射：
      - silent：heartbeat 长间隔近似。10000 连接只认证、保持空闲；少部分 active sender
        以极低速率发心跳，其余连接的 keepalive 间隔远大于测量窗口，从而近似静默。
      - heartbeat：heartbeat-only。全部连接按配置速率发心跳。
      - active：chat。1% 连接为 active sender，另 1% 为 slow reader，其余保持空闲心跳。

    每类画像默认重复 3 轮（每轮 10–15 分钟），满足「每个候选至少三轮短样本」；
    可用 -Repeats 调整，-Smoke 用于快速验证脚本链路（小连接数、短时长、单轮）。

    报告：每类画像每轮生成独立 capacity-curve 报告（含 Linux memory attribution 表），
    脚本末尾再产出跨画像的 memory-profile 汇总 JSON/Markdown。

.PARAMETER Profiles
    要执行的画像子集：silent / heartbeat / active。默认全部。

.PARAMETER Repeats
    每类画像的轮数。默认 3。

.PARAMETER DurationSeconds
    每轮测量时长（秒）。范围 600–900（10–15 分钟）。默认 600。

.PARAMETER WarmupSeconds
    全部连接建立后的稳定期（秒）。不计入正式测量。默认 30。

.PARAMETER TcpConnections
    每轮 TCP 连接数。默认 10000。

.PARAMETER TcpConnectionsPerSecond
    连接 ramp 速率。默认 1000/s。

.PARAMETER GatewayCount
    Gateway 实例数。默认 2。

.PARAMETER HeartbeatMessagesPerSecond
    heartbeat 画像中每个 active sender 的心跳速率（msg/s）。默认 2。

.PARAMETER TcpPayloadBytes
    chat 画像业务负载字节数。默认 512。

.PARAMETER ActiveSenderFraction
    active 画像中 active sender 与 slow reader 各占连接的比例。默认 0.01（1%）。

.PARAMETER SilentInactiveHeartbeatSeconds
    silent 画像非 active 连接的 keepalive 间隔；应远大于测量窗口（上限 900s）以近似静默。
    默认 3600（受底层容量曲线校验上限约束）。

.PARAMETER ActiveInactiveHeartbeatSeconds
    heartbeat/active 画像非发送连接的 keepalive 间隔。默认 30。

.PARAMETER ActiveMessagesPerSecond
    active 画像每个 active sender 的消息速率（msg/s）。默认 1。

.PARAMETER ReportDirectory
    报告输出目录。默认 .artifacts\performance。

.PARAMETER SkipBuild
    跳过 Gateway/Realtime 构建（需已用同等源码构建）。

.PARAMETER Smoke
    快速冒烟：单轮、短时长、小连接数，用于验证脚本链路而非产出正式证据。

.EXAMPLE
    .\Run-MemoryProfile.ps1
    以默认参数执行全部三类画像（3 轮 × 10 分钟）。

.EXAMPLE
    .\Run-MemoryProfile.ps1 -Profiles silent -Repeats 1 -DurationSeconds 600
    仅 silent 画像单轮 10 分钟。

.EXAMPLE
    .\Run-MemoryProfile.ps1 -Smoke
    快速验证脚本链路。
#>
[CmdletBinding()]
param(
    [ValidateSet('silent', 'heartbeat', 'active')]
    [string[]] $Profiles = @('silent', 'heartbeat', 'active'),
    [ValidateRange(1, 5)] [int] $Repeats = 3,
    [ValidateRange(30, 900)] [int] $DurationSeconds = 600,
    [ValidateRange(0, 600)] [int] $WarmupSeconds = 30,
    [ValidateRange(100, 100000)] [int] $TcpConnections = 10000,
    [ValidateRange(100, 100000)] [int] $TcpConnectionsPerSecond = 1000,
    [ValidateRange(1, 16)] [int] $GatewayCount = 2,
    [ValidateRange(0.001, 100)] [double] $HeartbeatMessagesPerSecond = 2.0,
    [ValidateRange(1, 1048576)] [int] $TcpPayloadBytes = 512,
    [ValidateRange(0.0001, 0.5)] [double] $ActiveSenderFraction = 0.01,
    [ValidateRange(0, 3600)] [int] $SilentInactiveHeartbeatSeconds = 3600,
    [ValidateRange(0, 3600)] [int] $ActiveInactiveHeartbeatSeconds = 30,
    [ValidateRange(0.001, 100)] [double] $ActiveMessagesPerSecond = 1.0,
    [ValidateRange(1024, 65535)] [int] $GatewayBasePort = 18888,
    [ValidateRange(1024, 65535)] [int] $RealtimePort = 18080,
    [ValidateRange(1024, 65535)] [int] $NatsPort = 4222,
    [ValidateRange(1024, 65535)] [int] $NatsMonitorPort = 18222,
    [ValidateRange(1024, 65535)] [int] $PostgresPort = 15432,
    [ValidateRange(1024, 65535)] [int] $GarnetPort = 16379,
    [ValidateSet('Pipelines', 'DirectSocket')] [string] $InboundTransportMode = 'DirectSocket',
    [ValidateSet('PersistentSendLoop', 'OnDemandSendPump', 'PerSessionDrain')] [string] $OutboundSendMode = 'PersistentSendLoop',
    [ValidateSet('BoundedChannel', 'LazySegmented')] [string] $OutboundQueueMode = 'BoundedChannel',
    [ValidateRange(0, 1024)] [int] $OnDemandSendWorkerCount = 0,
    [ValidateRange(1, 1024)] [int] $OnDemandSendBurstLimit = 16,
    [string] $ReportDirectory,
    [switch] $SkipBuild,
    [switch] $Smoke
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if ($Smoke) {
    # 冒烟链路验证：小连接、短时长、单轮。不产出正式证据。
    $TcpConnections = 500
    $DurationSeconds = 60
    $Repeats = 1
    $WarmupSeconds = 10
    if ($TcpConnectionsPerSecond -gt 500) { $TcpConnectionsPerSecond = 500 }
}

$capacityScript = Join-Path $PSScriptRoot 'Run-CapacityCurve.ps1'
if (-not (Test-Path -LiteralPath $capacityScript -PathType Leaf)) {
    throw "Capacity-curve runner was not found: $capacityScript"
}
$gcdumpScript = Join-Path $PSScriptRoot 'Performance-Gcdump.ps1'
if (-not (Test-Path -LiteralPath $gcdumpScript -PathType Leaf)) {
    throw "Gcdump helpers were not found: $gcdumpScript"
}
$dependencyHelpers = Join-Path $PSScriptRoot 'Performance-DependencyPreflight.ps1'
if (-not (Test-Path -LiteralPath $dependencyHelpers -PathType Leaf)) {
    throw "Dependency preflight helpers were not found: $dependencyHelpers"
}
. $dependencyHelpers

$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..\..'))
if ([string]::IsNullOrWhiteSpace($ReportDirectory)) {
    $ReportDirectory = Join-Path $repositoryRoot '.artifacts\performance'
}
$ReportDirectory = [IO.Path]::GetFullPath($ReportDirectory)
[IO.Directory]::CreateDirectory($ReportDirectory) | Out-Null

$startedAt = [DateTimeOffset]::UtcNow
$stamp = $startedAt.ToString("yyyyMMdd-HHmmss'Z'")
$memoryProfileRoot = Join-Path $ReportDirectory "memory-profile-$stamp"
[IO.Directory]::CreateDirectory($memoryProfileRoot) | Out-Null

$rampSeconds = [Math]::Ceiling($TcpConnections / [double]$TcpConnectionsPerSecond)
if ($rampSeconds -lt 1) { $rampSeconds = 1 }

# 三类画像的确定性映射。silent 用 heartbeat 长间隔近似静默。
$onePercent = [int][Math]::Ceiling($TcpConnections * $ActiveSenderFraction)
$silentSenders = [Math]::Max($GatewayCount, [int][Math]::Ceiling($TcpConnections * 0.001))
$activeSenders = [Math]::Max($GatewayCount, $onePercent)
$slowReaders = [Math]::Max($GatewayCount, $onePercent)
$activePercent = [int]($ActiveSenderFraction * 100)

$profileConfigs = [ordered]@{
    silent = [ordered]@{
        Label = 'silent'
        Description = '10k authenticated idle (heartbeat long-interval approximation)'
        TcpMode = 'heartbeat'
        TcpActiveSenders = $silentSenders
        TcpMessagesPerSecond = 0.01
        TcpInactiveHeartbeatSeconds = $SilentInactiveHeartbeatSeconds
        TcpSlowReaders = 0
        TcpPayloadBytes = 128
    }
    heartbeat = [ordered]@{
        Label = 'heartbeat'
        Description = '10k authenticated heartbeat-only'
        TcpMode = 'heartbeat'
        TcpActiveSenders = 0
        TcpMessagesPerSecond = $HeartbeatMessagesPerSecond
        TcpInactiveHeartbeatSeconds = $ActiveInactiveHeartbeatSeconds
        TcpSlowReaders = 0
        TcpPayloadBytes = 128
    }
    active = [ordered]@{
        Label = 'active'
        Description = "10k, $activePercent percent active senders + $activePercent percent slow readers"
        TcpMode = 'chat'
        TcpActiveSenders = $activeSenders
        TcpMessagesPerSecond = $ActiveMessagesPerSecond
        TcpInactiveHeartbeatSeconds = $ActiveInactiveHeartbeatSeconds
        TcpSlowReaders = $slowReaders
        TcpPayloadBytes = $TcpPayloadBytes
    }
}

$selectedProfiles = [Collections.Generic.List[string]]::new()
foreach ($configuredProfile in $Profiles) {
    if ($profileConfigs.Contains($configuredProfile)) {
        $selectedProfiles.Add($configuredProfile)
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

# 在测量中段并行采集 gcdump 与 socket 证据的后台作业。
# 单独的发现阶段能容忍 build/startup 耗时：进程出现后再睡到测量中段。
$collectorScript = {
    param(
        [Parameter(Mandatory)] [string] $GcdumpHelpersPath,
        [Parameter(Mandatory)] [string] $OutputDirectory,
        [Parameter(Mandatory)] [double] $RampSeconds,
        [Parameter(Mandatory)] [double] $WarmupSeconds,
        [Parameter(Mandatory)] [double] $DurationSeconds,
        [Parameter(Mandatory)] [double] $DiscoveryTimeoutSeconds,
        [Parameter(Mandatory)] [int] $GatewayCount
    )

    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'

    function Invoke-MemoryProfileSocketSnapshot {
        param(
            [Parameter(Mandatory)] [string] $OutputDirectory,
            [AllowEmptyCollection()] [int[]] $Pids
        )
        $snapshot = [ordered]@{
            TimestampUtc = [DateTimeOffset]::UtcNow
            SocketCountByPid = [ordered]@{}
        }
        if (-not $IsLinux) {
            return $snapshot
        }

        $ssRaw = @(& ss -tinm -p 2>$null)
        $ssPath = Join-Path $OutputDirectory 'ss-tinm.txt'
        [IO.File]::WriteAllLines($ssPath, $ssRaw, [Text.UTF8Encoding]::new($false))

        foreach ($pid in $Pids) {
            $matching = @($ssRaw | Where-Object { $_ -match "(^|\s)pid=$pid," })
            $snapshot.SocketCountByPid["$pid"] = $matching.Count
        }

        $sockstatLines = @()
        foreach ($sockstatPath in @('/proc/net/sockstat', '/proc/net/sockstat6')) {
            if (Test-Path -LiteralPath $sockstatPath -PathType Leaf) {
                $sockstatLines += "# $sockstatPath"
                $sockstatLines += Get-Content -LiteralPath $sockstatPath
            }
        }
        if ($sockstatLines.Count -ne 0) {
            $sockstatFile = Join-Path $OutputDirectory 'proc-net-sockstat.txt'
            [IO.File]::WriteAllLines($sockstatFile, $sockstatLines, [Text.UTF8Encoding]::new($false))
        }
        return $snapshot
    }

    [IO.Directory]::CreateDirectory($OutputDirectory) | Out-Null
    . $GcdumpHelpersPath

    $result = [ordered]@{
        Gcdumps = @()
        SocketSnapshot = $null
        Error = $null
    }
    try {
        $pids = @()
        $deadline = [DateTime]::UtcNow.AddSeconds($DiscoveryTimeoutSeconds)
        while ([DateTime]::UtcNow -lt $deadline -and $pids.Count -eq 0) {
            $pids = @(Get-PerformanceGatewayProcessId)
            if ($pids.Count -eq 0) {
                Start-Sleep -Seconds 2
            }
        }
        if ($pids.Count -eq 0) {
            $result.Error = 'No Gateway process discovered before timeout; gcdump/socket evidence skipped.'
            return $result
        }

        $midSeconds = [Math]::Max(0.0, $RampSeconds + $WarmupSeconds + ($DurationSeconds / 2.0))
        Write-Host ("[memory-collector] {0} Gateway(s) discovered; snapshot at +{1:F0}s (mid-measurement)." -f $pids.Count, $midSeconds)
        Start-Sleep -Seconds ([int][Math]::Ceiling($midSeconds))

        $collected = [Collections.Generic.List[string]]::new()
        for ($index = 0; $index -lt $pids.Count; $index++) {
            try {
                $path = Invoke-PerformanceGcdumpCollect `
                    -ProcessId $pids[$index] `
                    -OutputDirectory $OutputDirectory `
                    -Label "gateway-$($index + 1)"
                $collected.Add($path)
            }
            catch {
                Write-Warning $_.Exception.Message
            }
        }
        $result.Gcdumps = $collected.ToArray()
        $result.SocketSnapshot = Invoke-MemoryProfileSocketSnapshot `
            -OutputDirectory $OutputDirectory `
            -Pids $pids
    }
    catch {
        $result.Error = $_.Exception.Message
    }
    return $result
}

$profileResults = [Collections.Generic.List[object]]::new()
$runErrors = [Collections.Generic.List[string]]::new()
$overallSucceeded = $true

foreach ($profileName in $selectedProfiles) {
    $config = $profileConfigs[$profileName]
    for ($repeat = 1; $repeat -le $Repeats; $repeat++) {
        $profileDirectory = Join-Path $memoryProfileRoot (
            "$profileName-$repeat")
        [IO.Directory]::CreateDirectory($profileDirectory) | Out-Null
        $evidenceDirectory = Join-Path $profileDirectory 'evidence'
        [IO.Directory]::CreateDirectory($evidenceDirectory) | Out-Null

        Write-Host ''
        Write-Host "=== TCP-MEM-1 profile '$profileName' repeat $repeat/$Repeats ==="
        Write-Host ("    {0} (conns={1}, ramp={2}/s, measure={3}s, warmup={4}s)" -f `
            $config.Description, $TcpConnections, $TcpConnectionsPerSecond, $DurationSeconds, $WarmupSeconds)

        $invocationManifestPath = Join-Path $profileDirectory "invocation-$repeat.json"
        $discoveryTimeoutSeconds = [Math]::Max(
            120.0,
            ($rampSeconds + $WarmupSeconds + $DurationSeconds + 120.0))

        # 后台采集器必须在负载启动前拉起，才能捕获 Gateway 进程并精准命中测量中段。
        $collectorJob = $null
        if ($IsLinux) {
            $collectorJob = Start-Job -ScriptBlock $collectorScript -ArgumentList @(
                $gcdumpScript,
                $evidenceDirectory,
                [double]$rampSeconds,
                [double]$WarmupSeconds,
                [double]$DurationSeconds,
                [double]$discoveryTimeoutSeconds,
                [int]$GatewayCount)
        }
        else {
            Write-Host '[memory-collector] 非 Linux 主机：跳过 gcdump/socket 证据采集（仅能力校验）。' -ForegroundColor Yellow
        }

        $arguments = [ordered]@{
            ValidationProfile = 'Change'
            Rates = @(1)
            DurationSeconds = $DurationSeconds
            WarmupSeconds = $WarmupSeconds
            TcpConnections = $TcpConnections
            TcpActiveSenders = [int]$config.TcpActiveSenders
            TcpMode = [string]$config.TcpMode
            TcpMessagesPerSecond = [double]$config.TcpMessagesPerSecond
            TcpDeliveryDrainSeconds = 30
            TcpInactiveHeartbeatSeconds = [int]$config.TcpInactiveHeartbeatSeconds
            TcpPayloadBytes = [int]$config.TcpPayloadBytes
            TcpSlowReaders = [int]$config.TcpSlowReaders
            TcpConnectionsPerSecond = $TcpConnectionsPerSecond
            GatewayBasePort = $GatewayBasePort
            RealtimePort = $RealtimePort
            RealtimeProcessingConcurrency = 4
            NatsPort = $NatsPort
            NatsMonitorPort = $NatsMonitorPort
            PostgresPort = $PostgresPort
            GarnetPort = $GarnetPort
            InboundTransportMode = $InboundTransportMode
            OutboundSendMode = $OutboundSendMode
            OutboundQueueMode = $OutboundQueueMode
            OnDemandSendWorkerCount = $OnDemandSendWorkerCount
            OnDemandSendBurstLimit = $OnDemandSendBurstLimit
            ReportDirectory = $profileDirectory
            InvocationManifestPath = $invocationManifestPath
            UseTcpMessagesPerSecond = $true
            NoPipeline = $true
        }
        if ($SkipBuild) {
            $arguments.SkipBuild = $true
        }

        $capacityExit = 1
        $invocationError = $null
        try {
            & $capacityScript @arguments | Out-Host
            $capacityExit = $LASTEXITCODE
        }
        catch {
            $invocationError = $_.Exception.Message
            $capacityExit = 1
        }

        $collectorOutcome = [ordered]@{
            Profile = $profileName
            Repeat = $repeat
            Gcdumps = @()
            SocketSnapshot = $null
            CollectorError = $null
        }
        if ($null -ne $collectorJob) {
            $jobResult = $null
            try {
                Wait-Job -Job $collectorJob -Timeout 600 | Out-Null
                $jobRaw = Receive-Job -Job $collectorJob -ErrorAction SilentlyContinue
                Remove-Job -Job $collectorJob -Force -ErrorAction SilentlyContinue
                $jobResult = @($jobRaw) | Where-Object { $_ -is [System.Management.Automation.PSCustomObject] } |
                    Select-Object -First 1
            }
            catch {
                $collectorOutcome.CollectorError = $_.Exception.Message
            }
            if ($null -ne $jobResult) {
                $collectorOutcome.Gcdumps = @(Get-OptionalProperty $jobResult 'Gcdumps' @())
                $collectorOutcome.SocketSnapshot = Get-OptionalProperty $jobResult 'SocketSnapshot' $null
                $collectorOutcome.CollectorError = Get-OptionalProperty $jobResult 'Error' $null
            }
        }

        # 读取本轮 capacity-curve 报告并提取 Linux 内存归因。
        $curveDirectory = $null
        if (Test-Path -LiteralPath $invocationManifestPath -PathType Leaf) {
            $manifest = Get-Content -LiteralPath $invocationManifestPath -Raw | ConvertFrom-Json
            $curveDirectory = [IO.Path]::GetFullPath([string](Get-OptionalProperty $manifest 'RunDirectory' ''))
        }
        $benchmarkReport = $null
        if (-not [string]::IsNullOrWhiteSpace($curveDirectory) -and
            (Test-Path -LiteralPath $curveDirectory -PathType Container)) {
            $reportFile = Get-ChildItem -LiteralPath $curveDirectory -Filter 'benchmark-report.json' -Recurse |
                Sort-Object LastWriteTimeUtc -Descending |
                Select-Object -First 1
            if ($null -ne $reportFile) {
                $benchmarkReport = Get-Content -LiteralPath $reportFile.FullName -Raw | ConvertFrom-Json
            }
        }

        $gatewayResources = @()
        if ($null -ne $benchmarkReport) {
            $gatewayResources = @($benchmarkReport.ProcessResources |
                Where-Object { (Get-OptionalProperty $_ 'Label' '') -like 'gateway-*' })
        }
        $profileResult = [ordered]@{
            Profile = $profileName
            Repeat = $repeat
            Description = [string]$config.Description
            TcpMode = [string]$config.TcpMode
            TcpConnections = $TcpConnections
            TcpActiveSenders = [int]$config.TcpActiveSenders
            TcpSlowReaders = [int]$config.TcpSlowReaders
            TcpMessagesPerSecond = [double]$config.TcpMessagesPerSecond
            TcpInactiveHeartbeatSeconds = [int]$config.TcpInactiveHeartbeatSeconds
            DurationSeconds = $DurationSeconds
            WarmupSeconds = $WarmupSeconds
            CapacityExitCode = $capacityExit
            InvocationError = $invocationError
            RunValid = $capacityExit -eq 0 -and $null -eq $invocationError -and
                $null -ne $benchmarkReport
            CurveDirectory = $curveDirectory
            BenchmarkReport = if ($null -ne $benchmarkReport) {
                Join-Path $curveDirectory 'benchmark-report.json'
            } else { $null }
            GatewayResources = $gatewayResources
            Gcdumps = $collectorOutcome.Gcdumps
            SocketSnapshot = $collectorOutcome.SocketSnapshot
            CollectorError = $collectorOutcome.CollectorError
        }
        $profileResults.Add($profileResult)
        if (-not [bool]$profileResult.RunValid) {
            $overallSucceeded = $false
            $runErrors.Add("$profileName/${repeat}: capacity exit=$capacityExit; error=$invocationError")
        }
    }
}

$summary = [ordered]@{
    Kind = 'memory-profile'
    SchemaVersion = 1
    RunId = $stamp
    StartedAtUtc = $startedAt
    CompletedAtUtc = [DateTimeOffset]::UtcNow
    OverallSucceeded = $overallSucceeded
    Profiles = $selectedProfiles.ToArray()
    Repeats = $Repeats
    Configuration = [ordered]@{
        TcpConnections = $TcpConnections
        TcpConnectionsPerSecond = $TcpConnectionsPerSecond
        GatewayCount = $GatewayCount
        DurationSeconds = $DurationSeconds
        WarmupSeconds = $WarmupSeconds
        SilentInactiveHeartbeatSeconds = $SilentInactiveHeartbeatSeconds
        ActiveInactiveHeartbeatSeconds = $ActiveInactiveHeartbeatSeconds
        HeartbeatMessagesPerSecond = $HeartbeatMessagesPerSecond
        ActiveSenderFraction = $ActiveSenderFraction
        ActiveMessagesPerSecond = $ActiveMessagesPerSecond
        TcpPayloadBytes = $TcpPayloadBytes
        InboundTransportMode = $InboundTransportMode
        OutboundSendMode = $OutboundSendMode
        OutboundQueueMode = $OutboundQueueMode
    }
    Results = $profileResults.ToArray()
    Errors = $runErrors.ToArray()
}

$summaryJson = Join-Path $memoryProfileRoot 'memory-profile-report.json'
Write-PerformanceJsonArtifact -Path $summaryJson -Value $summary -Depth 12

# 生成人类可读的归因汇总 Markdown。
$tee = [Collections.Generic.List[string]]::new()
$tee.Add('# TCP-MEM-1 memory attribution')
$tee.Add('')
$tee.Add("Run: **$stamp**; overall: **$(if ($overallSucceeded) { 'PASSED' } else { 'FAILED' })**.")
$tee.Add('')
$tee.Add("Profiles: $($selectedProfiles -join ', '); repeats=$Repeats; connections=$TcpConnections; " +
    "ramp=$TcpConnectionsPerSecond/s; measure=$DurationSeconds s; warmup=$WarmupSeconds s; gateways=$GatewayCount.")
$tee.Add('')
$tee.Add('PSS 来自 smaps_rollup，VmRSS/VmHWM 来自 /proc 状态，fd 峰值来自 /proc/{pid}/fd；' +
    '这些区分 committed/native cache；managed retained 需叠加同轮 gcdump。')
$tee.Add('')
foreach ($profileName in $selectedProfiles) {
    $profileRows = @($profileResults | Where-Object Profile -eq $profileName)
    if ($profileRows.Count -eq 0) { continue }
    $first = $profileRows[0]
    $tee.Add("## Profile: $profileName")
    $tee.Add('')
    $tee.Add($first.Description)
    $tee.Add('')
    foreach ($row in $profileRows) {
        $tee.Add("Repeat $($row.Repeat): **$(if ($row.RunValid) { 'VALID' } else { 'INVALID' })**" +
            " (exit=$($row.CapacityExitCode))")
        $gateways = @($row.GatewayResources)
        if ($gateways.Count -eq 0) {
            $tee.Add('- No gateway resource summary available.')
            $tee.Add('')
            continue
        }
        $tee.Add('')
        $tee.Add('| Gateway | Max PSS MiB | Max VmRSS MiB | Max VmHWM MiB | Max cgroup peak MiB | Max fd |')
        $tee.Add('|---|---:|---:|---:|---:|---:|')
        foreach ($gateway in $gateways) {
            $label = [string]$gateway.Label
            $pssMiB = ([double](Get-OptionalProperty $gateway 'MaximumPssBytes' 0)) / 1048576d
            $rssMiB = ([double](Get-OptionalProperty $gateway 'MaximumVmRssBytes' 0)) / 1048576d
            $hwmMiB = ([double](Get-OptionalProperty $gateway 'MaximumVmHwmBytes' 0)) / 1048576d
            $cgroupMiB = ([double](Get-OptionalProperty $gateway 'MaximumCgroupMemoryPeakBytes' 0)) / 1048576d
            $fd = [int](Get-OptionalProperty $gateway 'MaximumFileDescriptorCount' 0)
            $tee.Add(('| {0} | {1:F2} | {2:F2} | {3:F2} | {4:F2} | {5} |' -f `
                $label, $pssMiB, $rssMiB, $hwmMiB, $cgroupMiB, $fd))
        }
        $tee.Add('')
        $gcdumpCount = @($row.Gcdumps).Count
        $tee.Add("- gcdumps: $gcdumpCount; socket snapshot: $(if ($null -ne $row.SocketSnapshot) { 'captured' } else { 'none' })")
        if (-not [string]::IsNullOrWhiteSpace([string]$row.CollectorError)) {
            $tee.Add("- collector: $($row.CollectorError)")
        }
        $tee.Add('')
    }
}
if ($runErrors.Count -ne 0) {
    $tee.Add('## Errors')
    $tee.Add('')
    foreach ($errorText in $runErrors) {
        $tee.Add("- $errorText")
    }
    $tee.Add('')
}

$summaryMd = [IO.Path]::ChangeExtension($summaryJson, '.md')
[IO.File]::WriteAllLines($summaryMd, $tee, [Text.UTF8Encoding]::new($false))

Write-Host ''
Write-Host "TCP-MEM-1 memory profile summary: $($summaryMd)"
if ($overallSucceeded) {
    Write-Host 'TCP-MEM-1: PASSED (all profiles/repeats valid).' -ForegroundColor Green
    exit 0
}
Write-Host 'TCP-MEM-1: FAILED (one or more profiles/repeats invalid).' -ForegroundColor Red
exit 1