<#
.SYNOPSIS
    分层性能验证入口：日常修改优先分钟级反馈，8 小时仅保留给正式发布候选。

.DESCRIPTION
    Smoke/Change/Capacity/Candidate 使用容量编排器和临时依赖容器；每档仍执行逐消息 ACK、
    跨 Gateway 投递、重复/漏投、Outbox/JetStream、资源覆盖率和数据库诊断门禁。
    Formal 委托 Run-Soak.ps1，并继承其冻结快照强制校验和内存稳定性判定。

    Candidate 与 Formal 必须显式传 -ConfirmLongRun，避免日常开发误启动长测。

.EXAMPLE
    .\Run-PerformanceValidation.ps1 -Profile Change -SkipBuild
    .\Run-PerformanceValidation.ps1 -Profile Capacity -SkipBuild
    .\Run-PerformanceValidation.ps1 -Profile Candidate -ConfirmLongRun -SkipBuild
    .\Run-PerformanceValidation.ps1 -Profile Formal -ConfirmLongRun -SkipBuild
#>
param(
    [ValidateSet('Smoke','Change','Capacity','Candidate','Formal')]
    [string] $Profile = 'Change',
    [ValidateRange(1, 100000)] [int] $AggregateMessagesPerSecond = 80,
    [string] $ReportDirectory,
    [switch] $SkipBuild,
    [switch] $ConfirmLongRun,
    [switch] $DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..\..'))
if ([string]::IsNullOrWhiteSpace($ReportDirectory)) {
    $ReportDirectory = Join-Path $repositoryRoot '.artifacts\performance-validation'
}
$ReportDirectory = [IO.Path]::GetFullPath($ReportDirectory)

$profileSpec = switch ($Profile) {
    'Smoke' {
        [pscustomobject]@{
            Rates = @($AggregateMessagesPerSecond)
            DurationSeconds = 20
            WarmupSeconds = 5
            Connections = 1000
            DeliveryDrainSeconds = 10
            ResourceCoveragePercent = 70
            EvidenceClass = 'correctness-smoke'
        }
    }
    'Change' {
        [pscustomobject]@{
            Rates = @(80, 320)
            DurationSeconds = 45
            WarmupSeconds = 10
            Connections = 1000
            DeliveryDrainSeconds = 15
            ResourceCoveragePercent = 85
            EvidenceClass = 'change-feedback'
        }
    }
    'Capacity' {
        [pscustomobject]@{
            Rates = @(80, 320, 640)
            DurationSeconds = 90
            WarmupSeconds = 15
            Connections = 1000
            DeliveryDrainSeconds = 30
            ResourceCoveragePercent = 90
            EvidenceClass = 'capacity-screen'
        }
    }
    'Candidate' {
        [pscustomobject]@{
            Rates = @($AggregateMessagesPerSecond)
            DurationSeconds = 1800
            WarmupSeconds = 120
            Connections = 10000
            DeliveryDrainSeconds = 60
            ResourceCoveragePercent = 95
            EvidenceClass = 'release-candidate-screen'
        }
    }
    'Formal' {
        [pscustomobject]@{
            Rates = @($AggregateMessagesPerSecond)
            DurationSeconds = 28800
            WarmupSeconds = 300
            Connections = 10000
            DeliveryDrainSeconds = 60
            ResourceCoveragePercent = 95
            EvidenceClass = 'formal-release-evidence'
        }
    }
}

if ($Profile -in @('Candidate','Formal') -and -not $ConfirmLongRun) {
    throw "$Profile is a long-running profile. Pass -ConfirmLongRun explicitly."
}

function Get-AvailableTcpPort {
    param(
        [Parameter(Mandatory)]
        [AllowEmptyCollection()]
        [Collections.Generic.HashSet[int]] $Reserved)

    for ($attempt = 0; $attempt -lt 32; $attempt++) {
        $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
        try {
            $listener.Start()
            $port = ([Net.IPEndPoint]$listener.LocalEndpoint).Port
            if ($Reserved.Add($port)) { return $port }
        }
        finally {
            $listener.Stop()
        }
    }
    throw 'Unable to reserve an available local TCP port for performance validation.'
}

function Get-AvailableTcpPortPair {
    param(
        [Parameter(Mandatory)]
        [AllowEmptyCollection()]
        [Collections.Generic.HashSet[int]] $Reserved)

    for ($attempt = 0; $attempt -lt 64; $attempt++) {
        $firstPort = Get-Random -Minimum 20000 -Maximum 60000
        if ($Reserved.Contains($firstPort) -or $Reserved.Contains($firstPort + 1)) { continue }
        $first = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $firstPort)
        $second = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $firstPort + 1)
        try {
            $first.Start()
            $second.Start()
            [void]$Reserved.Add($firstPort)
            [void]$Reserved.Add($firstPort + 1)
            return $firstPort
        }
        catch [Net.Sockets.SocketException] {
            continue
        }
        finally {
            $first.Stop()
            $second.Stop()
        }
    }
    throw 'Unable to reserve two consecutive local TCP ports for the Gateway pair.'
}

$selectedPorts = $null
if ($Profile -ne 'Formal') {
    $reservedPorts = [Collections.Generic.HashSet[int]]::new()
    $selectedPorts = [pscustomobject]@{
        GatewayBasePort = Get-AvailableTcpPortPair -Reserved $reservedPorts
        RealtimePort = Get-AvailableTcpPort -Reserved $reservedPorts
        NatsPort = Get-AvailableTcpPort -Reserved $reservedPorts
        NatsMonitorPort = Get-AvailableTcpPort -Reserved $reservedPorts
        PostgresPort = Get-AvailableTcpPort -Reserved $reservedPorts
        GarnetPort = Get-AvailableTcpPort -Reserved $reservedPorts
    }
}

$stamp = [DateTimeOffset]::UtcNow.ToString("yyyyMMdd-HHmmss'Z'")
$runDirectory = Join-Path $ReportDirectory "validation-$($Profile.ToLowerInvariant())-$stamp"
[IO.Directory]::CreateDirectory($runDirectory) | Out-Null
$profileManifestPath = Join-Path $runDirectory 'validation-profile.json'
$estimatedSeconds = if ($Profile -eq 'Formal') {
    $profileSpec.DurationSeconds + $profileSpec.WarmupSeconds + $profileSpec.DeliveryDrainSeconds
} else {
    $profileSpec.Rates.Count * (
        $profileSpec.DurationSeconds + $profileSpec.WarmupSeconds +
        $profileSpec.DeliveryDrainSeconds + 45)
}
$profileManifest = [pscustomobject]@{
    SchemaVersion = 1
    ValidationProfile = $Profile
    EvidenceClass = $profileSpec.EvidenceClass
    CreatedAtUtc = [DateTimeOffset]::UtcNow
    EstimatedMaximumSeconds = $estimatedSeconds
    LongRunConfirmed = [bool]$ConfirmLongRun
    DryRun = [bool]$DryRun
    Configuration = [pscustomobject]@{
        Rates = $profileSpec.Rates
        DurationSeconds = $profileSpec.DurationSeconds
        WarmupSeconds = $profileSpec.WarmupSeconds
        TcpConnections = $profileSpec.Connections
        TcpActiveSenders = $profileSpec.Connections
        TcpCrossGateway = $true
        TcpPayloadBytes = 512
        TcpDeliveryDrainSeconds = $profileSpec.DeliveryDrainSeconds
        RealtimeProcessingConcurrency = 16
        MinimumResourceSampleCoveragePercent = $profileSpec.ResourceCoveragePercent
        PostgresWalCompression = 'lz4'
        PostgresCheckpointTimeoutSeconds = 900
        PostgresMaxWalSizeMb = 4096
        Ports = $selectedPorts
    }
    ReportDirectory = $runDirectory
}
[IO.File]::WriteAllText(
    $profileManifestPath,
    ($profileManifest | ConvertTo-Json -Depth 8),
    [Text.UTF8Encoding]::new($false))

Write-Host "Performance validation profile: $Profile ($($profileSpec.EvidenceClass))"
Write-Host "Estimated upper-bound runtime: $estimatedSeconds seconds"
Write-Host "Profile manifest: $profileManifestPath"
if ($DryRun) {
    Write-Host 'Dry run complete; no services or containers were started.'
    return
}

if ($Profile -eq 'Formal') {
    $soakScript = Join-Path $PSScriptRoot 'Run-Soak.ps1'
    $perSenderRate = $AggregateMessagesPerSecond / [double]$profileSpec.Connections
    $arguments = @{
        ValidationProfile = 'Formal'
        DurationSeconds = $profileSpec.DurationSeconds
        WarmupSeconds = $profileSpec.WarmupSeconds
        TcpMode = 'chat'
        TcpConnections = $profileSpec.Connections
        TcpActiveSenders = $profileSpec.Connections
        TcpMessagesPerSecond = $perSenderRate
        TcpDeliveryDrainSeconds = $profileSpec.DeliveryDrainSeconds
        TcpPayloadBytes = 512
        TcpConnectionsPerSecond = 500
        RealtimeProcessingConcurrency = 16
        MinimumResourceSampleCoveragePercent = $profileSpec.ResourceCoveragePercent
        PostgresWalCompression = 'lz4'
        PostgresCheckpointTimeoutSeconds = 900
        PostgresMaxWalSizeMb = 4096
        ReportDirectory = $runDirectory
    }
    if ($SkipBuild) { $arguments.SkipBuild = $true }
    & $soakScript @arguments
    $childSucceeded = $?
    $childExitCode = $LASTEXITCODE
    if ($null -eq $childExitCode) { $childExitCode = if ($childSucceeded) { 0 } else { 1 } }
    exit $childExitCode
}

$capacityScript = Join-Path $PSScriptRoot 'Run-CapacityCurve.ps1'
$capacityArguments = @{
    ValidationProfile = $Profile
    Rates = $profileSpec.Rates
    DurationSeconds = $profileSpec.DurationSeconds
    WarmupSeconds = $profileSpec.WarmupSeconds
    TcpConnections = $profileSpec.Connections
    TcpActiveSenders = $profileSpec.Connections
    TcpMode = 'chat'
    TcpCrossGateway = $true
    TcpDeliveryDrainSeconds = $profileSpec.DeliveryDrainSeconds
    TcpInactiveHeartbeatSeconds = 30
    TcpPayloadBytes = 512
    TcpConnectionsPerSecond = 500
    RealtimeProcessingConcurrency = 16
    GatewayBasePort = $selectedPorts.GatewayBasePort
    RealtimePort = $selectedPorts.RealtimePort
    NatsPort = $selectedPorts.NatsPort
    NatsMonitorPort = $selectedPorts.NatsMonitorPort
    PostgresPort = $selectedPorts.PostgresPort
    GarnetPort = $selectedPorts.GarnetPort
    MinimumConnectionSuccessPercent = 99
    MinimumPeakConnectionPercent = 99
    MinimumThroughputAttainmentPercent = 90
    MinimumAcknowledgementPercent = 99
    MinimumDeliveryPercent = 95
    MinimumResourceSampleCoveragePercent = $profileSpec.ResourceCoveragePercent
    MaximumDeadLetters = 0
    PostgresWalCompression = 'lz4'
    PostgresCheckpointTimeoutSeconds = 900
    PostgresMaxWalSizeMb = 4096
    ReportDirectory = $runDirectory
    NoPipeline = $true
}
if ($SkipBuild) { $capacityArguments.SkipBuild = $true }
& $capacityScript @capacityArguments
$childSucceeded = $?
$childExitCode = $LASTEXITCODE
if ($null -eq $childExitCode) { $childExitCode = if ($childSucceeded) { 0 } else { 1 } }
exit $childExitCode
