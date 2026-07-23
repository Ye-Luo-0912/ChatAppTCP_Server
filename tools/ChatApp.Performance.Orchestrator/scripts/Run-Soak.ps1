param(
    [ValidateRange(300, 86400)] [int] $DurationSeconds = 28800,
    [ValidateRange(0, 3600)] [int] $WarmupSeconds = 300,
    [ValidateRange(1, 100000)] [int] $PipelineOperationsPerSecond = 80,
    [ValidateRange(1, 1024)] [int] $PipelineConcurrency = 32,
    [ValidateRange(1, 1048576)] [int] $PipelinePayloadBytes = 512,
    [ValidateRange(1, 100000)] [int] $TcpConnections = 1000,
    [string] $ReportDirectory,
    [switch] $SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$capacityScript = Join-Path $PSScriptRoot 'Run-CapacityCurve.ps1'
if (-not (Test-Path -LiteralPath $capacityScript -PathType Leaf)) {
    throw "Capacity-curve runner was not found: $capacityScript"
}

Write-Host "Starting soak run: duration=$DurationSeconds s; pipeline target=$PipelineOperationsPerSecond/s."
Write-Host 'The generated benchmark report includes process, Docker, GC/heap, Npgsql connection, JetStream and Outbox trends.'

$arguments = @{
    Rates = @($PipelineOperationsPerSecond)
    DurationSeconds = $DurationSeconds
    WarmupSeconds = $WarmupSeconds
    PipelineConcurrency = $PipelineConcurrency
    PipelinePayloadBytes = $PipelinePayloadBytes
    TcpConnections = $TcpConnections
}
if (-not [string]::IsNullOrWhiteSpace($ReportDirectory)) {
    $arguments.ReportDirectory = $ReportDirectory
}
if ($SkipBuild) {
    $arguments.SkipBuild = $true
}

& $capacityScript @arguments
exit $LASTEXITCODE