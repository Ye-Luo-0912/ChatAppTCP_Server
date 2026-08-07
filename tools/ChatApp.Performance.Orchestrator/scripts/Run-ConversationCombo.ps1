param(
    [ValidateRange(30, 86400)] [int] $DurationSeconds = 120,
    [ValidateRange(0, 3600)] [int] $WarmupSeconds = 15,
    [ValidateRange(1, 100000)] [int] $PipelineOperationsPerSecond = 40,
    [ValidateRange(1, 1024)] [int] $PipelineConcurrency = 16,
    [ValidateRange(1, 1048576)] [int] $PipelinePayloadBytes = 512,
    [ValidateRange(1, 100000)] [int] $TcpConnections = 40,
    [ValidateRange(0, 100000)] [int] $TcpMessagesPerSecond = 10,
    [ValidateRange(0, 1000)] [int] $TcpSlowReaders = 5,
    [int] $GatewayBasePort = 18888,
    [int] $RealtimePort = 18080,
    [int] $NatsPort = 4222,
    [int] $NatsMonitorPort = 18222,
    [int] $PostgresPort = 15432,
    [int] $GarnetPort = 16379,
    [string] $NatsImage = 'nats:2.10.26-alpine',
    [string] $PostgresImage = 'postgres:16.8',
    [string] $GarnetImage = 'ghcr.io/microsoft/garnet:1.0.84',
    [string] $ReportDirectory,
    [switch] $SkipBuild,
    [switch] $SkipGate
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..\..'))
$realtimeRoot = [IO.Path]::GetFullPath((Join-Path $repositoryRoot '..\ChatApp.RealtimeServices'))
$orchestratorProject = Join-Path $repositoryRoot 'tools\ChatApp.Performance.Orchestrator'
$gateProject = Join-Path $repositoryRoot 'tools\ChatApp.Performance.Gate'
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
$runDirectory = Join-Path $ReportDirectory "conversation-combo-$stamp"
[IO.Directory]::CreateDirectory($runDirectory) | Out-Null
$dockerLifecycleHelpers = Join-Path $PSScriptRoot 'Performance-DockerLifecycle.ps1'
if (-not (Test-Path -LiteralPath $dockerLifecycleHelpers -PathType Leaf)) {
    throw "Docker lifecycle helpers were not found: $dockerLifecycleHelpers"
}
. $dockerLifecycleHelpers

if (-not $SkipBuild) {
    & dotnet build (Join-Path $repositoryRoot 'ChatApp.TcpGateway.sln') -c Release --no-restore
    if ($LASTEXITCODE -ne 0) { throw 'Gateway solution build failed.' }
    & dotnet build (Join-Path $realtimeRoot 'ChatApp.RealtimeServices.slnx') -c Release --no-restore
    if ($LASTEXITCODE -ne 0) { throw 'Realtime solution build failed.' }
}

$dbEnvName = 'CHATAPP_CONVERSATION_COMBO_DB'
$garnetEnvName = 'CHATAPP_CONVERSATION_COMBO_GARNET'
$oldDbEnv = [Environment]::GetEnvironmentVariable($dbEnvName, 'Process')
$oldGarnetEnv = [Environment]::GetEnvironmentVariable($garnetEnvName, 'Process')
$password = 'combo-' + [Guid]::NewGuid().ToString('N')
[Environment]::SetEnvironmentVariable(
    $dbEnvName,
    "Host=127.0.0.1;Port=$PostgresPort;Database=ChatAppDatabase;Username=postgres;Password=$password;Pooling=true;Minimum Pool Size=0;Maximum Pool Size=100;Connection Idle Lifetime=300;Timeout=5;Command Timeout=5;",
    'Process')
[Environment]::SetEnvironmentVariable(
    $garnetEnvName, "127.0.0.1:$GarnetPort,abortConnect=false", 'Process')

function Wait-Postgres([string] $Container) {
    for ($attempt = 1; $attempt -le 30; $attempt++) {
        & docker exec $Container pg_isready -U postgres -d ChatAppDatabase *> $null
        if ($LASTEXITCODE -eq 0) { return }
        Start-Sleep -Seconds 1
    }
    throw "PostgreSQL did not become ready: $Container"
}

$tag = $stamp.Replace('-', '').ToLowerInvariant()
$nats = "codex-chatapp-combo-nats-$tag"
$postgres = "codex-chatapp-combo-postgres-$tag"
$garnet = "codex-chatapp-combo-garnet-$tag"
$dockerRunId = "conversation-combo-$tag"
$created = [Collections.Generic.List[string]]::new()

Write-Host "Starting conversation combo: pipeline=$PipelineOperationsPerSecond/s + TCP chat($TcpConnections) slowReaders=$TcpSlowReaders."

try {
    try {
        Start-PerformanceDockerContainer `
            -Name $nats -RunId $dockerRunId -CreatedContainers $created `
            -CreateArguments @(
            '-p',"127.0.0.1:$($NatsPort):4222",
            '-p',"127.0.0.1:$($NatsMonitorPort):8222",
            $NatsImage,'-js','-m','8222')
        Start-PerformanceDockerContainer `
            -Name $postgres -RunId $dockerRunId -CreatedContainers $created `
            -CreateArguments @(
            '-e',"POSTGRES_PASSWORD=$password",
            '-e','POSTGRES_DB=ChatAppDatabase',
            '-p',"127.0.0.1:$($PostgresPort):5432",$PostgresImage)
        Start-PerformanceDockerContainer `
            -Name $garnet -RunId $dockerRunId -CreatedContainers $created `
            -CreateArguments @(
            '-p',"127.0.0.1:$($GarnetPort):6379",$GarnetImage)
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
            '--tcp-mode','chat',
            '--tcp-bootstrap-auth',
            '--tcp-bootstrap-user-id','9300000000',
            '--tcp-connections',"$TcpConnections",
            '--tcp-messages-per-second',"$TcpMessagesPerSecond",
            '--tcp-slow-readers',"$TcpSlowReaders",
            '--pipeline-concurrency',"$PipelineConcurrency",
            '--pipeline-operations-per-second',"$PipelineOperationsPerSecond",
            '--pipeline-payload-bytes',"$PipelinePayloadBytes",
            '--realtime-database-environment',$dbEnvName,
            '--garnet-environment',$garnetEnvName,
            '--docker-container',$nats,
            '--docker-container',$postgres,
            '--docker-container',$garnet,
            '--report-directory',$runDirectory)
        & dotnet @orchestratorArgs
        $orchestratorExit = $LASTEXITCODE

        $reportFile = Get-ChildItem -LiteralPath $runDirectory -Filter 'benchmark-report.json' -Recurse |
            Sort-Object LastWriteTimeUtc -Descending |
            Select-Object -First 1
        if ($null -eq $reportFile) {
            throw 'Benchmark report was not created for conversation combo.'
        }

        if (-not $SkipGate) {
            $gateOutput = Join-Path $reportFile.DirectoryName 'gate-result.json'
            & dotnet @(
                'run','--project',$gateProject,'-c','Release','--no-build','--',
                '--report',$reportFile.FullName,
                '--max-p95-ms','300',
                '--max-jetstream-pending','0',
                '--max-outbox-pending','16',
                '--max-outbox-oldest-age-seconds','5',
                '--require-conversation-stages',
                '--max-history-p95-ms','50',
                '--max-conversation-list-p95-ms','50',
                '--max-sync-bootstrap-p95-ms','100',
                '--output',$gateOutput)
            if ($LASTEXITCODE -ne 0) {
                throw "Performance gate failed. See $gateOutput"
            }
            Write-Host "Gate passed: $gateOutput"
        }

        if ($orchestratorExit -ne 0) {
            throw "Orchestrator exited with code $orchestratorExit."
        }

        Write-Host "Conversation combo completed: $($reportFile.FullName)"
        exit 0
    }
    finally {
        if ($created.Count -gt 0) {
            Remove-PerformanceDockerContainers `
                -Names @($created) -RunId $dockerRunId -RemoveVolumes
        }
    }
}
finally {
    [Environment]::SetEnvironmentVariable($dbEnvName, $oldDbEnv, 'Process')
    [Environment]::SetEnvironmentVariable($garnetEnvName, $oldGarnetEnv, 'Process')
}
