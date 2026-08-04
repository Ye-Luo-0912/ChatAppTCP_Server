param(
    [ValidateSet('concurrent-replay', 'redis-failover', 'circuit-breaker',
        'takeover-competition', 'reconnect-storm', 'recovery-convergence')]
    [string[]] $Scenarios = @(
        'concurrent-replay',
        'takeover-competition',
        'redis-failover',
        'circuit-breaker',
        'reconnect-storm',
        'recovery-convergence'),
    [ValidateRange(2, 16)] [int] $GatewayCount = 2,
    [ValidateRange(1, 1000)] [int] $UserCount = 50,
    [ValidateRange(10, 10000)] [int] $StormSize = 1000,
    [ValidateRange(5, 600)] [int] $FaultAfterSeconds = 10,
    [ValidateRange(3, 300)] [int] $FaultDurationSeconds = 15,
    [ValidateRange(10, 600)] [int] $RecoveryWindowSeconds = 60,
    [int] $GatewayBasePort = 18888,
    [int] $RealtimePort = 18080,
    [int] $NatsPort = 4222,
    [int] $NatsMonitorPort = 18222,
    [int] $PostgresPort = 15432,
    [int] $GarnetPort = 16379,
    [int] $MetricsPort = 19090,
    [string] $NatsImage = 'nats:2.10.26-alpine',
    [string] $PostgresImage = 'postgres:16.8',
    [string] $GarnetImage = 'ghcr.io/microsoft/garnet:1.0.84',
    [string] $ReportDirectory,
    [switch] $SkipBuild
)

# Resume 故障压力验证编排脚本。
# 复用 Run-FaultInjection.ps1 的 Docker/Gateway 启动模式，但：
# - Gateway 必须开启 RequireClientHello=true + EnableResume=true
# - 使用 ChatApp.ResumeVerification 工具驱动场景
# - 故障场景（redis-failover/circuit-breaker/recovery-convergence）协调 Garnet pause/resume

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..\..'))
$realtimeRoot = [IO.Path]::GetFullPath((Join-Path $repositoryRoot '..\ChatApp.RealtimeServices'))
$orchestratorProject = Join-Path $repositoryRoot 'tools\ChatApp.Performance.Orchestrator'
$verificationProject = Join-Path $repositoryRoot 'tools\ChatApp.ResumeVerification\ChatApp.ResumeVerification.csproj'
if (-not (Test-Path -LiteralPath $verificationProject -PathType Leaf)) {
    throw "ResumeVerification project was not found: $verificationProject"
}
if (-not (Test-Path -LiteralPath $realtimeRoot -PathType Container)) {
    throw "Realtime repository was not found: $realtimeRoot"
}
if ([string]::IsNullOrWhiteSpace($ReportDirectory)) {
    $ReportDirectory = Join-Path $repositoryRoot '.artifacts\resume-verification'
}
$ReportDirectory = [IO.Path]::GetFullPath($ReportDirectory)
$startedAt = [DateTimeOffset]::UtcNow
$stamp = $startedAt.ToString("yyyyMMdd-HHmmss'Z'")
$runDirectory = Join-Path $ReportDirectory "resume-stress-$stamp"
[IO.Directory]::CreateDirectory($runDirectory) | Out-Null

if (-not $SkipBuild) {
    Write-Host "Building Gateway solution..." -ForegroundColor Cyan
    & dotnet build (Join-Path $repositoryRoot 'ChatApp.TcpGateway.sln') -c Release --no-restore
    if ($LASTEXITCODE -ne 0) { throw 'Gateway solution build failed.' }
    Write-Host "Building Realtime solution..." -ForegroundColor Cyan
    & dotnet build (Join-Path $realtimeRoot 'ChatApp.RealtimeServices.slnx') -c Release --no-restore
    if ($LASTEXITCODE -ne 0) { throw 'Realtime solution build failed.' }
}

$dbEnvName = 'CHATAPP_RESUME_DB'
$garnetEnvName = 'CHATAPP_RESUME_GARNET'
$oldDbEnv = [Environment]::GetEnvironmentVariable($dbEnvName, 'Process')
$oldGarnetEnv = [Environment]::GetEnvironmentVariable($garnetEnvName, 'Process')
$password = 'resume-' + [Guid]::NewGuid().ToString('N')
[Environment]::SetEnvironmentVariable(
    $dbEnvName,
    "Host=127.0.0.1;Port=$PostgresPort;Database=ChatAppDatabase;Username=postgres;Password=$password;Pooling=true;Minimum Pool Size=0;Maximum Pool Size=100;Connection Idle Lifetime=300;Timeout=3;Command Timeout=5;",
    'Process')
[Environment]::SetEnvironmentVariable(
    $garnetEnvName, "127.0.0.1:$GarnetPort,abortConnect=false", 'Process')

# 定位构建产物（与 BenchmarkRunner.ResolveBinaries 同模式）。
function Find-BuildOutput {
    param([string] $ProjectPath, [string] $AssemblyName)
    $binDir = Join-Path (Split-Path $ProjectPath -Parent) "bin\Release"
    if (-not (Test-Path $binDir)) {
        throw "Build output not found: $binDir (run without -SkipBuild first)"
    }
    $found = Get-ChildItem -Path $binDir -Filter $AssemblyName -Recurse |
        Where-Object { $_.FullName -notmatch '\\(ref|publish)\\' } |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    if (-not $found) { throw "Assembly not found: $AssemblyName under $binDir" }
    return $found.FullName
}

$gatewayDll = Find-BuildOutput `
    (Join-Path $repositoryRoot 'ChatApp.TcpGateway.csproj') 'ChatApp.TcpGateway.dll'
$realtimeDll = Find-BuildOutput `
    (Join-Path $realtimeRoot 'ChatApp.RealtimeServices\ChatApp.RealtimeServices.csproj') `
    'ChatApp.RealtimeServices.dll'
$verificationDll = Find-BuildOutput $verificationProject 'ChatApp.ResumeVerification.dll'

function Invoke-Docker([string[]] $Arguments) {
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker $($Arguments[0]) failed with exit code $LASTEXITCODE."
    }
}

function Wait-Postgres([string] $Container) {
    for ($attempt = 1; $attempt -le 30; $attempt++) {
        & docker exec $Container pg_isready -U postgres -d ChatAppDatabase *> $null
        if ($LASTEXITCODE -eq 0) { return }
        Start-Sleep -Seconds 1
    }
    throw "PostgreSQL did not become ready: $Container"
}

function Wait-GatewayReady([int] $Port, [int] $TimeoutSeconds = 60) {
    # Gateway 是纯 TCP 服务（无 HTTP /metrics 端点），用 TCP 连接探测就绪状态。
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        try {
            $client = [System.Net.Sockets.TcpClient]::new()
            $iar = $client.BeginConnect('127.0.0.1', $Port, $null, $null)
            if ($iar.AsyncWaitHandle.WaitOne(500)) {
                $client.EndConnect($iar)
                $client.Close()
                return $true
            }
            $client.Close()
        } catch { }
        Start-Sleep -Milliseconds 500
    }
    return $false
}

function Start-ManagedProcess {
    param(
        [string] $Id,
        [string] $Assembly,
        [string[]] $Arguments,
        [string] $WorkingDirectory,
        [hashtable] $Environment,
        [string] $LogPath
    )
    # 使用 bash 包装脚本设置环境变量并重定向输出，避免 PowerShell Task.Run 的类型/作用域问题。
    $stderrLog = $LogPath + '.stderr'
    $wrapperDir = [IO.Path]::GetDirectoryName($LogPath)
    $wrapperPath = [IO.Path]::ChangeExtension($LogPath, '.sh')
    $quotedArgs = $Arguments | ForEach-Object {
        $v = $_
        if ($v -match '\s') { "'$v'" } else { $v }
    }
    $argLine = $quotedArgs -join ' '
    $envLines = $Environment.GetEnumerator() | ForEach-Object {
        $k = $_.Key
        $v = $_.Value -replace "'", "'\''"
        "export $k='$v'"
    }
    $envBlock = $envLines -join "`n"
    $quotedAssembly = $Assembly
    if ($quotedAssembly -match '\s') { $quotedAssembly = "'$quotedAssembly'" }
    $script = @"
#!/usr/bin/env bash
set -e
$envBlock
cd '$WorkingDirectory'
exec dotnet $quotedAssembly $argLine
"@
    [IO.File]::WriteAllText($wrapperPath, $script, [Text.UTF8Encoding]::new($false))
    & chmod +x $wrapperPath
    $proc = Start-Process -FilePath '/bin/bash' -ArgumentList @($wrapperPath) -NoNewWindow -PassThru `
        -RedirectStandardOutput $LogPath -RedirectStandardError $stderrLog
    return [pscustomobject]@{
        Id = $Id
        Process = $proc
        LogPath = $LogPath
        StderrLogPath = $stderrLog
        WrapperPath = $wrapperPath
    }
}

function Stop-ManagedProcess {
    param($Managed)
    if ($null -ne $Managed -and -not $Managed.Process.HasExited) {
        try { Stop-Process -Id $Managed.Process.Id -Force -ErrorAction SilentlyContinue } catch { }
        $Managed.Process.WaitForExit(5000) | Out-Null
    }
}

# 分类场景：基础场景（无 Redis 操控）vs 故障场景（需 Garnet pause/resume）
$basicScenarios = $Scenarios | Where-Object { $_ -in @('concurrent-replay', 'takeover-competition', 'reconnect-storm') }
$faultScenarios = $Scenarios | Where-Object { $_ -in @('redis-failover', 'circuit-breaker', 'recovery-convergence') }

$results = [Collections.Generic.List[object]]::new()
$createdContainers = [Collections.Generic.List[string]]::new()
$managedProcesses = [Collections.Generic.List[object]]::new()

try {
    Write-Host "Starting dependency containers..." -ForegroundColor Cyan
    $tag = $stamp.Replace('-', '').ToLowerInvariant()
    $nats = "resume-verify-nats-$tag"
    $postgres = "resume-verify-pg-$tag"
    $garnet = "resume-verify-garnet-$tag"

    Invoke-Docker @('run','-d','--name',$nats,
        '-p',"127.0.0.1:$($NatsPort):4222",
        '-p',"127.0.0.1:$($NatsMonitorPort):8222",
        $NatsImage,'-js','-m','8222')
    $createdContainers.Add($nats)

    Invoke-Docker @('run','-d','--name',$postgres,
        '-e',"POSTGRES_PASSWORD=$password",
        '-e','POSTGRES_DB=ChatAppDatabase',
        '-p',"127.0.0.1:$($PostgresPort):5432",$PostgresImage)
    $createdContainers.Add($postgres)

    Invoke-Docker @('run','-d','--name',$garnet,
        '-p',"127.0.0.1:$($GarnetPort):6379",$GarnetImage)
    $createdContainers.Add($garnet)

    Wait-Postgres $postgres

    Write-Host "Starting Realtime service..." -ForegroundColor Cyan
    $realtimeArgs = @(
        '--environment=Development',
        "--urls=http://127.0.0.1:$RealtimePort",
        "--Nats:Url=nats://127.0.0.1:$NatsPort",
        '--Nats:Mode=JetStream',
        '--RealtimeIntegration:Replicas=1',
        '--Observability:OtlpEnabled=false',
        '--Logging:LogLevel:Default=Warning'
    )
    $realtimeEnv = @{
        'DOTNET_ENVIRONMENT' = 'Development'
        'ConnectionStrings__RealtimeDatabase' = [Environment]::GetEnvironmentVariable($dbEnvName, 'Process')
        'ConnectionStrings__Garnet' = [Environment]::GetEnvironmentVariable($garnetEnvName, 'Process')
    }
    $realtimeLog = Join-Path $runDirectory 'realtime.stdout.log'
    $realtimeProc = Start-ManagedProcess `
        'realtime-1' $realtimeDll $realtimeArgs `
        (Join-Path $realtimeRoot 'ChatApp.RealtimeServices') $realtimeEnv $realtimeLog
    $managedProcesses.Add($realtimeProc)

    # 等待 Realtime 就绪
    $realtimeReady = $false
    for ($i = 0; $i -lt 60; $i++) {
        try {
            $resp = Invoke-WebRequest -Uri "http://127.0.0.1:$RealtimePort/ready" -TimeoutSec 2 -SkipHttpErrorCheck
            if ($resp.StatusCode -eq 200) { $realtimeReady = $true; break }
        } catch { }
        Start-Sleep -Seconds 1
    }
    if (-not $realtimeReady) { throw 'Realtime service did not become ready.' }
    Write-Host "Realtime service ready." -ForegroundColor Green

    # 启动多个 Gateway，全部开启 RequireClientHello + EnableResume
    Write-Host "Starting $GatewayCount Gateway instances with Resume enabled..." -ForegroundColor Cyan
    $gatewayEndpoints = @()
    for ($i = 0; $i -lt $GatewayCount; $i++) {
        $port = $GatewayBasePort + $i
        $admissionLimit = $StormSize + 256
        $gatewayArgs = @(
            "--TcpGateway:ListenAddress=127.0.0.1",
            "--TcpGateway:Port=$port",
            "--TcpGateway:MaxConnections=$admissionLimit",
            "--TcpGateway:MaxConnectionsPerIp=$admissionLimit",
            "--TcpGateway:MaxUnauthenticatedConnections=$admissionLimit",
            "--TcpGateway:InboundTransportMode=DirectSocket",
            "--TcpGateway:RequireClientHello=true",
            "--TcpGateway:EnableResume=true",
            "--TcpGateway:ResumeTokenTtl=00:01:00",
            "--TcpGateway:ReplaceSameDeviceSession=true",
            "--TcpGateway:IdleTimeout=00:02:00",
            "--Observability:OtlpEnabled=false",
            "--Logging:LogLevel:Default=Warning"
        )
        $gatewayEnv = @{
            'DOTNET_ENVIRONMENT' = 'Development'
            'RealtimeIntegration__Url' = "nats://127.0.0.1:$NatsPort"
            "RealtimeIntegration__InstanceId" = "resume-verify-gw-$($i + 1)"
            'Redis__ConnectionString' = [Environment]::GetEnvironmentVariable($garnetEnvName, 'Process')
        }
        $gwLog = Join-Path $runDirectory "gateway-$($i + 1).stdout.log"
        $gwProc = Start-ManagedProcess `
            "gateway-$($i + 1)" $gatewayDll $gatewayArgs `
            $repositoryRoot $gatewayEnv $gwLog
        $managedProcesses.Add($gwProc)
        $gatewayEndpoints += "127.0.0.1:$port"
    }

    # 等待所有 Gateway 就绪
    foreach ($ep in $gatewayEndpoints) {
        $port = ($ep -split ':')[1]
        if (-not (Wait-GatewayReady $port 60)) {
            throw "Gateway on port $port did not become ready."
        }
    }
    Write-Host "All Gateway instances ready." -ForegroundColor Green

    $garnetEnvValue = [Environment]::GetEnvironmentVariable($garnetEnvName, 'Process')

    # 运行基础场景（无 Redis 操控）
    foreach ($scenario in $basicScenarios) {
        Write-Host "`n=== Running basic scenario: $scenario ===" -ForegroundColor Yellow
        $scenarioDir = Join-Path $runDirectory $scenario
        [IO.Directory]::CreateDirectory($scenarioDir) | Out-Null
        $scenarioLog = Join-Path $scenarioDir 'verification.stdout.log'

        $stormArg = if ($scenario -eq 'reconnect-storm') { @('--storm-size', $StormSize) } else { @() }
        $toolArgs = @(
            $verificationDll,
            '--gateway-endpoint',$gatewayEndpoints[0])
        for ($i = 1; $i -lt $gatewayEndpoints.Count; $i++) {
            $toolArgs += '--gateway-endpoint',$gatewayEndpoints[$i]
        }
        $toolArgs += @(
            '--redis-connection-string',$garnetEnvValue,
            '--scenario',$scenario,
            '--user-count',$UserCount,
            '--report-directory',$scenarioDir,
            '--warmup-seconds','3',
            '--redis-down-delay-seconds','0',
            '--redis-recovery-delay-seconds','0'
        ) + $stormArg

        $sw = [Diagnostics.Stopwatch]::StartNew()
        $proc = Start-Process -FilePath 'dotnet' -ArgumentList $toolArgs -NoNewWindow -PassThru `
            -RedirectStandardOutput $scenarioLog -RedirectStandardError (Join-Path $scenarioDir 'verification.stderr.log')
        $proc.WaitForExit()
        $sw.Stop()

        $reportFile = Get-ChildItem -LiteralPath $scenarioDir -Filter 'resume-verification-*.json' -Recurse |
            Sort-Object LastWriteTime -Descending | Select-Object -First 1
        $report = if ($reportFile) { Get-Content $reportFile.FullName -Raw | ConvertFrom-Json } else { $null }
        $scenarioResult = if ($report) {
            $report.Scenarios | Where-Object { $_.Name -eq $scenario } | Select-Object -First 1
        } else { $null }
        $passed = if ($scenarioResult) { [bool]$scenarioResult.Passed } else { $false }
        $summary = if ($scenarioResult) { $scenarioResult.Summary } else { "No report generated (exit $($proc.ExitCode))" }

        $results.Add([pscustomobject]@{
            Scenario = $scenario
            Passed = $passed
            DurationSeconds = $sw.Elapsed.TotalSeconds
            Summary = $summary
            ExitCode = $proc.ExitCode
            Report = if ($reportFile) { $reportFile.FullName } else { $null }
        })
        $status = if ($passed) { 'PASSED' } else { 'FAILED' }
        Write-Host "  ${scenario}: $status ($([Math]::Round($sw.Elapsed.TotalSeconds, 1))s)" -ForegroundColor $(if ($passed) { 'Green' } else { 'Red' })
    }

    # 运行故障场景（协调 Garnet pause/resume）
    foreach ($scenario in $faultScenarios) {
        Write-Host "`n=== Running fault scenario: $scenario ===" -ForegroundColor Yellow
        $scenarioDir = Join-Path $runDirectory $scenario
        [IO.Directory]::CreateDirectory($scenarioDir) | Out-Null
        $scenarioLog = Join-Path $scenarioDir 'verification.stdout.log'

        # 故障场景参数：
        # --warmup-seconds 3 → 认证阶段（Redis 健康）
        # --redis-down-delay-seconds $FaultAfterSeconds → 认证后等待（Redis 健康）
        # 之后工具尝试 Resume（期望失败，因 Redis 已 pause）
        # --redis-recovery-delay-seconds $FaultDurationSeconds → Redis 恢复等待
        # 之后工具尝试 Resume（期望成功）
        $totalDuration = 3 + $FaultAfterSeconds + $FaultDurationSeconds + $RecoveryWindowSeconds + 10

        $toolArgs = @(
            $verificationDll,
            '--gateway-endpoint',$gatewayEndpoints[0])
        for ($i = 1; $i -lt $gatewayEndpoints.Count; $i++) {
            $toolArgs += '--gateway-endpoint',$gatewayEndpoints[$i]
        }
        $toolArgs += @(
            '--redis-connection-string',$garnetEnvValue,
            '--scenario',$scenario,
            '--user-count',$UserCount,
            '--report-directory',$scenarioDir,
            '--warmup-seconds','3',
            '--redis-down-delay-seconds',$FaultAfterSeconds,
            '--redis-recovery-delay-seconds',$FaultDurationSeconds
        )

        $sw = [Diagnostics.Stopwatch]::StartNew()
        $proc = Start-Process -FilePath 'dotnet' -ArgumentList $toolArgs -NoNewWindow -PassThru `
            -RedirectStandardOutput $scenarioLog -RedirectStandardError (Join-Path $scenarioDir 'verification.stderr.log')

        # 等待认证完成 + FaultAfterSeconds 后 pause Garnet
        # 时间线：3s warmup + FaultAfterSeconds 认证后等待 → 此时工具将尝试 Resume
        $pauseDelay = 3 + $FaultAfterSeconds
        Write-Host "  Waiting ${pauseDelay}s before pausing Garnet..." -ForegroundColor DarkGray
        Start-Sleep -Seconds $pauseDelay

        if (-not $proc.HasExited) {
            Write-Host "  Pausing Garnet container ($garnet)..." -ForegroundColor Magenta
            Invoke-Docker @('pause',$garnet)

            # 等待故障期 + 恢复延迟
            Start-Sleep -Seconds $FaultDurationSeconds

            if (-not $proc.HasExited) {
                Write-Host "  Resuming Garnet container..." -ForegroundColor Magenta
                Invoke-Docker @('unpause',$garnet)
            }
        }

        # 等待工具完成（含恢复窗口）
        $proc.WaitForExit()
        $sw.Stop()

        $reportFile = Get-ChildItem -LiteralPath $scenarioDir -Filter 'resume-verification-*.json' -Recurse |
            Sort-Object LastWriteTime -Descending | Select-Object -First 1
        $report = if ($reportFile) { Get-Content $reportFile.FullName -Raw | ConvertFrom-Json } else { $null }
        $scenarioResult = if ($report) {
            $report.Scenarios | Where-Object { $_.Name -eq $scenario } | Select-Object -First 1
        } else { $null }
        $passed = if ($scenarioResult) { [bool]$scenarioResult.Passed } else { $false }
        $summary = if ($scenarioResult) { $scenarioResult.Summary } else { "No report generated (exit $($proc.ExitCode))" }

        $results.Add([pscustomobject]@{
            Scenario = $scenario
            Passed = $passed
            DurationSeconds = $sw.Elapsed.TotalSeconds
            Summary = $summary
            ExitCode = $proc.ExitCode
            Report = if ($reportFile) { $reportFile.FullName } else { $null }
        })
        $status = if ($passed) { 'PASSED' } else { 'FAILED' }
        Write-Host "  ${scenario}: $status ($([Math]::Round($sw.Elapsed.TotalSeconds, 1))s)" -ForegroundColor $(if ($passed) { 'Green' } else { 'Red' })

        # 确保 Garnet 恢复（故障场景间隔离）
        try { Invoke-Docker @('unpause',$garnet) } catch { }
        Start-Sleep -Seconds 2
    }

} catch {
    Write-Error "Resume verification failed: $($_.Exception.Message)"
    throw
} finally {
    # 停止所有托管进程
    foreach ($mp in $managedProcesses) {
        Stop-ManagedProcess $mp
    }
    # 清理 Docker 容器
    if ($createdContainers.Count -gt 0) {
        Write-Host "Cleaning up containers..." -ForegroundColor DarkGray
        & docker rm -f @($createdContainers) 2>$null | Out-Null
    }
    [Environment]::SetEnvironmentVariable($dbEnvName, $oldDbEnv, 'Process')
    [Environment]::SetEnvironmentVariable($garnetEnvName, $oldGarnetEnv, 'Process')
}

# 生成汇总报告
$completedAt = [DateTimeOffset]::UtcNow
$summary = [pscustomobject]@{
    StartedAtUtc = $startedAt
    CompletedAtUtc = $completedAt
    Configuration = [pscustomobject]@{
        Scenarios = $Scenarios
        GatewayCount = $GatewayCount
        UserCount = $UserCount
        StormSize = $StormSize
        FaultAfterSeconds = $FaultAfterSeconds
        FaultDurationSeconds = $FaultDurationSeconds
        RecoveryWindowSeconds = $RecoveryWindowSeconds
        GatewayEndpoints = $gatewayEndpoints
    }
    Results = $results
    AllPassed = @($results | Where-Object { -not $_.Passed }).Count -eq 0
}

$jsonPath = Join-Path $runDirectory 'resume-fault-stress-report.json'
$markdownPath = Join-Path $runDirectory 'resume-fault-stress-report.md'
[IO.File]::WriteAllText($jsonPath, ($summary | ConvertTo-Json -Depth 10),
    [Text.UTF8Encoding]::new($false))

$lines = [Collections.Generic.List[string]]::new()
$lines.Add('# Resume fault stress verification')
$lines.Add('')
$lines.Add("Window: $($startedAt.ToString('O')) - $($completedAt.ToString('O'))")
$lines.Add("Overall: $(if ($summary.AllPassed) { 'PASSED' } else { 'FAILED' })")
$lines.Add('')
$lines.Add('| Scenario | Passed | Duration s | Exit Code | Summary |')
$lines.Add('|---|---|---:|---:|---|')
foreach ($r in $results) {
    $lines.Add([string]::Format(
        [Globalization.CultureInfo]::InvariantCulture,
        '| {0} | {1} | {2:F1} | {3} | {4} |',
        $r.Scenario, $r.Passed, $r.DurationSeconds, $r.ExitCode,
        ($r.Summary -replace '\|', '\|')))
}
$lines.Add('')
$lines.Add('Fault scenarios coordinate Garnet pause/unpause via Docker. ' +
    'Basic scenarios run without Redis manipulation.')
[IO.File]::WriteAllLines($markdownPath, $lines, [Text.UTF8Encoding]::new($false))

Write-Host "`n=== Resume Fault Stress Verification Complete ===" -ForegroundColor Cyan
Write-Host "JSON: $jsonPath"
Write-Host "Markdown: $markdownPath"
Write-Host "Overall: $(if ($summary.AllPassed) { 'PASSED' } else { 'FAILED' })" -ForegroundColor $(if ($summary.AllPassed) { 'Green' } else { 'Red' })

if (-not $summary.AllPassed) { exit 1 }
