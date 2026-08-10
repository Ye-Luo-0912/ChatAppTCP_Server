# TCP-MEM-1 gcdump 采集辅助函数。
# 只负责证据采集：定位 Gateway 进程、运行 dotnet-gcdump 快照、把 .gcdump 落盘。
# 不夹带任何功能或默认值改动；非 Linux 主机上函数返回空结果并给出明确说明。

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Test-PerformanceGcdumpTool {
    <#
    .SYNOPSIS
    检查 dotnet-gcdump 是否可用。
    #>
    [CmdletBinding()]
    [OutputType([bool])]
    param()

    return $null -ne (Get-Command 'dotnet-gcdump' -ErrorAction SilentlyContinue)
}

function Get-PerformanceGatewayProcessId {
    <#
    .SYNOPSIS
    通过 pgrep 定位 Gateway 进程（Linux）。返回进程 ID 数组；非 Linux 返回 @()。
    #>
    [CmdletBinding()]
    [OutputType([int[]])]
    param()

    if (-not $IsLinux) {
        Write-Host 'gcdump 采集仅在 Linux 上支持（依赖 pgrep 与 /proc）。' -ForegroundColor Yellow
        return @()
    }
    $gatewayProcesses = @(pgrep -f 'ChatApp.TcpGateway.dll') 2>$null
    return @($gatewayProcesses | ForEach-Object { [int]$_ })
}

function Invoke-PerformanceGcdumpCollect {
    <#
    .SYNOPSIS
    对单个进程取一次 dotnet-gcdump 快照，输出到指定目录。
    #>
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory)] [int] $ProcessId,
        [Parameter(Mandatory)] [string] $OutputDirectory,
        [Parameter(Mandatory)] [string] $Label
    )

    [IO.Directory]::CreateDirectory($OutputDirectory) | Out-Null
    $outputFile = Join-Path $OutputDirectory (
        "$($Label)-pid$ProcessId.gcdump")
    if (-not (Test-PerformanceGcdumpTool)) {
        throw 'dotnet-gcdump 未安装。请先执行: dotnet tool install -g dotnet-gcdump'
    }
    Write-Host "采集 $Label (pid=$ProcessId) gcdump -> $outputFile"
    & dotnet-gcdump collect -p $ProcessId -o $outputFile
    if ($LASTEXITCODE -ne 0) {
        throw "dotnet-gcdump 采集失败 (pid=$ProcessId, exit=$LASTEXITCODE)"
    }
    return $outputFile
}

function Collect-PerformanceGcdumps {
    <#
    .SYNOPSIS
    在测量的中段对全部 Gateway 进程各取一次 gcdump 快照。
    返回 .gcdump 文件路径数组；未安装工具或非 Linux 时返回 @()。
    #>
    [CmdletBinding()]
    [OutputType([string[]])]
    param(
        [Parameter(Mandatory)] [string] $OutputDirectory,
        # 连接 ramp 秒数：快照在 ramp+warmup+measurement/2 处触发。
        [Parameter(Mandatory)] [double] $RampSeconds,
        [Parameter(Mandatory)] [double] $WarmupSeconds,
        [Parameter(Mandatory)] [double] $DurationSeconds,
        # 等待 Gateway 进程出现的超时（秒）。
        [Parameter(Mandatory)] [double] $GatewayDiscoveryTimeoutSeconds
    )

    [IO.Directory]::CreateDirectory($OutputDirectory) | Out-Null
    if (-not $IsLinux) {
        Write-Host 'gcdump 采集仅在 Linux 上支持，跳过。' -ForegroundColor Yellow
        return @()
    }
    if (-not (Test-PerformanceGcdumpTool)) {
        Write-Host 'dotnet-gcdump 未安装，跳过 gcdump 采集。' -ForegroundColor Yellow
        return @()
    }

    $discovered = @()
    $deadline = [DateTime]::UtcNow.AddSeconds($GatewayDiscoveryTimeoutSeconds)
    while ([DateTime]::UtcNow -lt $deadline -and $discovered.Count -eq 0) {
        $discovered = @(Get-PerformanceGatewayProcessId)
        if ($discovered.Count -eq 0) {
            Start-Sleep -Seconds 2
        }
    }
    if ($discovered.Count -eq 0) {
        Write-Host '未发现 Gateway 进程，跳过 gcdump 采集。' -ForegroundColor Yellow
        return @()
    }

    $midMeasurementSeconds = [Math]::Max(0.0, $RampSeconds + $WarmupSeconds + ($DurationSeconds / 2.0))
    Write-Host ("将在 {0:F0}s 后于测量中段采集 {1} 个 Gateway gcdump..." -f $midMeasurementSeconds, $discovered.Count)
    Start-Sleep -Seconds ([int][Math]::Ceiling($midMeasurementSeconds))

    $collected = @()
    for ($index = 0; $index -lt $discovered.Count; $index++) {
        $pidValue = $discovered[$index]
        try {
            $path = Invoke-PerformanceGcdumpCollect `
                -ProcessId $pidValue `
                -OutputDirectory $OutputDirectory `
                -Label "gateway-$($index + 1)"
            $collected += $path
        }
        catch {
            Write-Warning $_.Exception.Message
        }
    }
    return @($collected)
}