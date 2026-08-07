Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$helper = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot `
    '..\..\tools\ChatApp.Performance.Orchestrator\scripts\Performance-DockerLifecycle.ps1'))
. $helper

$script:dockerState = [pscustomobject]@{
    Containers = @{}
    FailStarts = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    FailCreatesAfterCreation = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    Removed = [Collections.Generic.List[string]]::new()
}

function docker {
    param([Parameter(ValueFromRemainingArguments)] [object[]] $DockerArguments)

    $values = @($DockerArguments | ForEach-Object { [string]$_ })
    if ($values.Count -ge 1 -and $values[0] -eq 'create') {
        $nameIndex = [Array]::IndexOf($values, '--name')
        $labelIndex = [Array]::IndexOf($values, '--label')
        $name = $values[$nameIndex + 1]
        $label = $values[$labelIndex + 1]
        $runId = $label.Substring($label.IndexOf('=', [StringComparison]::Ordinal) + 1)
        if ($script:dockerState.Containers.ContainsKey($name)) {
            $global:LASTEXITCODE = 125
            return
        }
        $script:dockerState.Containers[$name] = [pscustomobject]@{
            Name = $name
            RunId = $runId
            Running = $false
        }
        if ($script:dockerState.FailCreatesAfterCreation.Contains($name)) {
            $global:LASTEXITCODE = 125
            return
        }
        $global:LASTEXITCODE = 0
        "mock-id-$name"
        return
    }

    if ($values.Count -eq 2 -and $values[0] -eq 'start') {
        $name = $values[1]
        if ($script:dockerState.FailStarts.Contains($name)) {
            $global:LASTEXITCODE = 125
            return
        }
        $script:dockerState.Containers[$name].Running = $true
        $global:LASTEXITCODE = 0
        $name
        return
    }

    if ($values.Count -eq 3 -and $values[0] -eq 'container' -and
        $values[1] -eq 'inspect') {
        $name = $values[2]
        if (-not $script:dockerState.Containers.ContainsKey($name)) {
            $global:LASTEXITCODE = 1
            return
        }
        $container = $script:dockerState.Containers[$name]
        $global:LASTEXITCODE = 0
        @([pscustomobject]@{
            Name = "/$name"
            Config = [pscustomobject]@{
                Labels = @{
                    'chatapp.performance.run-id' = $container.RunId
                }
            }
        }) | ConvertTo-Json -Depth 5
        return
    }

    if ($values.Count -ge 3 -and $values[0] -eq 'rm') {
        $name = $values[-1]
        if (-not $script:dockerState.Containers.ContainsKey($name)) {
            $global:LASTEXITCODE = 1
            return
        }
        $script:dockerState.Containers.Remove($name)
        $script:dockerState.Removed.Add($name)
        $global:LASTEXITCODE = 0
        return
    }

    throw "Unexpected mock docker invocation: $($values -join ' ')"
}

function Assert-True([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

$created = [Collections.Generic.List[string]]::new()
$script:dockerState.FailStarts.Add('start-fails') | Out-Null
try {
    Start-PerformanceDockerContainer `
        -Name 'start-fails' -RunId 'run-a' -CreatedContainers $created `
        -CreateArguments @('image:test')
    throw 'Expected docker start failure.'
}
catch {
    Assert-True ($_.Exception.Message -like 'docker start failed*') `
        "Start failure was not reported by the lifecycle helper: $($_.Exception.Message)"
}
Assert-True ($created.Contains('start-fails')) `
    'A created container must be tracked before docker start is attempted.'
Remove-PerformanceDockerContainers `
    -Names @('start-fails', 'already-missing') -RunId 'run-a' -RemoveVolumes
Assert-True (-not $script:dockerState.Containers.ContainsKey('start-fails')) `
    'The container left by a failed start was not removed.'

$script:dockerState.Containers['foreign'] = [pscustomobject]@{
    Name = 'foreign'
    RunId = 'another-run'
    Running = $true
}
$foreignCreated = [Collections.Generic.List[string]]::new()
try {
    Start-PerformanceDockerContainer `
        -Name 'foreign' -RunId 'run-a' -CreatedContainers $foreignCreated `
        -CreateArguments @('image:test')
    throw 'Expected duplicate-name create failure.'
}
catch {
    Assert-True ($_.Exception.Message -like 'docker create failed*') `
        "Create failure was not reported by the lifecycle helper: $($_.Exception.Message)"
}
Remove-PerformanceDockerContainers -Names @('foreign') -RunId 'run-a' -RemoveVolumes
Assert-True ($script:dockerState.Containers.ContainsKey('foreign')) `
    'Cleanup removed a same-name container that is not owned by this run.'
Assert-True ($foreignCreated.Count -eq 0) `
    'A same-name foreign container must not be recorded as created by this run.'

$script:dockerState.FailCreatesAfterCreation.Add('partial-create') | Out-Null
$partialCreated = [Collections.Generic.List[string]]::new()
try {
    Start-PerformanceDockerContainer `
        -Name 'partial-create' -RunId 'run-a' -CreatedContainers $partialCreated `
        -CreateArguments @('image:test')
    throw 'Expected partial create failure.'
}
catch {
    Assert-True ($_.Exception.Message -like 'docker create failed*') `
        "Partial create failure was not reported by the lifecycle helper: $($_.Exception.Message)"
}
Assert-True ($partialCreated.Contains('partial-create')) `
    'An owned container left by a failed create must still be tracked.'
Remove-PerformanceDockerContainers `
    -Names @('partial-create') -RunId 'run-a' -RemoveVolumes
Assert-True (-not $script:dockerState.Containers.ContainsKey('partial-create')) `
    'An owned container left by a failed create was not removed.'

Write-Host 'Performance Docker lifecycle tests passed.'
