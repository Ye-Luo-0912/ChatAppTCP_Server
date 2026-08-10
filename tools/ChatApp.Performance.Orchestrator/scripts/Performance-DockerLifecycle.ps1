$script:PerformanceDockerRunLabel = 'chatapp.performance.run-id'

function Test-PerformanceDockerContainerOwnership {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $Name,
        [Parameter(Mandatory)] [string] $RunId
    )

    $inspectionJson = & docker container inspect $Name 2>$null
    if ($LASTEXITCODE -ne 0) {
        return $false
    }

    try {
        $inspection = @($inspectionJson | ConvertFrom-Json)
    }
    catch {
        Write-Warning "Could not parse Docker inspection data for '$Name'; leaving it untouched."
        return $false
    }
    if ($inspection.Count -ne 1) {
        return $false
    }

    $actualName = [string]$inspection[0].Name
    if ($actualName.StartsWith('/', [StringComparison]::Ordinal)) {
        $actualName = $actualName.Substring(1)
    }
    $labels = $inspection[0].Config.Labels
    $ownership = if ($null -eq $labels) {
        $null
    } else {
        $property = $labels.psobject.Properties[$script:PerformanceDockerRunLabel]
        if ($null -eq $property) { $null } else { [string]$property.Value }
    }

    return [string]::Equals($actualName, $Name, [StringComparison]::Ordinal) -and
        [string]::Equals($ownership, $RunId, [StringComparison]::Ordinal)
}

function Start-PerformanceDockerContainer {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $Name,
        [Parameter(Mandatory)] [string] $RunId,
        [Parameter(Mandatory)] [string[]] $CreateArguments,
        [Parameter(Mandatory)] [AllowEmptyCollection()]
        [Collections.Generic.List[string]] $CreatedContainers
    )

    if ([string]::IsNullOrWhiteSpace($Name) -or [string]::IsNullOrWhiteSpace($RunId)) {
        throw 'Temporary Docker container name and run id must be non-empty.'
    }

    # docker run combines create/start. A port-bind failure can leave a created
    # container behind while returning non-zero before the caller records it.
    # Split the phases so ownership is recorded before start is attempted.
    $dockerArguments = @(
        'create',
        '--name', $Name,
        '--label', "$script:PerformanceDockerRunLabel=$RunId"
    ) + $CreateArguments
    & docker @dockerArguments
    $createExitCode = $LASTEXITCODE
    if ($createExitCode -eq 0 -or
        (Test-PerformanceDockerContainerOwnership -Name $Name -RunId $RunId)) {
        if (-not $CreatedContainers.Contains($Name)) {
            $CreatedContainers.Add($Name)
        }
    }
    if ($createExitCode -ne 0) {
        throw "docker create failed for '$Name' with exit code $createExitCode."
    }

    & docker start $Name
    if ($LASTEXITCODE -ne 0) {
        throw "docker start failed for '$Name' with exit code $LASTEXITCODE."
    }
}

function Wait-PerformancePostgres {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $Container,
        [string] $Database = 'ChatAppDatabase',
        [ValidateRange(1, 300)] [int] $TimeoutSeconds = 60
    )

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        # The official image starts a temporary postmaster during initdb and then
        # shuts it down before exec'ing the final postgres process as PID 1.
        # pg_isready alone can therefore return a short-lived false positive.
        $pidOneCommand = & docker exec $Container sh -c 'cat /proc/1/comm' 2>$null
        $pidOneIsPostgres = $LASTEXITCODE -eq 0 -and
            [string]::Equals(
                ([string]::Join('', @($pidOneCommand))).Trim(),
                'postgres',
                [StringComparison]::Ordinal)
        if ($pidOneIsPostgres) {
            & docker exec $Container pg_isready -U postgres -d $Database *> $null
            if ($LASTEXITCODE -eq 0) {
                # Require a second successful probe after a short scheduling gap,
                # so a process transition cannot be mistaken for steady readiness.
                Start-Sleep -Milliseconds 250
                & docker exec $Container pg_isready -U postgres -d $Database *> $null
                if ($LASTEXITCODE -eq 0) {
                    return
                }
            }
        }

        Start-Sleep -Milliseconds 250
    }

    throw "PostgreSQL did not reach steady readiness: $Container"
}

function Remove-PerformanceDockerContainers {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string[]] $Names,
        [Parameter(Mandatory)] [string] $RunId,
        [switch] $RemoveVolumes
    )

    foreach ($name in @($Names | Where-Object {
        -not [string]::IsNullOrWhiteSpace($_)
    } | Select-Object -Unique)) {
        # A missing container is already clean. A same-name container without
        # this run's label is not ours and must never be removed.
        if (-not (Test-PerformanceDockerContainerOwnership -Name $name -RunId $RunId)) {
            continue
        }

        $removeArguments = [Collections.Generic.List[string]]::new()
        $removeArguments.Add('rm')
        $removeArguments.Add('-f')
        if ($RemoveVolumes) {
            $removeArguments.Add('-v')
        }
        $removeArguments.Add($name)
        & docker @($removeArguments) *> $null
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "Temporary container cleanup failed for '$name'."
        }
    }
}
