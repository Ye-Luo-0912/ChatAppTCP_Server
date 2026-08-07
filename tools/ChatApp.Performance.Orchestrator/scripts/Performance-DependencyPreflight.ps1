Set-StrictMode -Version Latest

function Write-PerformanceJsonArtifact {
    param(
        [Parameter(Mandatory)] [string] $Path,
        [Parameter(Mandatory)] [object] $Value,
        [int] $Depth = 10
    )

    $fullPath = [IO.Path]::GetFullPath($Path)
    $parent = [IO.Path]::GetDirectoryName($fullPath)
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        [IO.Directory]::CreateDirectory($parent) | Out-Null
    }

    $json = $Value | ConvertTo-Json -Depth $Depth
    $temporaryPath = "$fullPath.tmp-$([Guid]::NewGuid().ToString('N'))"
    [IO.File]::WriteAllText(
        $temporaryPath,
        $json,
        [Text.UTF8Encoding]::new($false))
    [IO.File]::Move($temporaryPath, $fullPath, $true)
}

function Get-PerformanceOpenFileLimitPreflight {
    param(
        [Parameter(Mandatory)] [int] $TcpConnections,
        [ValidateRange(1, 128)] [int] $GatewayCount = 2,
        [ValidateRange(0, 1048576)] [int] $SafetyMargin = 1024
    )

    $required = [long][Math]::Ceiling($TcpConnections / [double]$GatewayCount) + $SafetyMargin
    $runningOnLinux = [Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
        [Runtime.InteropServices.OSPlatform]::Linux)
    if (-not $runningOnLinux) {
        return [pscustomobject]@{
            Platform = [Runtime.InteropServices.RuntimeInformation]::OSDescription
            Applicable = $false
            Passed = $true
            SoftLimit = $null
            HardLimit = $null
            RequiredSoftLimit = $required
            Recommendation = $null
        }
    }

    $line = Get-Content -LiteralPath '/proc/self/limits' |
        Where-Object { $_ -match '^Max open files\s+' } |
        Select-Object -First 1
    if ($null -eq $line -or $line -notmatch '^Max open files\s+(\S+)\s+(\S+)') {
        return [pscustomobject]@{
            Platform = [Runtime.InteropServices.RuntimeInformation]::OSDescription
            Applicable = $true
            Passed = $false
            SoftLimit = $null
            HardLimit = $null
            RequiredSoftLimit = $required
            Recommendation = 'Unable to read Max open files from /proc/self/limits; set ulimit -n 65535 before launch.'
        }
    }

    $softText = $Matches[1]
    $hardText = $Matches[2]
    $soft = if ($softText -eq 'unlimited') { [long]::MaxValue } else { [long]$softText }
    $hard = if ($hardText -eq 'unlimited') { [long]::MaxValue } else { [long]$hardText }
    return [pscustomobject]@{
        Platform = [Runtime.InteropServices.RuntimeInformation]::OSDescription
        Applicable = $true
        Passed = $soft -ge $required
        SoftLimit = $soft
        HardLimit = $hard
        RequiredSoftLimit = $required
        Recommendation = if ($soft -ge $required) { $null } else { 'Run `ulimit -n 65535` in the launch shell before starting the benchmark.' }
    }
}

function Read-RespLine {
    param([Parameter(Mandatory)] [IO.Stream] $Stream)

    $bytes = [Collections.Generic.List[byte]]::new()
    while ($true) {
        $value = $Stream.ReadByte()
        if ($value -lt 0) {
            throw 'Garnet closed the connection while a RESP line was being read.'
        }
        if ($value -eq 13) {
            $lineFeed = $Stream.ReadByte()
            if ($lineFeed -ne 10) {
                throw 'Garnet returned an invalid RESP line terminator.'
            }
            return [Text.Encoding]::UTF8.GetString($bytes.ToArray())
        }
        $bytes.Add([byte]$value)
    }
}

function Read-RespValue {
    param([Parameter(Mandatory)] [IO.Stream] $Stream)

    $prefix = $Stream.ReadByte()
    if ($prefix -lt 0) {
        throw 'Garnet closed the connection before returning a RESP value.'
    }

    switch ([char]$prefix) {
        '+' { return Read-RespLine -Stream $Stream }
        '-' {
            $errorText = Read-RespLine -Stream $Stream
            throw "Garnet RESP error: $errorText"
        }
        ':' { return [long](Read-RespLine -Stream $Stream) }
        '$' {
            $length = [int](Read-RespLine -Stream $Stream)
            if ($length -eq -1) { return $null }
            if ($length -lt 0) { throw "Garnet returned invalid bulk-string length $length." }
            $buffer = [byte[]]::new($length)
            $offset = 0
            while ($offset -lt $length) {
                $read = $Stream.Read($buffer, $offset, $length - $offset)
                if ($read -le 0) {
                    throw 'Garnet closed the connection while a RESP bulk string was being read.'
                }
                $offset += $read
            }
            if ($Stream.ReadByte() -ne 13 -or $Stream.ReadByte() -ne 10) {
                throw 'Garnet returned an invalid RESP bulk-string terminator.'
            }
            return [Text.Encoding]::UTF8.GetString($buffer)
        }
        default { throw "Garnet returned unsupported RESP prefix '$([char]$prefix)'." }
    }
}

function Invoke-GarnetRespCommand {
    param(
        [Parameter(Mandatory)] [string] $HostName,
        [Parameter(Mandatory)] [int] $Port,
        [Parameter(Mandatory)] [string[]] $Command,
        [ValidateRange(100, 60000)] [int] $TimeoutMilliseconds = 3000
    )

    $client = [Net.Sockets.TcpClient]::new()
    try {
        $connect = $client.ConnectAsync($HostName, $Port)
        if (-not $connect.Wait($TimeoutMilliseconds)) {
            throw "Timed out connecting to Garnet at ${HostName}:$Port."
        }

        $client.ReceiveTimeout = $TimeoutMilliseconds
        $client.SendTimeout = $TimeoutMilliseconds
        $stream = $client.GetStream()
        $builder = [Text.StringBuilder]::new()
        [void]$builder.Append('*').Append($Command.Count).Append("`r`n")
        foreach ($argument in $Command) {
            $argumentBytes = [Text.Encoding]::UTF8.GetBytes($argument)
            [void]$builder.Append('$').Append($argumentBytes.Length).Append("`r`n")
            [void]$builder.Append($argument).Append("`r`n")
        }
        $request = [Text.Encoding]::UTF8.GetBytes($builder.ToString())
        $stream.Write($request, 0, $request.Length)
        $stream.Flush()
        return Read-RespValue -Stream $stream
    }
    finally {
        $client.Dispose()
    }
}

function Wait-PerformanceDependencies {
    param(
        [Parameter(Mandatory)] [int] $NatsMonitorPort,
        [Parameter(Mandatory)] [int] $GarnetPort,
        [Parameter(Mandatory)] [string] $ArtifactPath,
        [ValidateRange(1, 300)] [int] $TimeoutSeconds = 60
    )

    $startedAt = [DateTimeOffset]::UtcNow
    $deadline = $startedAt.AddSeconds($TimeoutSeconds)
    $natsResult = $null
    $garnetResult = $null
    $lastNatsError = $null
    $lastGarnetError = $null

    try {
        do {
            try {
                $healthUri = "http://127.0.0.1:$NatsMonitorPort/healthz?js-enabled-only=true"
                $jetStreamUri = "http://127.0.0.1:$NatsMonitorPort/jsz"
                $health = Invoke-RestMethod -Uri $healthUri -Method Get -TimeoutSec 3
                if ($health.status -ne 'ok') {
                    throw "NATS health status was '$($health.status)'."
                }
                $jetStream = Invoke-RestMethod -Uri $jetStreamUri -Method Get -TimeoutSec 3
                if ($null -eq $jetStream -or $null -eq $jetStream.psobject.Properties['server_id']) {
                    throw 'NATS /jsz did not return JetStream server metadata.'
                }
                $natsResult = [pscustomobject]@{
                    Health = 'ok'
                    JetStream = 'enabled'
                    ServerId = [string]$jetStream.server_id
                    Streams = if ($null -ne $jetStream.psobject.Properties['streams']) { [long]$jetStream.streams } else { 0 }
                }
                break
            }
            catch {
                $lastNatsError = $_.Exception.Message
                if ([DateTimeOffset]::UtcNow -ge $deadline) { break }
                Start-Sleep -Milliseconds 500
            }
        } while ([DateTimeOffset]::UtcNow -lt $deadline)

        if ($null -eq $natsResult) {
            throw "NATS health/JetStream preflight failed within ${TimeoutSeconds}s: $lastNatsError"
        }

        $key = "chatapp:perf:preflight:$([Guid]::NewGuid().ToString('N'))"
        $value = [Guid]::NewGuid().ToString('N')
        do {
            try {
                $ping = Invoke-GarnetRespCommand -HostName '127.0.0.1' -Port $GarnetPort -Command @('PING')
                if ($ping -ne 'PONG') { throw "Garnet PING returned '$ping'." }
                $set = Invoke-GarnetRespCommand -HostName '127.0.0.1' -Port $GarnetPort -Command @('SET', $key, $value)
                if ($set -ne 'OK') { throw "Garnet SET returned '$set'." }
                $read = Invoke-GarnetRespCommand -HostName '127.0.0.1' -Port $GarnetPort -Command @('GET', $key)
                if ($read -ne $value) { throw 'Garnet GET did not return the value written by SET.' }
                $script = "return redis.call('GET', KEYS[1])"
                $evaluated = Invoke-GarnetRespCommand -HostName '127.0.0.1' -Port $GarnetPort -Command @('EVAL', $script, '1', $key)
                if ($evaluated -ne $value) { throw 'Garnet EVAL did not return the expected value.' }
                [void](Invoke-GarnetRespCommand -HostName '127.0.0.1' -Port $GarnetPort -Command @('DEL', $key))
                $garnetResult = [pscustomobject]@{
                    Ping = 'PONG'
                    WriteRead = 'passed'
                    LuaEval = 'passed'
                }
                break
            }
            catch {
                $lastGarnetError = $_.Exception.Message
                try {
                    [void](Invoke-GarnetRespCommand -HostName '127.0.0.1' -Port $GarnetPort -Command @('DEL', $key))
                }
                catch {
                }
                if ([DateTimeOffset]::UtcNow -ge $deadline) { break }
                Start-Sleep -Milliseconds 500
            }
        } while ([DateTimeOffset]::UtcNow -lt $deadline)

        if ($null -eq $garnetResult) {
            throw "Garnet PING/write-read/EVAL preflight failed within ${TimeoutSeconds}s: $lastGarnetError"
        }

        $result = [pscustomobject]@{
            Status = 'passed'
            StartedAtUtc = $startedAt
            CompletedAtUtc = [DateTimeOffset]::UtcNow
            Nats = $natsResult
            Garnet = $garnetResult
            Error = $null
        }
        Write-PerformanceJsonArtifact -Path $ArtifactPath -Value $result
        return $result
    }
    catch {
        $result = [pscustomobject]@{
            Status = 'failed'
            StartedAtUtc = $startedAt
            CompletedAtUtc = [DateTimeOffset]::UtcNow
            Nats = $natsResult
            Garnet = $garnetResult
            Error = $_.Exception.Message
        }
        Write-PerformanceJsonArtifact -Path $ArtifactPath -Value $result
        throw
    }
}

function Save-PerformanceContainerDiagnostics {
    param(
        [Parameter(Mandatory)] [string] $Container,
        [Parameter(Mandatory)] [string] $Directory
    )

    [IO.Directory]::CreateDirectory($Directory) | Out-Null
    $safeName = $Container -replace '[^A-Za-z0-9_.-]', '_'
    try {
        & docker inspect $Container *> (Join-Path $Directory "$safeName-inspect.json")
    }
    catch {
    }
    try {
        & docker stats --no-stream --format '{{json .}}' $Container *> (Join-Path $Directory "$safeName-stats.jsonl")
    }
    catch {
    }
    try {
        # Keep a bounded tail. A malformed load can otherwise create multi-gigabyte
        # container logs and make diagnostic collection itself destabilize the host.
        & docker logs --timestamps --tail 5000 $Container *> (Join-Path $Directory "$safeName-logs-tail.txt")
    }
    catch {
    }
}
