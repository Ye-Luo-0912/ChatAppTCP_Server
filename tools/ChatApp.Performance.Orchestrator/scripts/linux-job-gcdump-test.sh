#!/usr/bin/env bash
export PATH=/home/yeluo/.local/bin:/home/yeluo/.dotnet/tools:/home/yeluo/.dotnet:/usr/local/bin:/usr/bin:/bin:$PATH
pid=$(pgrep -f 'ChatApp.TcpGateway.dll' | head -1)
echo "target gateway pid=$pid"
pwsh -NoProfile -Command "
\$job = Start-Job -ScriptBlock {
    param(\$p)
    'JOB PATH: ' + \$env:PATH
    'dotnet-gcdump cmd: ' + (Get-Command 'dotnet-gcdump' -ErrorAction SilentlyContinue).Source
    & dotnet-gcdump collect -p \$p -o /home/yeluo/chatapp-perf/jobtest.gcdump 2>&1
    'exit=' + \$LASTEXITCODE
} -ArgumentList $pid
Receive-Job \$job -Wait
Remove-Job \$job
"
ls -la /home/yeluo/chatapp-perf/jobtest.gcdump 2>&1