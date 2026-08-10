#!/usr/bin/env bash
# 在 Start-Job 上下文中测试 dotnet-gcdump，验证 PATH 与 attach
export PATH=/home/yeluo/.local/bin:/home/yeluo/.dotnet/tools:/home/yeluo/.dotnet:/usr/local/bin:/usr/bin:/bin:$PATH
for i in $(seq 1 60); do
  pid=$(pgrep -f 'ChatApp.TcpGateway.dll' | head -1)
  if [ -n "$pid" ]; then
    echo "found gateway pid=$pid after ${i}x2s"
    break
  fi
  sleep 2
done
if [ -z "$pid" ]; then
  echo "no gateway within timeout"
  exit 1
fi
cat > /home/yeluo/chatapp-perf/jobtest.ps1 <<'PSEOF'
param([int]$p)
Set-StrictMode -Version Latest
'JOB PATH: ' + $env:PATH
'gcdump cmd: ' + (Get-Command 'dotnet-gcdump' -ErrorAction SilentlyContinue).Source
& dotnet-gcdump collect -p $p -o /home/yeluo/chatapp-perf/jobtest.gcdump 2>&1
'exit=' + $LASTEXITCODE
PSEOF
pwsh -NoProfile -File /home/yeluo/chatapp-perf/jobtest.ps1 -p "$pid"
echo "--- job test file ---"
ls -la /home/yeluo/chatapp-perf/jobtest.gcdump 2>&1