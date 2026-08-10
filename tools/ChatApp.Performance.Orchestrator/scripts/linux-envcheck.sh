#!/usr/bin/env bash
export PATH=/usr/bin:/bin:/home/yeluo/.dotnet:/home/yeluo/.dotnet/tools:$PATH
export DOTNET_ROOT=/home/yeluo/.dotnet
echo "=== dotnet SDKs ==="
dotnet --list-sdks 2>&1
echo "=== pwsh ==="
which pwsh 2>&1
ls /opt/microsoft/powershell/7/ 2>&1 | head
echo "=== ulimit ==="
bash -c 'ulimit -Sn'
echo "=== cgroup ==="
mount | grep -i cgroup 2>&1 | head
echo "=== dotnet-gcdump ==="
ls /home/yeluo/.dotnet/tools/dotnet-gcdump 2>&1
echo "=== done ==="