#!/usr/bin/env bash
# 在 Linux 上干净 restore + 构建 Gateway solution，重建本地包缓存
set -o pipefail
export PATH=/usr/bin:/bin:/usr/local/bin:/home/yeluo/.dotnet:/home/yeluo/.dotnet/tools:/home/yeluo/.local/bin:$PATH
export DOTNET_ROOT=/home/yeluo/.dotnet

cd /home/yeluo/chatapp-perf/ChatAppTCP_Server
echo "=== RESTORE GATEWAY ==="
dotnet restore ChatApp.TcpGateway.sln 2>&1 | tail -20
echo "=== RESTORE EXIT=${PIPESTATUS[0]} ==="
echo "=== BUILD GATEWAY ==="
dotnet build ChatApp.TcpGateway.sln -c Release --no-restore 2>&1 | tail -15
echo "=== BUILD EXIT=${PIPESTATUS[0]} ==="