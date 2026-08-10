#!/usr/bin/env bash
# 强制清理 Linux 上 chatapp.protocol.tcp 缓存并重新 restore+build Core
set -o pipefail
export PATH=/usr/bin:/bin:/usr/local/bin:/home/yeluo/.dotnet:/home/yeluo/.dotnet/tools:/home/yeluo/.local/bin:$PATH
export DOTNET_ROOT=/home/yeluo/.dotnet

cd /home/yeluo/chatapp-perf/ChatAppTCP_Server

echo "=== CLEAN CACHE ==="
rm -rf .nuget/packages/chatapp.protocol.tcp/0.4.1
rm -rf Core/obj/project.assets.json Core/obj/*.nuget.*
echo "cache cleaned"

echo "=== RESTORE (force) ==="
dotnet restore Core/Core.csproj --force 2>&1 | tail -8
echo "=== RESTORE EXIT=${PIPESTATUS[0]} ==="

echo "=== BUILD Core ==="
dotnet build Core/Core.csproj -c Release --no-restore 2>&1 | tail -12
echo "=== BUILD EXIT=${PIPESTATUS[0]} ==="