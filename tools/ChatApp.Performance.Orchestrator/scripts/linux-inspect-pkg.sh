#!/usr/bin/env bash
DLL=/home/yeluo/chatapp-perf/ChatAppTCP_Server/.nuget/packages/chatapp.protocol.tcp/0.4.1/lib/net10.0/ChatApp.Protocol.Tcp.dll
echo "=== md5 ==="
md5sum "$DLL"
echo "=== size ==="
stat -c %s "$DLL"
echo "=== DissolveGroup in dll ==="
strings "$DLL" | grep -i dissolve 2>&1
echo "=== nupkg md5 (feed) ==="
md5sum /home/yeluo/chatapp-perf/ChatAppTCP_Server/packages/ChatApp.Protocol.Tcp.0.4.1.nupkg
echo "=== nuspec ==="
cat /home/yeluo/chatapp-perf/ChatAppTCP_Server/.nuget/packages/chatapp.protocol.tcp/0.4.1/chatapp.protocol.tcp.nuspec | head -20