#!/usr/bin/env bash
ln -sf /home/yeluo/.dotnet/tools/dotnet-gcdump /home/yeluo/.local/bin/dotnet-gcdump
echo "symlink exit=$?"
chmod +x /home/yeluo/.local/bin/dotnet-gcdump
ls -la /home/yeluo/.local/bin/dotnet-gcdump
which dotnet-gcdump
dotnet-gcdump --help >/dev/null 2>&1 && echo "dotnet-gcdump OK" || echo "dotnet-gcdump FAIL"