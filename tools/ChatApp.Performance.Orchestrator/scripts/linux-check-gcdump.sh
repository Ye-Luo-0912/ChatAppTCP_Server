#!/usr/bin/env bash
ls -la /home/yeluo/.dotnet/tools/dotnet-gcdump 2>&1
echo "--- dotnet tools dir writable? ---"
ls -ld /home/yeluo/.dotnet/tools 2>&1
echo "--- PATH ---"
echo "$PATH"
echo "--- which dotnet ---"
which dotnet 2>&1
dotnet --version 2>&1