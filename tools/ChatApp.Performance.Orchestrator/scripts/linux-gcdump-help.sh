#!/usr/bin/env bash
export PATH=/home/yeluo/.local/bin:/home/yeluo/.dotnet/tools:/home/yeluo/.dotnet:/usr/local/bin:/usr/bin:/bin:$PATH
dotnet-gcdump collect --help 2>&1 | head -50