# ActorRuntime microbenchmark

Runs a warm-up, pre-activates every key, then executes a bounded FIFO steady-state
throughput pass. Actor/admission creation is deliberately outside the measured
allocation window so the result represents the long-lived gateway hot path. It
reports accepted message throughput, producer backpressure retries, managed
allocation per message and GC counts.

```powershell
dotnet run -c Release --project tools/ChatApp.ActorRuntime.Benchmarks -- \
  --messages 5000000 --keys 16384 --producers 8 --shards 8
```

This is a local hot-path benchmark, not a replacement for the multi-process
gateway capacity curve or Linux soak tests.
