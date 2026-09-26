# promql-engine benchmarks

`kernels` times the engine's kernels without DataFusion, `engine` whole
queries through DataFusion, and `memory` measures peak heap of single
queries instead of time. The doc comment at the top of each file says
how to compare two states of the code.

## Profiling

Every criterion group runs pprof-rs as its profiler, so `--profile-time`
profiles instead of measuring:

```sh
cargo bench -p promql-engine --bench kernels -- --profile-time 15 'selector/apply'
```

The profile lands in
`target/criterion/<group>/<benchmark>/profile/profile.pb`, one directory
per benchmark id with `/` replaced by `_`. Open it with Go's pprof:

```sh
go tool pprof -http=: target/criterion/selector_apply/1000x1000/profile/profile.pb
go tool pprof -top -nodecount=40 target/criterion/selector_apply/1000x1000/profile/profile.pb
```

It samples at 100 Hz, so 15 s gives about 1500 samples; profile longer
for a bench whose hot path is under a few percent.

For ad-hoc profiling of a whole process on macOS, including threads the
bench does not own, `samply record` on the bench binary opens the result
in the Firefox Profiler.

## Heap profiles

`memory` counts live bytes under its own global allocator. With the
`heap-profile` feature that allocator wraps jemalloc with profiling on,
and `MEMORY_HEAP_DIR` makes it write a pprof heap profile per case:

```sh
MEMORY_HEAP_DIR=/tmp/heap cargo bench -p promql-engine --features heap-profile \
  --bench memory -- streamed/30d_1000/chunk120_x360/selector
go tool pprof -sample_index=inuse_space -top -cum /tmp/heap/streamed_30d_1000_chunk120_x360_selector.pb
```

Run one case per process; the doc comment in `memory.rs` says why.
