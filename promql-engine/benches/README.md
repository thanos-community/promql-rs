# promql-engine benchmarks

`kernels` times the engine's kernels without DataFusion, `engine` whole
queries through DataFusion. The doc comment at the top of each file says
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
