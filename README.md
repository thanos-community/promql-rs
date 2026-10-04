# promql-rs

A PromQL query engine and a PromQL parser in Rust. The engine compiles a parsed
query into a DataFusion logical plan over a store trait. The parser is a
structural hand-port of the Go parser in
[`prometheus/prometheus`](https://github.com/prometheus/prometheus). Both are
measured against Prometheus's own test corpora, vendored in this repository and
pinned to one upstream commit.

Neither is complete. See [Status](#status) for what passes today and where the
numbers come from.

## Layout

The workspace has six crates:

| Crate | What it is |
|---|---|
| `promql-engine/` | Evaluates PromQL as DataFusion plans. Defines `SeriesSource`, the trait a store implements, and the Arrow schema its data comes back in. |
| `promql-parser/` | The parser. Grammar, lexer, AST, and error types ported from upstream `promql/parser`. |
| `promql-conformance/` | Replays Prometheus's promqltest corpus against the engine and records what passes. |
| `promql-sync/` | Developer-invoked CLI that checks, pulls, and regenerates the vendored upstream files. |
| `thanos-store/` | The Thanos Store API client crate: the vendored protos built with `protox`, the Prometheus XOR chunk codec, endpoint discovery over the Info API, the fan-out proxy and the `SeriesSource` it exposes. |
| `thanos-query-rs/` | The `thanos query` look-alike: the Prometheus HTTP API served by axum and evaluated by `promql-engine` over `thanos-store`. See [thanos-query-rs](#thanos-query-rs). |

Vendored upstream material sits next to the crate that consumes it.
`promql-parser/upstream/` holds the Go sources the port is derived from.
`promql-conformance/testdata/prometheus/` holds the `.test` corpus. Both record
their provenance in an `UPSTREAM.md` and a per-file SHA-256 in a
`MANIFEST.toml`.

The design notes are in `docs/`:

- `docs/engine.md`: how the engine bounds memory, and the Prometheus and Thanos
  lineage it takes that from.
- `docs/series-source.md`: the `SeriesSource` trait and the Arrow shape a store
  returns.
- `docs/engine-blocks.md`: how a range function walks a block and crosses a
  block edge. `docs/engine-blocks.html` is the same walkthrough with diagrams.
- `docs/parser-sync.md`: the translation discipline for the parser port and the
  sync tooling.

`scripts/gen-xor-fixtures` is a Go program that encodes XOR chunks with
Prometheus's own `chunkenc` into the fixtures the `thanos-store` codec tests
replay. `scripts/gen-conformance` is a Go program that reads upstream's parser
tests with `go/ast` and writes the JSON fixture the parser replays.
`scripts/progress` reconstructs the promqltest pass count for
every commit that changed it, by walking git rather than by reading a metrics
database.

## How the engine runs a query

The engine turns an `Expr` into a DataFusion `LogicalPlan` that sits on top of
whatever `ExecutionPlan` the store returned from `SeriesSource::select`. A
series is a row from the store to the result, and the selector, aggregation, and
range operators are DataFusion aggregate functions over the samples list. A
series may arrive as several rows, and the engine folds them where it evaluates,
so the whole series never exists as one array. `docs/engine.md` states the
property that discipline buys. The unit of materialisation is the window, never
the series.

The crate reaches Arrow only through `datafusion::arrow`. A direct `arrow`
dependency would pin a second Arrow version for any consumer that patches
DataFusion to a fork, and every `RecordBatch` a store hands over would then be a
type mismatch.

`MemorySeriesSource` is an in-memory `SeriesSource` for tests and benchmarks.
`promql-engine/benches` has three criterion targets, `kernels`, `engine`, and
`memory`. `promql-engine/benches/README.md` covers taking a baseline and reading
a profile.

## Why the parser port needs its own tooling

A hand-ported parser rots quietly. Upstream adds a function, a keyword, or an
AST field, nothing here fails to compile, and the port falls behind until
someone notices that a query behaves differently than it does in Prometheus.
The tooling makes that divergence visible and cheap to close.

**Upstream is vendored, not just referenced.** `promql-sync check` verifies
every vendored file against its recorded hash. It needs no network, so CI runs
it on every pull request. A hand-edited vendored file fails the check by name.

**The grammar is generated, not hand-maintained in place.**
`promql-sync generate-grammar` rebuilds `promql-parser/src/grammar.y` from the
vendored `upstream/generated_parser.y` for structure, and from two
hand-maintained sidecar files, `grammar-actions.toml` and `grammar-tokens.toml`,
for the Rust parts. A rule that changed on the Rust side shows up as a small
sidecar diff instead of a diff buried in a large generated file. An upstream
alternative with no sidecar entry gets an `Err(())` placeholder rather than
blocking the build, and the tool prints every unmapped alternative at the end.
`generate-grammar` prints how many placeholders `grammar.y` still holds.

**Upgrades are a worktree diff, not a pipeline.** Nothing here commits,
branches, or opens a pull request:

```sh
cargo run -p promql-sync -- pull --target <upstream-sha-or-tag>
cargo run -p promql-sync -- generate-grammar
# add a [[actions]] entry in grammar-actions.toml for each alternative
# the tool reported as unmapped, then regenerate
cargo run -p promql-sync -- generate-grammar
cargo test -p promql-parser
```

`pull` takes `--set parser` or `--set promqltest` and handles one vendored set
per run. Re-pinning the promqltest set moves test content, so
`promql-conformance/testdata/prometheus/UPSTREAM.md` describes the regeneration
step that has to land in the same change.

## Status

Both vendored sets are pinned to `prometheus/prometheus@83962c35`.

**Neither the engine nor the parser is complete, and most of the corpus does
not pass.** The engine plans a narrow slice of PromQL, so most evals fail on an
expression it cannot plan at all, and whole corpus files score zero. The parser
misses native-histogram descriptors, duration arithmetic, and the anchored and
smoothed selectors. `promql-conformance/testdata/prometheus/UNSUPPORTED.md`
records the pass count, the per-file scoreboard, and every missing feature with
the number of evals it blocks. `PROMQL_PROMQLTEST_BLESS=1` regenerates it from a
real run. `docs/parser-sync.md` draws the same line for the parser port.

CI gates the engine on an allowlist rather than on that count.
`promql-conformance/testdata/prometheus/SUPPORTED.toml` names the evals that
must keep passing. A listed eval that stops passing turns CI red, and so does an
unlisted eval that starts passing, which forces the new coverage to be declared.

## thanos-query-rs

A stand-in for `thanos query` that evaluates with promql-rs. The
`thanos-store` crate speaks the client side of the
[Thanos Store API](https://thanos.io/tip/thanos/integrations.md/#storeapi)
over gRPC and implements `promql-engine`'s `SeriesSource`; the
`thanos-query-rs` binary serves the Prometheus HTTP API in front of it.
Point it at any Store API endpoint (a Sidecar, Store Gateway, Receiver or
another Querier) and Grafana's Prometheus data source works for the
PromQL subset the engine supports.

```sh
cargo run -p thanos-query-rs -- --endpoint localhost:10901 --http-address 0.0.0.0:10902
curl -s 'localhost:10902/api/v1/query?query=up'
```

Flags are the Thanos ones in kebab-case (`--help` lists them all):

| Flag | Default | Thanos flag |
|---|---|---|
| `--http-address` | `0.0.0.0:10902` | `--http-address` |
| `--endpoint <host:port>` (repeatable) | | `--endpoint` |
| `--endpoint-info-interval` | `30s` | (Thanos refreshes every 5s) |
| `--query-timeout` | `2m` | `--query.timeout` |
| `--query-lookback-delta` | `5m` | `--query.lookback-delta` |
| `--query-default-step` | `1s` | `--query.default-step` |
| `--query-max-concurrent` | `20` | `--query.max-concurrent` |
| `--query-partial-response[=false]` | `true` | `--query.partial-response` |
| `--query-block-duration` | `2h` | (none; sizes the engine's blocks only, stores are asked once per selector as in Thanos) |
| `--web-route-prefix` | | `--web.route-prefix` |
| `--web-disable-cors` | `false` | `--web.disable-cors` |
| `--log-level`, `--log-format` | `info`, `text` | `--log.level`, `--log.format` |

Endpoints:

| Path | What it does |
|---|---|
| `GET\|POST /api/v1/query` | Evaluated as a one-step range query at `time` and returned as a `vector`. |
| `GET\|POST /api/v1/query_range` | Evaluated by `promql-engine`; the result is a `matrix`. |
| `GET\|POST /api/v1/labels` | The `LabelNames` RPC fanned out to the stores in range, union sorted. |
| `GET /api/v1/label/{name}/values` | The `LabelValues` RPC, likewise. |
| `GET /api/v1/status/buildinfo` | Enough for Grafana's health check. |
| `GET /metrics` | Request counter and duration histogram per handler. |

Parameter parsing, validation order and every error message are ported
from Thanos's `pkg/api/query/v1.go`, so a client sees the same 400s.
`query`, `time`, `start`, `end`, `step`, `timeout`, `lookback_delta`,
`partial_response`, `storeMatch[]`, `match[]`, `limit`, `dedup` and
`replicaLabels[]` are honoured. `max_source_resolution`, `engine` and
`shard_info` are validated and ignored; `stats` and `analyze` are
ignored. Where it deliberately differs from Thanos:

- **Replica deduplication** is on for `query` and `query_range` as in
  Thanos: `--query.replica-label` (repeatable or comma separated) names
  the labels that tell replicas apart, `--deduplication.func` picks
  `penalty` (the default) or `chain`, `dedup=false` returns the replicas
  as separate series, and `replicaLabels[]` replaces the flag for one
  request. Without a replica label nothing is merged. The metadata
  endpoints do not deduplicate, which matches Thanos.
- **Floats and raw data only.** Histogram chunks are skipped and
  downsampled data is never requested.
- **What the engine cannot evaluate yet** (subqueries, histogram functions,
  topk and more) is a 422 `execution` error reading
  `<feature> is not supported yet`, which Prometheus clients show as a
  query error. Steps below one millisecond are a 400; the engine's grid
  is milliseconds.
- **Vectors are sorted by label set** like matrices; Go leaves vectors
  in evaluation order. The `analysis` field Thanos adds to query
  responses is not emitted.
- **Static endpoints over plaintext gRPC.** No TLS, no DNS or file
  service discovery, no gRPC Store API server side.

`scripts/e2e-thanos.sh` starts Prometheus scraping itself, a Thanos
Sidecar, the Go `thanos query` and `thanos-query-rs`, sends the same
range, instant, label and malformed requests to both queriers and diffs
the normalized JSON.

## Building

```sh
cargo build --workspace
cargo test --workspace
```

CI runs those two, and then:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace --benches
cargo run -p promql-sync -- check
```

CI builds the benchmarks but does not run them, so a bench that stops compiling
fails the build instead of rotting unnoticed. The `bench.yml` workflow runs them
on demand.

After those, CI re-blesses the promqltest allowlist and fails if the tree is
dirty, which is what makes each commit's recorded pass count trustworthy.

To see where the engine stands on the corpus, run the conformance suite
directly. It prints a per-file scoreboard and the missing-feature table on every
run, gated or not:

```sh
cargo test -p promql-conformance --test promqltest

# every eval as its own trial, failing as it really is
PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest

# narrowed to one file, or to the feature you are implementing
PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest -- operators/
PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest -- topk
```

`AGENTS.md` describes the five gates an engine feature moves through before it
is done.

## License

Apache-2.0, see [LICENSE](LICENSE). The vendored files under
`promql-parser/upstream/` and
`promql-conformance/testdata/prometheus/` are copied verbatim from
[`prometheus/prometheus`](https://github.com/prometheus/prometheus), also
Apache-2.0. Their upstream copyright headers are retained.
