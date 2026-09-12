# promql-rs

A standalone Rust PromQL parser: a structural hand-port of
[`prometheus/prometheus`](https://github.com/prometheus/prometheus)'s
`promql/parser` package, plus the tooling that keeps the port in sync
with upstream as Prometheus evolves.

## Layout

| Path | What it is |
|---|---|
| `promql-parser/` | The parser crate. Grammar, lexer, AST, and error types ported from upstream. |
| `promql-parser/upstream/` | Verbatim vendored copies of the upstream Go source the port is derived from. |
| `promql-sync/` | Developer-invoked CLI that checks, pulls, and regenerates the port against upstream. |
| `scripts/gen-conformance/` | Go tool that turns upstream's own parser test suite into a JSON fixture. |
| `docs/parser-sync.md` | Design rationale for the parser and the sync tooling. |
| `promql-engine/` | The PromQL engine crate: the `SeriesSource` trait every store implements, the series batch it returns, and the selector, aggregation and range-function operators over it. |
| `docs/series-source.md` | Design note for `SeriesSource`: the trait and the Arrow shape. |
| `thanos-store/` | The Thanos Store API client crate: the vendored protos built with `protox`, the Prometheus XOR chunk codec, endpoint discovery over the Info API, the fan-out proxy and the `SeriesSource` it exposes. |
| `thanos-query-rs/` | The `thanos query` look-alike: the Prometheus HTTP API served by axum and evaluated by `promql-engine` over `thanos-store`. See [thanos-query-rs](#thanos-query-rs). |
| `scripts/gen-xor-fixtures/` | Go tool that encodes XOR chunks with Prometheus's own `chunkenc` into the byte-exact fixtures the codec tests replay. |
| `scripts/e2e-thanos.sh` | Runs Prometheus, a Sidecar, `thanos query` and `thanos-query-rs` side by side and diffs their answers. |

## Why hand-porting a parser needs its own tooling

A hand-ported parser rots quietly. Upstream adds a function, a keyword,
or an AST field; nothing here fails to compile; the port just silently
falls behind until someone notices a query behaves differently than it
does in Prometheus. The tooling in this repo exists to make that
divergence visible and cheap to close, instead of relying on someone
remembering to diff two codebases by hand.

**Upstream is vendored, not just referenced.** The Go sources this port
is derived from live verbatim under `promql-parser/upstream/`, each
file's SHA-256 recorded in `upstream/MANIFEST.toml` alongside the
pinned commit in `upstream/UPSTREAM.md`. `promql-sync check` verifies
every vendored file still matches its recorded hash — no network
access, so it runs in CI on every PR. If someone hand-edits a vendored
file instead of going through the sync tool, the check fails and says
exactly which file drifted.

**The grammar is generated, not hand-maintained in place.** `src/grammar.y`
is not edited directly. `promql-sync generate-grammar` rebuilds it from
three inputs: the vendored `upstream/generated_parser.y` for structure
(rule ordering, alternatives, precedence), and two hand-maintained
sidecar files, `grammar-actions.toml` and `grammar-tokens.toml`, for the
Rust-specific parts (per-rule return types, per-alternative action
bodies, upstream-to-grmtools token renames). Splitting it this way means
a grammar rule that changed on the Rust side shows up as a small sidecar
diff, not a diff buried inside a large generated file. Any upstream
grammar alternative that has no matching sidecar entry gets an
`Err(())` placeholder instead of blocking the build, and the tool prints
the full list of unmapped alternatives at the end, so a sync always
leaves a concrete, visible to-do list rather than a silent gap.

**Conformance is checked against upstream's own tests.** `scripts/gen-conformance`
reads upstream's `promql/parser/parse_test.go` with `go/ast` and emits
`promql-parser/tests/fixtures/parse_test_corpus.json`, which
`promql-parser/tests/conformance.rs` runs the port against. This closes
the usual gap with hand-ported parsers, where the test suite is written
once by the porter and slowly stops reflecting what upstream actually
tests. Re-running the generator after an upstream bump refreshes the
corpus straight from upstream's source of truth.

**Upgrades are a worktree diff, not a branch-and-PR pipeline.** Everything
here is a plain CLI a developer runs locally:

```sh
cargo run -p promql-sync -- pull --target <upstream-sha-or-tag>
cargo run -p promql-sync -- generate-grammar
# fill in any new [[actions]] entries in grammar-actions.toml
# for alternatives the tool reported as unmapped
cargo run -p promql-sync -- generate-grammar
cargo test -p promql-parser
```

Nothing here commits, branches, or opens a PR on its own — `pull`
rewrites the vendored files and reports what changed; the developer
reviews the resulting `git diff`, does the translation work the report
calls out, and commits when tests pass. That keeps upstream sync a
routine, low-ceremony task instead of a rare, dreaded one.

## Status

Not yet complete. What's missing:

- **Vector-matching modifiers** (`bool`, `on`, `ignoring`, `group_left`,
  `group_right`) parse but aren't wired into `BinaryExpr` semantics yet.
- **Series descriptions and histogram descriptors** aren't implemented
  in the lexer or grammar.
- **Duration-expression arithmetic** (e.g. `[5m+1m]`) isn't implemented;
  only a literal duration is supported inside range/subquery/offset
  expressions.
- **Experimental productions** (`fill`, `trim_upper`, `trim_lower`,
  `anchored`, `smoothed`) aren't implemented.
- **The custom lexer** (`promql-parser/src/lexer.rs`, structurally
  ported from upstream `lex.go`) exists but isn't wired into the
  grmtools parser yet; the parser currently uses the regex-based
  `lrlex` lexer in `src/lexer.l`, which is a placeholder.
- **Conformance** against upstream's own parser test corpus currently
  sits at a 55% pass-rate floor (`promql-parser/tests/conformance.rs`);
  the gaps above are why.

`promql-sync` implements `check`, `pull`, and `generate-grammar` —
byte-level diffing of vendored files plus a Markdown/JSON report of
what changed. `docs/parser-sync.md` also describes a further structural
Green/Yellow/Red change-classification system (auto-apply safe changes,
flag risky ones, refuse silent ones) that isn't built yet; see the note
at the top of that document for the line between what's implemented
and what's planned.

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
`partial_response`, `storeMatch[]`, `match[]` and `limit` are honoured.
`dedup`, `max_source_resolution`, `engine` and `shard_info` are
validated and ignored; `replicaLabels[]`, `stats` and `analyze` are
ignored. Where it deliberately differs from Thanos:

- **No deduplication yet.** Replica labels stay in the results and
  `dedup=true` does nothing.
- **Floats and raw data only.** Histogram chunks are skipped and
  downsampled data is never requested.
- **What the engine cannot evaluate yet** (binary operators, most
  functions) is a 422 `execution` error reading
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

## License

Apache-2.0, see [LICENSE](LICENSE). The vendored files under
`promql-parser/upstream/` are copied verbatim from
[`prometheus/prometheus`](https://github.com/prometheus/prometheus),
also Apache-2.0; their upstream copyright headers are retained.
