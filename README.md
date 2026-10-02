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

The workspace has four crates:

| Crate | What it is |
|---|---|
| `promql-engine/` | Evaluates PromQL as DataFusion plans. Defines `SeriesSource`, the trait a store implements, and the Arrow schema its data comes back in. |
| `promql-parser/` | The parser. Grammar, lexer, AST, and error types ported from upstream `promql/parser`. |
| `promql-conformance/` | Replays Prometheus's promqltest corpus against the engine and records what passes. |
| `promql-sync/` | Developer-invoked CLI that checks, pulls, and regenerates the vendored upstream files. |

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

Two tools live in `scripts/`. `scripts/gen-conformance` is a Go program that
reads upstream's parser tests with `go/ast` and writes the JSON fixture the
parser replays. `scripts/progress` reconstructs the promqltest pass count for
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
