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
