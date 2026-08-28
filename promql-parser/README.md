# promql-parser

Standalone PromQL parser. Structural hand-port of
`prometheus/prometheus@<sha>` `promql/parser/`.

## Status

- Crate compiles end to end with grmtools (`lrlex` + `lrpar` +
  `cfgrammar`).
- AST types, token types, position ranges, and error types are ported
  verbatim from upstream.
- Grammar and lexer are **placeholders**. The real structural port of
  `upstream/generated_parser.y` (grammar + actions) and `upstream/lex.go`
  (custom state-machine lexer) lands as follow-up tasks.
- Public entry points (`parse_expr`, `parse_metric_selector`) return
  `ParseErrors` until the real grammar lands.

See `docs/parser-sync.md` for the full plan.

## Layout

```
src/
├── lib.rs            Public API + grmtools module wiring
├── ast.rs            Rust mirror of upstream ast.go
├── token.rs          ItemType / Item; mirror of upstream lex.go
├── posrange.rs       PositionRange; mirror of upstream posrange/
├── error.rs          ParseError + ParseErrors accumulator
├── parser.rs         Public entry points (currently stubs)
├── grammar.y         grmtools yacc grammar (placeholder)
└── lexer.l           lrlex regex lexer (placeholder)

upstream/             Vendored verbatim from prometheus/prometheus
├── UPSTREAM.md       Provenance + sync workflow
├── MANIFEST.toml     sha256 per vendored file
├── generated_parser.y
├── lex.go
├── ast.go
├── parse.go
├── functions.go
└── printer.go

sync-meta.toml        Machine-readable state for the sync tool
```

## Translation discipline

- Grammar file stays structural-one-to-one with upstream: non-terminal
  names, rule ordering, precedence declarations preserved. Only action
  bodies are rewritten.
- Actions are one-line calls into a dedicated `actions.rs` so the
  grammar file remains diff-friendly against future upstream revisions.
- Error accumulation matches upstream: `ParserCtx` holds a
  `Vec<ParseError>` and parsing continues after faults, producing a
  best-effort AST with diagnostics.
- Experimental-feature gating is in-action (mirroring upstream's
  `options.Enable*` checks), not a post-parse pass.

## Upstream sync

Developer-invoked. The `promql-sync` tool (separate crate) has three
subcommands:

- `promql-sync check` — CI-friendly. Verifies `upstream/<file>` hashes
  against `upstream/MANIFEST.toml`; no network, no mutations.
- `promql-sync pull --target <sha>` — fetches upstream at the target
  revision and overwrites the vendored files + `MANIFEST.toml` +
  `sync-meta.toml`. Never commits.
- `promql-sync generate-grammar` — reads
  `upstream/generated_parser.y` + `grammar-actions.toml` +
  `grammar-tokens.toml` and rewrites `src/grammar.y`. The *structure*
  of the grammar (rule ordering, alternatives, precedence
  declarations) comes from upstream; per-rule return types and
  per-alt Rust action bodies live in `grammar-actions.toml`. Any alt
  without a sidecar mapping gets an `Err(())` placeholder so the
  grammar still compiles; the CLI prints the list of unmapped alts
  at the end.

Typical upgrade flow:

```sh
cargo run -p promql-sync -- pull --target <sha>
cargo run -p promql-sync -- generate-grammar
# inspect git diff promql-parser/
# for any new unmapped alts, add a [[actions]] entry in
# grammar-actions.toml and (if needed) a helper in src/actions.rs.
cargo run -p promql-sync -- generate-grammar
cargo test -p promql-parser
```

See `upstream/UPSTREAM.md` for the vendoring provenance.
