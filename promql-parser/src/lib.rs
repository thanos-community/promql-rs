//! Standalone PromQL parser.
//!
//! Structural hand-port of upstream `prometheus/prometheus@<sha>` under
//! `promql/parser/`. Upstream sources are vendored verbatim under
//! `upstream/`; see `upstream/UPSTREAM.md` for provenance and the sync
//! workflow, and `docs/parser-sync.md` for the translation discipline.
//!
//! The crate is layered:
//! - [`ast`]: Rust mirrors of `upstream/ast.go` node types.
//! - [`posrange`]: byte-offset position ranges matching upstream's
//!   `promql/parser/posrange`.
//! - [`token`]: `Item` / `ItemType` matching upstream `lex.go`.
//! - [`lexer`]: standalone state-machine tokenizer mirroring
//!   `upstream/lex.go`. Wired to grmtools as a custom lexer in a
//!   follow-up; today grmtools uses the regex-based lrlex lexers in
//!   `src/lexer.l` and `src/series.l`.
//! - [`error`]: accumulated `ParseError` / `ParseErrors` matching
//!   upstream's multi-error parse result.
//! - [`actions`]: per-production Rust action helpers invoked from
//!   `src/grammar.y`.
//! - [`parser`]: public entry points ([`parse_expr`],
//!   [`parse_metric_selector`], [`parse_series_desc`], [`parse_metric`]).
//!
//! The grmtools grammar lives in `src/grammar.y` and is compiled into
//! `OUT_DIR/grammar_y.rs` by `build.rs`; likewise `src/lexer.l` →
//! `OUT_DIR/lexer_l.rs` and `src/series.l` → `OUT_DIR/series_l.rs`.
//!
//! # Parse modes
//!
//! As upstream does, one grammar serves several entry points: its
//! `start` rule dispatches on a leading `START_*` pseudo-token that
//! `parser` injects into the token stream. That is what lets series
//! descriptions (promqltest load lines such as
//! `http_requests_total{pod="nginx-1"} 46.00+13.00x40`) reuse the same
//! `metric` and `label_set` rules as expressions instead of getting a
//! parallel grammar. See [`parser`] for the lexer-mode side of it.

pub mod actions;
pub mod ast;
pub mod error;
pub mod lexer;
pub mod parser;
pub mod posrange;
pub mod token;

use lrlex::lrlex_mod;
use lrpar::lrpar_mod;

lrlex_mod!("lexer.l");
lrlex_mod!("series.l");
lrpar_mod!("grammar.y");

pub use crate::ast::Expr;
pub use crate::ast::{SequenceValue, SeriesDescription};
pub use crate::error::{ParseError, ParseErrors};
pub use crate::parser::{parse_expr, parse_metric, parse_metric_selector, parse_series_desc};
