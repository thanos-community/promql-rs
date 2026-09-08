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
//!   follow-up; today grmtools uses the regex-based lrlex lexer in
//!   `src/lexer.l`.
//! - [`error`]: accumulated `ParseError` / `ParseErrors` matching
//!   upstream's multi-error parse result.
//! - [`actions`]: per-production Rust action helpers invoked from
//!   `src/grammar.y`.
//! - [`parser`]: public entry points (`parse_expr`, `parse_metric_selector`).
//!
//! The grmtools grammar lives in `src/grammar.y` and is compiled into
//! `OUT_DIR/grammar_y.rs` by `build.rs`; likewise `src/lexer.l` →
//! `OUT_DIR/lexer_l.rs`.

pub mod actions;
pub mod ast;
pub mod error;
pub mod lexer;
pub mod parser;
pub mod posrange;
pub mod token;

use lrlex::lrlex_mod;

lrlex_mod!("lexer.l");

/// Wrapper around the generated parser module so clippy lints that only
/// fire on grmtools' generated code can be silenced here rather than
/// crate-wide.
mod grammar {
    #![allow(clippy::needless_question_mark)]

    use lrpar::lrpar_mod;

    lrpar_mod!("grammar.y");

    pub use grammar_y::parse;
}

pub use crate::ast::Expr;
pub use crate::error::{ParseError, ParseErrors};
pub use crate::parser::{parse_expr, parse_metric_selector};
