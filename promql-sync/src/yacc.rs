//! Tiny goyacc-subset parser.
//!
//! Reads the Prometheus `generated_parser.y` file as far as we need for
//! mechanical re-emission into grmtools' `.y` dialect. Non-goals:
//!
//! - Semantic validation of the grammar (left-recursion, reachability,
//!   shift-reduce safety) — grmtools handles those when it ingests our
//!   output.
//! - The Go action bodies. We capture them as opaque text so the sync
//!   report can show them alongside our Rust replacements, but we
//!   never attempt to translate them.
//! - `%union` bodies — skipped wholesale.
//!
//! The parser is structural: whitespace- and comment-insensitive, but
//! it does preserve declaration order for `%left`/`%right`/`%nonassoc`
//! (which is load-bearing for precedence). It's hand-rolled to avoid
//! pulling in a yacc-parsing crate for a one-off tool.
//!
//! Input size is a single file on the order of 1500 lines. No
//! performance concerns.

use anyhow::{anyhow, bail, Result};

/// Parsed representation of an upstream goyacc grammar.
#[derive(Debug, Default)]
pub struct Grammar {
    /// Order-preserving list of precedence declarations. `%left`,
    /// `%right`, `%nonassoc` apply in source order with later decls
    /// binding tighter than earlier ones — emission must preserve it.
    pub precedence: Vec<PrecDecl>,
    /// The grammar's `%start` symbol, if declared.
    pub start: Option<String>,
    /// Rules in source order.
    pub rules: Vec<Rule>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Assoc {
    Left,
    Right,
    Nonassoc,
}

#[derive(Debug)]
pub struct PrecDecl {
    pub assoc: Assoc,
    pub tokens: Vec<String>,
}

#[derive(Debug)]
pub struct Rule {
    pub lhs: String,
    pub alternatives: Vec<Alternative>,
}

#[derive(Debug)]
pub struct Alternative {
    /// Symbols on the right-hand side in source order. Uppercase names
    /// are terminals (tokens); lowercase / mixed-case are non-terminals.
    /// `error` is passed through verbatim.
    pub symbols: Vec<String>,
    /// The goyacc action body as raw text (without the surrounding
    /// braces). `None` if the alternative has no explicit action.
    /// Not used for emission — the tool looks up action bodies in the
    /// sidecar — but captured so future tooling (e.g. a "what did
    /// upstream's action do" report) can surface it.
    #[allow(dead_code)]
    pub upstream_action: Option<String>,
}

impl Alternative {
    /// Canonical string signature used as the sidecar lookup key. Uses
    /// upstream token names verbatim (no renames applied) so a
    /// renaming table change doesn't invalidate every entry.
    pub fn signature(&self) -> String {
        self.symbols.join(" ")
    }
}

pub fn parse(input: &str) -> Result<Grammar> {
    let mut p = Parser::new(input);
    p.skip_prologue()?;
    let mut grammar = Grammar::default();
    p.parse_declarations(&mut grammar)?;
    p.parse_rules(&mut grammar)?;
    // Epilogue (after the second %%) is ignored.
    Ok(grammar)
}

// ---------------- hand-rolled lexer+parser ----------------

struct Parser<'a> {
    src: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(src: &'a str) -> Self {
        Self { src, pos: 0 }
    }

    // --- basic cursor helpers ---

    fn peek_byte(&self) -> Option<u8> {
        self.src.as_bytes().get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let b = self.peek_byte()?;
        self.pos += 1;
        Some(b)
    }

    fn starts_with(&self, s: &str) -> bool {
        self.src[self.pos..].starts_with(s)
    }

    fn eat(&mut self, s: &str) -> bool {
        if self.starts_with(s) {
            self.pos += s.len();
            true
        } else {
            false
        }
    }

    fn eof(&self) -> bool {
        self.pos >= self.src.len()
    }

    /// Advance past whitespace and comments (`// ...` and `/* ... */`).
    /// goyacc preserves some of this semantically (e.g. location info)
    /// but for our purposes it's all noise.
    fn skip_trivia(&mut self) {
        loop {
            while let Some(b) = self.peek_byte() {
                if b.is_ascii_whitespace() {
                    self.pos += 1;
                } else {
                    break;
                }
            }
            if self.starts_with("//") {
                while let Some(b) = self.peek_byte() {
                    if b == b'\n' {
                        break;
                    }
                    self.pos += 1;
                }
                continue;
            }
            if self.starts_with("/*") {
                self.pos += 2;
                while !self.eof() {
                    if self.starts_with("*/") {
                        self.pos += 2;
                        break;
                    }
                    self.pos += 1;
                }
                continue;
            }
            break;
        }
    }

    // --- prologue / declarations / rules ---

    /// Skip the `%{ ... %}` Go prologue if present. Does nothing if the
    /// grammar has none.
    fn skip_prologue(&mut self) -> Result<()> {
        self.skip_trivia();
        if self.eat("%{") {
            while !self.eof() {
                if self.eat("%}") {
                    return Ok(());
                }
                self.bump();
            }
            bail!("unterminated %{{ prologue");
        }
        Ok(())
    }

    /// Parse declarations up to the first `%%`.
    fn parse_declarations(&mut self, g: &mut Grammar) -> Result<()> {
        loop {
            self.skip_trivia();
            if self.eof() {
                bail!("unexpected EOF in declarations");
            }
            if self.eat("%%") {
                return Ok(());
            }
            if !self.eat("%") {
                bail!("expected %-declaration or %% at byte {}", self.pos);
            }
            let kw = self.read_ident();
            match kw.as_str() {
                "union" => self.skip_braced_block()?,
                "token" => {
                    // `%token [<type>] NAME NAME NAME` — ignore for our
                    // purposes; grmtools infers tokens from usage.
                    self.skip_angle_type();
                    self.read_ident_list();
                }
                "type" => {
                    // `%type <type> NAME NAME` — ignored, we use
                    // per-rule return types via the sidecar.
                    self.skip_angle_type();
                    self.read_ident_list();
                }
                "left" | "right" | "nonassoc" => {
                    let assoc = match kw.as_str() {
                        "left" => Assoc::Left,
                        "right" => Assoc::Right,
                        "nonassoc" => Assoc::Nonassoc,
                        _ => unreachable!(),
                    };
                    self.skip_angle_type();
                    let tokens = self.read_ident_list();
                    if tokens.is_empty() {
                        bail!("empty {kw:?} declaration");
                    }
                    g.precedence.push(PrecDecl { assoc, tokens });
                }
                "start" => {
                    let names = self.read_ident_list();
                    if names.len() != 1 {
                        bail!("%start expects a single symbol, got {}", names.len());
                    }
                    g.start = Some(names.into_iter().next().unwrap());
                }
                "expect" | "pure-parser" | "expect-rr" => {
                    // Consume numeric/flag args on the same line.
                    self.read_until_line_end();
                }
                "%" => {
                    // Stray %%, already handled above. Shouldn't reach
                    // here; treat as end of declarations.
                    return Ok(());
                }
                _ => {
                    // Unknown directive — skip to end of line so we
                    // don't lose sync on harmless additions.
                    self.read_until_line_end();
                }
            }
        }
    }

    fn parse_rules(&mut self, g: &mut Grammar) -> Result<()> {
        loop {
            self.skip_trivia();
            if self.eof() || self.eat("%%") {
                return Ok(());
            }
            let lhs = self.read_ident();
            if lhs.is_empty() {
                bail!("expected rule LHS at byte {}", self.pos);
            }
            self.skip_trivia();
            if !self.eat(":") {
                bail!("expected ':' after rule {lhs:?} at byte {}", self.pos);
            }
            let alternatives = self.parse_alternatives()?;
            self.skip_trivia();
            // Semicolon is optional in goyacc: the next rule's `LHS :`
            // implicitly terminates the current one (upstream relies on
            // this for `anchored_expr` / `smoothed_expr`).
            let _ = self.eat(";");
            g.rules.push(Rule { lhs, alternatives });
        }
    }

    fn parse_alternatives(&mut self) -> Result<Vec<Alternative>> {
        let mut alts = Vec::new();
        loop {
            let alt = self.parse_alternative()?;
            alts.push(alt);
            self.skip_trivia();
            if self.peek_byte() == Some(b'|') {
                self.pos += 1;
                continue;
            }
            break;
        }
        Ok(alts)
    }

    fn parse_alternative(&mut self) -> Result<Alternative> {
        let mut symbols = Vec::new();
        let mut upstream_action = None;
        loop {
            self.skip_trivia();
            match self.peek_byte() {
                None => break,
                Some(b'|') | Some(b';') => break,
                Some(b'{') => {
                    upstream_action = Some(self.read_braced_body()?);
                    break;
                }
                Some(b'%') => {
                    // `%prec TOKEN` association override. Consume the
                    // directive + the token. We don't emit these
                    // currently, but record for future use.
                    if self.eat("%prec") {
                        self.skip_trivia();
                        let _ = self.read_ident();
                        continue;
                    }
                    break;
                }
                Some(b'\'') => {
                    // Character literal token: `'+'` etc. Upstream
                    // goyacc accepts them; our subset doesn't use them,
                    // but pass through verbatim.
                    symbols.push(self.read_char_literal()?);
                }
                _ => {
                    let sym = self.read_ident();
                    if sym.is_empty() {
                        break;
                    }
                    symbols.push(sym);
                }
            }
        }
        Ok(Alternative {
            symbols,
            upstream_action,
        })
    }

    // --- low-level scanners ---

    fn read_ident(&mut self) -> String {
        self.skip_trivia();
        let start = self.pos;
        while let Some(b) = self.peek_byte() {
            if b.is_ascii_alphanumeric() || b == b'_' {
                self.pos += 1;
            } else {
                break;
            }
        }
        self.src[start..self.pos].to_string()
    }

    fn read_ident_list(&mut self) -> Vec<String> {
        let mut out = Vec::new();
        loop {
            self.skip_trivia();
            if let Some(b) = self.peek_byte() {
                // Stop at the next `%` declaration, `%%`, or end of
                // meaningful line content.
                if b == b'%' {
                    break;
                }
            } else {
                break;
            }
            let ident = self.read_ident();
            if ident.is_empty() {
                break;
            }
            out.push(ident);
        }
        out
    }

    fn read_char_literal(&mut self) -> Result<String> {
        let start = self.pos;
        if self.bump() != Some(b'\'') {
            bail!("expected char literal");
        }
        while let Some(b) = self.bump() {
            if b == b'\\' {
                self.bump();
                continue;
            }
            if b == b'\'' {
                return Ok(self.src[start..self.pos].to_string());
            }
        }
        Err(anyhow!("unterminated char literal"))
    }

    /// Skip an optional `<type>` annotation after a `%token` / `%type`
    /// / `%left` directive. Go-style type names (identifier with
    /// optional `[]` / `*`) are consumed whole.
    fn skip_angle_type(&mut self) {
        self.skip_trivia();
        if self.peek_byte() != Some(b'<') {
            return;
        }
        self.pos += 1;
        while let Some(b) = self.bump() {
            if b == b'>' {
                return;
            }
        }
    }

    fn skip_braced_block(&mut self) -> Result<()> {
        self.skip_trivia();
        if self.peek_byte() != Some(b'{') {
            bail!("expected '{{' at byte {}", self.pos);
        }
        let _ = self.read_braced_body()?;
        Ok(())
    }

    /// Read the contents of a `{ ... }` block, balancing nested braces
    /// and ignoring braces that appear inside string / rune / line
    /// comments. Returns the body without the outer braces.
    fn read_braced_body(&mut self) -> Result<String> {
        if self.bump() != Some(b'{') {
            bail!("expected '{{'");
        }
        let start = self.pos;
        let mut depth = 1;
        while let Some(b) = self.peek_byte() {
            if self.starts_with("//") {
                while let Some(c) = self.peek_byte() {
                    if c == b'\n' {
                        break;
                    }
                    self.pos += 1;
                }
                continue;
            }
            if self.starts_with("/*") {
                self.pos += 2;
                while !self.eof() && !self.starts_with("*/") {
                    self.pos += 1;
                }
                if self.eat("*/") {
                    continue;
                }
                break;
            }
            match b {
                b'"' | b'`' | b'\'' => {
                    self.skip_go_string_like(b);
                    continue;
                }
                b'{' => {
                    depth += 1;
                    self.pos += 1;
                }
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        let body = self.src[start..self.pos].to_string();
                        self.pos += 1;
                        return Ok(body);
                    }
                    self.pos += 1;
                }
                _ => {
                    self.pos += 1;
                }
            }
        }
        Err(anyhow!("unterminated {{ block"))
    }

    fn skip_go_string_like(&mut self, quote: u8) {
        self.pos += 1;
        while let Some(b) = self.peek_byte() {
            self.pos += 1;
            if b == quote {
                return;
            }
            if quote != b'`' && b == b'\\' {
                self.bump();
            }
        }
    }

    fn read_until_line_end(&mut self) {
        while let Some(b) = self.peek_byte() {
            self.pos += 1;
            if b == b'\n' {
                return;
            }
        }
    }
}

// ---------------- tests ----------------

#[cfg(test)]
mod tests {
    use super::*;

    const TINY: &str = r#"
        %{
        package foo
        %}
        %token <item> IDENT NUMBER
        %type <node> expr
        %left PLUS
        %right POW
        %start start

        %%

        start : expr ;

        expr : expr PLUS expr        { $$ = yylex.(*parser).add($1, $3) }
             | expr POW expr         { $$ = yylex.(*parser).pow($1, $3) }
             | IDENT                 { $$ = $1 }
             | NUMBER                { $$ = $1 }
             ;

        %%
    "#;

    #[test]
    fn parse_minimal_grammar() {
        let g = parse(TINY).unwrap();
        assert_eq!(g.start.as_deref(), Some("start"));
        assert_eq!(g.precedence.len(), 2);
        assert_eq!(g.precedence[0].assoc, Assoc::Left);
        assert_eq!(g.precedence[0].tokens, vec!["PLUS".to_string()]);
        assert_eq!(g.precedence[1].assoc, Assoc::Right);
        assert_eq!(g.rules.len(), 2);
        assert_eq!(g.rules[0].lhs, "start");
        assert_eq!(g.rules[0].alternatives.len(), 1);
        assert_eq!(g.rules[0].alternatives[0].symbols, vec!["expr"]);
        let expr_rule = &g.rules[1];
        assert_eq!(expr_rule.lhs, "expr");
        assert_eq!(expr_rule.alternatives.len(), 4);
        assert_eq!(
            expr_rule.alternatives[0].symbols,
            vec!["expr", "PLUS", "expr"]
        );
        assert!(expr_rule.alternatives[0]
            .upstream_action
            .as_deref()
            .unwrap()
            .contains("add"));
    }

    #[test]
    fn parses_upstream_grammar_end_to_end() {
        // The real thing. We don't assert every rule here — just that
        // the parser doesn't blow up and produces plausible counts.
        let src = match std::fs::read_to_string("../promql-parser/upstream/generated_parser.y") {
            Ok(s) => s,
            Err(_) => {
                // Run from the workspace root during `cargo test --workspace`.
                std::fs::read_to_string("promql-parser/upstream/generated_parser.y")
                    .expect("locate upstream grammar")
            }
        };
        let g = parse(&src).expect("parse upstream");
        assert_eq!(g.start.as_deref(), Some("start"));
        // Upstream declares six precedence levels for binary operators
        // + OFFSET nonassoc + LEFT_BRACKET right. That's at least 8.
        assert!(
            g.precedence.len() >= 8,
            "got {} precedence decls",
            g.precedence.len()
        );
        // And at least a dozen rules covering the core expression
        // grammar.
        assert!(g.rules.len() >= 20, "got {} rules", g.rules.len());
    }
}
