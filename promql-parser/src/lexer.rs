//! Hand-rolled state-machine lexer. Structural port of
//! `upstream/lex.go`. State function names match upstream so diffs on
//! future revisions are reviewable line-by-line:
//!
//! - [`Lexer::lex_statements`] ↔ `lexStatements`
//! - [`Lexer::lex_inside_braces`] ↔ `lexInsideBraces`
//! - [`Lexer::lex_identifier`] ↔ `lexIdentifier`
//! - [`Lexer::lex_keyword_or_identifier`] ↔ `lexKeywordOrIdentifier`
//! - [`Lexer::lex_number`] ↔ `lexNumber`
//! - [`Lexer::lex_number_or_duration`] ↔ `lexNumberOrDuration`
//! - [`Lexer::lex_string`] ↔ `lexString`
//! - [`Lexer::lex_raw_string`] ↔ `lexRawString`
//! - [`Lexer::lex_escape`] ↔ `lexEscape`
//! - [`Lexer::lex_space`] ↔ `lexSpace`
//! - [`Lexer::lex_line_comment`] ↔ `lexLineComment`
//! - [`Lexer::lex_duration_expr`] ↔ `lexDurationExpr`
//!
//! Series-description and histogram-descriptor states (`lexHistogram`,
//! `lexBuckets`, `lexValueSequence`, `lexHistogramDescriptor`) are
//! deliberately omitted — only the expression grammar is covered so far.
//!
//! This module exposes a standalone tokenizer ([`tokenize`]). Wiring
//! into grmtools as a custom `NonStreamingLexer` lands alongside the
//! grammar port.

use crate::error::ParseError;
use crate::posrange::{Pos, PositionRange};
use crate::token::{Item, ItemType};

/// Tokenize `input` and return the resulting `Item` sequence plus any
/// accumulated lexer errors. An `Eof` `Item` is always appended.
pub fn tokenize(input: &str) -> (Vec<Item>, Vec<ParseError>) {
    let mut l = Lexer::new(input);
    l.run();
    (l.items, l.errors)
}

/// The lexer state, mirroring upstream `stateFn`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Statements,
    InsideBraces,
    Identifier,
    KeywordOrIdentifier,
    NumberOrDuration,
    String_,
    RawString,
    Escape,
    Space,
    LineComment,
    DurationExpr,
    End,
}

/// Tokenizer. Mirrors upstream `Lexer` struct. Series-description fields
/// (`seriesDesc`, `histogramState`) are not yet implemented.
pub struct Lexer<'a> {
    input: &'a str,
    pos: usize,
    start: usize,
    /// Byte width of the most recently read character. Used by
    /// `backup()` to step back exactly one char without re-decoding.
    width: usize,

    items: Vec<Item>,
    errors: Vec<ParseError>,

    paren_depth: i32,
    brace_open: bool,
    bracket_open: bool,
    got_colon: bool,
    got_duration: bool,
    string_open: Option<char>,
}

const LINE_COMMENT: &str = "#";

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            input,
            pos: 0,
            start: 0,
            width: 0,
            items: Vec::new(),
            errors: Vec::new(),
            paren_depth: 0,
            brace_open: false,
            bracket_open: false,
            got_colon: false,
            got_duration: false,
            string_open: None,
        }
    }

    /// Drive the state machine to completion.
    pub fn run(&mut self) {
        let mut state = State::Statements;
        loop {
            state = match state {
                State::Statements => self.lex_statements(),
                State::InsideBraces => self.lex_inside_braces(),
                State::Identifier => self.lex_identifier(),
                State::KeywordOrIdentifier => self.lex_keyword_or_identifier(),
                State::NumberOrDuration => self.lex_number_or_duration(),
                State::String_ => self.lex_string(),
                State::RawString => self.lex_raw_string(),
                State::Escape => self.lex_escape(),
                State::Space => self.lex_space(),
                State::LineComment => self.lex_line_comment(),
                State::DurationExpr => self.lex_duration_expr(),
                // State functions that terminate the run (EOF or error)
                // have already emitted their final item; just return.
                State::End => return,
            };
        }
    }

    // ---------- primitives (upstream: next/peek/backup/emit/ignore/accept…) ----------

    /// Return the next char and advance. Records byte-width so
    /// [`Self::backup`] can step back exactly one char. At EOF, width is
    /// set to zero so a following `backup()` is a no-op — mirroring
    /// upstream's `next()` behaviour.
    fn next(&mut self) -> Option<char> {
        let rest = &self.input[self.pos..];
        let mut chars = rest.char_indices();
        let Some((_, c)) = chars.next() else {
            self.width = 0;
            return None;
        };
        let width = chars.next().map(|(i, _)| i).unwrap_or(rest.len());
        self.width = width;
        self.pos += width;
        Some(c)
    }

    fn peek(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn backup(&mut self) {
        self.pos -= self.width;
        self.width = 0;
    }

    fn emit(&mut self, typ: ItemType) {
        let val = &self.input[self.start..self.pos];
        self.items
            .push(Item::new(typ, self.start as Pos, val.to_string()));
        self.start = self.pos;
    }

    fn ignore(&mut self) {
        self.start = self.pos;
    }

    /// Consume the next char if it is in `valid`. Otherwise, back up.
    fn accept(&mut self, valid: &str) -> bool {
        match self.next() {
            Some(c) if valid.contains(c) => true,
            Some(_) => {
                self.backup();
                false
            }
            None => false,
        }
    }

    /// Peek and return true if the next char is in `valid`.
    fn is(&self, valid: &str) -> bool {
        match self.peek() {
            Some(c) => valid.contains(c),
            None => false,
        }
    }

    fn accept_run(&mut self, valid: &str) {
        while self.accept(valid) {}
    }

    /// Emit an `Error` item carrying the formatted message and terminate
    /// the state machine.
    fn errorf(&mut self, msg: impl Into<String>) -> State {
        let m = msg.into();
        self.errors.push(ParseError::new(
            m.clone(),
            PositionRange::new(self.start as Pos, self.pos as Pos),
        ));
        self.items
            .push(Item::new(ItemType::Error, self.start as Pos, m));
        self.start = self.pos;
        State::End
    }

    // ---------- state functions ----------

    /// Top-level expression state. Mirrors upstream `lexStatements`.
    fn lex_statements(&mut self) -> State {
        if self.brace_open {
            return State::InsideBraces;
        }
        if self.input[self.pos..].starts_with(LINE_COMMENT) {
            return State::LineComment;
        }

        let Some(r) = self.next() else {
            if self.paren_depth != 0 {
                return self.errorf("unclosed left parenthesis");
            }
            if self.bracket_open {
                return self.errorf("unclosed left bracket");
            }
            self.emit(ItemType::Eof);
            return State::End;
        };

        match r {
            ',' => self.emit(ItemType::Comma),
            c if is_space(c) => return State::Space,
            '*' => self.emit(ItemType::Mul),
            '/' => self.emit(ItemType::Div),
            '%' => self.emit(ItemType::Mod),
            '+' => self.emit(ItemType::Add),
            '-' => self.emit(ItemType::Sub),
            '^' => self.emit(ItemType::Pow),
            '=' => match self.peek() {
                Some('=') => {
                    self.next();
                    self.emit(ItemType::EqlC);
                }
                Some('~') => {
                    return self.errorf("unexpected character after '=': '~'");
                }
                _ => self.emit(ItemType::Eql),
            },
            '!' => match self.next() {
                Some('=') => self.emit(ItemType::Neq),
                other => {
                    return self.errorf(format!(
                        "unexpected character after '!': {:?}",
                        other.unwrap_or('\0')
                    ));
                }
            },
            '<' => match self.peek() {
                Some('=') => {
                    self.next();
                    self.emit(ItemType::Lte);
                }
                Some('/') => {
                    self.next();
                    self.emit(ItemType::TrimUpper);
                }
                _ => self.emit(ItemType::Lss),
            },
            '>' => match self.peek() {
                Some('=') => {
                    self.next();
                    self.emit(ItemType::Gte);
                }
                Some('/') => {
                    self.next();
                    self.emit(ItemType::TrimLower);
                }
                _ => self.emit(ItemType::Gtr),
            },
            c if is_digit(c) || (c == '.' && matches!(self.peek(), Some(n) if is_digit(n))) => {
                self.backup();
                return State::NumberOrDuration;
            }
            '"' | '\'' => {
                self.string_open = Some(r);
                return State::String_;
            }
            '`' => {
                self.string_open = Some(r);
                return State::RawString;
            }
            c if is_alpha(c) || c == ':' => {
                if !self.bracket_open {
                    self.backup();
                    return State::KeywordOrIdentifier;
                }
                match c {
                    ':' => {
                        if self.got_colon {
                            return self.errorf(format!("unexpected colon {:?}", c));
                        }
                        self.emit(ItemType::Colon);
                        self.got_colon = true;
                        return State::Statements;
                    }
                    's' | 'S' | 'm' | 'M' => {
                        if self.scan_duration_keyword() {
                            return State::Statements;
                        }
                        return self
                            .errorf(format!("unexpected character: {:?}, expected \":\"", c));
                    }
                    _ => {
                        return self
                            .errorf(format!("unexpected character: {:?}, expected \":\"", c));
                    }
                }
            }
            '(' => {
                self.emit(ItemType::LeftParen);
                self.paren_depth += 1;
                return State::Statements;
            }
            ')' => {
                self.emit(ItemType::RightParen);
                self.paren_depth -= 1;
                if self.paren_depth < 0 {
                    return self.errorf("unexpected right parenthesis");
                }
                return State::Statements;
            }
            '{' => {
                self.emit(ItemType::LeftBrace);
                self.brace_open = true;
                return State::InsideBraces;
            }
            '[' => {
                if self.bracket_open {
                    return self.errorf("unexpected left bracket");
                }
                self.got_colon = false;
                self.got_duration = false;
                self.emit(ItemType::LeftBracket);
                if matches!(self.peek(), Some(n) if is_space(n)) {
                    self.skip_spaces();
                }
                self.bracket_open = true;
                return State::DurationExpr;
            }
            ']' => {
                if !self.bracket_open {
                    return self.errorf("unexpected right bracket");
                }
                self.emit(ItemType::RightBracket);
                self.bracket_open = false;
            }
            '@' => self.emit(ItemType::At),
            c => return self.errorf(format!("unexpected character: {:?}", c)),
        }
        State::Statements
    }

    /// Scan inside `{...}`. Bareword identifiers are always labels here
    /// — keywords are not recognised. Mirrors upstream `lexInsideBraces`.
    fn lex_inside_braces(&mut self) -> State {
        if self.input[self.pos..].starts_with(LINE_COMMENT) {
            return State::LineComment;
        }
        let Some(r) = self.next() else {
            return self.errorf("unexpected end of input inside braces");
        };
        match r {
            c if is_space(c) => return State::Space,
            c if is_alpha(c) => {
                self.backup();
                return State::Identifier;
            }
            ',' => self.emit(ItemType::Comma),
            '"' | '\'' => {
                self.string_open = Some(r);
                return State::String_;
            }
            '`' => {
                self.string_open = Some(r);
                return State::RawString;
            }
            '=' => match self.next() {
                Some('~') => self.emit(ItemType::EqlRegex),
                _ => {
                    self.backup();
                    self.emit(ItemType::Eql);
                }
            },
            '!' => match self.next() {
                Some('~') => self.emit(ItemType::NeqRegex),
                Some('=') => self.emit(ItemType::Neq),
                other => {
                    return self.errorf(format!(
                        "unexpected character after '!' inside braces: {:?}",
                        other.unwrap_or('\0')
                    ));
                }
            },
            '{' => return self.errorf("unexpected left brace"),
            '}' => {
                self.emit(ItemType::RightBrace);
                self.brace_open = false;
                return State::Statements;
            }
            c => return self.errorf(format!("unexpected character inside braces: {:?}", c)),
        }
        State::InsideBraces
    }

    /// Upstream `lexIdentifier`. Used only inside braces; does not allow colons.
    fn lex_identifier(&mut self) -> State {
        while let Some(c) = self.next() {
            if !is_alphanumeric(c) {
                self.backup();
                break;
            }
        }
        self.emit(ItemType::Identifier);
        State::InsideBraces
    }

    /// Upstream `lexKeywordOrIdentifier`. Colons allowed; keywords
    /// recognised via the `key` map.
    fn lex_keyword_or_identifier(&mut self) -> State {
        while let Some(c) = self.next() {
            if !(is_alphanumeric(c) || c == ':') {
                self.backup();
                break;
            }
        }
        let word = &self.input[self.start..self.pos];
        let lower = word.to_ascii_lowercase();
        match keyword(&lower) {
            Some(kw) => {
                // Fill / fill_left / fill_right are only keywords when
                // followed by '(' — this keeps them usable as metric names.
                if matches!(
                    kw,
                    ItemType::Fill | ItemType::FillLeft | ItemType::FillRight
                ) && !self.peek_followed_by_left_paren()
                {
                    self.emit(ItemType::Identifier);
                } else {
                    self.emit(kw);
                }
            }
            None if !word.contains(':') => self.emit(ItemType::Identifier),
            None => self.emit(ItemType::MetricIdentifier),
        }
        State::Statements
    }

    /// Upstream `lexNumberOrDuration`.
    fn lex_number_or_duration(&mut self) -> State {
        if self.scan_number() {
            self.emit(ItemType::Number);
            return State::Statements;
        }
        if self.accept_remaining_duration() {
            self.backup();
            self.emit(ItemType::Duration);
            return State::Statements;
        }
        let bad = self.input[self.start..self.pos].to_string();
        self.errorf(format!("bad number or duration syntax: {:?}", bad))
    }

    fn scan_duration_keyword(&mut self) -> bool {
        while let Some(c) = self.next() {
            if !is_alpha(c) {
                self.backup();
                let word = &self.input[self.start..self.pos];
                let lower = word.to_ascii_lowercase();
                match lower.as_str() {
                    "step" => {
                        self.emit(ItemType::Step);
                        return true;
                    }
                    "range" => {
                        self.emit(ItemType::Range);
                        return true;
                    }
                    "min" => {
                        self.emit(ItemType::Min);
                        return true;
                    }
                    "max" => {
                        self.emit(ItemType::Max);
                        return true;
                    }
                    _ => return false,
                }
            }
        }
        false
    }

    fn accept_remaining_duration(&mut self) -> bool {
        if !self.accept("smhdwy") {
            return false;
        }
        self.accept("s");
        while self.accept("0123456789") {
            while self.accept("0123456789") {}
            if !self.accept("smhdw") {
                return false;
            }
            self.accept("s");
        }
        !matches!(self.next(), Some(c) if is_alphanumeric(c))
    }

    /// Upstream `scanNumber`. Faithful port.
    fn scan_number(&mut self) -> bool {
        let initial = self.pos;
        let mut digit_pattern: &str = "0123456789";
        // Hex prefix (upstream disallows in series descriptions; this
        // lexer doesn't have series-desc mode yet, so this always applies.)
        if self.accept("0") && self.accept("xX") {
            self.accept("_");
            digit_pattern = "0123456789abcdefABCDEF";
        }

        const DOT: &str = ".";
        const EXP: &str = "eE";
        const UNDER: &str = "_";
        const DOT_ANTI: &str = "_.";
        const EXP_ANTI: &str = "._eE";
        const UNDER_ANTI: &str = "._eE";

        self.accept(DOT);
        self.accept(digit_pattern);

        let mut dot_consumed = false;
        let mut exp_consumed = false;

        loop {
            if !self.is(&[digit_pattern, DOT, UNDER, EXP].concat()) {
                break;
            }
            if self.is(DOT) && dot_consumed {
                self.accept(DOT);
                return false;
            }
            if self.is(EXP) && exp_consumed {
                self.accept(EXP);
                return false;
            }
            if self.accept(DOT) {
                dot_consumed = true;
                if self.accept(DOT_ANTI) {
                    return false;
                }
                // Fractional hex not allowed.
                if digit_pattern.len() > 10 {
                    return false;
                }
                continue;
            }
            if self.accept(EXP) {
                exp_consumed = true;
                self.accept("+-");
                if self.accept(EXP_ANTI) || self.peek().is_none() {
                    return false;
                }
                continue;
            }
            if self.accept(UNDER) {
                if self.accept(UNDER_ANTI) || self.peek().is_none() {
                    return false;
                }
                continue;
            }
            self.accept_run(digit_pattern);
        }
        if self.pos == initial {
            return false;
        }
        match self.peek() {
            Some(c) if !is_alphanumeric(c) => true,
            None => true,
            _ => false,
        }
    }

    /// Upstream `lexString`. The opening quote has been consumed.
    fn lex_string(&mut self) -> State {
        loop {
            match self.next() {
                Some('\\') => return State::Escape,
                Some('\u{FFFD}') => {
                    return self.errorf("invalid UTF-8 rune");
                }
                None | Some('\n') => {
                    return self.errorf("unterminated quoted string");
                }
                Some(c) if Some(c) == self.string_open => break,
                _ => {}
            }
        }
        self.emit(ItemType::String);
        if self.brace_open {
            State::InsideBraces
        } else {
            State::Statements
        }
    }

    /// Upstream `lexRawString`. Backtick-delimited, no escape processing.
    fn lex_raw_string(&mut self) -> State {
        loop {
            match self.next() {
                Some('\u{FFFD}') => {
                    return self.errorf("invalid UTF-8 rune");
                }
                None => {
                    return self.errorf("unterminated raw string");
                }
                Some(c) if Some(c) == self.string_open => break,
                _ => {}
            }
        }
        self.emit(ItemType::String);
        if self.brace_open {
            State::InsideBraces
        } else {
            State::Statements
        }
    }

    /// Upstream `lexEscape`. Called after a `\` inside a quoted string.
    fn lex_escape(&mut self) -> State {
        let (n, base, max_val) = match self.next() {
            Some(c)
                if matches!(c, 'a' | 'b' | 'f' | 'n' | 'r' | 't' | 'v' | '\\')
                    || Some(c) == self.string_open =>
            {
                return State::String_;
            }
            Some('0'..='7') => (3, 8, 255),
            Some('x') => {
                self.next();
                (2, 16, 255)
            }
            Some('u') => {
                self.next();
                (4, 16, 0x0010_FFFF)
            }
            Some('U') => {
                self.next();
                (8, 16, 0x0010_FFFF)
            }
            None => {
                self.errorf("escape sequence not terminated");
                return State::String_;
            }
            Some(c) => {
                self.errorf(format!("unknown escape sequence {:?}", c));
                return State::String_;
            }
        };

        let mut x: u32 = 0;
        let mut remaining = n;
        while remaining > 0 {
            let ch = self.peek();
            let d = match ch {
                Some(c) => digit_val(c),
                None => {
                    self.errorf("escape sequence not terminated");
                    return State::String_;
                }
            };
            if d as u32 >= base {
                self.errorf(format!(
                    "illegal character {:?} in escape sequence",
                    ch.unwrap_or('\0')
                ));
                return State::String_;
            }
            self.next();
            x = x * base + d as u32;
            remaining -= 1;
        }
        if x > max_val || (0xD800..=0xDFFF).contains(&x) {
            self.errorf("escape sequence is an invalid Unicode code point");
        }
        State::String_
    }

    /// Upstream `lexSpace`. Consume the space run, ignore it, return to
    /// statements.
    fn lex_space(&mut self) -> State {
        while let Some(c) = self.peek() {
            if is_space(c) {
                self.next();
            } else {
                break;
            }
        }
        self.ignore();
        if self.brace_open {
            State::InsideBraces
        } else if self.bracket_open {
            State::DurationExpr
        } else {
            State::Statements
        }
    }

    fn skip_spaces(&mut self) {
        while let Some(c) = self.peek() {
            if is_space(c) {
                self.next();
            } else {
                break;
            }
        }
        self.ignore();
    }

    /// Upstream `lexLineComment`. `#` through end-of-line; emits a
    /// `Comment` item that the parser layer typically drops.
    fn lex_line_comment(&mut self) -> State {
        self.pos += LINE_COMMENT.len();
        while let Some(c) = self.next() {
            if is_end_of_line(c) {
                self.backup();
                break;
            }
        }
        self.emit(ItemType::Comment);
        State::Statements
    }

    /// Upstream `lexDurationExpr`. Inside `[...]` after `LEFT_BRACKET`.
    fn lex_duration_expr(&mut self) -> State {
        let Some(r) = self.next() else {
            return self.errorf("unexpected end of input in duration expression");
        };
        match r {
            ']' => {
                self.emit(ItemType::RightBracket);
                self.bracket_open = false;
                self.got_colon = false;
                State::Statements
            }
            ':' => {
                self.emit(ItemType::Colon);
                if !self.got_duration {
                    return self.errorf("unexpected colon before duration in duration expression");
                }
                if self.got_colon {
                    return self.errorf("unexpected repeated colon in duration expression");
                }
                self.got_colon = true;
                State::DurationExpr
            }
            '(' => {
                self.emit(ItemType::LeftParen);
                self.paren_depth += 1;
                State::DurationExpr
            }
            ')' => {
                self.emit(ItemType::RightParen);
                self.paren_depth -= 1;
                if self.paren_depth < 0 {
                    return self.errorf("unexpected right parenthesis");
                }
                State::DurationExpr
            }
            c if is_space(c) => {
                self.skip_spaces();
                State::DurationExpr
            }
            '+' => {
                self.emit(ItemType::Add);
                State::DurationExpr
            }
            '-' => {
                self.emit(ItemType::Sub);
                State::DurationExpr
            }
            '*' => {
                self.emit(ItemType::Mul);
                State::DurationExpr
            }
            '/' => {
                self.emit(ItemType::Div);
                State::DurationExpr
            }
            '%' => {
                self.emit(ItemType::Mod);
                State::DurationExpr
            }
            '^' => {
                self.emit(ItemType::Pow);
                State::DurationExpr
            }
            ',' => {
                self.emit(ItemType::Comma);
                State::DurationExpr
            }
            's' | 'S' | 'm' | 'M' | 'r' | 'R' => {
                if self.scan_duration_keyword() {
                    return State::DurationExpr;
                }
                self.errorf(format!(
                    "unexpected character in duration expression: {:?}",
                    r
                ))
            }
            c if is_digit(c) || (c == '.' && matches!(self.peek(), Some(n) if is_digit(n))) => {
                self.backup();
                self.got_duration = true;
                State::NumberOrDuration
            }
            c => self.errorf(format!(
                "unexpected character in duration expression: {:?}",
                c
            )),
        }
    }

    /// Upstream `peekFollowedByLeftParen`. Look past whitespace to see
    /// whether the next significant character is `(`.
    fn peek_followed_by_left_paren(&self) -> bool {
        let bytes = self.input.as_bytes();
        let mut i = self.pos;
        while i < bytes.len() {
            let c = self.input[i..].chars().next().unwrap_or('\0');
            if !is_space(c) {
                return c == '(';
            }
            i += c.len_utf8();
        }
        false
    }
}

// ---------- character classes (upstream: isSpace/isAlpha/isDigit/...) ----------

fn is_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r')
}
fn is_end_of_line(c: char) -> bool {
    matches!(c, '\r' | '\n')
}
fn is_digit(c: char) -> bool {
    c.is_ascii_digit()
}
fn is_alpha(c: char) -> bool {
    c == '_' || c.is_ascii_alphabetic()
}
fn is_alphanumeric(c: char) -> bool {
    is_alpha(c) || is_digit(c)
}

fn digit_val(c: char) -> u8 {
    match c {
        '0'..='9' => c as u8 - b'0',
        'a'..='f' => c as u8 - b'a' + 10,
        'A'..='F' => c as u8 - b'A' + 10,
        _ => 16,
    }
}

/// Keyword table. Mirrors upstream `key` map verbatim.
fn keyword(lower: &str) -> Option<ItemType> {
    Some(match lower {
        // Operators
        "and" => ItemType::Land,
        "or" => ItemType::Lor,
        "unless" => ItemType::Lunless,
        "atan2" => ItemType::Atan2,
        // Aggregators
        "sum" => ItemType::Sum,
        "avg" => ItemType::Avg,
        "count" => ItemType::Count,
        "min" => ItemType::Min,
        "max" => ItemType::Max,
        "group" => ItemType::Group,
        "stddev" => ItemType::Stddev,
        "stdvar" => ItemType::Stdvar,
        "topk" => ItemType::Topk,
        "bottomk" => ItemType::Bottomk,
        "count_values" => ItemType::CountValues,
        "quantile" => ItemType::Quantile,
        "limitk" => ItemType::Limitk,
        "limit_ratio" => ItemType::LimitRatio,
        // Keywords
        "offset" => ItemType::Offset,
        "smoothed" => ItemType::Smoothed,
        "anchored" => ItemType::Anchored,
        "by" => ItemType::By,
        "without" => ItemType::Without,
        "on" => ItemType::On,
        "ignoring" => ItemType::Ignoring,
        "group_left" => ItemType::GroupLeft,
        "group_right" => ItemType::GroupRight,
        "fill" => ItemType::Fill,
        "fill_left" => ItemType::FillLeft,
        "fill_right" => ItemType::FillRight,
        "bool" => ItemType::Bool,
        // Preprocessors
        "start" => ItemType::Start,
        "end" => ItemType::End,
        "step" => ItemType::Step,
        "range" => ItemType::Range,
        // Special numeric keywords
        "inf" | "nan" => ItemType::Number,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(input: &str) -> Vec<ItemType> {
        tokenize(input).0.into_iter().map(|i| i.typ).collect()
    }

    fn vals(input: &str) -> Vec<(ItemType, String)> {
        tokenize(input)
            .0
            .into_iter()
            .map(|i| (i.typ, i.val))
            .collect()
    }

    #[test]
    fn identifier() {
        assert_eq!(kinds("up"), vec![ItemType::Identifier, ItemType::Eof]);
    }

    #[test]
    fn metric_identifier_with_colon() {
        assert_eq!(
            kinds("http:errors"),
            vec![ItemType::MetricIdentifier, ItemType::Eof]
        );
    }

    #[test]
    fn keywords_resolve() {
        assert_eq!(
            kinds("sum by (x)"),
            vec![
                ItemType::Sum,
                ItemType::By,
                ItemType::LeftParen,
                ItemType::Identifier,
                ItemType::RightParen,
                ItemType::Eof,
            ]
        );
    }

    #[test]
    fn nan_and_inf_are_numbers() {
        assert_eq!(kinds("NaN"), vec![ItemType::Number, ItemType::Eof]);
        assert_eq!(kinds("Inf"), vec![ItemType::Number, ItemType::Eof]);
    }

    #[test]
    fn binary_ops_simple() {
        assert_eq!(
            kinds("a + b"),
            vec![
                ItemType::Identifier,
                ItemType::Add,
                ItemType::Identifier,
                ItemType::Eof
            ]
        );
    }

    #[test]
    fn comparison_ops() {
        assert_eq!(
            kinds("a == b != c <= d >= e < f > g"),
            vec![
                ItemType::Identifier,
                ItemType::EqlC,
                ItemType::Identifier,
                ItemType::Neq,
                ItemType::Identifier,
                ItemType::Lte,
                ItemType::Identifier,
                ItemType::Gte,
                ItemType::Identifier,
                ItemType::Lss,
                ItemType::Identifier,
                ItemType::Gtr,
                ItemType::Identifier,
                ItemType::Eof,
            ]
        );
    }

    #[test]
    fn matcher_with_regex() {
        let got = vals(r#"foo{bar!~"5.."}"#);
        let expected: Vec<(ItemType, &str)> = vec![
            (ItemType::Identifier, "foo"),
            (ItemType::LeftBrace, "{"),
            (ItemType::Identifier, "bar"),
            (ItemType::NeqRegex, "!~"),
            (ItemType::String, "\"5..\""),
            (ItemType::RightBrace, "}"),
            (ItemType::Eof, ""),
        ];
        assert_eq!(
            got,
            expected
                .into_iter()
                .map(|(t, v)| (t, v.to_string()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn number_forms() {
        assert_eq!(kinds("3.14"), vec![ItemType::Number, ItemType::Eof]);
        assert_eq!(kinds("1e6"), vec![ItemType::Number, ItemType::Eof]);
        assert_eq!(kinds("0xdead"), vec![ItemType::Number, ItemType::Eof]);
        assert_eq!(kinds("1_000"), vec![ItemType::Number, ItemType::Eof]);
    }

    #[test]
    fn duration_literals() {
        assert_eq!(kinds("5m"), vec![ItemType::Duration, ItemType::Eof]);
        assert_eq!(kinds("1h30m"), vec![ItemType::Duration, ItemType::Eof]);
        assert_eq!(kinds("500ms"), vec![ItemType::Duration, ItemType::Eof]);
    }

    #[test]
    fn range_selector() {
        assert_eq!(
            kinds("foo[5m]"),
            vec![
                ItemType::Identifier,
                ItemType::LeftBracket,
                ItemType::Duration,
                ItemType::RightBracket,
                ItemType::Eof
            ]
        );
    }

    #[test]
    fn subquery_selector() {
        assert_eq!(
            kinds("foo[5m:30s]"),
            vec![
                ItemType::Identifier,
                ItemType::LeftBracket,
                ItemType::Duration,
                ItemType::Colon,
                ItemType::Duration,
                ItemType::RightBracket,
                ItemType::Eof
            ]
        );
    }

    #[test]
    fn at_modifier() {
        assert_eq!(
            kinds("foo @ 1700000000"),
            vec![
                ItemType::Identifier,
                ItemType::At,
                ItemType::Number,
                ItemType::Eof
            ]
        );
    }

    #[test]
    fn line_comment_is_dropped_by_parser_but_emitted_here() {
        let got = kinds("foo # a comment\nbar");
        assert_eq!(
            got,
            vec![
                ItemType::Identifier,
                ItemType::Comment,
                ItemType::Identifier,
                ItemType::Eof,
            ]
        );
    }

    #[test]
    fn string_escapes_and_backtick() {
        assert_eq!(
            kinds(r#""hello\nworld""#),
            vec![ItemType::String, ItemType::Eof]
        );
        assert_eq!(kinds("`raw`"), vec![ItemType::String, ItemType::Eof]);
    }

    #[test]
    fn utf8_quoted_label_name_inside_braces() {
        // Prom 3.0 allows UTF-8 label names quoted as strings inside `{}`.
        assert_eq!(
            kinds(r#"{"attributes.http.method"="GET"}"#),
            vec![
                ItemType::LeftBrace,
                ItemType::String,
                ItemType::Eql,
                ItemType::String,
                ItemType::RightBrace,
                ItemType::Eof,
            ]
        );
    }

    #[test]
    fn fill_is_identifier_when_not_followed_by_paren() {
        // `fill` used as a metric name should lex as Identifier.
        assert_eq!(
            kinds("fill + fill"),
            vec![
                ItemType::Identifier,
                ItemType::Add,
                ItemType::Identifier,
                ItemType::Eof,
            ]
        );
        // `fill(` keeps its keyword role.
        assert_eq!(
            kinds("fill(1)"),
            vec![
                ItemType::Fill,
                ItemType::LeftParen,
                ItemType::Number,
                ItemType::RightParen,
                ItemType::Eof,
            ]
        );
    }
}
