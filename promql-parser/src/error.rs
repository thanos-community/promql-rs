//! Accumulated parse errors. Mirrors upstream's multi-error parse result:
//! actions attach errors to the parser context and continue, and the
//! caller sees the AST along with the error list.

use crate::posrange::PositionRange;

#[derive(Debug, Clone, thiserror::Error)]
#[error("{message} at {}..{}", range.start, range.end)]
pub struct ParseError {
    pub message: String,
    pub range: PositionRange,
}

impl ParseError {
    pub fn new(message: impl Into<String>, range: PositionRange) -> Self {
        Self {
            message: message.into(),
            range,
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("PromQL parse errors: {}", format_all(.0))]
pub struct ParseErrors(pub Vec<ParseError>);

fn format_all(errs: &[ParseError]) -> String {
    errs.iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

impl From<ParseError> for ParseErrors {
    fn from(e: ParseError) -> Self {
        Self(vec![e])
    }
}
