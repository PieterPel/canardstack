//! A parser and validator for [TraceQL](https://grafana.com/docs/tempo/latest/traceql/), Grafana
//! Tempo's query language for traces.
//!
//! [`parse`] turns query text into a syntax tree ([`ast::Query`]); [`validate`] applies the type
//! rules (`{ .a = 1 }` is fine, `{ 1 + "x" }` is not) and reports features that parse but are not
//! supported. No evaluation, no storage, no dependencies.
//!
//! ```
//! use traceql_syntax::{ast::QueryKind, parse, validate};
//!
//! let query = parse(r#"{ resource.service.name = "api" && duration > 1s } | count() > 3"#).unwrap();
//! assert!(matches!(query.kind, QueryKind::Pipeline(_)));
//! validate(&query).unwrap();
//! assert!(parse("{ .a < }").is_err());
//! ```
//!

#![forbid(unsafe_code)]

pub mod ast;
mod lex;
mod parse;
mod validate;

pub use parse::parse;
pub use validate::{regex_syntax_ok, validate, validate_with, ValidateError};

/// Why a query did not parse, and where (a byte offset into the query).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseError {
    pub position: usize,
    pub message: String,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "parse error at position {}: {}", self.position, self.message)
    }
}

impl std::error::Error for ParseError {}

pub type Result<T> = std::result::Result<T, ParseError>;
