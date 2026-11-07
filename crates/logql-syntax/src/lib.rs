//! A parser for [LogQL](https://grafana.com/docs/loki/latest/query/), Grafana Loki's query language.
//!
//! It turns query text into a syntax tree ([`ast::Expr`]) and does nothing else: no evaluation, no
//! storage, no dependencies. It covers stream selectors, every pipeline stage, range and vector
//! aggregations, `unwrap`, `offset`, grouping, binary operators with vector matching,
//! `label_replace` and `vector()`.
//!
//! ```
//! use logql_syntax::{ast::Expr, parse};
//!
//! let expr = parse(r#"sum by (level) (rate({app="api"} |= "error" | json [5m]))"#).unwrap();
//! assert!(matches!(expr, Expr::Vector(_)));
//!
//! let err = parse(r#"{app="api""#).unwrap_err();
//! assert!(err.to_string().starts_with("parse error at position"));
//! ```

#![forbid(unsafe_code)]

pub mod ast;
mod lex;
mod parse;

pub use parse::parse;

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
