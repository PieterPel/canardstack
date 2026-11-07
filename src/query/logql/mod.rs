//! LogQL: the syntax comes from the `logql-syntax` crate; the evaluator over the visible log rows
//! is added in later modules.

pub use logql_syntax::ast;

use crate::validation::{ApiError, ApiResult};

pub fn parse(input: &str) -> ApiResult<ast::Expr> {
    logql_syntax::parse(input).map_err(|e| ApiError::new(400, "bad_data", e.to_string()))
}
