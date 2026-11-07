//! LogQL: parser and (in later modules) the evaluator over the visible log rows.

pub mod ast;
mod lex;
pub mod parse;

pub use parse::parse;
