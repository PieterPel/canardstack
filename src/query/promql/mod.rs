//! PromQL evaluator over the gauge and sum rows visible in DuckLake.
//!
//! The query is parsed with `promql-parser`, the raw samples of every selector are fetched in one
//! read transaction (bounded by time window and a sample cap), and the expression is then
//! evaluated in memory at every step of the requested grid with Prometheus semantics: 5 minute
//! lookback for instant vectors, left-open range windows, extrapolated `rate`, `by`/`without`
//! grouping and vector matching. Unsupported constructs fail with `unsupported_promql`.

use crate::query::QueryEngine;
use crate::semantic_labels::{self, LabelScope};
use crate::storage::Storage;
use crate::validation::{ApiError, ApiResult};
use promql_parser::label::{MatchOp, METRIC_NAME};
use promql_parser::parser::{
    self, Expr, LabelModifier, Offset, VectorMatchCardinality, VectorSelector,
};
use regex::Regex;
use std::collections::{BTreeMap, HashMap};

mod aggregate;
mod binary;
mod eval;
mod fetch;
mod functions;

use eval::{Ev, Samples};
use fetch::{collect, fetch, selector_key, offset_ms, Raw};
use fetch::anchored;
use functions::{quantile, variance};

pub const LOOKBACK_MS: i64 = 300_000;
const MAX_SAMPLES: usize = 1_000_000;
const DOT: &str = "__DOT__";

pub type Labels = BTreeMap<String, String>;

#[derive(Clone, Copy)]
pub struct Grid {
    pub start_ms: i64,
    pub step_ms: i64,
    pub steps: usize,
}

impl Grid {
    pub fn ts(&self, i: usize) -> i64 {
        self.start_ms + self.step_ms * i as i64
    }
    fn end_ms(&self) -> i64 {
        self.ts(self.steps.saturating_sub(1))
    }
}

pub struct Series {
    pub labels: Labels,
    pub vals: Vec<Option<f64>>,
}

pub enum Value {
    Scalar(Vec<f64>),
    Vector(Vec<Series>),
    Str(String),
}

fn bad(msg: impl Into<String>) -> ApiError {
    ApiError::new(400, "bad_data", msg)
}

fn unsupported(msg: impl Into<String>) -> ApiError {
    ApiError::new(400, "unsupported_promql", msg)
}

fn exec_err(msg: impl Into<String>) -> ApiError {
    ApiError::new(422, "execution", msg)
}

/// The parser rejects dots in unquoted identifiers; OpenTelemetry names have them.
fn escape_dots(query: &str) -> String {
    let chars: Vec<char> = query.chars().collect();
    let mut out = String::with_capacity(query.len());
    let mut quote_ch: Option<char> = None;
    let mut ident_alpha = false;
    let mut in_ident = false;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if let Some(q) = quote_ch {
            out.push(c);
            if c == '\\' && q != '`' {
                if let Some(next) = chars.get(i + 1) {
                    out.push(*next);
                    i += 1;
                }
            } else if c == q {
                quote_ch = None;
            }
        } else if matches!(c, '"' | '\'' | '`') {
            quote_ch = Some(c);
            in_ident = false;
            out.push(c);
        } else if c.is_alphanumeric() || c == '_' || c == ':' {
            if !in_ident {
                in_ident = true;
                ident_alpha = !c.is_ascii_digit();
            }
            out.push(c);
        } else if c == '.'
            && in_ident
            && ident_alpha
            && chars
                .get(i + 1)
                .is_some_and(|n| n.is_alphabetic() || *n == '_')
        {
            out.push_str(DOT);
        } else {
            in_ident = false;
            out.push(c);
        }
        i += 1;
    }
    out
}

fn unescape(name: &str) -> String {
    name.replace(DOT, ".")
}

fn canon(name: &str) -> String {
    semantic_labels::canonical_for_alias(LabelScope::Metrics, name)
        .map(str::to_string)
        .unwrap_or_else(|| name.to_string())
}

pub fn evaluate(
    engine: &QueryEngine,
    storage: &Storage,
    query: &str,
    grid: Grid,
) -> ApiResult<Value> {
    let expr = parser::parse(&escape_dots(query)).map_err(bad)?;
    let mut reqs = Vec::new();
    collect(&expr, &mut reqs)?;
    let data = if reqs.is_empty() {
        HashMap::new()
    } else {
        fetch(engine, storage, &reqs, &grid)?
    };
    Ev {
        grid: &grid,
        data: &data,
    }
    .eval(&expr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dotted_names_are_escaped_outside_strings_and_numbers() {
        assert_eq!(
            escape_dots(r#"rate(a.b.c{service.name="x.y"}[1.5m]) * 2.5"#),
            format!(r#"rate(a{DOT}b{DOT}c{{service{DOT}name="x.y"}}[1.5m]) * 2.5"#)
        );
    }

    #[test]
    fn quantile_interpolates_like_prometheus() {
        assert_eq!(quantile(0.5, vec![3.0, 1.0, 2.0]), 2.0);
        assert_eq!(quantile(0.5, vec![1.0, 2.0]), 1.5);
        assert_eq!(quantile(1.5, vec![1.0]), f64::INFINITY);
    }

    #[test]
    fn rate_over_a_dense_window_is_the_slope() {
        let samples: Vec<(i64, f64)> = (1..=60).map(|k| (k * 1000, k as f64)).collect();
        let rate = super::functions::extrapolated(&samples, 0, 60_000, true, true).unwrap();
        assert!((rate - 1.0).abs() < 1e-9, "{rate}");
    }
}
