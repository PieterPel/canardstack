//! TraceQL: the syntax comes from the `traceql-syntax` crate; this evaluates search queries over
//! the spans of the traces visible in DuckLake.

mod eval;
mod metrics;
mod model;
mod pushdown;
mod value;

use crate::query::QueryEngine;
use crate::storage::Storage;
use crate::validation::{ApiError, ApiResult};
use chrono::{DateTime, Utc};
use eval::Eval;
use traceql_syntax::ast::{Query, QueryKind};
use value::Regexes;

pub use metrics::{evaluate as evaluate_metrics, Range as MetricsRange, Series as MetricsSeries};
pub use traceql_syntax::ast;
pub use value::Val;

pub fn parse(input: &str) -> ApiResult<Query> {
    let query =
        traceql_syntax::parse(input).map_err(|e| ApiError::new(400, "bad_data", e.to_string()))?;
    traceql_syntax::validate(&query).map_err(|e| ApiError::new(400, "bad_data", e.to_string()))?;
    Ok(query)
}

pub struct SpanHit {
    pub span_id: String,
    pub name: String,
    pub start_ns: i64,
    pub duration_ns: i64,
}

pub struct SetHit {
    /// Spans the spanset holds; `spans` is cut to the requested number per spanset.
    pub matched: usize,
    pub spans: Vec<SpanHit>,
}

pub struct TraceHit {
    pub trace_id: String,
    pub root_service: String,
    pub root_name: String,
    pub start_ns: i64,
    pub duration_ns: i64,
    pub sets: Vec<SetHit>,
}

/// Runs a search query: the traces with a span in `[from, to)` that have at least one spanset
/// the query selects, newest first, at most `limit`, with up to `spss` spans per spanset.
pub fn search(
    engine: &QueryEngine,
    storage: &Storage,
    query: &Query,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    limit: usize,
    spss: usize,
) -> ApiResult<Vec<TraceHit>> {
    if matches!(
        query.kind,
        QueryKind::Metrics(_) | QueryKind::MetricsMath { .. }
    ) {
        return Err(ApiError::new(
            400,
            "bad_data",
            "metrics queries are not search queries",
        ));
    }
    let traces = model::load(
        engine,
        storage,
        from,
        to,
        &pushdown::requirements(&query.kind),
        pushdown::needs(&query.kind),
    )?;
    let regexes = Regexes::default();
    let mut hits = Vec::new();
    for trace in &traces {
        let eval = Eval {
            trace,
            regexes: &regexes,
        };
        let groups = match &query.kind {
            QueryKind::Pipeline(p) => eval.pipeline(p, eval.input())?,
            QueryKind::Spansets(s) => {
                let mut out = Vec::new();
                for group in eval.input() {
                    out.extend(eval.pipeline(
                        &ast::Pipeline {
                            elements: vec![ast::Element::Spanset(s.clone())],
                        },
                        vec![group],
                    )?);
                }
                out
            }
            QueryKind::ScalarFilter(f) => eval.scalar_query(f)?,
            _ => Vec::new(),
        };
        if groups.is_empty() {
            continue;
        }
        let sets = groups
            .into_iter()
            .map(|group| SetHit {
                matched: group.len(),
                spans: group
                    .iter()
                    .take(spss)
                    .map(|&i| {
                        let s = &trace.spans[i];
                        SpanHit {
                            span_id: s.span_id.clone(),
                            name: s.name.clone(),
                            start_ns: s.start_ns,
                            duration_ns: s.duration_ns,
                        }
                    })
                    .collect(),
            })
            .collect();
        hits.push(TraceHit {
            trace_id: trace.id.clone(),
            root_service: trace.root_service.clone(),
            root_name: trace.root_name.clone(),
            start_ns: trace.start_ns,
            duration_ns: trace.end_ns - trace.start_ns,
            sets,
        });
    }
    hits.sort_by(|a, b| {
        b.start_ns
            .cmp(&a.start_ns)
            .then_with(|| a.trace_id.cmp(&b.trace_id))
    });
    hits.truncate(limit);
    Ok(hits)
}
