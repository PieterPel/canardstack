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

/// Loads the traces `ids` names. A page that holds more spans than one load may, because its traces
/// are large, is split and loaded in halves; a single trace larger than that is refused.
fn load_page(
    engine: &QueryEngine,
    storage: &Storage,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    ids: &[String],
    needs: pushdown::Needs,
) -> ApiResult<Vec<model::Trace>> {
    match model::load_ids(engine, storage, from, to, ids, needs) {
        Err(error) if ids.len() > 1 && error.status == 422 => {
            let (head, tail) = ids.split_at(ids.len() / 2);
            let mut traces = load_page(engine, storage, from, to, head, needs)?;
            traces.extend(load_page(engine, storage, from, to, tail, needs)?);
            Ok(traces)
        }
        other => other,
    }
}

/// The hit for one trace, if the query selects at least one spanset of it.
fn hit(
    query: &Query,
    trace: &model::Trace,
    regexes: &Regexes,
    spss: usize,
) -> ApiResult<Option<TraceHit>> {
    let eval = Eval { trace, regexes };
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
        return Ok(None);
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
    Ok(Some(TraceHit {
        trace_id: trace.id.clone(),
        root_service: trace.root_service.clone(),
        root_name: trace.root_name.clone(),
        start_ns: trace.start_ns,
        duration_ns: trace.end_ns - trace.start_ns,
        sets,
    }))
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
    let requirements = pushdown::requirements(&query.kind);
    let needs = pushdown::needs(&query.kind);
    let regexes = Regexes::default();
    let mut hits = Vec::new();
    // Candidate traces come newest first, which is the order of the result, so the first `limit` that
    // match are the answer. They are read a page at a time and the next page only when the query
    // dropped too many of this one (a pipeline, an aggregate or a structural operator can reject a
    // trace that has the spans the pushed-down conditions ask for), instead of loading the window.
    let mut offset = 0;
    let mut page = limit.max(10) * 2;
    loop {
        let ids = model::candidates(engine, storage, from, to, &requirements, offset, page)?;
        let mut traces = load_page(engine, storage, from, to, &ids, needs)?;
        traces.sort_by(|a, b| b.start_ns.cmp(&a.start_ns).then_with(|| a.id.cmp(&b.id)));
        for trace in &traces {
            if let Some(hit) = hit(query, trace, &regexes, spss)? {
                hits.push(hit);
            }
        }
        if hits.len() >= limit || ids.len() < page {
            break;
        }
        offset += page;
        page = page.saturating_mul(4);
    }
    hits.sort_by(|a, b| {
        b.start_ns
            .cmp(&a.start_ns)
            .then_with(|| a.trace_id.cmp(&b.trace_id))
    });
    hits.truncate(limit);
    Ok(hits)
}
