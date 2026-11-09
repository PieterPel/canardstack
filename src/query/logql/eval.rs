//! Evaluates a log query (selector plus pipeline) into streams.

use super::fetch::{fetch, Order, Window};
use super::pipeline::Pipeline;
use crate::query::prom_labels::Labels;
use crate::query::QueryEngine;
use crate::storage::Storage;
use crate::validation::{ApiError, ApiResult};
use chrono::{DateTime, Utc};
use logql_syntax::ast::LogExpr;
use std::collections::BTreeMap;

pub struct Stream {
    pub labels: Labels,
    /// `(timestamp in ns, line)`, in the requested direction.
    pub entries: Vec<(i64, String)>,
}

/// The newest (or, forward, the oldest) `limit` lines in `[from, to)`, grouped by label set.
pub fn log_streams(
    engine: &QueryEngine,
    storage: &Storage,
    log: &LogExpr,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    limit: usize,
    forward: bool,
) -> ApiResult<Vec<Stream>> {
    let pipeline = Pipeline::compile(&log.stages)?;
    let order = if forward { Order::Oldest } else { Order::Newest };
    // Read a few times the lines wanted, in the order wanted, and read more only when the pipeline
    // dropped too many of them to fill the limit. The database stops at the row limit, so a query for
    // the newest 100 lines does not read the window.
    let max = engine.max_log_lines();
    let mut rows = limit.saturating_mul(2).max(1_000).min(max);
    let mut kept = loop {
        let fetched = fetch(engine, storage, &log.matchers, &log.stages, Window { from, to, order, rows })?;
        let kept: Vec<(i64, String, Labels)> = fetched
            .entries
            .iter()
            .filter_map(|e| pipeline.process(e.ts_ns, &e.line, &e.labels).map(|(line, labels)| (e.ts_ns, line, labels)))
            .collect();
        if kept.len() >= limit || fetched.exhausted {
            break kept;
        }
        if rows >= max {
            return Err(ApiError::new(
                422,
                "execution",
                format!("query reads more than {max} log lines without finding {limit} that match; narrow the selector or the time range (the limit is CANARDSTACK_QUERY_MAX_LOG_LINES)"),
            ));
        }
        rows = rows.saturating_mul(4).min(max);
    };
    kept.truncate(limit);
    let mut streams: BTreeMap<Labels, Vec<(i64, String)>> = BTreeMap::new();
    for (ts, line, labels) in kept {
        streams.entry(labels).or_default().push((ts, line));
    }
    Ok(streams.into_iter().map(|(labels, entries)| Stream { labels, entries }).collect())
}
