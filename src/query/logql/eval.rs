//! Evaluates a log query (selector plus pipeline) into streams.

use super::fetch::fetch;
use super::pipeline::Pipeline;
use crate::query::prom_labels::Labels;
use crate::query::QueryEngine;
use crate::storage::Storage;
use crate::validation::ApiResult;
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
    let entries = fetch(engine, storage, &log.matchers, &log.stages, from, to)?;
    let mut kept: Vec<(i64, String, Labels)> = entries
        .iter()
        .filter_map(|e| pipeline.process(&e.line, &e.labels).map(|(line, labels)| (e.ts_ns, line, labels)))
        .collect();
    if !forward {
        kept.reverse();
    }
    kept.truncate(limit);
    let mut streams: BTreeMap<Labels, Vec<(i64, String)>> = BTreeMap::new();
    for (ts, line, labels) in kept {
        streams.entry(labels).or_default().push((ts, line));
    }
    Ok(streams.into_iter().map(|(labels, entries)| Stream { labels, entries }).collect())
}
