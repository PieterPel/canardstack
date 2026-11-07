//! Reads the log rows a selector and its leading line filters can match, bounded by a time window
//! and a row cap.

use super::labels::{labels_from_row, ROW_COLUMNS};
use super::pipeline::ValueTest;
use crate::db::sql::{quote, time_predicate};
use crate::query::prom_labels::Labels;
use crate::query::QueryEngine;
use crate::storage::Storage;
use crate::validation::{ApiError, ApiResult};
use chrono::{DateTime, Utc};
use logql_syntax::ast::{FilterValue, LineOp, MatchOp, Matcher, Stage};
use std::collections::HashMap;
use std::sync::Arc;

pub const MAX_ROWS: usize = 1_000_000;

pub struct Entry {
    pub ts_ns: i64,
    pub line: String,
    /// Shared by every entry of the same stream.
    pub labels: Arc<Labels>,
}

/// `service_name` is a column, so an equality on it narrows the scan; everything else is matched
/// on the labels after the rows are read.
fn pushdown(matchers: &[Matcher], stages: &[Stage]) -> Vec<String> {
    let mut sql = Vec::new();
    for m in matchers {
        if m.op == MatchOp::Eq && m.name == "service_name" && !m.value.is_empty() {
            sql.push(format!("service_name = {}", quote(&m.value)));
        }
    }
    // Leading plain-text line filters run before any parser can change the line.
    for stage in stages {
        let Stage::LineFilter { op: op @ (LineOp::Contains | LineOp::NotContains), values } = stage else {
            break;
        };
        let texts: Option<Vec<String>> = values
            .iter()
            .map(|v| match v {
                FilterValue::Str(s) => Some(format!("contains(coalesce(body, ''), {})", quote(s))),
                FilterValue::Ip(_) => None,
            })
            .collect();
        let Some(texts) = texts else { break };
        let any = format!("({})", texts.join(" OR "));
        sql.push(if *op == LineOp::Contains { any } else { format!("NOT {any}") });
    }
    sql
}

pub fn fetch(
    engine: &QueryEngine,
    storage: &Storage,
    matchers: &[Matcher],
    stages: &[Stage],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> ApiResult<Vec<Entry>> {
    let tests = matchers
        .iter()
        .map(|m| Ok((m.name.clone(), ValueTest::new(m.op, &m.value)?)))
        .collect::<ApiResult<Vec<_>>>()?;
    let mut filters = vec![time_predicate("time_unix_nano", from, to)];
    filters.extend(pushdown(matchers, stages));
    let sql = format!(
        "SELECT epoch_ns(time_unix_nano), coalesce(body, ''), {ROW_COLUMNS} FROM {{prefix}}otlp_logs WHERE {} ORDER BY time_unix_nano LIMIT {}",
        filters.join(" AND "),
        MAX_ROWS + 1
    );
    engine.run_interactive(storage, |conn, prefix| {
        let mut stmt = conn.prepare(&sql.replace("{prefix}", prefix))?;
        let mut rows = stmt.query([])?;
        let mut streams: HashMap<String, Option<Arc<Labels>>> = HashMap::new();
        let mut out = Vec::new();
        let mut seen = 0usize;
        while let Some(row) = rows.next()? {
            seen += 1;
            if seen > MAX_ROWS {
                return Ok(Err(ApiError::new(
                    422,
                    "execution",
                    format!("query reads more than {MAX_ROWS} log lines; narrow the selector or the time range"),
                )));
            }
            let mut cols: Vec<Option<String>> = Vec::with_capacity(8);
            for i in 0..8 {
                cols.push(row.get(2 + i)?);
            }
            let key = cols.iter().map(|c| c.as_deref().unwrap_or("")).collect::<Vec<_>>().join("\u{1f}");
            let labels = streams.entry(key).or_insert_with(|| {
                let refs: [Option<&str>; 8] = std::array::from_fn(|i| cols[i].as_deref());
                let labels = labels_from_row(refs);
                tests
                    .iter()
                    .all(|(name, test)| test.ok(labels.get(name).map_or("", String::as_str)))
                    .then(|| Arc::new(labels))
            });
            if let Some(labels) = labels {
                out.push(Entry { ts_ns: row.get(0)?, line: row.get(1)?, labels: labels.clone() });
            }
        }
        Ok(Ok(out))
    })?
}
