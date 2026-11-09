//! Reads the log rows a selector and its leading line filters can match, bounded by a time window
//! and a row cap (`CANARDSTACK_QUERY_MAX_LOG_LINES`), or counts them in SQL without reading them.

use super::labels::{labels_from_row, ROW_COLUMNS};
use super::pipeline::ValueTest;
use crate::db::sql::{quote, time_predicate};
use crate::query::prom_labels::Labels;
use crate::query::QueryEngine;
use crate::storage::Storage;
use crate::validation::{ApiError, ApiResult};
use chrono::{DateTime, TimeZone, Utc};
use logql_syntax::ast::{FilterValue, LineOp, MatchOp, Matcher, Stage};
use std::collections::HashMap;
use std::sync::Arc;

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
        let Stage::LineFilter {
            op: op @ (LineOp::Contains | LineOp::NotContains),
            values,
        } = stage
        else {
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
        sql.push(if *op == LineOp::Contains {
            any
        } else {
            format!("NOT {any}")
        });
    }
    sql
}

/// Which end of the time window a fetch starts from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Order {
    Oldest,
    Newest,
}

/// The part of the time window a fetch reads, and how many rows of it.
#[derive(Clone, Copy)]
pub struct Window {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub order: Order,
    pub rows: usize,
}

pub struct Fetched {
    /// The rows whose labels satisfy the selector, in the requested order.
    pub entries: Vec<Entry>,
    /// True when the window holds no rows beyond the ones read.
    pub exhausted: bool,
}

fn cap_error(max: usize) -> ApiError {
    ApiError::new(
        422,
        "execution",
        format!("query reads more than {max} log lines; narrow the selector or the time range (the limit is CANARDSTACK_QUERY_MAX_LOG_LINES)"),
    )
}

/// Reads at most `rows` rows of the window, starting from the end `order` names. The database
/// orders and limits, so a query that needs only the newest lines does not read the rest.
pub fn fetch(
    engine: &QueryEngine,
    storage: &Storage,
    matchers: &[Matcher],
    stages: &[Stage],
    window: Window,
) -> ApiResult<Fetched> {
    let Window {
        from,
        to,
        order,
        rows,
    } = window;
    let tests = matchers
        .iter()
        .map(|m| Ok((m.name.clone(), ValueTest::new(m.op, &m.value)?)))
        .collect::<ApiResult<Vec<_>>>()?;
    let mut filters = vec![time_predicate("time_unix_nano", from, to)];
    filters.extend(pushdown(matchers, stages));
    let direction = if order == Order::Oldest {
        "ASC"
    } else {
        "DESC"
    };
    let sql = format!(
        "SELECT epoch_ns(time_unix_nano), coalesce(body, ''), {ROW_COLUMNS} FROM {{prefix}}otlp_logs WHERE {} ORDER BY time_unix_nano {direction} LIMIT {rows}",
        filters.join(" AND "),
    );
    engine.run_interactive(storage, |conn, prefix| {
        let mut stmt = conn.prepare(&sql.replace("{prefix}", prefix))?;
        let mut result = stmt.query([])?;
        let mut streams: HashMap<String, Option<Arc<Labels>>> = HashMap::new();
        let mut out = Vec::new();
        let mut seen = 0usize;
        while let Some(row) = result.next()? {
            seen += 1;
            let mut cols: Vec<Option<String>> = Vec::with_capacity(8);
            for i in 0..8 {
                cols.push(row.get(2 + i)?);
            }
            let key = cols
                .iter()
                .map(|c| c.as_deref().unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\u{1f}");
            let labels = streams.entry(key).or_insert_with(|| {
                let refs: [Option<&str>; 8] = std::array::from_fn(|i| cols[i].as_deref());
                let labels = labels_from_row(refs);
                tests
                    .iter()
                    .all(|(name, test)| test.ok(labels.get(name).map_or("", String::as_str)))
                    .then(|| Arc::new(labels))
            });
            if let Some(labels) = labels {
                out.push(Entry {
                    ts_ns: row.get(0)?,
                    line: row.get(1)?,
                    labels: labels.clone(),
                });
            }
        }
        Ok(Fetched {
            entries: out,
            exhausted: seen < rows,
        })
    })
}

/// Every row of the window, oldest first, for evaluations that need all of them. Refused beyond the
/// configured number of lines.
pub fn fetch_all(
    engine: &QueryEngine,
    storage: &Storage,
    matchers: &[Matcher],
    stages: &[Stage],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> ApiResult<Vec<Entry>> {
    let max = engine.max_log_lines();
    let window = Window {
        from,
        to,
        order: Order::Oldest,
        rows: max + 1,
    };
    let fetched = fetch(engine, storage, matchers, stages, window)?;
    if !fetched.exhausted {
        return Err(cap_error(max));
    }
    Ok(fetched.entries)
}

/// The lines of one stream, counted per time bucket.
pub struct Counted {
    pub labels: Arc<Labels>,
    /// `(bucket, lines, bytes)`; a bucket without lines is absent.
    pub buckets: Vec<(i64, u64, f64)>,
}

/// True when [`counts`] gives the same answer as reading the lines: every stage is a plain
/// `|=` or `!=` that [`pushdown`] already put in the query.
pub fn countable(stages: &[Stage]) -> bool {
    stages.iter().all(|stage| {
        matches!(
            stage,
            Stage::LineFilter { op: LineOp::Contains | LineOp::NotContains, values }
                if values.iter().all(|v| matches!(v, FilterValue::Str(_)))
        )
    })
}

/// Counts the lines and their bytes per stream and time bucket, inside the database. The lines in
/// `(origin_ns, end_ns]` are put in buckets `bucket_ns` wide, bucket `b` holding
/// `(origin + b * width, origin + (b + 1) * width]`. No line is read into memory, so only the number
/// of streams times buckets is bounded.
pub fn counts(
    engine: &QueryEngine,
    storage: &Storage,
    matchers: &[Matcher],
    stages: &[Stage],
    origin_ns: i64,
    end_ns: i64,
    bucket_ns: i64,
) -> ApiResult<Vec<Counted>> {
    let tests = matchers
        .iter()
        .map(|m| Ok((m.name.clone(), ValueTest::new(m.op, &m.value)?)))
        .collect::<ApiResult<Vec<_>>>()?;
    let ms = |ns: i64| Utc.timestamp_millis_opt(ns.div_euclid(1_000_000)).single();
    let (from, to) = (
        ms(origin_ns).ok_or_else(|| ApiError::new(400, "bad_data", "time out of range"))?,
        ms(end_ns + 1_000_000)
            .ok_or_else(|| ApiError::new(400, "bad_data", "time out of range"))?,
    );
    // The coarse predicate lets the files be skipped; the nanosecond one makes the window exact.
    let mut filters = vec![
        time_predicate("time_unix_nano", from, to),
        format!("epoch_ns(time_unix_nano) > {origin_ns} AND epoch_ns(time_unix_nano) <= {end_ns}"),
    ];
    filters.extend(pushdown(matchers, stages));
    let wherever = filters.join(" AND ");
    // A stream is identified by all its label columns, and every log attribute is one, so a query can
    // span hundreds of thousands of streams. Counting groups on a hash of those columns and returns
    // numbers only; the label columns themselves come once per stream, from a second query.
    let key = "hash(concat_ws(chr(31), coalesce(service_name, ''), coalesce(service_namespace, ''), \
         coalesce(service_instance_id, ''), coalesce(resource_attributes::VARCHAR, ''), \
         coalesce(log_attributes::VARCHAR, ''), coalesce(severity_text, ''), coalesce(trace_id, ''), \
         coalesce(span_id, '')))";
    let count_sql = format!(
        "SELECT {key} AS sk, (epoch_ns(time_unix_nano) - {origin_ns} - 1) // {bucket_ns} AS bucket, count(*), \
         sum(strlen(coalesce(body, '')))::DOUBLE FROM {{prefix}}otlp_logs WHERE {wherever} GROUP BY sk, bucket"
    );
    let stream_sql = format!(
        "SELECT {key} AS sk, any_value(service_name), any_value(service_namespace), any_value(service_instance_id), \
         any_value(resource_attributes), any_value(log_attributes), any_value(severity_text), any_value(trace_id), \
         any_value(span_id) FROM {{prefix}}otlp_logs WHERE {wherever} GROUP BY sk"
    );
    let max = engine.max_log_lines();
    engine.run_interactive(storage, |conn, prefix| {
        // Counts first: they are bounded, and a query that exceeds the bound stops before it reads labels.
        let mut by_stream: HashMap<u64, Vec<(i64, u64, f64)>> = HashMap::new();
        let mut groups = 0usize;
        {
            let mut stmt = conn.prepare(&count_sql.replace("{prefix}", prefix))?;
            let mut result = stmt.query([])?;
            while let Some(row) = result.next()? {
                groups += 1;
                if groups > max {
                    return Ok(Err(ApiError::new(
                        422,
                        "execution",
                        format!("query produces more than {max} series-buckets; use a larger step or a narrower time range (the limit is CANARDSTACK_QUERY_MAX_LOG_LINES)"),
                    )));
                }
                let (sk, bucket, lines, bytes): (u64, i64, i64, f64) = (row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?);
                by_stream.entry(sk).or_default().push((bucket, lines as u64, bytes));
            }
        }
        let mut out = Vec::with_capacity(by_stream.len());
        let mut stmt = conn.prepare(&stream_sql.replace("{prefix}", prefix))?;
        let mut result = stmt.query([])?;
        while let Some(row) = result.next()? {
            let sk: u64 = row.get(0)?;
            // A stream whose lines arrived between the two queries has no counts here; skip it.
            let Some(buckets) = by_stream.remove(&sk) else { continue };
            let mut cols: Vec<Option<String>> = Vec::with_capacity(8);
            for i in 0..8 {
                cols.push(row.get(1 + i)?);
            }
            let refs: [Option<&str>; 8] = std::array::from_fn(|i| cols[i].as_deref());
            let labels = labels_from_row(refs);
            if tests
                .iter()
                .all(|(name, test)| test.ok(labels.get(name).map_or("", String::as_str)))
            {
                out.push(Counted { labels: Arc::new(labels), buckets });
            }
        }
        Ok(Ok(out))
    })?
}
