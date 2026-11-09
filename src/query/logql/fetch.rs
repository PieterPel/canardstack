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

/// The lines to count: those in `(origin_ns, end_ns]`, in buckets `bucket_ns` wide, bucket `b`
/// holding `(origin + b * width, origin + (b + 1) * width]`.
#[derive(Clone, Copy)]
pub struct CountWindow {
    pub origin_ns: i64,
    pub end_ns: i64,
    pub bucket_ns: i64,
}

/// The label columns of a log row, in the order [`labels_from_row`] takes them.
const LABEL_COLUMNS: [&str; 8] = [
    "service_name",
    "service_namespace",
    "service_instance_id",
    "resource_attributes",
    "log_attributes",
    "severity_text",
    "trace_id",
    "span_id",
];

/// The columns that alone decide the labels `names`, or `None` when some name can only come from an
/// attribute and the attribute columns are needed.
fn columns_for(names: &[String]) -> Option<Vec<&'static str>> {
    let mut columns = Vec::new();
    for name in names {
        let column = LABEL_COLUMNS
            .iter()
            .copied()
            .find(|c| *c == name.as_str() && !c.ends_with("_attributes"))?;
        if !columns.contains(&column) {
            columns.push(column);
        }
    }
    Some(columns)
}

/// `hash(...)` of the stream columns in `columns`, and the select list that returns each label
/// column of the stream (`NULL` for the ones left out).
fn stream_sql_parts(columns: Option<&[&'static str]>) -> (String, String) {
    let used = |c: &str| columns.is_none_or(|cols| cols.contains(&c));
    let key_parts: Vec<String> = LABEL_COLUMNS
        .iter()
        .filter(|c| used(c))
        .map(|c| format!("coalesce({c}::VARCHAR, '')"))
        .collect();
    let key = if key_parts.is_empty() {
        "hash(0)".to_string()
    } else {
        format!("hash(concat_ws(chr(31), {}))", key_parts.join(", "))
    };
    let select = LABEL_COLUMNS
        .iter()
        .map(|c| {
            if used(c) {
                format!("any_value({c})")
            } else {
                "NULL::VARCHAR".to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    (key, select)
}

/// Counts the lines and their bytes per stream and time bucket, inside the database. No line is read
/// into memory, so only the number of streams times buckets is bounded.
///
/// A stream is identified by all its label columns, and every log attribute is one, so a query can
/// span hundreds of thousands of streams. When the caller needs only the labels `labels` (the ones its
/// selector and its `sum by (...)` use) and each comes from a column that holds a value in every row
/// of the window, the streams are those columns alone, which is a handful. A column that is empty in
/// some row lets an attribute of the same name supply the label there, so then, as for a label that
/// only an attribute can supply, the streams are told apart by every column and the answer is exact.
pub fn counts(
    engine: &QueryEngine,
    storage: &Storage,
    matchers: &[Matcher],
    stages: &[Stage],
    window: CountWindow,
    labels: Option<&[String]>,
) -> ApiResult<Vec<Counted>> {
    let CountWindow {
        origin_ns,
        end_ns,
        bucket_ns,
    } = window;
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
    let reduced = labels.and_then(columns_for);
    let max = engine.max_log_lines();
    engine.run_interactive(storage, |conn, prefix| {
        // Only columns that are filled in every row can stand in for the attributes' say.
        let mut columns = reduced.clone();
        if let Some(cols) = &columns {
            if !cols.is_empty() {
                let empty = cols
                    .iter()
                    .map(|c| format!("{c} IS NULL OR {c} = ''"))
                    .collect::<Vec<_>>()
                    .join(" OR ");
                let probe = format!("SELECT count(*) FROM {prefix}otlp_logs WHERE {wherever} AND ({empty})");
                let gaps: i64 = conn.query_row(&probe, [], |row| row.get(0))?;
                if gaps > 0 {
                    columns = None;
                }
            }
        }
        let (key, select) = stream_sql_parts(columns.as_deref());
        let count_sql = format!(
            "SELECT {key} AS sk, (epoch_ns(time_unix_nano) - {origin_ns} - 1) // {bucket_ns} AS bucket, count(*), \
             sum(strlen(coalesce(body, '')))::DOUBLE FROM {prefix}otlp_logs WHERE {wherever} GROUP BY sk, bucket"
        );
        let stream_sql = format!(
            "SELECT {key} AS sk, {select} FROM {prefix}otlp_logs WHERE {wherever} GROUP BY sk"
        );
        // Counts first: they are bounded, and a query that exceeds the bound stops before it reads labels.
        let mut by_stream: HashMap<u64, Vec<(i64, u64, f64)>> = HashMap::new();
        let mut groups = 0usize;
        {
            let mut stmt = conn.prepare(&count_sql)?;
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
                let (sk, bucket, lines, bytes): (u64, i64, i64, f64) =
                    (row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?);
                by_stream.entry(sk).or_default().push((bucket, lines as u64, bytes));
            }
        }
        let mut out = Vec::with_capacity(by_stream.len());
        let mut stmt = conn.prepare(&stream_sql)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn labels_that_have_a_column_are_told_apart_by_those_columns_alone() {
        let columns = columns_for(&names(&["service_name", "severity_text", "service_name"]));
        assert_eq!(columns, Some(vec!["service_name", "severity_text"]));
        assert_eq!(columns_for(&[]), Some(Vec::new()));
    }

    #[test]
    fn a_label_only_an_attribute_can_supply_needs_every_column() {
        assert_eq!(columns_for(&names(&["service_name", "user"])), None);
        assert_eq!(columns_for(&names(&["log_attributes"])), None);
        assert_eq!(columns_for(&names(&["resource_attributes"])), None);
    }

    #[test]
    fn the_stream_key_and_select_list_follow_the_columns_used() {
        let (key, select) = stream_sql_parts(Some(&["service_name", "trace_id"]));
        assert!(key.contains("coalesce(service_name::VARCHAR, '')"));
        assert!(key.contains("coalesce(trace_id::VARCHAR, '')"));
        assert!(!key.contains("log_attributes"));
        // Eight columns come back whatever is used; the unused ones are NULL.
        assert_eq!(select.split(", ").count(), 8);
        assert_eq!(select.matches("NULL::VARCHAR").count(), 6);
        let (key, select) = stream_sql_parts(None);
        assert!(key.contains("log_attributes"));
        assert!(!select.contains("NULL"));
        assert_eq!(stream_sql_parts(Some(&[])).0, "hash(0)");
    }
}
