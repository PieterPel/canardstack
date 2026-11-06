//! LogQL metric queries: `count_over_time`, `rate`, `bytes_over_time` and `bytes_rate` over a log
//! selector, optionally wrapped in `sum|avg|min|max|count` with `by`/`without`.
//!
//! The matching lines are fetched once (bounded by time window and a row cap) and counted per
//! step in memory over the left-open window `(t - range, t]`, like the PromQL evaluator.

use crate::query::log::parse_loki_query;
use crate::query::plan::TimeBounds;
use crate::query::promql::{Grid, Labels, Series};
use crate::query::QueryEngine;
use crate::semantic_labels::{self, LabelScope};
use crate::storage::Storage;
use crate::validation::{ApiError, ApiResult};
use chrono::{TimeZone, Utc};
use regex::Regex;
use std::collections::{BTreeMap, HashMap};
use std::sync::OnceLock;

const MAX_ROWS: usize = 1_000_000;

#[derive(Clone, Copy, Debug)]
enum Func {
    Count,
    Rate,
    Bytes,
    BytesRate,
}

#[derive(Debug)]
enum Grouping {
    By(Vec<String>),
    Without(Vec<String>),
}

#[derive(Debug)]
pub struct Metric {
    func: Func,
    selector: String,
    range_ms: i64,
    agg: Option<(String, Option<Grouping>)>,
}

fn unsupported(msg: &str) -> ApiError {
    ApiError::new(400, "unsupported_logql", msg)
}

fn agg_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?s)^(sum|avg|min|max|count)\s*(?:(by|without)\s*\(([^)]*)\))?\s*\((.*?)\)\s*(?:(by|without)\s*\(([^)]*)\))?$")
            .expect("static regex")
    })
}

fn inner_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?s)^(count_over_time|rate|bytes_over_time|bytes_rate)\s*\((.*)\[\s*([0-9]+)(ms|s|m|h|d|w)\s*\]\s*\)$")
            .expect("static regex")
    })
}

fn label_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(|l| {
            semantic_labels::canonical_for_alias(LabelScope::Logs, l)
                .unwrap_or(l)
                .to_string()
        })
        .collect()
}

/// Returns `None` for a log (stream) query, which starts with a selector.
pub fn parse(query: &str) -> ApiResult<Option<Metric>> {
    let q = query.trim();
    if q.starts_with('{') {
        return Ok(None);
    }
    let (agg, inner) = match agg_re().captures(q) {
        Some(c) => {
            let grouping = c
                .get(2)
                .zip(c.get(3))
                .or_else(|| c.get(5).zip(c.get(6)))
                .map(|(kind, labels)| {
                    let labels = label_list(labels.as_str());
                    if kind.as_str() == "by" {
                        Grouping::By(labels)
                    } else {
                        Grouping::Without(labels)
                    }
                });
            (Some((c[1].to_string(), grouping)), c[4].trim().to_string())
        }
        None => (None, q.to_string()),
    };
    let c = inner_re().captures(&inner).ok_or_else(|| {
        unsupported(
            "supported metric queries are count_over_time, rate, bytes_over_time and bytes_rate over a log selector, optionally inside sum/avg/min/max/count [by|without (...)]",
        )
    })?;
    let amount: i64 = c[3]
        .parse()
        .map_err(|_| unsupported("range is out of bounds"))?;
    let unit_ms = match &c[4] {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => 604_800_000,
    };
    Ok(Some(Metric {
        func: match &c[1] {
            "count_over_time" => Func::Count,
            "rate" => Func::Rate,
            "bytes_over_time" => Func::Bytes,
            _ => Func::BytesRate,
        },
        selector: c[2].trim().to_string(),
        range_ms: amount.saturating_mul(unit_ms).max(1),
        agg,
    }))
}

pub fn evaluate(
    engine: &QueryEngine,
    storage: &Storage,
    metric: &Metric,
    grid: Grid,
) -> ApiResult<Vec<Series>> {
    let ms = |ms: i64| {
        Utc.timestamp_millis_opt(ms)
            .single()
            .ok_or_else(|| ApiError::new(400, "bad_data", "time out of range"))
    };
    let bounds = TimeBounds {
        from: ms(grid.start_ms - metric.range_ms)?,
        to: ms(grid.ts(grid.steps - 1) + 1)?,
    };
    let plan = parse_loki_query(&metric.selector, bounds, 1, "forward")?;
    let where_sql = super::log_where_sql(&plan)?;
    let stream_labels = semantic_labels::loki_stream_labels();
    let exprs: Vec<String> = stream_labels
        .iter()
        .filter_map(|l| semantic_labels::label_expr(LabelScope::Logs, l))
        .collect();
    let columns: String = exprs
        .iter()
        .map(|e| format!(", CAST({e} AS VARCHAR)"))
        .collect();
    let sql = format!(
        "SELECT epoch_ms(time_unix_nano), strlen(coalesce(body, ''))::BIGINT{columns} FROM {{prefix}}otlp_logs WHERE {} LIMIT {}",
        where_sql.join(" AND "),
        MAX_ROWS + 1
    );
    let names: Vec<&String> = stream_labels
        .iter()
        .filter(|l| semantic_labels::label_expr(LabelScope::Logs, l).is_some())
        .collect();
    let fetched = engine.run_interactive(storage, |conn, prefix| {
        let mut stmt = conn.prepare(&sql.replace("{prefix}", prefix))?;
        let mut rows = stmt.query([])?;
        let mut streams: HashMap<Labels, Vec<(i64, i64)>> = HashMap::new();
        let mut total = 0usize;
        while let Some(row) = rows.next()? {
            total += 1;
            if total > MAX_ROWS {
                return Ok(Err(ApiError::new(
                    422,
                    "execution",
                    format!("query selects more than {MAX_ROWS} log lines; narrow the selector or the time range"),
                )));
            }
            let mut labels = Labels::new();
            for (i, name) in names.iter().enumerate() {
                if let Some(v) = row.get::<_, Option<String>>(2 + i)?.filter(|v| !v.is_empty()) {
                    labels.insert((*name).clone(), v);
                }
            }
            streams
                .entry(labels)
                .or_default()
                .push((row.get(0)?, row.get(1)?));
        }
        Ok(Ok(streams))
    })??;
    let mut series: Vec<Series> = fetched
        .into_iter()
        .map(|(labels, mut lines)| {
            lines.sort_by_key(|l| l.0);
            let vals = (0..grid.steps)
                .map(|i| {
                    let end = grid.ts(i);
                    let lo = lines.partition_point(|l| l.0 <= end - metric.range_ms);
                    let hi = lines.partition_point(|l| l.0 <= end);
                    (lo < hi).then(|| {
                        let window = &lines[lo..hi];
                        let secs = metric.range_ms as f64 / 1000.0;
                        match metric.func {
                            Func::Count => window.len() as f64,
                            Func::Rate => window.len() as f64 / secs,
                            Func::Bytes => window.iter().map(|l| l.1).sum::<i64>() as f64,
                            Func::BytesRate => window.iter().map(|l| l.1).sum::<i64>() as f64 / secs,
                        }
                    })
                })
                .collect();
            Series { labels, vals }
        })
        .collect();
    series.sort_by(|a, b| a.labels.cmp(&b.labels));
    Ok(match &metric.agg {
        None => series,
        Some((op, grouping)) => aggregate(op, grouping, series, grid.steps),
    })
}

fn aggregate(op: &str, grouping: &Option<Grouping>, series: Vec<Series>, steps: usize) -> Vec<Series> {
    let mut groups: BTreeMap<Labels, Vec<Series>> = BTreeMap::new();
    for s in series {
        let key: Labels = match grouping {
            None => Labels::new(),
            Some(Grouping::By(l)) => s
                .labels
                .iter()
                .filter(|(k, _)| l.contains(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            Some(Grouping::Without(l)) => s
                .labels
                .iter()
                .filter(|(k, _)| !l.contains(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        };
        groups.entry(key).or_default().push(s);
    }
    groups
        .into_iter()
        .map(|(labels, members)| {
            let vals = (0..steps)
                .map(|i| {
                    let vs: Vec<f64> = members.iter().filter_map(|s| s.vals[i]).collect();
                    if vs.is_empty() {
                        return None;
                    }
                    Some(match op {
                        "sum" => vs.iter().sum(),
                        "avg" => vs.iter().sum::<f64>() / vs.len() as f64,
                        "min" => vs.iter().copied().fold(f64::INFINITY, f64::min),
                        "max" => vs.iter().copied().fold(f64::NEG_INFINITY, f64::max),
                        _ => vs.len() as f64,
                    })
                })
                .collect();
            Series { labels, vals }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_aggregation_forms_and_ranges() {
        let m = parse(r#"sum by (service_name) (count_over_time({a="b"} |= "x" [5m]))"#)
            .unwrap()
            .unwrap();
        assert_eq!(m.range_ms, 300_000);
        assert_eq!(m.selector, r#"{a="b"} |= "x""#);
        assert!(matches!(m.agg, Some((ref op, Some(Grouping::By(_)))) if op == "sum"));
        let m = parse(r#"sum(rate({a="b"}[1h])) by (job)"#).unwrap().unwrap();
        assert!(matches!(m.agg, Some((_, Some(Grouping::By(ref l)))) if l.len() == 1));
        assert!(parse(r#"{a="b"}"#).unwrap().is_none());
        assert_eq!(
            parse("topk(1, rate({a=\"b\"}[1m]))").unwrap_err().reason,
            "unsupported_logql"
        );
    }
}
