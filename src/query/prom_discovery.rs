//! Prometheus discovery (label names and values, series, metric metadata) computed from the raw
//! gauge and sum rows in a time window, using the same label model as queries.

use crate::db::sql::time_predicate;
use crate::query::prom_labels::{labels_from_row, Labels, SERIES_COLUMNS};
use crate::query::prom_names::{prom_name, Kind};
use crate::query::QueryEngine;
use crate::signal::StorageSignal;
use crate::storage::Storage;
use crate::validation::{ApiError, ApiResult};
use chrono::{DateTime, Utc};
use promql_parser::label::MatchOp;
use promql_parser::parser::{self, Expr};
use regex::Regex;
use std::collections::{BTreeMap, BTreeSet};

/// Upper bound on distinct series read for one discovery request.
const SERIES_LIMIT: usize = 10_000;
const METRIC_NAME: &str = "__name__";

enum Test {
    Eq(String),
    Ne(String),
    Re(Regex),
    NotRe(Regex),
}

/// The `match[]` selectors of a discovery request: a series is selected when any selector
/// matches it. No selectors select everything.
#[derive(Default)]
pub struct Selectors(Vec<Vec<(String, Test)>>);

impl Selectors {
    /// Parses the raw `match[]` values (each a vector selector such as `up{job="a"}`).
    pub fn parse(raw: &[String]) -> ApiResult<Selectors> {
        let bad = |msg: String| ApiError::new(400, "bad_data", msg);
        let mut out = Vec::new();
        for text in raw {
            let Expr::VectorSelector(vs) = parser::parse(text).map_err(|e| bad(format!("invalid match[] selector: {e}")))? else {
                return Err(bad(format!("match[] must be a series selector: {text}")));
            };
            if !vs.matchers.or_matchers.is_empty() {
                return Err(bad("`or` between label matchers is not supported".into()));
            }
            let mut tests: Vec<(String, Test)> = Vec::new();
            if let Some(name) = &vs.name {
                tests.push((METRIC_NAME.to_string(), Test::Eq(name.clone())));
            }
            for m in &vs.matchers.matchers {
                let anchored = |p: &str| Regex::new(&format!("^(?:{p})$")).map_err(|e| bad(format!("invalid regex: {e}")));
                let test = match &m.op {
                    MatchOp::Equal => Test::Eq(m.value.clone()),
                    MatchOp::NotEqual => Test::Ne(m.value.clone()),
                    MatchOp::Re(_) => Test::Re(anchored(&m.value)?),
                    MatchOp::NotRe(_) => Test::NotRe(anchored(&m.value)?),
                };
                tests.push((m.name.clone(), test));
            }
            out.push(tests);
        }
        Ok(Selectors(out))
    }

    pub fn matches(&self, labels: &Labels) -> bool {
        self.0.is_empty()
            || self.0.iter().any(|tests| {
                tests.iter().all(|(name, test)| {
                    let value = labels.get(name).map(String::as_str).unwrap_or("");
                    match test {
                        Test::Eq(v) => value == v,
                        Test::Ne(v) => value != v,
                        Test::Re(r) => r.is_match(value),
                        Test::NotRe(r) => !r.is_match(value),
                    }
                })
            })
    }
}

/// Distinct label sets (including `__name__`) of the series with samples in the window that the
/// selectors match.
pub fn series(
    queries: &QueryEngine,
    storage: &Storage,
    selectors: &Selectors,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> ApiResult<Vec<Labels>> {
    let pred = time_predicate("time_unix_nano", from, to);
    let select = |signal: StorageSignal| {
        format!(
            "SELECT name, {SERIES_COLUMNS}, unit, {} FROM {{prefix}}{} WHERE {pred}",
            match signal {
                StorageSignal::MetricSum => "is_monotonic",
                _ => "NULL::BOOLEAN",
            },
            signal.as_str()
        )
    };
    let sql = format!(
        "SELECT DISTINCT * FROM ({} UNION ALL {}) ORDER BY 1, 2 LIMIT {SERIES_LIMIT}",
        select(StorageSignal::MetricGauge),
        select(StorageSignal::MetricSum)
    );
    let suffixes = queries.prom_name_suffixes();
    let mut sets: BTreeSet<Labels> = BTreeSet::new();
    queries.run_interactive(storage, |conn, prefix| {
        let mut stmt = conn.prepare(&sql.replace("{prefix}", prefix))?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let col = |i: usize| row.get::<_, Option<String>>(i);
            let mut labels = labels_from_row(
                col(1)?.as_deref(),
                col(2)?.as_deref(),
                col(3)?.as_deref(),
                col(4)?.as_deref(),
                col(5)?.as_deref(),
                col(6)?.as_deref(),
            );
            let kind = match row.get::<_, Option<bool>>(8)? {
                Some(true) => Kind::Counter,
                _ => Kind::Gauge,
            };
            let name: String = row.get(0)?;
            let unit: Option<String> = row.get(7)?;
            labels.insert(METRIC_NAME.to_string(), prom_name(&name, unit.as_deref(), kind, suffixes));
            if selectors.matches(&labels) {
                sets.insert(labels);
            }
        }
        Ok(())
    })?;
    Ok(sets.into_iter().collect())
}

pub fn label_names(
    queries: &QueryEngine,
    storage: &Storage,
    selectors: &Selectors,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> ApiResult<Vec<String>> {
    let names: BTreeSet<String> = series(queries, storage, selectors, from, to)?
        .into_iter()
        .flat_map(|labels| labels.into_keys())
        .collect();
    Ok(names.into_iter().collect())
}

pub fn label_values(
    queries: &QueryEngine,
    storage: &Storage,
    name: &str,
    selectors: &Selectors,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> ApiResult<Vec<String>> {
    let values: BTreeSet<String> = series(queries, storage, selectors, from, to)?
        .into_iter()
        .filter_map(|mut labels| labels.remove(name))
        .collect();
    Ok(values.into_iter().collect())
}

/// Type, unit and help text per metric name. A sum is a counter only when monotonic.
pub fn metric_metadata(
    queries: &QueryEngine,
    storage: &Storage,
) -> ApiResult<BTreeMap<String, (String, String, String)>> {
    let sql = format!(
        "SELECT name, false, max(coalesce(unit, '')), max(coalesce(description, '')) FROM {{prefix}}{} GROUP BY name, unit \
         UNION ALL \
         SELECT name, bool_or(coalesce(is_monotonic, false)), max(coalesce(unit, '')), max(coalesce(description, '')) FROM {{prefix}}{} GROUP BY name, unit \
         LIMIT {SERIES_LIMIT}",
        StorageSignal::MetricGauge.as_str(),
        StorageSignal::MetricSum.as_str()
    );
    let suffixes = queries.prom_name_suffixes();
    let mut out = BTreeMap::new();
    queries.run_interactive(storage, |conn, prefix| {
        let mut stmt = conn.prepare(&sql.replace("{prefix}", prefix))?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let name: String = row.get(0)?;
            let monotonic: bool = row.get(1)?;
            let unit: String = row.get(2)?;
            let kind = if monotonic { Kind::Counter } else { Kind::Gauge };
            let shown = prom_name(&name, Some(&unit), kind, suffixes);
            let kind_name = if monotonic { "counter" } else { "gauge" };
            out.insert(shown, (kind_name.to_string(), unit, row.get(3)?));
        }
        Ok(())
    })?;
    Ok(out)
}
