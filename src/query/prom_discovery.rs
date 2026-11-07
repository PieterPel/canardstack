//! Prometheus discovery (label names and values, series, metric metadata) computed from the raw
//! gauge and sum rows in a time window, using the same label model as queries.

use crate::db::sql::time_predicate;
use crate::query::prom_labels::{labels_from_row, Labels, SERIES_COLUMNS};
use crate::query::QueryEngine;
use crate::signal::StorageSignal;
use crate::storage::Storage;
use crate::validation::ApiResult;
use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, BTreeSet};

/// Upper bound on distinct series read for one discovery request.
const SERIES_LIMIT: usize = 10_000;
const METRIC_NAME: &str = "__name__";

/// Distinct label sets (including `__name__`) of the series with samples in the window.
pub fn series(
    queries: &QueryEngine,
    storage: &Storage,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> ApiResult<Vec<Labels>> {
    let pred = time_predicate("time_unix_nano", from, to);
    let select = |signal: StorageSignal| {
        format!(
            "SELECT name, {SERIES_COLUMNS} FROM {{prefix}}{} WHERE {pred}",
            signal.as_str()
        )
    };
    let sql = format!(
        "SELECT DISTINCT * FROM ({} UNION ALL {}) ORDER BY 1, 2 LIMIT {SERIES_LIMIT}",
        select(StorageSignal::MetricGauge),
        select(StorageSignal::MetricSum)
    );
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
            labels.insert(METRIC_NAME.to_string(), row.get::<_, String>(0)?);
            sets.insert(labels);
        }
        Ok(())
    })?;
    Ok(sets.into_iter().collect())
}

pub fn label_names(
    queries: &QueryEngine,
    storage: &Storage,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> ApiResult<Vec<String>> {
    let names: BTreeSet<String> = series(queries, storage, from, to)?
        .into_iter()
        .flat_map(|labels| labels.into_keys())
        .collect();
    Ok(names.into_iter().collect())
}

pub fn label_values(
    queries: &QueryEngine,
    storage: &Storage,
    name: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> ApiResult<Vec<String>> {
    let values: BTreeSet<String> = series(queries, storage, from, to)?
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
        "SELECT name, 'gauge', max(coalesce(unit, '')), max(coalesce(description, '')) FROM {{prefix}}{} GROUP BY name \
         UNION ALL \
         SELECT name, CASE WHEN bool_or(coalesce(is_monotonic, false)) THEN 'counter' ELSE 'gauge' END, max(coalesce(unit, '')), max(coalesce(description, '')) FROM {{prefix}}{} GROUP BY name \
         LIMIT {SERIES_LIMIT}",
        StorageSignal::MetricGauge.as_str(),
        StorageSignal::MetricSum.as_str()
    );
    let mut out = BTreeMap::new();
    queries.run_interactive(storage, |conn, prefix| {
        let mut stmt = conn.prepare(&sql.replace("{prefix}", prefix))?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            out.insert(row.get(0)?, (row.get(1)?, row.get(2)?, row.get(3)?));
        }
        Ok(())
    })?;
    Ok(out)
}
