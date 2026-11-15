//! Loki label names, label values and series, read from the raw log rows in a time window with the
//! same label model as queries.

use super::labels::{labels_from_row, ROW_COLUMNS};
use crate::db::sql::time_predicate;
use crate::query::prom_labels::Labels;
use crate::query::QueryEngine;
use crate::storage::Storage;
use crate::validation::ApiResult;
use chrono::{DateTime, Utc};
use std::collections::BTreeSet;

/// Upper bound on distinct label sets read for one discovery request.
const SERIES_LIMIT: usize = 10_000;

pub fn series(
    engine: &QueryEngine,
    storage: &Storage,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> ApiResult<Vec<Labels>> {
    let sql = format!(
        "SELECT DISTINCT {ROW_COLUMNS} FROM {{prefix}}otlp_logs WHERE {} LIMIT {SERIES_LIMIT}",
        time_predicate("time_unix_nano", from, to)
    );
    let mut sets: BTreeSet<Labels> = BTreeSet::new();
    engine.run_interactive(storage, |conn, prefix| {
        let mut stmt = conn.prepare(&sql.replace("{prefix}", prefix))?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let mut cols: Vec<Option<String>> = Vec::with_capacity(8);
            for i in 0..8 {
                cols.push(row.get(i)?);
            }
            sets.insert(labels_from_row(std::array::from_fn(|i| cols[i].as_deref())));
        }
        Ok(())
    })?;
    Ok(sets.into_iter().collect())
}

pub fn label_names(
    engine: &QueryEngine,
    storage: &Storage,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> ApiResult<Vec<String>> {
    let names: BTreeSet<String> = series(engine, storage, from, to)?
        .into_iter()
        .flat_map(Labels::into_keys)
        .collect();
    Ok(names.into_iter().collect())
}

pub fn label_values(
    engine: &QueryEngine,
    storage: &Storage,
    name: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> ApiResult<Vec<String>> {
    let values: BTreeSet<String> = series(engine, storage, from, to)?
        .into_iter()
        .filter_map(|mut labels| labels.remove(name))
        .collect();
    Ok(values.into_iter().collect())
}
