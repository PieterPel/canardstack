//! Derived metadata refresh stage (the read side).
//!
//! These are the bounded discovery adapters for the Prometheus, Loki, and Tempo
//! compatibility surfaces (label/tag values, series, metric metadata). They
//! prefer the pre-aggregated `metadata_summary` table when a writer maintains
//! it. Prometheus metric label and series discovery falls back to bounded raw
//! metric-table scans so query-only deployments backed by external OTLP writers
//! still expose usable metric discovery.
//!
//! Reads are served through a small generation-keyed in-process cache. Each
//! cached entry is tagged with the storage `metadata_generation`; the refresh
//! stage bumps that generation whenever it re-aggregates `metadata_summary`, so
//! a generation mismatch invalidates the cached answer and the adapter rebuilds
//! it from the latest summary rows.

use crate::db::sql::quote as sql_quote;
use crate::query::{logql, prom_discovery, QueryEngine};
use crate::semantic_labels;
use crate::signal::StorageSignal;
use crate::storage::Storage;
use crate::validation::ApiResult;
use crate::LockExt;
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// An external writer adds rows without telling canardstack, so a cached discovery answer is
/// only trusted for this long; the storage generation alone changes only on catalog re-attach.
const CACHE_TTL: Duration = Duration::from_secs(10);
const MAX_CACHE_ENTRIES: usize = 128;
const MAX_CACHE_VALUE_BYTES: usize = 128 * 1024;
const DISCOVERY_LIMIT: usize = 1000;

#[derive(Default)]
pub struct Metadata {
    cache: Mutex<DiscoveryCache>,
}

impl Metadata {
    pub fn new() -> Self {
        Self {
            cache: Mutex::new(DiscoveryCache::default()),
        }
    }

    pub fn cache_entries(&self) -> usize {
        self.cache.lock_or_poisoned().entries.len()
    }

    pub fn prometheus_label_names(
        &self,
        queries: &QueryEngine,
        storage: &Storage,
        matches: &[String],
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> ApiResult<Vec<String>> {
        let selectors = prom_discovery::Selectors::parse(matches)?;
        let key = CacheKey::window(Api::Prometheus, Discovery::LabelName, &matches.join("\u{1f}"), from, to);
        let value = self.cached(storage, key, || {
            prom_discovery::label_names(queries, storage, &selectors, from, to).map(|names| json!(names))
        })?;
        Ok(string_array(value))
    }

    pub fn prometheus_label_values(
        &self,
        queries: &QueryEngine,
        storage: &Storage,
        name: &str,
        matches: &[String],
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> ApiResult<Vec<String>> {
        let selectors = prom_discovery::Selectors::parse(matches)?;
        let cache_name = format!("{name}\u{1f}{}", matches.join("\u{1f}"));
        let key = CacheKey::window(Api::Prometheus, Discovery::LabelValue, &cache_name, from, to);
        let value = self.cached(storage, key, || {
            prom_discovery::label_values(queries, storage, name, &selectors, from, to).map(|v| json!(v))
        })?;
        Ok(string_array(value))
    }

    pub fn prometheus_series(
        &self,
        queries: &QueryEngine,
        storage: &Storage,
        matches: &[String],
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> ApiResult<Value> {
        let selectors = prom_discovery::Selectors::parse(matches)?;
        let key = CacheKey::window(Api::Prometheus, Discovery::Series, &matches.join("\u{1f}"), from, to);
        self.cached(storage, key, || {
            prom_discovery::series(queries, storage, &selectors, from, to).map(|sets| json!(sets))
        })
    }

    pub fn prometheus_metric_metadata(
        &self,
        queries: &QueryEngine,
        storage: &Storage,
    ) -> ApiResult<Value> {
        let key = CacheKey::static_key(Api::Prometheus, Discovery::MetricMetadata, "");
        self.cached(storage, key, || {
            let mut metadata = Map::new();
            for (name, (kind, unit, help)) in prom_discovery::metric_metadata(queries, storage)? {
                metadata.insert(name, json!([{"type": kind, "unit": unit, "help": help}]));
            }
            Ok(Value::Object(metadata))
        })
    }

    pub fn loki_label_names(
        &self,
        queries: &QueryEngine,
        storage: &Storage,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> ApiResult<Vec<String>> {
        let key = CacheKey::window(Api::Loki, Discovery::LabelName, "", from, to);
        let value = self.cached(storage, key, || {
            logql::discovery::label_names(queries, storage, from, to).map(|v| json!(v))
        })?;
        Ok(string_array(value))
    }

    pub fn loki_label_values(
        &self,
        queries: &QueryEngine,
        storage: &Storage,
        name: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> ApiResult<Vec<String>> {
        let key = CacheKey::window(Api::Loki, Discovery::LabelValue, name, from, to);
        let value = self.cached(storage, key, || {
            logql::discovery::label_values(queries, storage, name, from, to).map(|v| json!(v))
        })?;
        Ok(string_array(value))
    }

    pub fn loki_series(
        &self,
        queries: &QueryEngine,
        storage: &Storage,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> ApiResult<Value> {
        let key = CacheKey::window(Api::Loki, Discovery::Series, "", from, to);
        self.cached(storage, key, || {
            logql::discovery::series(queries, storage, from, to).map(|sets| json!(sets))
        })
    }

    pub fn tempo_tag_values(
        &self,
        queries: &QueryEngine,
        storage: &Storage,
        tag: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> ApiResult<Vec<String>> {
        let Some(name) = semantic_labels::tempo_tag_name(tag) else {
            return Ok(Vec::new());
        };
        let key = CacheKey::window(Api::Tempo, Discovery::TagValue, name, from, to);
        let value = self.cached(storage, key, || {
            label_values(
                queries,
                storage,
                &[StorageSignal::Spans],
                "tag_value",
                name,
                from,
                to,
            )
            .map(|values| json!(values))
        })?;
        Ok(string_array(value))
    }

    fn cached(
        &self,
        storage: &Storage,
        key: CacheKey,
        build: impl FnOnce() -> ApiResult<Value>,
    ) -> ApiResult<Value> {
        let generation = storage.metadata_generation();
        if let Some(value) = self.cache.lock_or_poisoned().get(&key, generation) {
            return Ok(value);
        }

        let value = build()?;
        self.cache
            .lock_or_poisoned()
            .insert(key, generation, value.clone());
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Api {
    Prometheus,
    Loki,
    Tempo,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Discovery {
    LabelName,
    LabelValue,
    Series,
    MetricMetadata,
    TagValue,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Scope {
    /// Not time-scoped; invalidated only by a metadata generation bump.
    Static,
    Window {
        start: String,
        end: String,
    },
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CacheKey {
    api: Api,
    kind: Discovery,
    name: String,
    scope: Scope,
}

impl CacheKey {
    fn window(
        api: Api,
        kind: Discovery,
        name: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Self {
        Self {
            api,
            kind,
            name: name.to_string(),
            scope: Scope::Window {
                start: cache_time(from),
                end: cache_time(to),
            },
        }
    }

    fn static_key(api: Api, kind: Discovery, name: &str) -> Self {
        Self {
            api,
            kind,
            name: name.to_string(),
            scope: Scope::Static,
        }
    }
}

#[derive(Clone)]
struct CacheEntry {
    generation: u64,
    /// Monotonic insertion counter; the smallest `seq` is the oldest entry.
    seq: u64,
    value: Value,
    created: Instant,
}

#[derive(Default)]
struct DiscoveryCache {
    entries: BTreeMap<CacheKey, CacheEntry>,
    next_seq: u64,
}

impl DiscoveryCache {
    fn get(&self, key: &CacheKey, generation: u64) -> Option<Value> {
        self.entries
            .get(key)
            .and_then(|entry| {
                (entry.generation == generation && entry.created.elapsed() < CACHE_TTL)
                    .then(|| entry.value.clone())
            })
    }

    fn insert(&mut self, key: CacheKey, generation: u64, value: Value) {
        if value.to_string().len() > MAX_CACHE_VALUE_BYTES {
            return;
        }
        if self.entries.len() >= MAX_CACHE_ENTRIES && !self.entries.contains_key(&key) {
            // Evict the oldest entry by insertion order (FIFO).
            if let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.seq)
                .map(|(key, _)| key.clone())
            {
                self.entries.remove(&oldest);
            }
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        self.entries.insert(
            key,
            CacheEntry {
                generation,
                seq,
                value,
                created: Instant::now(),
            },
        );
    }
}

/// Render the quoted value list for a SQL `signal IN (...)` filter from storage
/// signals, so metadata SQL never hard-codes a table name that could drift from
/// [`StorageSignal::as_str`].
fn signal_in_list(signals: &[StorageSignal]) -> String {
    signals
        .iter()
        .map(|signal| sql_quote(signal.as_str()))
        .collect::<Vec<_>>()
        .join(", ")
}

fn label_values(
    queries: &QueryEngine,
    storage: &Storage,
    signals: &[StorageSignal],
    kind: &str,
    name: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> ApiResult<Vec<String>> {
    let signal_list = signal_in_list(signals);
    let mut values = BTreeSet::new();
    queries.run_interactive(storage, |conn, prefix| {
        let sql = format!(
            "\
            SELECT value, sum(row_count) AS rows \
            FROM {prefix}metadata_summary \
            WHERE kind = {} \
              AND signal IN ({signal_list}) \
              AND name = {} \
              AND value IS NOT NULL \
              AND {} \
            GROUP BY 1 \
            ORDER BY value \
            LIMIT {DISCOVERY_LIMIT}",
            sql_quote(kind),
            sql_quote(name),
            metadata_time_predicate(from, to)
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| row.get::<_, Option<String>>(0))?;
        for row in rows {
            if let Some(value) = row? {
                values.insert(value);
            }
        }
        Ok(())
    })?;
    Ok(values.into_iter().collect())
}

fn metadata_time_predicate(from: DateTime<Utc>, to: DateTime<Utc>) -> String {
    format!(
        "first_seen < TIMESTAMP {} AND last_seen >= TIMESTAMP {}",
        sql_quote(&to.format("%Y-%m-%d %H:%M:%S%.3f").to_string()),
        sql_quote(&from.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
    )
}
fn cache_time(time: DateTime<Utc>) -> String {
    time.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn string_array(value: Value) -> Vec<String> {
    value
        .as_array()
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}
#[cfg(test)]
mod ttl_tests {
    use super::*;

    #[test]
    fn expired_entries_are_not_served() {
        let mut cache = DiscoveryCache::default();
        let key = CacheKey::window(
            Api::Loki,
            Discovery::LabelValue,
            "service_name",
            Utc::now(),
            Utc::now(),
        );
        cache.insert(key.clone(), 0, json!(["a"]));
        assert!(cache.get(&key, 0).is_some());
        cache.entries.get_mut(&key).unwrap().created = Instant::now() - CACHE_TTL * 2;
        assert!(cache.get(&key, 0).is_none());
    }
}
