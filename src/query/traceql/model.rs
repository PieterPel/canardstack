//! The spans of the traces that touch a time window, loaded from DuckLake into memory and indexed
//! by trace, so the evaluator can look at whole traces (structural operators, trace intrinsics).

use super::pushdown::Needs;
use super::value::{from_json, Val};
use crate::db::sql::{quote, time_predicate};
use crate::query::QueryEngine;
use crate::signal::StorageSignal;
use crate::storage::Storage;
use crate::validation::{ApiError, ApiResult};
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use std::cell::OnceCell;
use std::collections::BTreeMap;

/// Spans of a trace that start outside the search window are still loaded when they are within
/// this margin of it, so a trace found near the edge of the window is complete.
const TRACE_MARGIN: Duration = Duration::minutes(15);

pub type Attrs = BTreeMap<String, Val>;

pub struct Event {
    pub name: String,
    pub time_ns: i64,
    pub attrs: Attrs,
}

pub struct Link {
    pub trace_id: String,
    pub span_id: String,
    pub attrs: Attrs,
}

/// A value parsed from its stored JSON the first time it is read: most spans of a loaded trace
/// are never looked at beyond their intrinsics.
pub struct Lazy<T> {
    raw: Option<String>,
    cell: OnceCell<T>,
}

impl<T> Lazy<T> {
    pub fn raw(raw: Option<String>) -> Self {
        Lazy {
            raw,
            cell: OnceCell::new(),
        }
    }

    #[cfg(test)]
    pub fn ready(value: T) -> Self {
        Lazy {
            raw: None,
            cell: OnceCell::from(value),
        }
    }

    fn get(&self, parse: impl FnOnce(Option<&str>) -> T) -> &T {
        self.cell.get_or_init(|| parse(self.raw.as_deref()))
    }
}

pub struct Span {
    pub span_id: String,
    pub parent_id: String,
    pub name: String,
    pub kind: i32,
    pub status: i32,
    pub status_message: String,
    pub start_ns: i64,
    pub duration_ns: i64,
    /// `service.name`, `service.namespace` and `service.instance.id` from their own columns.
    pub service: [Option<String>; 3],
    pub resource: Lazy<Attrs>,
    pub attributes: Lazy<Attrs>,
    pub scope_name: String,
    pub scope_version: String,
    pub scope: Lazy<Attrs>,
    pub events: Lazy<Vec<Event>>,
    pub links: Lazy<Vec<Link>>,
    /// Index of the parent within the trace; `None` for roots and for spans whose parent was not
    /// recorded.
    pub parent: Option<usize>,
    pub children: usize,
    pub left: i64,
    pub right: i64,
}

impl Span {
    /// Resource attributes, with the service columns filling in what the JSON lacks.
    pub fn resource(&self) -> &Attrs {
        self.resource.get(|raw| {
            let mut map = attrs(raw);
            for (key, value) in ["service.name", "service.namespace", "service.instance.id"]
                .into_iter()
                .zip(&self.service)
            {
                if let Some(v) = value.as_ref().filter(|v| !v.is_empty()) {
                    map.entry(key.to_string()).or_insert(Val::Str(v.clone()));
                }
            }
            map
        })
    }

    pub fn span_attrs(&self) -> &Attrs {
        self.attributes.get(attrs)
    }

    pub fn scope_attrs(&self) -> &Attrs {
        self.scope.get(attrs)
    }

    pub fn events(&self) -> &[Event] {
        self.events.get(|raw| {
            array(raw)
                .iter()
                .map(|e| Event {
                    name: text(e, "name"),
                    time_ns: e.get("time_unix_nano").and_then(Value::as_i64).unwrap_or(0),
                    attrs: json_attrs(e),
                })
                .collect()
        })
    }

    pub fn links(&self) -> &[Link] {
        self.links.get(|raw| {
            array(raw)
                .iter()
                .map(|l| Link {
                    trace_id: text(l, "trace_id"),
                    span_id: text(l, "span_id"),
                    attrs: json_attrs(l),
                })
                .collect()
        })
    }
}

pub struct Trace {
    pub id: String,
    pub spans: Vec<Span>,
    pub root_name: String,
    pub root_service: String,
    pub start_ns: i64,
    pub end_ns: i64,
}

fn attrs(json: Option<&str>) -> Attrs {
    let Some(Value::Object(map)) = json.and_then(|j| serde_json::from_str(j).ok()) else {
        return Attrs::new();
    };
    map.iter()
        .filter_map(|(k, v)| Some((k.clone(), from_json(v)?)))
        .collect()
}

fn array(json: Option<&str>) -> Vec<Value> {
    match json.and_then(|j| serde_json::from_str(j).ok()) {
        Some(Value::Array(items)) => items,
        _ => Vec::new(),
    }
}

fn text(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn json_attrs(value: &Value) -> Attrs {
    match value.get("attributes") {
        Some(Value::Object(map)) => map
            .iter()
            .filter_map(|(k, v)| Some((k.clone(), from_json(v)?)))
            .collect(),
        _ => Attrs::new(),
    }
}

impl Trace {
    pub(super) fn build(id: String, mut spans: Vec<Span>) -> Trace {
        spans.sort_by(|a, b| {
            a.start_ns
                .cmp(&b.start_ns)
                .then_with(|| a.span_id.cmp(&b.span_id))
        });
        let by_id: BTreeMap<&str, usize> = spans
            .iter()
            .enumerate()
            .map(|(i, s)| (s.span_id.as_str(), i))
            .collect();
        let parents: Vec<Option<usize>> = spans
            .iter()
            .enumerate()
            .map(|(i, s)| {
                by_id
                    .get(s.parent_id.as_str())
                    .copied()
                    .filter(|&p| p != i && !s.parent_id.is_empty())
            })
            .collect();
        let mut kids: Vec<Vec<usize>> = vec![Vec::new(); spans.len()];
        for (i, parent) in parents.iter().enumerate() {
            if let Some(p) = parent {
                kids[*p].push(i);
            }
        }
        for (span, parent) in spans.iter_mut().zip(&parents) {
            span.parent = *parent;
        }
        // Nested set numbering: every root (also an orphan) starts a tree.
        let mut counter = 1;
        for root in (0..spans.len()).filter(|&i| parents[i].is_none()) {
            let mut stack = vec![(root, false)];
            while let Some((i, leaving)) = stack.pop() {
                if leaving {
                    spans[i].right = counter;
                    counter += 1;
                    continue;
                }
                if spans[i].left != 0 {
                    continue;
                }
                spans[i].left = counter;
                spans[i].children = kids[i].len();
                counter += 1;
                stack.push((i, true));
                for &child in kids[i].iter().rev() {
                    stack.push((child, false));
                }
            }
        }
        let start_ns = spans.iter().map(|s| s.start_ns).min().unwrap_or(0);
        let end_ns = spans
            .iter()
            .map(|s| s.start_ns + s.duration_ns)
            .max()
            .unwrap_or(0);
        let root = spans.iter().find(|s| s.parent_id.is_empty());
        let root_name = root.map(|s| s.name.clone()).unwrap_or_default();
        let root_service = root
            .and_then(|s| match s.resource().get("service.name") {
                Some(Val::Str(v)) => Some(v.clone()),
                _ => None,
            })
            .unwrap_or_default();
        Trace {
            id,
            spans,
            root_name,
            root_service,
            start_ns,
            end_ns,
        }
    }
}

/// The select list of a span, with the columns the query cannot read as NULL.
fn span_columns(needs: Needs) -> String {
    let wide = |wanted: bool, column: &str| {
        if wanted {
            column.to_string()
        } else {
            "NULL".to_string()
        }
    };
    format!(
        "epoch_ns(start_time_unix_nano) AS c0, duration_time_unix_nano AS c1, lower(trace_id) AS c2, lower(span_id) AS c3, lower(coalesce(parent_span_id, '')) AS c4, name AS c5, kind AS c6, status_code AS c7, {} AS c8, service_name AS c9, service_namespace AS c10, service_instance_id AS c11, {} AS c12, {} AS c13, {} AS c14, {} AS c15, {} AS c16, {} AS c17, {} AS c18",
        wide(needs.status_message, "status_status_message"),
        wide(needs.resource, "resource_attributes"),
        wide(needs.scope_names, "scope_name"),
        wide(needs.scope_names, "scope_version"),
        wide(needs.scope_attrs, "scope_attributes"),
        wide(needs.span_attrs, "span_attributes"),
        wide(needs.events, "events_json"),
        wide(needs.links, "links_json"),
    )
}

/// Runs `sql`, which selects [`span_columns`] as `c0` to `c18` ordered by `c2`, and builds the traces
/// from its rows. More than the configured number of spans is refused.
fn read_traces(engine: &QueryEngine, storage: &Storage, sql: String) -> ApiResult<Vec<Trace>> {
    let max = engine.max_spans();
    engine.run_interactive(storage, |conn, prefix| {
        let mut stmt = conn.prepare(&sql.replace("{prefix}", prefix))?;
        let mut rows = stmt.query([])?;
        let mut traces = Vec::new();
        let mut current: Option<(String, Vec<Span>)> = None;
        let mut total = 0usize;
        while let Some(row) = rows.next()? {
            total += 1;
            if total > max {
                return Ok(Err(spans_cap_error(max)));
            }
            let string = |i: usize| row.get::<_, Option<String>>(i);
            let trace_id = string(2)?.unwrap_or_default();
            let span = Span {
                span_id: string(3)?.unwrap_or_default(),
                parent_id: string(4)?.unwrap_or_default(),
                name: string(5)?.unwrap_or_default(),
                kind: row.get::<_, Option<i32>>(6)?.unwrap_or(0),
                status: row.get::<_, Option<i32>>(7)?.unwrap_or(0),
                status_message: string(8)?.unwrap_or_default(),
                start_ns: row.get::<_, Option<i64>>(0)?.unwrap_or(0),
                duration_ns: row.get::<_, Option<i64>>(1)?.unwrap_or(0),
                service: [string(9)?, string(10)?, string(11)?],
                resource: Lazy::raw(string(12)?),
                attributes: Lazy::raw(string(16)?),
                scope_name: string(13)?.unwrap_or_default(),
                scope_version: string(14)?.unwrap_or_default(),
                scope: Lazy::raw(string(15)?),
                events: Lazy::raw(string(17)?),
                links: Lazy::raw(string(18)?),
                parent: None,
                children: 0,
                left: 0,
                right: 0,
            };
            match &mut current {
                Some((id, spans)) if *id == trace_id => spans.push(span),
                slot => {
                    if let Some((id, spans)) = slot.take() {
                        traces.push(Trace::build(id, spans));
                    }
                    *slot = Some((trace_id, vec![span]));
                }
            }
        }
        if let Some((id, spans)) = current {
            traces.push(Trace::build(id, spans));
        }
        Ok(Ok(traces))
    })?
}

pub fn spans_cap_error(max: usize) -> ApiError {
    ApiError::new(
        422,
        "execution",
        format!("query reads more than {max} spans; narrow the time range (the limit is CANARDSTACK_QUERY_MAX_SPANS)"),
    )
}

/// The margin around a window within which a trace's spans are loaded.
fn margin(from: DateTime<Utc>, to: DateTime<Utc>) -> String {
    time_predicate(
        "start_time_unix_nano",
        from - TRACE_MARGIN,
        to + TRACE_MARGIN,
    )
}

/// Loads every trace with a span that starts in `[from, to)` and, for each of the `requirements`
/// (SQL conditions on one span, see `pushdown`), at least one span that satisfies it.
pub fn load(
    engine: &QueryEngine,
    storage: &Storage,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    requirements: &[String],
    needs: Needs,
) -> ApiResult<Vec<Trace>> {
    let table = StorageSignal::Spans.as_str();
    // One scan over the margin: window functions tell, per trace, whether any span starts in the
    // window and whether any span meets each requirement, and only such traces are returned.
    let margin = margin(from, to);
    let flags: String = std::iter::once(time_predicate("start_time_unix_nano", from, to))
        .chain(requirements.iter().cloned())
        .enumerate()
        .map(|(i, cond)| format!(", max(CASE WHEN {cond} THEN 1 ELSE 0 END) OVER w AS f{i}"))
        .collect();
    let wanted: String = (0..=requirements.len())
        .map(|i| format!(" AND f{i} = 1"))
        .collect();
    let columns = span_columns(needs);
    let sql = format!(
        "SELECT c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18 FROM (SELECT {columns}{flags} FROM {{prefix}}{table} WHERE {margin} WINDOW w AS (PARTITION BY lower(trace_id))) WHERE true{wanted} ORDER BY c2 LIMIT {}",
        engine.max_spans() + 1
    );
    read_traces(engine, storage, sql)
}

/// The ids of the traces [`load`] would return, newest first (by the start of the trace's earliest
/// span, then by id), `count` of them after skipping `offset`. This reads three columns and
/// aggregates, so it is cheap next to loading the spans.
pub fn candidates(
    engine: &QueryEngine,
    storage: &Storage,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    requirements: &[String],
    offset: usize,
    count: usize,
) -> ApiResult<Vec<String>> {
    let table = StorageSignal::Spans.as_str();
    let margin = margin(from, to);
    let having: String = std::iter::once(time_predicate("start_time_unix_nano", from, to))
        .chain(requirements.iter().cloned())
        .map(|cond| format!("max(CASE WHEN {cond} THEN 1 ELSE 0 END) = 1"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "SELECT lower(trace_id) AS tid FROM {{prefix}}{table} WHERE {margin} GROUP BY tid HAVING {having} ORDER BY min(start_time_unix_nano) DESC, tid LIMIT {count} OFFSET {offset}"
    );
    engine.run_interactive(storage, |conn, prefix| {
        let mut stmt = conn.prepare(&sql.replace("{prefix}", prefix))?;
        let mut rows = stmt.query([])?;
        let mut ids = Vec::new();
        while let Some(row) = rows.next()? {
            ids.push(row.get::<_, String>(0)?);
        }
        Ok(ids)
    })
}

/// Loads the traces with these ids, in full (every span within the margin of the window), ordered
/// by trace id.
pub fn load_ids(
    engine: &QueryEngine,
    storage: &Storage,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    ids: &[String],
    needs: Needs,
) -> ApiResult<Vec<Trace>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let table = StorageSignal::Spans.as_str();
    let margin = margin(from, to);
    let list = ids
        .iter()
        .map(|id| quote(id))
        .collect::<Vec<_>>()
        .join(", ");
    let columns = span_columns(needs);
    let sql = format!(
        "SELECT * FROM (SELECT {columns} FROM {{prefix}}{table} WHERE {margin} AND lower(trace_id) IN ({list})) ORDER BY c2 LIMIT {}",
        engine.max_spans() + 1
    );
    read_traces(engine, storage, sql)
}
