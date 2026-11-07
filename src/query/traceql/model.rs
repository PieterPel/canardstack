//! The spans of the traces that touch a time window, loaded from DuckLake into memory and indexed
//! by trace, so the evaluator can look at whole traces (structural operators, trace intrinsics).

use super::value::{from_json, Val};
use crate::db::sql::time_predicate;
use crate::query::QueryEngine;
use crate::signal::StorageSignal;
use crate::storage::Storage;
use crate::validation::{ApiError, ApiResult};
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use std::collections::BTreeMap;

/// Most spans one search loads; beyond it the query must be narrowed.
pub const MAX_SPANS: usize = 300_000;

/// Spans of a trace that start outside the search window are still loaded when they are within
/// this margin of it, so a trace found near the edge of the window is complete.
const TRACE_MARGIN: Duration = Duration::hours(1);

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

pub struct Span {
    pub span_id: String,
    pub parent_id: String,
    pub name: String,
    pub kind: i32,
    pub status: i32,
    pub status_message: String,
    pub start_ns: i64,
    pub duration_ns: i64,
    pub resource: Attrs,
    pub span: Attrs,
    pub scope_name: String,
    pub scope_version: String,
    pub scope: Attrs,
    pub events: Vec<Event>,
    pub links: Vec<Link>,
    /// Index of the parent within the trace; `None` for roots and for spans whose parent was not
    /// recorded.
    pub parent: Option<usize>,
    pub children: usize,
    pub left: i64,
    pub right: i64,
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
    map.iter().filter_map(|(k, v)| Some((k.clone(), from_json(v)?))).collect()
}

fn array(json: Option<&str>) -> Vec<Value> {
    match json.and_then(|j| serde_json::from_str(j).ok()) {
        Some(Value::Array(items)) => items,
        _ => Vec::new(),
    }
}

fn text(value: &Value, key: &str) -> String {
    value.get(key).and_then(Value::as_str).unwrap_or_default().to_string()
}

fn json_attrs(value: &Value) -> Attrs {
    match value.get("attributes") {
        Some(Value::Object(map)) => map.iter().filter_map(|(k, v)| Some((k.clone(), from_json(v)?))).collect(),
        _ => Attrs::new(),
    }
}

impl Trace {
    pub(super) fn build(id: String, mut spans: Vec<Span>) -> Trace {
        spans.sort_by(|a, b| a.start_ns.cmp(&b.start_ns).then_with(|| a.span_id.cmp(&b.span_id)));
        let by_id: BTreeMap<&str, usize> = spans.iter().enumerate().map(|(i, s)| (s.span_id.as_str(), i)).collect();
        let parents: Vec<Option<usize>> = spans
            .iter()
            .enumerate()
            .map(|(i, s)| by_id.get(s.parent_id.as_str()).copied().filter(|&p| p != i && !s.parent_id.is_empty()))
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
        let end_ns = spans.iter().map(|s| s.start_ns + s.duration_ns).max().unwrap_or(0);
        let root = spans.iter().find(|s| s.parent_id.is_empty());
        let root_name = root.map(|s| s.name.clone()).unwrap_or_default();
        let root_service = root
            .and_then(|s| match s.resource.get("service.name") {
                Some(Val::Str(v)) => Some(v.clone()),
                _ => None,
            })
            .unwrap_or_default();
        Trace { id, spans, root_name, root_service, start_ns, end_ns }
    }
}

/// Loads every trace with a span that starts in `[from, to)`.
pub fn load(engine: &QueryEngine, storage: &Storage, from: DateTime<Utc>, to: DateTime<Utc>) -> ApiResult<Vec<Trace>> {
    let table = StorageSignal::Spans.as_str();
    let sql = format!(
        "SELECT epoch_ns(start_time_unix_nano), duration_time_unix_nano, lower(trace_id), lower(span_id), lower(coalesce(parent_span_id, '')), name, kind, status_code, status_status_message, service_name, service_namespace, service_instance_id, resource_attributes, scope_name, scope_version, scope_attributes, span_attributes, events_json, links_json FROM {{prefix}}{table} WHERE {} AND lower(trace_id) IN (SELECT lower(trace_id) FROM {{prefix}}{table} WHERE {}) ORDER BY 3 LIMIT {}",
        time_predicate("start_time_unix_nano", from - TRACE_MARGIN, to + TRACE_MARGIN),
        time_predicate("start_time_unix_nano", from, to),
        MAX_SPANS + 1
    );
    engine.run_interactive(storage, |conn, prefix| {
        let mut stmt = conn.prepare(&sql.replace("{prefix}", prefix))?;
        let mut rows = stmt.query([])?;
        let mut traces = Vec::new();
        let mut current: Option<(String, Vec<Span>)> = None;
        let mut total = 0usize;
        while let Some(row) = rows.next()? {
            total += 1;
            if total > MAX_SPANS {
                return Ok(Err(ApiError::new(
                    422,
                    "execution",
                    format!("query reads more than {MAX_SPANS} spans; narrow the time range"),
                )));
            }
            let string = |i: usize| row.get::<_, Option<String>>(i);
            let trace_id = string(2)?.unwrap_or_default();
            let mut resource = attrs(string(12)?.as_deref());
            for (key, column) in [("service.name", 9), ("service.namespace", 10), ("service.instance.id", 11)] {
                if let Some(v) = string(column)?.filter(|v| !v.is_empty()) {
                    resource.entry(key.to_string()).or_insert(Val::Str(v));
                }
            }
            let events = array(string(17)?.as_deref())
                .iter()
                .map(|e| Event {
                    name: text(e, "name"),
                    time_ns: e.get("time_unix_nano").and_then(Value::as_i64).unwrap_or(0),
                    attrs: json_attrs(e),
                })
                .collect();
            let links = array(string(18)?.as_deref())
                .iter()
                .map(|l| Link { trace_id: text(l, "trace_id"), span_id: text(l, "span_id"), attrs: json_attrs(l) })
                .collect();
            let span = Span {
                span_id: string(3)?.unwrap_or_default(),
                parent_id: string(4)?.unwrap_or_default(),
                name: string(5)?.unwrap_or_default(),
                kind: row.get::<_, Option<i32>>(6)?.unwrap_or(0),
                status: row.get::<_, Option<i32>>(7)?.unwrap_or(0),
                status_message: string(8)?.unwrap_or_default(),
                start_ns: row.get::<_, Option<i64>>(0)?.unwrap_or(0),
                duration_ns: row.get::<_, Option<i64>>(1)?.unwrap_or(0),
                resource,
                span: attrs(string(16)?.as_deref()),
                scope_name: string(13)?.unwrap_or_default(),
                scope_version: string(14)?.unwrap_or_default(),
                scope: attrs(string(15)?.as_deref()),
                events,
                links,
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
