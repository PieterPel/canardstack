use super::params::{
    optional_range, optional_time, parse_any_time_to_utc, parse_step, parse_usize, required_param,
    result_rows, validate_range,
};
use super::prometheus::{prom_value, ts_json};
use crate::query::log::parse_loki_query;
use crate::query::logmetric;
use crate::query::promql::Grid;
use crate::query::plan::{LogPlan, TimeBounds};
use crate::semantic_labels;
use crate::validation::{ApiError, ApiResult};
use crate::AppState;
use chrono::Utc;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};

const INTERACTIVE_RANGE_SECS: i64 = 24 * 60 * 60;

pub fn loki_query(state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    if let Some(probe) = loki_probe_vector(params) {
        return Ok(probe);
    }
    if let Some(metric) = logmetric::parse(required_param(params, "query")?)? {
        let at = optional_time(params, "time")?
            .or(optional_time(params, "end")?)
            .unwrap_or_else(Utc::now);
        let grid = Grid {
            start_ms: at.timestamp_millis(),
            step_ms: 1000,
            steps: 1,
        };
        let series = logmetric::evaluate(&state.queries, &state.storage, &metric, grid)?;
        let result = series.into_iter().filter_map(|s| {
            let v = s.vals[0]?;
            Some(json!({"metric": s.labels, "value": [ts_json(grid.start_ms), prom_value(v)]}))
        });
        return Ok(loki_success(
            json!({"resultType": "vector", "result": result.collect::<Vec<_>>()}),
        ));
    }
    loki_query_inner(state, params, false)
}

fn loki_metric_range(
    state: &AppState,
    params: &HashMap<String, String>,
    metric: &logmetric::Metric,
) -> ApiResult<Value> {
    let end = optional_time(params, "end")?.unwrap_or_else(Utc::now);
    let start = optional_time(params, "start")?.unwrap_or(end - chrono::Duration::hours(1));
    validate_range(start, end, INTERACTIVE_RANGE_SECS)?;
    let step = parse_step(params.get("step").map(String::as_str).unwrap_or("60"))?.max(1);
    let steps = (end - start).num_seconds() / step + 1;
    if steps > 11_000 {
        return Err(ApiError::new(
            400,
            "bad_data",
            "exceeded maximum resolution of 11,000 points per timeseries; try decreasing the query resolution (?step=XX)",
        ));
    }
    let grid = Grid {
        start_ms: start.timestamp_millis(),
        step_ms: step * 1000,
        steps: steps as usize,
    };
    let series = logmetric::evaluate(&state.queries, &state.storage, metric, grid)?;
    let result = series.into_iter().filter_map(|s| {
        let values: Vec<Value> = s
            .vals
            .iter()
            .enumerate()
            .filter_map(|(i, v)| Some(json!([ts_json(grid.ts(i)), prom_value((*v)?)])))
            .collect();
        (!values.is_empty()).then(|| json!({"metric": s.labels, "values": values}))
    });
    Ok(loki_success(
        json!({"resultType": "matrix", "result": result.collect::<Vec<_>>()}),
    ))
}

/// Grafana's Loki "Save & test" runs the metric query `vector(1)+vector(1)` and expects a
/// vector result. Metric queries are not implemented, so answer just that probe.
fn loki_probe_vector(params: &HashMap<String, String>) -> Option<Value> {
    let query: String = params.get("query")?.split_whitespace().collect();
    if query != "vector(1)+vector(1)" {
        return None;
    }
    let at = optional_time(params, "time")
        .ok()
        .flatten()
        .unwrap_or_else(Utc::now);
    Some(loki_success(json!({
        "resultType": "vector",
        "result": [{"metric": {}, "value": [at.timestamp(), "2"]}]
    })))
}

pub fn loki_query_range(state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    if let Some(metric) = logmetric::parse(required_param(params, "query")?)? {
        return loki_metric_range(state, params, &metric);
    }
    loki_query_inner(state, params, true)
}

pub fn loki_labels(_state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    let _ = optional_range(params, INTERACTIVE_RANGE_SECS)?;
    Ok(loki_success(json!(semantic_labels::loki_label_names())))
}

pub fn loki_label_values(
    state: &AppState,
    name: &str,
    params: &HashMap<String, String>,
) -> ApiResult<Value> {
    let (from, to) = optional_range(params, INTERACTIVE_RANGE_SECS)?;
    let values =
        state
            .metadata
            .loki_label_values(&state.queries, &state.storage, name, from, to)?;
    Ok(loki_success(json!(values)))
}

pub fn loki_series(state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    let (from, to) = optional_range(params, INTERACTIVE_RANGE_SECS)?;
    let out = state
        .metadata
        .loki_series(&state.queries, &state.storage, from, to)?;
    Ok(loki_success(out))
}
pub(super) fn loki_query_inner(
    state: &AppState,
    params: &HashMap<String, String>,
    range: bool,
) -> ApiResult<Value> {
    let plan = loki_plan(params, range)?;
    let result = state.queries.execute_logs(&state.storage, &plan)?;
    let mut streams: BTreeMap<String, (Map<String, Value>, Vec<Value>)> = BTreeMap::new();
    for row in result_rows(&result) {
        let mut labels = Map::new();
        for label in &plan.stream_labels {
            if let Some(value) = row
                .get(label.as_str())
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
            {
                labels.insert(label.to_string(), json!(value));
            }
        }
        let key = serde_json::to_string(&labels).unwrap_or_default();
        let ts = row
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(parse_any_time_to_utc)
            .unwrap_or(plan.time_bounds.to)
            .timestamp_nanos_opt()
            .unwrap_or_else(|| plan.time_bounds.to.timestamp_micros() * 1000);
        let line = match &plan.line_format {
            Some(text) => text.clone(),
            None => row
                .get("body")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        };
        streams
            .entry(key)
            .or_insert_with(|| (labels, Vec::new()))
            .1
            .push(json!([ts.to_string(), line]));
    }
    let mut result = streams
        .into_values()
        .map(|(stream, values)| json!({"stream": stream, "values": values}))
        .collect::<Vec<_>>();
    if plan.direction.is_forward() {
        result.reverse();
    }
    Ok(loki_success(
        json!({"resultType": "streams", "result": result}),
    ))
}

fn loki_plan(params: &HashMap<String, String>, range: bool) -> ApiResult<LogPlan> {
    let query = required_param(params, "query")?;
    let end = optional_time(params, "end")?.unwrap_or_else(Utc::now);
    let start = optional_time(params, "start")?.unwrap_or(end - chrono::Duration::hours(1));
    validate_range(start, end, INTERACTIVE_RANGE_SECS)?;
    let limit = parse_usize(params.get("limit"), 100, 1000)?;
    let direction = params
        .get("direction")
        .map(String::as_str)
        .unwrap_or("backward");
    let time_bounds = TimeBounds {
        from: start,
        to: if range {
            end
        } else {
            end + chrono::Duration::seconds(1)
        },
    };
    parse_loki_query(query, time_bounds, limit, direction)
}

fn loki_success(data: Value) -> Value {
    json!({"status": "success", "data": data})
}
