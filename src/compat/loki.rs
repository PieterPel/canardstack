use super::params::{
    optional_range, optional_time, parse_step, parse_usize, required_param, validate_range,
};
use super::prometheus::{prom_value, ts_json};
use crate::query::logmetric;
use crate::query::logql;
use crate::query::promql::Grid;
use crate::semantic_labels;
use crate::validation::{ApiError, ApiResult};
use crate::AppState;
use chrono::Utc;
use serde_json::{json, Value};
use std::collections::HashMap;

const INTERACTIVE_RANGE_SECS: i64 = 24 * 60 * 60;

pub fn loki_query(state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    if let Some(probe) = loki_probe_vector(params) {
        return Ok(probe);
    }
    if let Some(response) = loki_log_query(state, params, false)? {
        return Ok(response);
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
    Err(ApiError::new(400, "bad_data", "unsupported query"))
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
    if let Some(response) = loki_log_query(state, params, true)? {
        return Ok(response);
    }
    if let Some(metric) = logmetric::parse(required_param(params, "query")?)? {
        return loki_metric_range(state, params, &metric);
    }
    Err(ApiError::new(400, "bad_data", "unsupported query"))
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
/// Answers a log query (a selector with a pipeline) with streams, or None for other queries.
fn loki_log_query(
    state: &AppState,
    params: &HashMap<String, String>,
    range: bool,
) -> ApiResult<Option<Value>> {
    let query = required_param(params, "query")?;
    let log = match logql::parse(query) {
        Ok(logql::ast::Expr::Log(log)) => log,
        Ok(_) => return Ok(None),
        Err(err) => return Err(err),
    };
    let end = optional_time(params, "end")?
        .or(if range { None } else { optional_time(params, "time")? })
        .unwrap_or_else(Utc::now);
    let start = optional_time(params, "start")?.unwrap_or(end - chrono::Duration::hours(1));
    validate_range(start, end, INTERACTIVE_RANGE_SECS)?;
    let limit = parse_usize(params.get("limit"), 100, 1000)?;
    let forward = params.get("direction").is_some_and(|d| d.eq_ignore_ascii_case("forward"));
    let to = if range { end } else { end + chrono::Duration::seconds(1) };
    let streams = logql::log_streams(&state.queries, &state.storage, &log, start, to, limit, forward)?;
    let result: Vec<Value> = streams
        .into_iter()
        .map(|s| {
            let values: Vec<Value> = s
                .entries
                .iter()
                .map(|(ts, line)| json!([ts.to_string(), line]))
                .collect();
            json!({"stream": s.labels, "values": values})
        })
        .collect();
    Ok(Some(loki_success(json!({"resultType": "streams", "result": result}))))
}

fn loki_success(data: Value) -> Value {
    json!({"status": "success", "data": data})
}
