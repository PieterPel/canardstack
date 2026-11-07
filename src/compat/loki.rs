use super::params::{
    optional_range, optional_time, parse_step, parse_usize, required_param, validate_range,
};
use super::prometheus::{prom_value, ts_json};
use crate::query::logql::{self, ast::Expr, MetricValue};
use crate::query::vector::Grid;
use crate::validation::{ApiError, ApiResult};
use crate::AppState;
use chrono::Utc;
use serde_json::{json, Value};
use std::collections::HashMap;

const INTERACTIVE_RANGE_SECS: i64 = 24 * 60 * 60;
const MAX_RANGE_STEPS: i64 = 11_000;

pub fn loki_query(state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    match logql::parse(required_param(params, "query")?)? {
        Expr::Log(log) => loki_log_query(state, params, &log, false),
        expr => {
            let at = optional_time(params, "time")?
                .or(optional_time(params, "end")?)
                .unwrap_or_else(Utc::now);
            let grid = Grid {
                start_ms: at.timestamp_millis(),
                step_ms: 1000,
                steps: 1,
            };
            let value = logql::evaluate_metric(&state.queries, &state.storage, &expr, grid)?;
            Ok(loki_success(match value {
                MetricValue::Scalar(v) => {
                    json!({"resultType": "scalar", "result": [ts_json(grid.start_ms), prom_value(v[0])]})
                }
                MetricValue::Vector(series) => {
                    let result: Vec<Value> = series
                        .into_iter()
                        .filter_map(|s| {
                            let v = s.vals[0]?;
                            Some(json!({"metric": s.labels, "value": [ts_json(grid.start_ms), prom_value(v)]}))
                        })
                        .collect();
                    json!({"resultType": "vector", "result": result})
                }
            }))
        }
    }
}

pub fn loki_query_range(state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    let expr = logql::parse(required_param(params, "query")?)?;
    if let Expr::Log(log) = &expr {
        return loki_log_query(state, params, log, true);
    }
    let end = optional_time(params, "end")?.unwrap_or_else(Utc::now);
    let start = optional_time(params, "start")?.unwrap_or(end - chrono::Duration::hours(1));
    validate_range(start, end, INTERACTIVE_RANGE_SECS)?;
    let seconds = (end - start).num_seconds();
    // Loki's default step aims for about 250 points.
    let step = match params.get("step") {
        Some(step) => parse_step(step)?,
        None => (seconds / 250).max(1),
    }
    .max(1);
    // Like Loki, evaluate at multiples of the step: the start rounds down and the end rounds up.
    let step_ms = step * 1000;
    let start_ms = start.timestamp_millis().div_euclid(step_ms) * step_ms;
    let end_ms = (end.timestamp_millis() + step_ms - 1).div_euclid(step_ms) * step_ms;
    let steps = (end_ms - start_ms) / step_ms + 1;
    if steps > MAX_RANGE_STEPS {
        return Err(ApiError::new(
            400,
            "bad_data",
            "exceeded maximum resolution of 11,000 points per timeseries; try decreasing the query resolution (?step=XX)",
        ));
    }
    let grid = Grid {
        start_ms,
        step_ms,
        steps: steps as usize,
    };
    let series = match logql::evaluate_metric(&state.queries, &state.storage, &expr, grid)? {
        MetricValue::Scalar(v) => vec![logql::Series { labels: Default::default(), vals: v.into_iter().map(Some).collect() }],
        MetricValue::Vector(series) => series,
    };
    let result: Vec<Value> = series
        .into_iter()
        .filter_map(|s| {
            let values: Vec<Value> = s
                .vals
                .iter()
                .enumerate()
                .filter_map(|(i, v)| Some(json!([ts_json(grid.ts(i)), prom_value((*v)?)])))
                .collect();
            (!values.is_empty()).then(|| json!({"metric": s.labels, "values": values}))
        })
        .collect();
    Ok(loki_success(json!({"resultType": "matrix", "result": result})))
}

pub fn loki_labels(state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    let (from, to) = optional_range(params, INTERACTIVE_RANGE_SECS)?;
    let names = state
        .metadata
        .loki_label_names(&state.queries, &state.storage, from, to)?;
    Ok(loki_success(json!(names)))
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
/// Answers a log query (a selector with a pipeline) with streams.
fn loki_log_query(
    state: &AppState,
    params: &HashMap<String, String>,
    log: &logql::ast::LogExpr,
    range: bool,
) -> ApiResult<Value> {
    let end = optional_time(params, "end")?
        .or(if range { None } else { optional_time(params, "time")? })
        .unwrap_or_else(Utc::now);
    let start = optional_time(params, "start")?.unwrap_or(end - chrono::Duration::hours(1));
    validate_range(start, end, INTERACTIVE_RANGE_SECS)?;
    let limit = parse_usize(params.get("limit"), 100, 1000)?;
    let forward = params.get("direction").is_some_and(|d| d.eq_ignore_ascii_case("forward"));
    let to = if range { end } else { end + chrono::Duration::seconds(1) };
    let streams = logql::log_streams(&state.queries, &state.storage, log, start, to, limit, forward)?;
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
    Ok(loki_success(json!({"resultType": "streams", "result": result})))
}

fn loki_success(data: Value) -> Value {
    json!({"status": "success", "data": data})
}
