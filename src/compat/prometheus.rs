use super::params::{
    optional_range, optional_time, parse_step, required_param, required_time, validate_range,
};
use crate::query::promql::{self, Grid, Value as PromValue};
use crate::validation::{ApiError, ApiResult, MULTI_VALUE_SEPARATOR};
use crate::AppState;
use chrono::Utc;
use serde_json::{json, Value};
use std::collections::HashMap;

const METRIC_RANGE_SECS: i64 = 30 * 24 * 60 * 60;
const MAX_RANGE_STEPS: i64 = 11_000;

pub fn prometheus_query(state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    let query = required_param(params, "query")?;
    let at = optional_time(params, "time")?.unwrap_or_else(Utc::now);
    let grid = Grid {
        start_ms: at.timestamp_millis(),
        step_ms: 1000,
        steps: 1,
    };
    let value = promql::evaluate(&state.queries, &state.storage, query, grid)?;
    let result = match value {
        PromValue::Scalar(v) => json!({
            "resultType": "scalar",
            "result": [ts_json(grid.start_ms), prom_value(v[0])]
        }),
        PromValue::Str(s) => json!({
            "resultType": "string",
            "result": [ts_json(grid.start_ms), s]
        }),
        PromValue::Vector(series) => json!({
            "resultType": "vector",
            "result": series.into_iter().filter_map(|s| {
                let v = s.vals[0]?;
                Some(json!({"metric": s.labels, "value": [ts_json(grid.start_ms), prom_value(v)]}))
            }).collect::<Vec<_>>()
        }),
    };
    Ok(prom_success(result))
}

pub fn prometheus_query_range(
    state: &AppState,
    params: &HashMap<String, String>,
) -> ApiResult<Value> {
    let query = required_param(params, "query")?;
    let start = required_time(params, "start")?;
    let end = required_time(params, "end")?;
    validate_range(start, end, METRIC_RANGE_SECS)?;
    let step = parse_step(params.get("step").map(String::as_str).unwrap_or("60"))?;
    let steps = (end - start).num_seconds() / step.max(1) + 1;
    if steps > MAX_RANGE_STEPS {
        return Err(ApiError::new(
            400,
            "bad_data",
            "exceeded maximum resolution of 11,000 points per timeseries; try decreasing the query resolution (?step=XX)",
        ));
    }
    let grid = Grid {
        start_ms: start.timestamp_millis(),
        step_ms: step.max(1) * 1000,
        steps: steps as usize,
    };
    let value = promql::evaluate(&state.queries, &state.storage, query, grid)?;
    let series = match value {
        PromValue::Scalar(v) => vec![promql::Series {
            labels: Default::default(),
            vals: v.into_iter().map(Some).collect(),
        }],
        PromValue::Vector(series) => series,
        PromValue::Str(_) => return Err(ApiError::new(400, "bad_data", "string result in a range query")),
    };
    let result = series
        .into_iter()
        .filter_map(|s| {
            let values = s
                .vals
                .iter()
                .enumerate()
                .filter_map(|(i, v)| Some(json!([ts_json(grid.ts(i)), prom_value((*v)?)])))
                .collect::<Vec<_>>();
            (!values.is_empty()).then(|| json!({"metric": s.labels, "values": values}))
        })
        .collect::<Vec<_>>();
    Ok(prom_success(
        json!({"resultType": "matrix", "result": result}),
    ))
}

pub(super) fn ts_json(ms: i64) -> Value {
    if ms % 1000 == 0 {
        json!(ms / 1000)
    } else {
        json!(ms as f64 / 1000.0)
    }
}

/// The values of a repeated `match[]` parameter.
fn matches(params: &HashMap<String, String>) -> Vec<String> {
    params
        .get("match[]")
        .map(|raw| raw.split(MULTI_VALUE_SEPARATOR).map(str::to_string).collect())
        .unwrap_or_default()
}

pub fn prometheus_labels(state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    let (from, to) = optional_range(params, METRIC_RANGE_SECS)?;
    let names = state
        .metadata
        .prometheus_label_names(&state.queries, &state.storage, &matches(params), from, to)?;
    Ok(prom_success(json!(names)))
}

pub fn prometheus_label_values(
    state: &AppState,
    name: &str,
    params: &HashMap<String, String>,
) -> ApiResult<Value> {
    let (from, to) = optional_range(params, METRIC_RANGE_SECS)?;
    let values =
        state
            .metadata
            .prometheus_label_values(&state.queries, &state.storage, name, &matches(params), from, to)?;
    Ok(prom_success(json!(values)))
}

pub fn prometheus_series(state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    let (from, to) = optional_range(params, METRIC_RANGE_SECS)?;
    let out = state
        .metadata
        .prometheus_series(&state.queries, &state.storage, &matches(params), from, to)?;
    Ok(prom_success(out))
}

pub fn prometheus_metadata(state: &AppState) -> ApiResult<Value> {
    let metadata = state
        .metadata
        .prometheus_metric_metadata(&state.queries, &state.storage)?;
    Ok(prom_success(metadata))
}

pub(super) fn prom_value(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value > 0.0 { "+Inf" } else { "-Inf" }.to_string()
    } else {
        value.to_string()
    }
}

fn prom_success(data: Value) -> Value {
    json!({"status": "success", "data": data})
}
