use super::params::{
    optional_range, optional_time, parse_step, required_param, required_time, validate_range,
};
use crate::query::native_histogram::Hist;
use crate::query::promql::{self, Grid, Sample, Value as PromValue};
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
        PromValue::Samples(series) => json!({
            "resultType": "vector",
            "result": series.into_iter().filter_map(|s| {
                Some(match s.vals[0].as_ref()? {
                    Sample::F(v) => json!({"metric": s.labels, "value": [ts_json(grid.start_ms), prom_value(*v)]}),
                    Sample::H(h) => json!({"metric": s.labels, "histogram": [ts_json(grid.start_ms), hist_json(h)]}),
                })
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
        PromValue::Samples(series) => {
            let result = series
                .into_iter()
                .filter_map(|s| {
                    let (mut values, mut histograms) = (Vec::new(), Vec::new());
                    for (i, v) in s.vals.iter().enumerate() {
                        match v {
                            Some(Sample::F(x)) => values.push(json!([ts_json(grid.ts(i)), prom_value(*x)])),
                            Some(Sample::H(h)) => histograms.push(json!([ts_json(grid.ts(i)), hist_json(h)])),
                            None => {}
                        }
                    }
                    let mut series = json!({"metric": s.labels});
                    if !values.is_empty() {
                        series["values"] = json!(values);
                    }
                    if !histograms.is_empty() {
                        series["histograms"] = json!(histograms);
                    }
                    (!values.is_empty() || !histograms.is_empty()).then_some(series)
                })
                .collect::<Vec<_>>();
            return Ok(prom_success(json!({"resultType": "matrix", "result": result})));
        }
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

/// A float the way Prometheus' API prints it: plain decimal notation, except for very small and
/// very large magnitudes, which use an exponent.
pub(super) fn prom_value(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    let abs = value.abs();
    if abs != 0.0 && (abs < 1e-6 || abs >= 1e21) {
        // Rust prints `1.5e-7`; Go prints `1.5e-07` (a sign and at least two exponent digits).
        let text = format!("{value:e}");
        let (mantissa, exponent) = text.split_once('e').unwrap_or((&text, "0"));
        let exponent: i32 = exponent.parse().unwrap_or(0);
        return format!("{mantissa}e{}{:02}", if exponent < 0 { '-' } else { '+' }, exponent.abs());
    }
    value.to_string()
}

/// A native histogram sample as Prometheus' API prints it: count, sum and the non-empty buckets as
/// `[boundaries, lower, upper, count]` (0: lower exclusive, upper inclusive; 1: the reverse; 2: both
/// exclusive; 3: both inclusive).
fn hist_json(h: &Hist) -> Value {
    let mut out = json!({"count": prom_value(h.count), "sum": prom_value(h.sum)});
    let buckets: Vec<Value> = h
        .all_buckets()
        .into_iter()
        .filter(|b| b.count != 0.0)
        .map(|b| {
            let boundaries = match (b.lower_inclusive, b.upper_inclusive) {
                (true, true) => 3,
                (true, false) => 1,
                (false, true) => 0,
                (false, false) => 2,
            };
            json!([boundaries, prom_value(b.lower), prom_value(b.upper), prom_value(b.count)])
        })
        .collect();
    if !buckets.is_empty() {
        out["buckets"] = json!(buckets);
    }
    out
}

fn prom_success(data: Value) -> Value {
    json!({"status": "success", "data": data})
}
