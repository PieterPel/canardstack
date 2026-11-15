use crate::validation::{self, ApiError, ApiResult};
use chrono::{DateTime, TimeZone, Utc};
use serde_json::{json, Value};
use std::collections::HashMap;

pub(super) fn required_param<'a>(
    params: &'a HashMap<String, String>,
    name: &'static str,
) -> ApiResult<&'a str> {
    params
        .get(name)
        .map(String::as_str)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| ApiError::new(400, "missing_parameter", format!("{name} is required")))
}

pub(super) fn optional_range(
    params: &HashMap<String, String>,
    max_secs: i64,
) -> ApiResult<(DateTime<Utc>, DateTime<Utc>)> {
    let to = optional_time(params, "end")?
        .or_else(|| optional_time(params, "to").ok().flatten())
        .unwrap_or_else(Utc::now);
    let from = optional_time(params, "start")?
        .or_else(|| optional_time(params, "from").ok().flatten())
        .unwrap_or(to - chrono::Duration::hours(1));
    validate_range(from, to, max_secs)?;
    Ok((from, to))
}

fn invalid_time(name: &str) -> ApiError {
    ApiError::new(
        400,
        "invalid_time_range",
        format!("{name} must be RFC3339, Unix seconds, or Unix nanoseconds"),
    )
}

pub(super) fn required_time(
    params: &HashMap<String, String>,
    name: &'static str,
) -> ApiResult<DateTime<Utc>> {
    let raw = required_param(params, name)?;
    parse_time(raw).ok_or_else(|| invalid_time(name))
}

pub(super) fn optional_time(
    params: &HashMap<String, String>,
    name: &'static str,
) -> ApiResult<Option<DateTime<Utc>>> {
    match params
        .get(name)
        .map(String::as_str)
        .filter(|v| !v.is_empty())
    {
        Some(raw) => parse_time(raw).map(Some).ok_or_else(|| invalid_time(name)),
        None => Ok(None),
    }
}

pub(super) fn parse_time(raw: &str) -> Option<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw) {
        return Some(dt.with_timezone(&Utc));
    }
    if let Ok(value) = raw.parse::<i64>() {
        if value > 10_000_000_000_000 {
            return Some(Utc.timestamp_nanos(value));
        }
        return Utc.timestamp_opt(value, 0).single();
    }
    if let Ok(value) = raw.parse::<f64>() {
        // A fractional epoch is rounded to the millisecond, as Prometheus does. Truncating the
        // fraction is wrong: 1700001499.85 is the float 1700001499.8499999..., so it would land a
        // millisecond early and exclude the sample stored at exactly that instant.
        return value
            .is_finite()
            .then(|| (value * 1000.0).round() as i64)
            .and_then(|millis| Utc.timestamp_millis_opt(millis).single());
    }
    None
}

pub(super) fn parse_any_time_to_utc(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
                .ok()
                .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
        })
        .or_else(|| {
            DateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f%#z")
                .map(|dt| dt.with_timezone(&Utc))
                .ok()
        })
}

pub(super) fn validate_range(
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    max_secs: i64,
) -> ApiResult<()> {
    validation::validate_range(from, to, max_secs)
}

pub(super) fn parse_step(raw: &str) -> ApiResult<i64> {
    raw.strip_suffix('s')
        .unwrap_or(raw)
        .parse::<i64>()
        .map_err(|_| {
            ApiError::new(
                400,
                "invalid_step",
                "step must be seconds or a duration ending in s",
            )
        })
}

pub(super) fn parse_usize(value: Option<&String>, default: usize, max: usize) -> ApiResult<usize> {
    let parsed = value
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default);
    validation::parse_limit(Some(&json!(parsed)), default, max)
}

pub(super) fn result_rows(result: &Value) -> Vec<Value> {
    result
        .get("rows")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn millis(raw: &str) -> Option<i64> {
        parse_time(raw).map(|t| t.timestamp_millis())
    }

    #[test]
    fn a_fractional_epoch_keeps_its_milliseconds() {
        // These floats sit just below their decimal value; truncation would drop a millisecond.
        assert_eq!(millis("1700001499.85"), Some(1_700_001_499_850));
        assert_eq!(millis("1700001499.97"), Some(1_700_001_499_970));
        assert_eq!(millis("1700001500.001"), Some(1_700_001_500_001));
        assert_eq!(millis("1767225600.850"), Some(1_767_225_600_850));
    }

    #[test]
    fn whole_seconds_and_nanoseconds_are_unchanged() {
        assert_eq!(millis("1700001499"), Some(1_700_001_499_000));
        assert_eq!(
            parse_time("1700001499850000000").map(|t| t.timestamp_nanos_opt()),
            Some(Some(1_700_001_499_850_000_000))
        );
        assert_eq!(millis("2026-01-01T00:00:00.850Z"), Some(1_767_225_600_850));
    }

    #[test]
    fn what_is_not_a_time_is_refused() {
        assert_eq!(millis("NaN"), None);
        assert_eq!(millis("inf"), None);
        assert_eq!(millis("soon"), None);
    }
}
