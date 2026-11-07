//! Prometheus labels of a stored metric series, following the OpenTelemetry-to-Prometheus
//! convention: `job` and `instance` come from the resource, every metric attribute is a label
//! (name sanitised to `[a-zA-Z0-9_]`), and a non-empty instrumentation scope becomes
//! `otel_scope_name` / `otel_scope_version`. Other resource attributes are not labels.

use serde_json::Value;
use std::collections::BTreeMap;

pub type Labels = BTreeMap<String, String>;

/// SQL for the `job` label: `service.namespace/service.name`, or just the name.
pub const JOB_SQL: &str = "(CASE WHEN coalesce(service_namespace, '') <> '' THEN service_namespace || '/' || service_name ELSE service_name END)";
/// SQL for the `instance` label.
pub const INSTANCE_SQL: &str = "service_instance_id";
/// Columns `labels_from_row` reads, in order.
pub const SERIES_COLUMNS: &str =
    "service_name, service_namespace, service_instance_id, scope_name, scope_version, metric_attributes";

/// Prometheus label names allow `[a-zA-Z_][a-zA-Z0-9_]*`.
pub fn sanitize_label_name(key: &str) -> String {
    let mut out: String = key
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect();
    if out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    out
}

fn attribute_value(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// Metric attribute labels from the JSON object stored in `metric_attributes`.
pub fn attribute_labels(json: Option<&str>) -> Labels {
    let Some(Value::Object(map)) = json.and_then(|j| serde_json::from_str(j).ok()) else {
        return Labels::new();
    };
    map.iter()
        .filter_map(|(k, v)| Some((sanitize_label_name(k), attribute_value(v)?)))
        .collect()
}

/// Builds the label set from the [`SERIES_COLUMNS`] values of one row (without `__name__`).
pub fn labels_from_row(
    service_name: Option<&str>,
    service_namespace: Option<&str>,
    service_instance_id: Option<&str>,
    scope_name: Option<&str>,
    scope_version: Option<&str>,
    metric_attributes: Option<&str>,
) -> Labels {
    let mut labels = attribute_labels(metric_attributes);
    let non_empty = |v: Option<&str>| v.filter(|s| !s.is_empty()).map(str::to_string);
    if let Some(name) = non_empty(service_name) {
        let job = match non_empty(service_namespace) {
            Some(ns) => format!("{ns}/{name}"),
            None => name,
        };
        labels.insert("job".into(), job);
    }
    if let Some(instance) = non_empty(service_instance_id) {
        labels.insert("instance".into(), instance);
    }
    if let Some(scope) = non_empty(scope_name) {
        labels.insert("otel_scope_name".into(), scope);
    }
    if let Some(version) = non_empty(scope_version) {
        labels.insert("otel_scope_version".into(), version);
    }
    labels
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_and_attributes_become_prometheus_labels() {
        let labels = labels_from_row(
            Some("api"),
            Some("shop"),
            Some("host-1:80"),
            Some(""),
            None,
            Some(r#"{"http.route":"/a","code":200,"ok":true,"gone":null}"#),
        );
        assert_eq!(labels["job"], "shop/api");
        assert_eq!(labels["instance"], "host-1:80");
        assert_eq!(labels["http_route"], "/a");
        assert_eq!(labels["code"], "200");
        assert_eq!(labels["ok"], "true");
        assert!(!labels.contains_key("gone"));
        assert!(!labels.contains_key("otel_scope_name"));
        assert_eq!(sanitize_label_name("9a-b"), "_9a_b");
    }
}
