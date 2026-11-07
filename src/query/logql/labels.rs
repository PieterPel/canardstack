//! Labels of a stored log line: every resource attribute, every log attribute, the severity text
//! and the trace and span ids, with names escaped to `[a-zA-Z0-9_]`. Loki would index only a
//! promoted subset of resource attributes and keep the rest as structured metadata; here nothing
//! is indexed, so all of them are selectable, and identical label sets form one stream.

use crate::query::prom_labels::{attribute_labels, Labels};

/// The columns [`labels_from_row`] reads, in order.
pub const ROW_COLUMNS: &str = "service_name, service_namespace, service_instance_id, resource_attributes, log_attributes, severity_text, trace_id, span_id";

pub fn labels_from_row(cols: [Option<&str>; 8]) -> Labels {
    let [service_name, service_namespace, service_instance_id, resource, log, severity, trace_id, span_id] = cols;
    // Log attributes first so a resource attribute of the same name wins the stream label.
    let mut labels = attribute_labels(log);
    labels.extend(attribute_labels(resource));
    let mut put = |name: &str, value: Option<&str>| {
        if let Some(value) = value.filter(|v| !v.is_empty()) {
            labels.insert(name.to_string(), value.to_string());
        }
    };
    put("service_name", service_name);
    put("service_namespace", service_namespace);
    put("service_instance_id", service_instance_id);
    put("severity_text", severity);
    put("trace_id", trace_id);
    put("span_id", span_id);
    labels
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_and_log_attributes_become_labels() {
        let labels = labels_from_row([
            Some("api"),
            None,
            Some("i-1"),
            Some(r#"{"job":"varlogs","k8s.pod.name":"p"}"#),
            Some(r#"{"job":"shadowed","user":"u"}"#),
            Some("INFO"),
            Some(""),
            None,
        ]);
        assert_eq!(labels["service_name"], "api");
        assert_eq!(labels["job"], "varlogs"); // the resource attribute wins
        assert_eq!(labels["k8s_pod_name"], "p");
        assert_eq!(labels["user"], "u");
        assert_eq!(labels["severity_text"], "INFO");
        assert!(!labels.contains_key("trace_id"));
    }
}
