//! OpenTelemetry metric name to Prometheus metric name, as Prometheus' OTLP receiver does with
//! its default translation (underscore escaping with suffixes): characters outside
//! `[a-zA-Z0-9_:]` become `_`, a known unit adds a suffix (`s` -> `_seconds`), a gauge with unit `1`
//! gets `_ratio`, and a monotonic sum gets `_total`; a histogram is exposed as `<name>_bucket{le}`,
//! `<name>_sum` and `<name>_count`. With suffixes off only the escaping applies.

/// What the stored metric is, as far as naming goes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    Gauge,
    /// A monotonic sum.
    Counter,
    /// An explicit-bucket histogram: the unit suffix applies, then `_bucket`, `_sum` or `_count`.
    Histogram,
}

/// SQL for a stored name after escaping, for narrowing a fetch before the exact comparison.
pub const ESCAPED_NAME_SQL: &str =
    "regexp_replace(regexp_replace(name, '[^a-zA-Z0-9_:]', '_', 'g'), '_+', '_', 'g')";

pub fn escape(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for c in name.chars() {
        let c = if c.is_ascii_alphanumeric() || c == '_' || c == ':' { c } else { '_' };
        if c == '_' && out.ends_with('_') {
            continue;
        }
        out.push(c);
    }
    if out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    out
}

fn unit_word(unit: &str) -> Option<&'static str> {
    Some(match unit {
        "d" => "days",
        "h" => "hours",
        "min" => "minutes",
        "s" => "seconds",
        "ms" => "milliseconds",
        "us" => "microseconds",
        "ns" => "nanoseconds",
        "By" => "bytes",
        "KiBy" => "kibibytes",
        "MiBy" => "mebibytes",
        "GiBy" => "gibibytes",
        "TiBy" => "tebibytes",
        "KBy" => "kilobytes",
        "MBy" => "megabytes",
        "GBy" => "gigabytes",
        "TBy" => "terabytes",
        "m" => "meters",
        "V" => "volts",
        "A" => "amperes",
        "J" => "joules",
        "W" => "watts",
        "g" => "grams",
        "Cel" => "celsius",
        "Hz" => "hertz",
        "%" => "percent",
        _ => return None,
    })
}

fn per_word(unit: &str) -> Option<&'static str> {
    Some(match unit {
        "s" => "second",
        "m" => "minute",
        "h" => "hour",
        "d" => "day",
        "w" => "week",
        "mo" => "month",
        "y" => "year",
        _ => return None,
    })
}

/// The suffix word for a UCUM unit, without annotations like `{requests}`. None when the unit adds
/// nothing (empty, `1`, annotation only, or unknown).
fn unit_suffix(unit: &str) -> Option<String> {
    let unit: String = {
        let mut depth = 0;
        unit.chars()
            .filter(|c| match c {
                '{' => {
                    depth += 1;
                    false
                }
                '}' => {
                    depth -= 1;
                    false
                }
                _ => depth == 0,
            })
            .collect()
    };
    let unit = unit.trim();
    if unit.is_empty() || unit == "1" {
        return None;
    }
    if let Some((num, den)) = unit.split_once('/') {
        let den = per_word(den.trim())?;
        return Some(match (num.trim(), unit_word(num.trim())) {
            ("" | "1", _) => format!("per_{den}"),
            (_, Some(n)) => format!("{n}_per_{den}"),
            _ => return None,
        });
    }
    unit_word(unit).map(str::to_string)
}

pub fn prom_name(name: &str, unit: Option<&str>, kind: Kind, suffixes: bool) -> String {
    let mut out = escape(name);
    if !suffixes {
        return out;
    }
    let unit = unit.unwrap_or("");
    let word = unit_suffix(unit).or_else(|| {
        (kind == Kind::Gauge && unit.trim() == "1").then(|| "ratio".to_string())
    });
    if let Some(word) = word {
        if !out.ends_with(&format!("_{word}")) {
            out.push('_');
            out.push_str(&word);
        }
    }
    if kind == Kind::Counter && !out.ends_with("_total") {
        out.push_str("_total");
    }
    out
}

/// Escaped stored names that could translate to `prom`: it with `_total` and up to three trailing
/// words (a unit such as `seconds_per_meter`) removed. Narrows the SQL fetch only; the exact check
/// is [`prom_name`] on the row.
pub fn stored_name_candidates(prom: &str) -> Vec<String> {
    let mut bases = vec![prom.to_string()];
    if let Some(stripped) = prom.strip_suffix("_total") {
        bases.push(stripped.to_string());
    }
    let mut out = Vec::new();
    for base in bases {
        out.push(base.clone());
        let mut current = base;
        for _ in 0..3 {
            match current.rfind('_') {
                Some(i) if i > 0 => {
                    current.truncate(i);
                    out.push(current.clone());
                }
                _ => break,
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translates_like_the_prometheus_otlp_receiver() {
        let t = |n, u, k| prom_name(n, u, k, true);
        assert_eq!(t("http.server.duration", Some("s"), Kind::Gauge), "http_server_duration_seconds");
        assert_eq!(t("http.server.requests", Some("{request}"), Kind::Counter), "http_server_requests_total");
        assert_eq!(t("process.memory", Some("By"), Kind::Gauge), "process_memory_bytes");
        assert_eq!(t("cpu.utilization", Some("1"), Kind::Gauge), "cpu_utilization_ratio");
        assert_eq!(t("net.rate", Some("By/s"), Kind::Gauge), "net_rate_bytes_per_second");
        assert_eq!(t("already_total", None, Kind::Counter), "already_total");
        assert_eq!(t("latency_seconds", Some("s"), Kind::Gauge), "latency_seconds");
        assert_eq!(t("9lives.count", None, Kind::Gauge), "_9lives_count");
        assert_eq!(prom_name("a.b", Some("s"), Kind::Counter, false), "a_b");
    }

    #[test]
    fn candidates_cover_the_stored_name_behind_a_translated_one() {
        let c = stored_name_candidates("http_server_duration_seconds");
        assert!(c.contains(&"http_server_duration".to_string()));
        assert!(c.contains(&"http_server_duration_seconds".to_string()));
        let c = stored_name_candidates("net_rate_bytes_per_second_total");
        assert!(c.contains(&"net_rate".to_string()));
    }
}

/// The part of a histogram's series name that says which of its series it is.
pub const HISTOGRAM_COMPONENTS: [&str; 3] = ["_bucket", "_sum", "_count"];

/// `le` label text for a bucket bound, as Prometheus prints it.
pub fn le_text(bound: Option<f64>) -> String {
    match bound {
        Some(b) if b.is_finite() => b.to_string(),
        Some(b) if b < 0.0 => "-Inf".to_string(),
        _ => "+Inf".to_string(),
    }
}
