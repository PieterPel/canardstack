//! Per-request latency of the in-process query routes against a local DuckLake catalog.
//!
//! There is no network and no HTTP server here, so this measures canardstack's own cost per
//! request: admission, connection setup, planning, execution, row collection and JSON. Compare
//! it with the end-to-end numbers from `bench/` to see how much of a request is spent outside
//! the Rust code (remote catalog round trips, TLS proxy, the HTTP server).
//!
//!     cargo bench --bench query_engine
//!     ROWS=1000000 ROUNDS=500 cargo bench --bench query_engine

use canardstack::{http, AppState, Config};
use std::collections::HashMap;
use std::time::{Duration, Instant};

const SERVICES: usize = 5;
const T0: i64 = 1_767_225_600; // 2026-01-01T00:00:00Z

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn load(state: &AppState, rows: usize) {
    let seconds = rows.div_ceil(50);
    let traces = rows.min(2000);
    state
        .storage
        .with_conn(|conn, prefix| {
            conn.execute_batch(&format!(
                "INSERT INTO {prefix}otlp_logs \
                   (time_unix_nano, service_name, severity_text, body, resource_attributes, log_attributes) \
                 SELECT (TIMESTAMP '2026-01-01' + to_seconds(i // 50))::TIMESTAMP_NS + to_microseconds(i % 50), \
                        'svc-' || (i % {SERVICES}), \
                        CASE WHEN i % 10 = 0 THEN 'ERROR' ELSE 'INFO' END, \
                        'request handled path=/api/v1/items/' || i || ' error=' || (i % 10 = 0) || ' ' || repeat('x', 120), \
                        '{{}}', '{{}}' \
                 FROM range({rows}) t(i); \
                 INSERT INTO {prefix}otlp_metrics_gauge \
                   (time_unix_nano, name, double_value, service_name, resource_attributes, metric_attributes) \
                 SELECT (TIMESTAMP '2026-01-01' + to_seconds(i % {seconds}))::TIMESTAMP_NS, 'bench_gauge', \
                        (10 * (i % {SERVICES}) + (i % 7))::DOUBLE, 'svc-' || (i % {SERVICES}), '{{}}', '{{}}' \
                 FROM range({rows}) t(i); \
                 INSERT INTO {prefix}otlp_traces \
                   (start_time_unix_nano, duration_time_unix_nano, trace_id, span_id, parent_span_id, name, service_name) \
                 SELECT (TIMESTAMP '2026-01-01' + to_seconds(i))::TIMESTAMP_NS, 50000000, md5(i::VARCHAR), \
                        substr(md5('s' || i), 1, 16), CASE WHEN s = 0 THEN '' ELSE substr(md5('s' || i), 1, 16) END, \
                        'bench.span', 'svc-' || (i % {SERVICES}) \
                 FROM range({traces}) t(i), range(2) u(s);"
            ))?;
            Ok(())
        })
        .expect("load benchmark data");
}

struct Case {
    name: &'static str,
    path: String,
    params: Vec<(&'static str, String)>,
}

fn summarize(mut samples: Vec<Duration>) -> (f64, f64, f64) {
    samples.sort();
    let ms = |d: &Duration| d.as_secs_f64() * 1000.0;
    let at = |q: f64| ms(&samples[((samples.len() - 1) as f64 * q).round() as usize]);
    (at(0.5), at(0.95), ms(samples.last().unwrap()))
}

fn main() {
    let rows = env_usize("ROWS", 100_000);
    let rounds = env_usize("ROUNDS", 300);
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.operator.local_storage_dir = dir.path().join("storage");
    let state = AppState::new(config).expect("open canardstack");
    let started = Instant::now();
    load(&state, rows);
    eprintln!(
        "loaded {rows} rows per signal in {:.1}s",
        started.elapsed().as_secs_f64()
    );

    let end = T0 + rows.div_ceil(50) as i64;
    let trace_id = {
        let hex = state
            .storage
            .with_conn(|conn, prefix| {
                Ok(conn.query_row(
                    &format!("SELECT trace_id FROM {prefix}otlp_traces LIMIT 1"),
                    [],
                    |row| row.get::<_, String>(0),
                )?)
            })
            .expect("trace id");
        hex
    };
    let window = [("start", T0.to_string()), ("end", end.to_string())];
    let cases = vec![
        Case {
            name: "log_lines",
            path: "/loki/api/v1/query_range".into(),
            params: [
                &[
                    ("query", r#"{service_name="svc-1"}"#.to_string()),
                    ("limit", "1000".into()),
                ][..],
                &window,
            ]
            .concat(),
        },
        Case {
            name: "log_error_lines",
            path: "/loki/api/v1/query_range".into(),
            params: [
                &[
                    (
                        "query",
                        r#"{service_name="svc-1"} |= "error=true""#.to_string(),
                    ),
                    ("limit", "1000".into()),
                ][..],
                &window,
            ]
            .concat(),
        },
        Case {
            name: "gauge_last",
            path: "/api/v1/query".into(),
            params: vec![
                ("query", r#"max(bench_gauge{service_name="svc-1"})"#.into()),
                ("time", end.to_string()),
            ],
        },
        Case {
            name: "gauge_points",
            path: "/api/v1/query_range".into(),
            params: vec![
                ("query", r#"bench_gauge{service_name="svc-1"}"#.into()),
                ("start", T0.to_string()),
                ("end", end.to_string()),
                ("step", "1".into()),
            ],
        },
        Case {
            name: "trace_by_id",
            path: format!("/api/traces/{trace_id}"),
            params: vec![],
        },
    ];

    let headers = HashMap::from([
        ("authorization".to_string(), "Bearer test-key".to_string()),
        ("accept".to_string(), "application/json".to_string()),
    ]);
    println!(
        "{:<18} {:>9} {:>9} {:>9}   (ms, {rounds} requests each, {rows} rows per signal)",
        "route", "p50", "p95", "max"
    );
    for case in cases {
        let params: HashMap<String, String> = case
            .params
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        let run = || {
            let started = Instant::now();
            let response = http::route("GET", &case.path, &params, &headers, &[], &state);
            let elapsed = started.elapsed();
            assert_eq!(
                response.status(),
                200,
                "{}: {}",
                case.name,
                response.json_body()
            );
            elapsed
        };
        for _ in 0..20 {
            run();
        }
        let samples = (0..rounds).map(|_| run()).collect::<Vec<_>>();
        let (p50, p95, max) = summarize(samples);
        println!("{:<18} {p50:>9.2} {p95:>9.2} {max:>9.2}", case.name);
    }
}
