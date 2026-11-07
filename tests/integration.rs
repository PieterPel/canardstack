use canardstack::{http, AppState, Config};
use serde_json::Value;
use std::collections::HashMap;
use tempfile::tempdir;

#[cfg(unix)]
#[path = "support/external_writer.rs"]
mod external_writer;

mod transport {
    use super::*;
    use canardstack::admission_control::QueryClass;
    use std::net::SocketAddr;
    use std::sync::{mpsc, Arc};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::JoinHandle;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    struct Server {
        _dir: tempfile::TempDir,
        state: Arc<AppState>,
        addr: SocketAddr,
        stop: CancellationToken,
        task: JoinHandle<anyhow::Result<()>>,
    }

    impl Server {
        async fn start(configure: impl FnOnce(&mut Config)) -> Self {
            let dir = tempdir().unwrap();
            let mut config = Config::test(dir.path().join("canardstack.duckdb"));
            config.operator.query_admission_wait = Duration::from_millis(20);
            configure(&mut config);
            let state = Arc::new(
                tokio::task::spawn_blocking(move || AppState::new(config))
                    .await
                    .unwrap()
                    .unwrap(),
            );
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let stop = CancellationToken::new();
            let task = tokio::spawn(http::serve_listener(
                listener,
                state.clone(),
                stop.clone().cancelled_owned(),
            ));
            Self {
                _dir: dir,
                state,
                addr,
                stop,
                task,
            }
        }

        fn url(&self, path: &str) -> String {
            format!("http://{}{path}", self.addr)
        }

        async fn raw(&self, request: &[u8]) -> Vec<u8> {
            let mut stream = TcpStream::connect(self.addr).await.unwrap();
            stream.write_all(request).await.unwrap();
            let mut bytes = Vec::new();
            timeout(Duration::from_secs(3), stream.read_to_end(&mut bytes))
                .await
                .unwrap()
                .unwrap();
            bytes
        }

        async fn shutdown(self) {
            self.stop.cancel();
            timeout(Duration::from_secs(3), self.task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn axum_transport_preserves_query_auth_form_and_error_envelopes() {
        let server = Server::start(|_| {}).await;
        let client = reqwest::Client::new();
        for path in ["/api/v1/query", "/loki/api/v1/query_range"] {
            let response = client.get(server.url(path)).send().await.unwrap();
            assert_eq!(response.status(), 401);
            let error: Value = response.json().await.unwrap();
            assert_eq!(error["status"], "error");
            assert_eq!(error["errorType"], "missing_api_key");
        }
        let response: Value = client
            .post(server.url("/api/v1/query"))
            .bearer_auth("test-key")
            .header("content-type", "application/x-www-form-urlencoded")
            .body("query=1%2B1")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response["status"], "success");
        assert_eq!(response["data"]["resultType"], "scalar");
        assert_eq!(response["data"]["result"][1], "2");
        let response = client
            .get(server.url("/api/admin/health/queries"))
            .bearer_auth("test-key")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 403);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn axum_transport_rejects_ambiguous_lengths_and_invalid_versions() {
        let server = Server::start(|_| {}).await;
        for request in [
            &b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"[..],
            &b"GET /healthz NOTHTTP\r\nHost: localhost\r\nConnection: close\r\n\r\n"[..],
        ] {
            let response = server.raw(request).await;
            assert!(response.starts_with(b"HTTP/1.1 400"), "{}", String::from_utf8_lossy(&response));
        }
        let response = server.raw(b"GET /api/v1/query?query=1 HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer test-key\r\nAuthorization: Bearer wrong\r\nConnection: close\r\n\r\n").await;
        assert!(response.starts_with(b"HTTP/1.1 400"));
        server.shutdown().await;
    }

    #[tokio::test]
    async fn axum_transport_preserves_pipelining_and_chunked_message_boundaries() {
        let server = Server::start(|_| {}).await;
        let response = server.raw(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\nGET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").await;
        assert_eq!(
            String::from_utf8_lossy(&response)
                .matches("HTTP/1.1 200 OK")
                .count(),
            2
        );
        let response = server.raw(b"POST /api/v1/query HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer test-key\r\nTransfer-Encoding: chunked\r\nContent-Length: 0\r\nContent-Type: application/x-www-form-urlencoded\r\nConnection: close\r\n\r\n7\r\nquery=1\r\n0\r\n\r\n").await;
        assert!(
            response.starts_with(b"HTTP/1.1 200"),
            "{}",
            String::from_utf8_lossy(&response)
        );
        assert_eq!(
            String::from_utf8_lossy(&response)
                .matches("HTTP/1.1 200 OK")
                .count(),
            1
        );
        assert!(String::from_utf8_lossy(&response).contains("\"status\":\"success\""));
        server.shutdown().await;
    }

    #[tokio::test]
    async fn axum_transport_bounds_fixed_and_chunked_request_bodies() {
        let server = Server::start(|config| config.operator.max_body_bytes = 8).await;
        for request in [
            &b"POST /api/v1/query HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer test-key\r\nContent-Length: 9\r\nConnection: close\r\n\r\n123456789"[..],
            &b"POST /api/v1/query HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer test-key\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n9\r\n123456789\r\n0\r\n\r\n"[..],
        ] {
            let response = server.raw(request).await;
            assert!(response.starts_with(b"HTTP/1.1 413"), "{}", String::from_utf8_lossy(&response));
            assert!(String::from_utf8_lossy(&response).contains("\"errorType\":\"payload_too_large\""));
        }
        server.shutdown().await;
    }

    #[tokio::test]
    async fn axum_transport_admission_does_not_block_other_routes() {
        let server = Server::start(|_| {}).await;
        let permit = server
            .state
            .admission
            .reserve_query(QueryClass::Cheap, &server.state.metrics)
            .unwrap();
        let client = reqwest::Client::new();
        let response = client
            .get(server.url("/api/v1/query?query=1"))
            .bearer_auth("test-key")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 429);
        assert_eq!(response.headers()["retry-after"], "1");
        assert_eq!(
            response.json::<Value>().await.unwrap()["errorType"],
            "cheap_query_admission_full"
        );
        let response = client
            .get(server.url("/api/status/buildinfo"))
            .bearer_auth("test-key")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        drop(permit);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn axum_transport_retains_permit_until_timed_out_blocking_work_finishes() {
        let server =
            Server::start(|config| config.operator.query_interactive.timeout_secs = 1).await;
        let (held_tx, held_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let storage = server.state.storage.clone();
        let blocker = tokio::task::spawn_blocking(move || {
            storage.with_conn(|_, _| {
                held_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
        });
        held_rx.await.unwrap();
        let client = reqwest::Client::new();
        let response = client
            .get(server.url("/api/v1/label/__name__/values"))
            .bearer_auth("test-key")
            .send()
            .await
            .unwrap();
        let status = response.status();
        let error: Value = response.json().await.unwrap();
        let active = server.state.admission.snapshot().cheap_query_active;
        release_tx.send(()).unwrap();
        blocker.await.unwrap().unwrap();
        timeout(Duration::from_secs(2), async {
            while server.state.admission.snapshot().cheap_query_active != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(status, 503);
        assert_eq!(error["errorType"], "query_timeout");
        assert_eq!(active, 1);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn axum_transport_body_deadline_and_idle_shutdown_are_bounded() {
        let server = Server::start(|config| {
            config.operator.socket_read_timeout = Duration::from_millis(150)
        })
        .await;
        let response = server.raw(b"POST /api/v1/query HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer test-key\r\nContent-Length: 20\r\n\r\nq").await;
        assert!(
            response.starts_with(b"HTTP/1.1 408"),
            "{}",
            String::from_utf8_lossy(&response)
        );
        let _idle = TcpStream::connect(server.addr).await.unwrap();
        server.shutdown().await;
    }

    #[tokio::test]
    async fn axum_transport_absolute_body_deadline_stops_trickled_requests() {
        let server = Server::start(|config| {
            config.operator.socket_read_timeout = Duration::from_millis(250)
        })
        .await;
        let mut stream = TcpStream::connect(server.addr).await.unwrap();
        stream.write_all(b"POST /api/v1/query HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer test-key\r\nContent-Length: 1000\r\n\r\nq").await.unwrap();
        let (mut reader, mut writer) = stream.into_split();
        // Trickle one byte every 40 ms until the server answers, then only read. Writing to a
        // socket the server has already closed makes the OS send a reset that can discard the
        // unread 408, so reading is polled first and trickling stops as soon as data arrives.
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 256];
        let mut answered = false;
        timeout(Duration::from_secs(2), async {
            loop {
                tokio::select! {
                    biased;
                    read = reader.read(&mut chunk) => match read {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            answered = true;
                            bytes.extend_from_slice(&chunk[..n]);
                        }
                    },
                    _ = tokio::time::sleep(Duration::from_millis(40)), if !answered => {
                        let _ = writer.write_all(b"x").await;
                    }
                }
            }
        })
        .await
        .unwrap();
        assert!(
            bytes.starts_with(b"HTTP/1.1 408"),
            "{}",
            String::from_utf8_lossy(&bytes)
        );
        server.shutdown().await;
    }

    #[tokio::test]
    async fn axum_transport_preserves_tempo_binary_and_json_content_negotiation() {
        let server = Server::start(|_| {}).await;
        let client = reqwest::Client::new();
        let path = "/api/v2/traces/0123456789abcdef0123456789abcdef";
        let response = client
            .get(server.url(path))
            .bearer_auth("test-key")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["content-type"], "application/protobuf");
        assert!(!response.bytes().await.unwrap().is_empty());
        let response = client
            .get(server.url(path))
            .bearer_auth("test-key")
            .header("accept", "application/json")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["content-type"], "application/json");
        assert!(response.json::<Value>().await.unwrap()["batches"].is_array());
        server.shutdown().await;
    }

    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn axum_transport_serves_tls_directly_and_preserves_large_bodies() {
        let server = Server::start(|config| {
            config.operator.tls.enabled = true;
            config.operator.tls.mode = canardstack::config::TlsMode::EphemeralSelfSigned;
        })
        .await;
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        let response = client
            .post(format!("https://{}/api/v1/query", server.addr))
            .bearer_auth("test-key")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(format!("query=1&padding={}", "x".repeat(120_000)))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.json::<Value>().await.unwrap()["status"], "success");
        server.shutdown().await;
    }
}

fn app() -> (tempfile::TempDir, AppState) {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.operator.local_storage_dir = dir.path().join("storage");
    (dir, AppState::new(config).unwrap())
}

fn app_with_metric_without_summary() -> (tempfile::TempDir, AppState) {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.operator.local_storage_dir = dir.path().join("storage");
    let state = AppState::new_with_storage_hook_for_tests(config, |storage| {
        storage
            .with_conn(|conn, prefix| {
                conn.execute_batch(&format!(
                    "\
                    INSERT INTO {prefix}otlp_metrics_sum \
                      (time_unix_nano, start_time_unix_nano, name, description, unit, int_value, \
                       double_value, service_name, resource_attributes, metric_attributes, \
                       aggregation_temporality, is_monotonic) \
                    VALUES \
                      (current_timestamp::TIMESTAMP_NS, current_timestamp::TIMESTAMP_NS, \
                       'demo.payment.transactions', 'payment transactions', '1', 7, NULL, \
                       'payment', '{{\"deployment.environment\":\"demo\"}}', '{{}}', 2, true);"
                ))?;
                Ok(())
            })
            .unwrap();
    })
    .unwrap();
    (dir, state)
}

fn api_headers() -> HashMap<String, String> {
    HashMap::from([("authorization".to_string(), "Bearer test-key".to_string())])
}

#[test]
fn healthz_reports_storage_readiness() {
    let (_dir, state) = app();

    let response = http::route(
        "GET",
        "/healthz",
        &HashMap::new(),
        &HashMap::new(),
        &[],
        &state,
    );

    assert_eq!(response.status(), 200, "{}", response.json_body());
    assert_eq!(response.json_body()["status"], "ok");
    assert!(response.json_body()["storage"]["healthy"]
        .as_bool()
        .unwrap());
}

#[test]
fn otlp_ingest_routes_are_not_served() {
    let (_dir, state) = app();

    for path in ["/v1/logs", "/v1/traces", "/v1/metrics"] {
        let response = http::route(
            "POST",
            path,
            &HashMap::new(),
            &api_headers(),
            br#"{}"#,
            &state,
        );
        assert_eq!(response.status(), 404, "{path}: {}", response.json_body());
    }
}

#[test]
fn buildinfo_query_route_still_works() {
    let (_dir, state) = app();

    let response = http::route(
        "GET",
        "/api/status/buildinfo",
        &HashMap::new(),
        &api_headers(),
        &[],
        &state,
    );

    assert_eq!(response.status(), 200, "{}", response.json_body());
    assert!(matches!(response.json_body(), Value::Object(_)));
    assert_eq!(response.json_body()["revision"], "canardstack");
}

#[test]
fn prometheus_discovery_reads_the_raw_metric_tables() {
    let (_dir, state) = app_with_metric_without_summary();
    let params = HashMap::new();

    let labels = http::route(
        "GET",
        "/api/v1/label/__name__/values",
        &params,
        &api_headers(),
        &[],
        &state,
    );
    assert_eq!(labels.status(), 200, "{}", labels.json_body());
    assert_eq!(
        labels.json_body()["data"],
        serde_json::json!(["demo_payment_transactions_total"])
    );

    let series = http::route(
        "GET",
        "/api/v1/series",
        &params,
        &api_headers(),
        &[],
        &state,
    );
    assert_eq!(series.status(), 200, "{}", series.json_body());
    assert_eq!(
        series.json_body()["data"],
        serde_json::json!([{
            "__name__": "demo_payment_transactions_total",
            "job": "payment"
        }])
    );
}

/// Two gauge series sampled at t0, t0+1s, t0+2s (t0 = 2026-01-01T00:00:00Z):
/// `a` = 1, 9, 2 and `b` = 5, 4, 4. Latest values are a=2, b=4.
fn app_with_gauge_series() -> (tempfile::TempDir, AppState) {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.operator.local_storage_dir = dir.path().join("storage");
    let state = AppState::new_with_storage_hook_for_tests(config, |storage| {
        storage
            .with_conn(|conn, prefix| {
                let rows = [("a", [1.0, 9.0, 2.0]), ("b", [5.0, 4.0, 4.0])]
                    .iter()
                    .flat_map(|(service, values)| {
                        values.iter().enumerate().map(move |(i, value)| {
                            format!(
                                "(TIMESTAMP_NS '2026-01-01 00:00:0{i}', 'sem_gauge', {value}, \
                                 '{service}', '{{}}', '{{}}')"
                            )
                        })
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                conn.execute_batch(&format!(
                    "INSERT INTO {prefix}otlp_metrics_gauge \
                     (time_unix_nano, name, double_value, service_name, resource_attributes, \
                      metric_attributes) VALUES {rows};"
                ))?;
                Ok(())
            })
            .unwrap();
    })
    .unwrap();
    (dir, state)
}

/// Four log lines for service `a` at t0..t0+3 (2026-01-01T00:00:0N Z).
fn app_with_log_lines() -> (tempfile::TempDir, AppState) {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.operator.local_storage_dir = dir.path().join("storage");
    let state = AppState::new_with_storage_hook_for_tests(config, |storage| {
        storage
            .with_conn(|conn, prefix| {
                conn.execute_batch(&format!(
                    "INSERT INTO {prefix}otlp_logs (time_unix_nano, body, service_name, \
                     resource_attributes, log_attributes) VALUES \
                     (TIMESTAMP_NS '2026-01-01 00:00:00', 'event 0 error', 'a', '{{}}', '{{}}'), \
                     (TIMESTAMP_NS '2026-01-01 00:00:01', 'event 1', 'a', '{{}}', '{{}}'), \
                     (TIMESTAMP_NS '2026-01-01 00:00:02', 'event 2 warn', 'a', '{{}}', '{{}}'), \
                     (TIMESTAMP_NS '2026-01-01 00:00:03', 'event 3', 'a', '{{}}', '{{}}');"
                ))?;
                Ok(())
            })
            .unwrap();
    })
    .unwrap();
    (dir, state)
}

fn loki_lines(state: &AppState, query: &str) -> Vec<String> {
    let body = prom_get(
        state,
        "/loki/api/v1/query_range",
        &[
            ("query", query),
            ("start", "1767225599"),
            ("end", "1767225610"),
        ],
    );
    body["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|stream| stream["values"].as_array().unwrap().iter())
        .map(|v| v[1].as_str().unwrap().to_string())
        .collect()
}

#[test]
fn loki_line_filters_chain_negate_and_format() {
    let (_dir, state) = app_with_log_lines();
    let mut lines = loki_lines(&state, r#"{service_name="a"} |= "event" != "error""#);
    lines.sort();
    assert_eq!(lines, ["event 1", "event 2 warn", "event 3"]);
    let lines = loki_lines(&state, r#"{service_name="a"} !~ "error|warn" != "3""#);
    assert_eq!(lines, ["event 1"]);
    let lines = loki_lines(&state, r#"{service_name="a"} |= "warn" | line_format "L""#);
    assert_eq!(lines, ["L"]);
    // Literal and case-sensitive: no AND/OR operators, no parenthesis restriction.
    assert!(loki_lines(&state, r#"{service_name="a"} |= "WARN""#).is_empty());
    assert!(loki_lines(&state, r#"{service_name="a"} |= "event 1 AND event 2""#).is_empty());
    assert!(loki_lines(&state, r#"{service_name="a"} |= "(x)""#).is_empty());
    assert_eq!(loki_lines(&state, r#"{service_name="a"} != "EVENT""#).len(), 4);
}

/// `mem` samples with arbitrary metric attributes, as a scraped Prometheus metric would have.
fn app_with_attribute_labels() -> (tempfile::TempDir, AppState) {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.operator.local_storage_dir = dir.path().join("storage");
    let state = AppState::new_with_storage_hook_for_tests(config, |storage| {
        storage
            .with_conn(|conn, prefix| {
                conn.execute_batch(&format!(
                    "INSERT INTO {prefix}otlp_metrics_gauge \
                     (time_unix_nano, name, double_value, service_name, service_instance_id, \
                      resource_attributes, metric_attributes) VALUES \
                     (TIMESTAMP_NS '2026-01-01 00:00:00', 'mem', 10, 'demo', 'h1:80', '{{}}', '{{\"type\":\"free\",\"http.route\":\"/a\"}}'), \
                     (TIMESTAMP_NS '2026-01-01 00:00:00', 'mem', 30, 'demo', 'h1:80', '{{}}', '{{\"type\":\"used\"}}'), \
                     (TIMESTAMP_NS '2026-01-01 00:00:00', 'mem', 50, 'demo', 'h2:80', '{{}}', '{{\"type\":\"free\"}}');"
                ))?;
                Ok(())
            })
            .unwrap();
    })
    .unwrap();
    (dir, state)
}

#[test]
fn prometheus_metric_attributes_are_labels() {
    let (_dir, state) = app_with_attribute_labels();
    let query = |q: &str| {
        prom_get(&state, "/api/v1/query", &[("query", q), ("time", "1767225600")])["data"]["result"].clone()
    };
    let free = query(r#"mem{type="free"}"#);
    assert_eq!(free.as_array().unwrap().len(), 2, "{free}");
    let first = free.as_array().unwrap().iter().find(|s| s["metric"]["instance"] == "h1:80").unwrap();
    assert_eq!(first["metric"]["__name__"], "mem");
    assert_eq!(first["metric"]["job"], "demo");
    assert_eq!(first["metric"]["type"], "free");
    assert_eq!(first["metric"]["http_route"], "/a");
    let by_type = query("sum by (type) (mem)");
    let sums: HashMap<String, String> = by_type
        .as_array()
        .unwrap()
        .iter()
        .map(|s| (s["metric"]["type"].as_str().unwrap().to_string(), s["value"][1].as_str().unwrap().to_string()))
        .collect();
    assert_eq!(sums, HashMap::from([("free".into(), "60".into()), ("used".into(), "30".into())]));
    assert_eq!(query(r#"mem{instance="h2:80"}"#).as_array().unwrap().len(), 1);
    assert_eq!(query(r#"mem{type=~"fr.*", http_route!=""}"#).as_array().unwrap().len(), 1);

    let window = [("start", "1767225500"), ("end", "1767225700")];
    let names = prom_get(&state, "/api/v1/labels", &window);
    assert_eq!(names["data"], serde_json::json!(["__name__", "http_route", "instance", "job", "type"]));
    let types = prom_get(&state, "/api/v1/label/type/values", &window);
    assert_eq!(types["data"], serde_json::json!(["free", "used"]));
    let routes = prom_get(&state, "/api/v1/label/http_route/values", &window);
    assert_eq!(routes["data"], serde_json::json!(["/a"]));
}

/// One cumulative histogram series, `http.server.duration` in seconds with bounds 0.1, 0.5 and 1,
/// scraped twice ten seconds apart.
fn app_with_histogram() -> (tempfile::TempDir, AppState) {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.operator.local_storage_dir = dir.path().join("storage");
    let state = AppState::new_with_storage_hook_for_tests(config, |storage| {
        storage
            .with_conn(|conn, prefix| {
                conn.execute_batch(&format!(
                    "INSERT INTO {prefix}otlp_metrics_histogram \
                     (time_unix_nano, start_time_unix_nano, name, unit, count, sum, bucket_counts, explicit_bounds, \
                      service_name, service_instance_id, resource_attributes, metric_attributes, aggregation_temporality) VALUES \
                     (TIMESTAMP_NS '2026-01-01 00:00:00', TIMESTAMP_NS '2025-12-31 23:59:00', 'http.server.duration', 's', 10, 12.5, \
                      [1, 2, 3, 4], [0.1, 0.5, 1.0], 'web', 'h1', '{{}}', '{{\"route\":\"/a\"}}', 2), \
                     (TIMESTAMP_NS '2026-01-01 00:00:10', TIMESTAMP_NS '2025-12-31 23:59:00', 'http.server.duration', 's', 20, 30.0, \
                      [2, 4, 6, 8], [0.1, 0.5, 1.0], 'web', 'h1', '{{}}', '{{\"route\":\"/a\"}}', 2);"
                ))?;
                Ok(())
            })
            .unwrap();
    })
    .unwrap();
    (dir, state)
}

#[test]
fn promql_reads_explicit_histograms_as_classic_bucket_sum_and_count_series() {
    let (_dir, state) = app_with_histogram();
    let at = |q: &str, t: &str| prom_get(&state, "/api/v1/query", &[("query", q), ("time", t)])["data"]["result"].clone();
    // 2026-01-01T00:00:00Z is 1767225600; the first scrape.
    let buckets = at("http_server_duration_seconds_bucket", "1767225600");
    let mut by_le: Vec<(String, String)> = buckets
        .as_array()
        .unwrap()
        .iter()
        .map(|s| (s["metric"]["le"].as_str().unwrap().to_string(), s["value"][1].as_str().unwrap().to_string()))
        .collect();
    by_le.sort_by(|a, b| a.0.cmp(&b.0));
    // Bucket counts are cumulative and the last bucket is `+Inf`.
    assert_eq!(
        by_le,
        [("+Inf", "10"), ("0.1", "1"), ("0.5", "3"), ("1", "6")].map(|(a, b)| (a.to_string(), b.to_string()))
    );
    let first = &buckets[0]["metric"];
    assert_eq!(first["__name__"], "http_server_duration_seconds_bucket");
    assert_eq!(first["job"], "web");
    assert_eq!(first["instance"], "h1");
    assert_eq!(first["route"], "/a");
    assert_eq!(at("http_server_duration_seconds_count", "1767225600")[0]["value"][1], "10");
    assert_eq!(at("http_server_duration_seconds_sum", "1767225600")[0]["value"][1], "12.5");
    assert_eq!(at(r#"http_server_duration_seconds_bucket{le="0.5"}"#, "1767225610")[0]["value"][1], "6");
    // The quantile interpolates inside the bucket: rank 5 of 10 lies between 0.5 (3) and 1 (6).
    let q = at("histogram_quantile(0.5, http_server_duration_seconds_bucket)", "1767225600");
    let value: f64 = q[0]["value"][1].as_str().unwrap().parse().unwrap();
    assert!((value - (0.5 + 0.5 * 2.0 / 3.0)).abs() < 1e-9, "{q}");
    // Over the two scrapes, 10 more observations arrived, spread like the first ten.
    let rate = at("histogram_quantile(0.5, sum by (le) (rate(http_server_duration_seconds_bucket[1m])))", "1767225610");
    let value: f64 = rate[0]["value"][1].as_str().unwrap().parse().unwrap();
    assert!((value - (0.5 + 0.5 * 2.0 / 3.0)).abs() < 1e-9, "{rate}");
    // A selector that cannot be a histogram series does not see them.
    assert_eq!(at("http_server_duration_seconds", "1767225600").as_array().unwrap().len(), 0);

    // Discovery lists the three kinds of series, the `le` bounds and the histogram's type.
    let window = [("start", "1767225500"), ("end", "1767225700")];
    let names = prom_get(&state, "/api/v1/label/__name__/values", &window)["data"].clone();
    assert_eq!(
        names,
        serde_json::json!(["http_server_duration_seconds_bucket", "http_server_duration_seconds_count", "http_server_duration_seconds_sum"])
    );
    let les = prom_get(&state, "/api/v1/label/le/values", &window)["data"].clone();
    assert_eq!(les, serde_json::json!(["+Inf", "0.1", "0.5", "1"]));
    assert!(prom_get(&state, "/api/v1/labels", &window)["data"].as_array().unwrap().iter().any(|l| l == "le"));
    let series = prom_get(&state, "/api/v1/series", &[("start", "1767225500"), ("end", "1767225700"), ("match[]", "http_server_duration_seconds_sum")])["data"].clone();
    assert_eq!(series.as_array().unwrap().len(), 1, "{series}");
    let metadata = prom_get(&state, "/api/v1/metadata", &[])["data"].clone();
    assert_eq!(metadata["http_server_duration_seconds"][0]["type"], "histogram");
}

/// One native (exponential) histogram series `lat` in seconds, scale 0, scraped twice ten seconds
/// apart. First scrape: 1 in the zero bucket, 2 in (1, 2], 3 in (2, 4]; second: 2, 4 and 6.
fn app_with_native_histogram() -> (tempfile::TempDir, AppState) {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.operator.local_storage_dir = dir.path().join("storage");
    let state = AppState::new_with_storage_hook_for_tests(config, |storage| {
        storage
            .with_conn(|conn, prefix| {
                conn.execute_batch(&format!(
                    "INSERT INTO {prefix}otlp_metrics_exp_histogram \
                     (time_unix_nano, start_time_unix_nano, name, unit, count, sum, scale, zero_count, zero_threshold, \
                      positive_offset, positive_bucket_counts, negative_offset, negative_bucket_counts, \
                      service_name, service_instance_id, resource_attributes, metric_attributes, aggregation_temporality) VALUES \
                     (TIMESTAMP_NS '2026-01-01 00:00:00', TIMESTAMP_NS '2025-12-31 23:59:00', 'lat', 's', 6, 10.0, 0, 1, 0.0, \
                      0, [2, 3], 0, [], 'web', 'h1', '{{}}', '{{\"route\":\"/a\"}}', 2), \
                     (TIMESTAMP_NS '2026-01-01 00:00:10', TIMESTAMP_NS '2025-12-31 23:59:00', 'lat', 's', 12, 20.0, 0, 2, 0.0, \
                      0, [4, 6], 0, [], 'web', 'h1', '{{}}', '{{\"route\":\"/a\"}}', 2);"
                ))?;
                Ok(())
            })
            .unwrap();
    })
    .unwrap();
    (dir, state)
}

#[test]
fn promql_evaluates_native_histograms() {
    let (_dir, state) = app_with_native_histogram();
    let at = |q: &str, t: &str| prom_get(&state, "/api/v1/query", &[("query", q), ("time", t)])["data"]["result"].clone();
    let float = |q: &str, t: &str| -> f64 { at(q, t)[0]["value"][1].as_str().unwrap_or_else(|| panic!("no float for {q}: {}", at(q, t))).parse().unwrap() };
    // 2026-01-01T00:00:00Z = 1767225600. The sample is a histogram with its non-empty buckets.
    let sample = at("lat_seconds", "1767225600");
    assert_eq!(sample[0]["metric"]["__name__"], "lat_seconds");
    assert_eq!(sample[0]["metric"]["job"], "web");
    assert_eq!(sample[0]["metric"]["route"], "/a");
    let h = &sample[0]["histogram"][1];
    assert_eq!((h["count"].as_str(), h["sum"].as_str()), (Some("6"), Some("10")));
    // `1e-128` is how Prometheus prints the default zero threshold.
    assert_eq!(
        h["buckets"],
        serde_json::json!([[3, "-1e-128", "1e-128", "1"], [0, "1", "2", "2"], [0, "2", "4", "3"]])
    );
    assert_eq!(float("histogram_count(lat_seconds)", "1767225600"), 6.0);
    assert_eq!(float("histogram_sum(lat_seconds)", "1767225610"), 20.0);
    assert_eq!(float("histogram_avg(lat_seconds)", "1767225610"), 20.0 / 12.0);
    // 6 more observations in 10 s, extrapolated to the start of the 1 m window like a float rate:
    // (6 * (10 + 5) / 10) / 60.
    let rate = float("histogram_count(rate(lat_seconds[1m]))", "1767225610");
    assert!((rate - 6.0 * 1.5 / 60.0).abs() < 1e-12, "{rate}");
    assert_eq!(float("histogram_quantile(0.5, lat_seconds)", "1767225600"), 2.0);
    assert!(at("histogram_quantile(0.5, lat_seconds)", "1767225600")[0]["metric"].get("__name__").is_none());
    // Aggregation sums histograms; count counts series; arithmetic scales them and rejects floats.
    assert_eq!(at("sum(lat_seconds)", "1767225610")[0]["histogram"][1]["count"], "12");
    assert_eq!(float("count(lat_seconds)", "1767225610"), 1.0);
    assert_eq!(at("lat_seconds * 2", "1767225610")[0]["histogram"][1]["count"], "24");
    assert_eq!(at("lat_seconds / 4", "1767225610")[0]["histogram"][1]["sum"], "5");
    assert_eq!(at("lat_seconds + 1", "1767225610"), serde_json::json!([]));
    assert_eq!(at("lat_seconds == lat_seconds", "1767225610").as_array().unwrap().len(), 1);
    assert_eq!(at("lat_seconds != lat_seconds", "1767225610"), serde_json::json!([]));
    assert_eq!(at("absent_over_time(lat_seconds[1m])", "1767225610"), serde_json::json!([]));
    assert_eq!(at("absent_over_time(lat_seconds[1m])", "1767226610").as_array().unwrap().len(), 1);
    // A range query returns the histograms under `histograms`.
    let range = prom_get(
        &state,
        "/api/v1/query_range",
        &[("query", "lat_seconds"), ("start", "1767225600"), ("end", "1767225610"), ("step", "10")],
    )["data"]["result"]
        .clone();
    assert!(range[0].get("values").is_none());
    assert_eq!(range[0]["histograms"].as_array().unwrap().len(), 2);
    // A counter reset is detected from the buckets: the third scrape is lower than the second.
    state
        .storage
        .with_conn(|conn, prefix| {
            conn.execute_batch(&format!(
                "INSERT INTO {prefix}otlp_metrics_exp_histogram \
                 (time_unix_nano, name, unit, count, sum, scale, zero_count, zero_threshold, positive_offset, \
                  positive_bucket_counts, negative_offset, negative_bucket_counts, service_name, service_instance_id, \
                  resource_attributes, metric_attributes) VALUES \
                 (TIMESTAMP_NS '2026-01-01 00:00:20', 'lat', 's', 3, 4.0, 0, 0, 0.0, 0, [1, 2], 0, [], 'web', 'h1', '{{}}', '{{\"route\":\"/a\"}}');"
            ))?;
            Ok(())
        })
        .unwrap();
    assert_eq!(float("resets(lat_seconds[1m])", "1767225620"), 1.0);
}

#[test]
fn prometheus_discovery_endpoints_honour_match_selectors() {
    let (_dir, state) = app_with_attribute_labels();
    fn window(m: &str) -> Vec<(&str, &str)> {
        vec![("start", "1767225500"), ("end", "1767225700"), ("match[]", m)]
    }
    let get = |path: &str, m: &str| prom_get(&state, path, &window(m))["data"].clone();
    // One selector narrows the label values and the series.
    assert_eq!(get("/api/v1/label/type/values", r#"mem{instance="h2:80"}"#), serde_json::json!(["free"]));
    assert_eq!(get("/api/v1/label/instance/values", r#"{type="used"}"#), serde_json::json!(["h1:80"]));
    assert_eq!(get("/api/v1/series", r#"mem{type=~"fr.*"}"#).as_array().unwrap().len(), 2);
    // Label names only list the labels of the selected series.
    assert_eq!(get("/api/v1/labels", r#"mem{type="used"}"#), serde_json::json!(["__name__", "instance", "job", "type"]));
    // Several selectors select the union (a repeated `match[]` arrives joined).
    let both = format!("{}\u{1f}{}", r#"mem{type="used"}"#, r#"mem{instance="h2:80"}"#);
    assert_eq!(get("/api/v1/label/type/values", &both), serde_json::json!(["free", "used"]));
    assert_eq!(get("/api/v1/label/type/values", r#"nothing"#), serde_json::json!([]));
    // A selector that is not a series selector is rejected.
    let params: HashMap<String, String> = window("rate(mem[1m])").into_iter().map(|(k, v)| (k.into(), v.into())).collect();
    let response = http::route("GET", "/api/v1/series", &params, &api_headers(), &[], &state);
    assert_eq!(response.status(), 400, "{}", response.json_body());
}

#[test]
fn promql_calendar_regression_and_bucket_functions() {
    let (_dir, state) = app_with_gauge_series();
    // sem_gauge{job="a"} is 1, 9, 2 at 2026-01-01T00:00:00..02Z; evaluate at :02.
    let value = |q: &str| -> Option<String> {
        let body = prom_get(&state, "/api/v1/query", &[("query", q), ("time", "1767225602")]);
        body["data"]["result"][0]["value"][1].as_str().map(str::to_string)
    };
    assert_eq!(value(r#"timestamp(sem_gauge{job="a"})"#).as_deref(), Some("1767225602"));
    assert_eq!(value("year(vector(1767225602))").as_deref(), Some("2026"));
    assert_eq!(value("month(vector(1767225602))").as_deref(), Some("1"));
    assert_eq!(value("day_of_week(vector(1767225602))").as_deref(), Some("4")); // a Thursday
    assert_eq!(value("days_in_month(vector(1767225602))").as_deref(), Some("31"));
    assert_eq!(value("hour(vector(1767225602 + 3600))").as_deref(), Some("1"));
    assert_eq!(value(r#"absent_over_time(sem_gauge{job="a"}[1m])"#), None);
    assert_eq!(value(r#"absent_over_time(sem_gauge{job="nobody"}[1m])"#).as_deref(), Some("1"));
    // Points (0,1), (1,9), (2,2): slope (2-1)/2 = 0.5 per second.
    // The fitted line is 4.5 at :02 with slope 0.5, so two seconds later it is 5.5.
    assert_eq!(value(r#"deriv(sem_gauge{job="a"}[10s])"#).as_deref(), Some("0.5"));
    assert_eq!(value(r#"predict_linear(sem_gauge{job="a"}[10s], 2)"#).as_deref(), Some("5.5"));
    // The subquery evaluates at :01 and :02 (multiples of 1s inside (:00, :02]): values 9 and 2.
    assert_eq!(value(r#"max_over_time(sem_gauge{job="a"}[2s:1s])"#).as_deref(), Some("9"));
    assert_eq!(value(r#"min_over_time(sem_gauge{job="a"}[2s:1s] offset 1s)"#).as_deref(), Some("1"));
    let counted = prom_get(
        &state,
        "/api/v1/query",
        &[("query", r#"count_values("v", sem_gauge)"#), ("time", "1767225602")],
    );
    let by_value: HashMap<String, String> = counted["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| (s["metric"]["v"].as_str().unwrap().into(), s["value"][1].as_str().unwrap().into()))
        .collect();
    assert_eq!(by_value, HashMap::from([("2".into(), "1".into()), ("4".into(), "1".into())]));
    // Inverted clamp bounds give an empty result, as in Prometheus.
    assert_eq!(value(r#"clamp(sem_gauge{job="a"}, 5, 1)"#), None);
}

#[test]
fn prometheus_metric_names_follow_the_otel_translation() {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.operator.local_storage_dir = dir.path().join("storage");
    let state = AppState::new_with_storage_hook_for_tests(config, |storage| {
        storage
            .with_conn(|conn, prefix| {
                conn.execute_batch(&format!(
                    "INSERT INTO {prefix}otlp_metrics_gauge (time_unix_nano, name, unit, description, double_value, \
                      service_name, resource_attributes, metric_attributes) VALUES \
                      (TIMESTAMP_NS '2026-01-01 00:00:00', 'http.server.duration', 's', 'request time', 0.5, 'api', '{{}}', '{{}}'); \
                     INSERT INTO {prefix}otlp_metrics_sum (time_unix_nano, start_time_unix_nano, name, unit, description, \
                      double_value, service_name, resource_attributes, metric_attributes, aggregation_temporality, is_monotonic) VALUES \
                      (TIMESTAMP_NS '2026-01-01 00:00:00', TIMESTAMP_NS '2026-01-01 00:00:00', 'http.requests', '{{request}}', 'requests', 7, 'api', '{{}}', '{{}}', 2, true);"
                ))?;
                Ok(())
            })
            .unwrap();
    })
    .unwrap();
    let count = |q: &str| {
        prom_get(&state, "/api/v1/query", &[("query", q), ("time", "1767225600")])["data"]["result"]
            .as_array()
            .unwrap()
            .len()
    };
    assert_eq!(count("http_server_duration_seconds"), 1);
    assert_eq!(count("http_requests_total"), 1);
    assert_eq!(count("sum(http_requests_total)"), 1);
    // The stored names and the untranslated forms are not Prometheus names.
    assert_eq!(count("http.server.duration"), 0);
    assert_eq!(count("http_server_duration"), 0);
    assert_eq!(count("http_requests"), 0);
    assert_eq!(count(r#"{__name__=~"http_.*"}"#), 2);

    let window = [("start", "1767225500"), ("end", "1767225700")];
    let names = prom_get(&state, "/api/v1/label/__name__/values", &window);
    assert_eq!(names["data"], serde_json::json!(["http_requests_total", "http_server_duration_seconds"]));
    let metadata = prom_get(&state, "/api/v1/metadata", &[]);
    assert_eq!(metadata["data"]["http_requests_total"][0]["type"], "counter");
    assert_eq!(metadata["data"]["http_server_duration_seconds"][0]["unit"], "s");
}

#[test]
fn loki_pipelines_select_on_resource_attributes_and_parse_json() {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.operator.local_storage_dir = dir.path().join("storage");
    let state = AppState::new_with_storage_hook_for_tests(config, |storage| {
        storage
            .with_conn(|conn, prefix| {
                conn.execute_batch(&format!(
                    "INSERT INTO {prefix}otlp_logs (time_unix_nano, body, resource_attributes, log_attributes) VALUES \
                     (TIMESTAMP_NS '2026-01-01 00:00:00', '{{\"method\":\"GET\",\"status\":200,\"took\":\"5ms\"}}', '{{\"job\":\"varlogs\",\"filename\":\"/a.log\"}}', '{{}}'), \
                     (TIMESTAMP_NS '2026-01-01 00:00:01', '{{\"method\":\"POST\",\"status\":503,\"took\":\"2s\"}}', '{{\"job\":\"varlogs\",\"filename\":\"/a.log\"}}', '{{}}'), \
                     (TIMESTAMP_NS '2026-01-01 00:00:02', 'plain text, not json', '{{\"job\":\"varlogs\",\"filename\":\"/b.log\"}}', '{{\"user\":\"bob\"}}');"
                ))?;
                Ok(())
            })
            .unwrap();
    })
    .unwrap();
    let streams = |query: &str| -> Vec<(Value, Vec<String>)> {
        let body = prom_get(
            &state,
            "/loki/api/v1/query_range",
            &[("query", query), ("start", "1767225599"), ("end", "1767225610"), ("direction", "forward")],
        );
        body["data"]["result"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| {
                let lines = s["values"].as_array().unwrap().iter().map(|v| v[1].as_str().unwrap().to_string()).collect();
                (s["stream"].clone(), lines)
            })
            .collect()
    };
    // Any resource attribute is a stream label; log attributes join them.
    let all = streams(r#"{job="varlogs"}"#);
    assert_eq!(all.len(), 2, "{all:?}"); // /a.log's two lines share one stream
    assert!(all.iter().any(|(l, _)| l["filename"] == "/b.log" && l["user"] == "bob"));
    assert_eq!(streams(r#"{job=~"var.*", filename!="/b.log"}"#).len(), 1);
    assert_eq!(streams(r#"{filename="/a.log"} | json"#).len(), 2); // parsed labels split the stream
    // json adds parsed labels, so each distinct status becomes its own stream.
    let parsed = streams(r#"{filename="/a.log"} | json | status >= 500 and took > 1s"#);
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].0["method"], "POST");
    assert_eq!(parsed[0].0["status"], "503");
    assert!(parsed[0].1[0].contains("503"));
    // A line that is not JSON keeps flowing with an error label unless it is filtered on.
    let errors = streams(r#"{filename="/b.log"} | json"#);
    assert_eq!(errors[0].0["__error__"], "JSONParserErr");
    assert!(streams(r#"{filename="/b.log"} | json | __error__ = """#).is_empty());

    // Discovery reads the same labels.
    let window = [("start", "1767225500"), ("end", "1767225700")];
    let names = prom_get(&state, "/loki/api/v1/labels", &window);
    assert_eq!(names["data"], serde_json::json!(["filename", "job", "user"]));
    let files = prom_get(&state, "/loki/api/v1/label/filename/values", &window);
    assert_eq!(files["data"], serde_json::json!(["/a.log", "/b.log"]));
    let series = prom_get(&state, "/loki/api/v1/series", &window);
    assert_eq!(series["data"].as_array().unwrap().len(), 2);
}

#[test]
fn loki_label_values_fall_back_to_raw_logs_when_the_summary_is_empty() {
    let (_dir, state) = app_with_log_lines();
    let body = prom_get(
        &state,
        "/loki/api/v1/label/service_name/values",
        &[("start", "1767225500"), ("end", "1767225700")],
    );
    assert_eq!(body["data"], serde_json::json!(["a"]), "{body}");
}

#[test]
fn loki_metric_queries_count_rate_and_aggregate() {
    let (_dir, state) = app_with_log_lines();
    let value = |query: &str| {
        let body = prom_get(
            &state,
            "/loki/api/v1/query",
            &[("query", query), ("time", "1767225603")],
        );
        assert_eq!(body["data"]["resultType"], "vector", "{body}");
        body["data"]["result"][0]["value"][1]
            .as_str()
            .map(str::to_string)
    };
    assert_eq!(value(r#"sum(count_over_time({service_name="a"}[10m]))"#).as_deref(), Some("4"));
    assert_eq!(
        value(r#"sum by (service_name) (count_over_time({service_name="a"} != "error" [10m]))"#).as_deref(),
        Some("3")
    );
    assert_eq!(value(r#"sum(rate({service_name="a"}[10m]))"#).as_deref(), Some(&*(4.0f64 / 600.0).to_string()));
    // "event 0 error" + "event 1" + "event 2 warn" + "event 3"
    assert_eq!(value(r#"sum(bytes_over_time({service_name="a"}[10m]))"#).as_deref(), Some("39"));
    assert_eq!(value(r#"count_over_time({service_name="nobody"}[10m])"#), None);

    let body = prom_get(
        &state,
        "/loki/api/v1/query_range",
        &[
            ("query", r#"sum(count_over_time({service_name="a"}[2s]))"#),
            ("start", "1767225600"),
            ("end", "1767225603"),
            ("step", "1"),
        ],
    );
    let values: Vec<String> = body["data"]["result"][0]["values"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v[1].as_str().unwrap().to_string())
        .collect();
    // windows (t-2s, t]: t0 -> 1 line, t1 -> 2, t2 -> 2, t3 -> 2
    assert_eq!(values, ["1", "2", "2", "2"], "{body}");
}

#[test]
fn tempo_search_applies_traceql_duration_filters() {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.operator.local_storage_dir = dir.path().join("storage");
    let state = AppState::new_with_storage_hook_for_tests(config, |storage| {
        storage
            .with_conn(|conn, prefix| {
                // Two single-span traces: 100 ms and 3 s.
                conn.execute_batch(&format!(
                    "INSERT INTO {prefix}otlp_traces (start_time_unix_nano, duration_time_unix_nano, \
                     trace_id, span_id, name, service_name, resource_attributes, span_attributes) VALUES \
                     (TIMESTAMP_NS '2026-01-01 00:00:00', 100000000, 'aa', '01', 'fast', 'a', '{{}}', '{{}}'), \
                     (TIMESTAMP_NS '2026-01-01 00:00:01', 3000000000, 'bb', '02', 'slow', 'a', '{{}}', '{{}}');"
                ))?;
                Ok(())
            })
            .unwrap();
    })
    .unwrap();
    let count = |q: &str| {
        let body = prom_get(
            &state,
            "/api/search",
            &[("q", q), ("start", "1767225500"), ("end", "1767225700")],
        );
        body["traces"].as_array().unwrap().len()
    };
    assert_eq!(count("{ duration > 1s }"), 1);
    assert_eq!(count("{ duration < 1s }"), 1);
    assert_eq!(count("{ duration > 1h }"), 0);
    assert_eq!(count("{ duration >= 100ms }"), 2);
}

/// Two traces: `t1` is root -> mid -> leaf (the leaf fails), `t2` is a lone span 10 seconds later.
fn app_with_traces() -> (tempfile::TempDir, AppState) {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.operator.local_storage_dir = dir.path().join("storage");
    let state = AppState::new_with_storage_hook_for_tests(config, |storage| {
        storage
            .with_conn(|conn, prefix| {
                conn.execute_batch(&format!(
                    "INSERT INTO {prefix}otlp_traces (start_time_unix_nano, duration_time_unix_nano, \
                     trace_id, span_id, parent_span_id, name, kind, status_code, service_name, \
                     resource_attributes, span_attributes) VALUES \
                     (TIMESTAMP_NS '2026-01-01 00:00:01', 3000000000, 't1', 'a1', '', 'root', 2, 0, 'web', '{{}}', '{{\"n\":1}}'), \
                     (TIMESTAMP_NS '2026-01-01 00:00:02', 1000000000, 't1', 'a2', 'a1', 'mid', 1, 0, 'web', '{{}}', '{{\"n\":2}}'), \
                     (TIMESTAMP_NS '2026-01-01 00:00:03', 100000000, 't1', 'a3', 'a2', 'leaf', 3, 2, 'db', '{{}}', '{{\"n\":3}}'), \
                     (TIMESTAMP_NS '2026-01-01 00:00:12', 50000000, 't2', 'b1', '', 'solo', 2, 0, 'web', '{{}}', '{{}}');"
                ))?;
                Ok(())
            })
            .unwrap();
    })
    .unwrap();
    (dir, state)
}

#[test]
fn traceql_search_evaluates_structure_and_trace_intrinsics() {
    let (_dir, state) = app_with_traces();
    let search = |q: &str| {
        prom_get(&state, "/api/search", &[("q", q), ("start", "1767225500"), ("end", "1767225700"), ("spss", "10")])["traces"].clone()
    };
    let spans = |q: &str| -> Vec<String> {
        let traces = search(q);
        let mut ids: Vec<String> = traces
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|t| t["spanSets"].as_array().unwrap().iter().flat_map(|s| s["spans"].as_array().unwrap().iter().map(|x| x["spanID"].as_str().unwrap().to_string())))
            .collect();
        ids.sort();
        ids
    };
    assert_eq!(spans("{ status = error }"), ["a3"]);
    assert_eq!(spans(r#"{ name = "root" } >> { status = error }"#), ["a3"]);
    assert_eq!(spans(r#"{ name = "leaf" } << { name = "root" }"#), ["a1"]);
    assert_eq!(spans(r#"{ name = "mid" } > { }"#), ["a3"]);
    assert_eq!(spans("{ } | count() > 2"), ["a1", "a2", "a3"]);
    assert_eq!(spans("{ trace:rootName = \"solo\" }"), ["b1"]);
    assert_eq!(spans("{ .n >= 2 } | select(.n)"), ["a2", "a3"]);
    let traces = search("{ trace:duration >= 3s }");
    assert_eq!(traces.as_array().unwrap().len(), 1);
    assert_eq!(traces[0]["rootTraceName"], "root");
    assert_eq!(traces[0]["rootServiceName"], "web");
    // Tempo prints the trace id without leading zeros, and the duration in whole milliseconds.
    assert_eq!(traces[0]["durationMs"], 3000);
}

#[test]
fn traceql_metrics_bucket_spans_into_steps_and_compare_selections() {
    let (_dir, state) = app_with_traces();
    let query = |q: &str| {
        prom_get(&state, "/api/metrics/query_range", &[("q", q), ("start", "1767225600"), ("end", "1767225630"), ("step", "10s")])["series"].clone()
    };
    // Steps are labelled with their end; the first is the aligned start itself.
    let counts = |q: &str| -> Vec<(String, f64)> {
        query(q)[0]["samples"].as_array().unwrap().iter().map(|s| (s["timestampMs"].as_str().unwrap().to_string(), s["value"].as_f64().unwrap())).collect()
    };
    assert_eq!(
        counts("{ } | count_over_time()"),
        [("1767225600000".to_string(), 0.0), ("1767225610000".to_string(), 3.0), ("1767225620000".to_string(), 1.0), ("1767225630000".to_string(), 0.0)]
    );
    let by_service = query("{ } | count_over_time() by (resource.service.name)");
    assert_eq!(by_service.as_array().unwrap().len(), 2, "{by_service}");
    let max = query("{ } | max_over_time(duration)");
    assert_eq!(max[0]["samples"][0]["value"], 3.0);
    // compare(): the failing span is the selection, everything else the baseline.
    let compare = query("{ } | compare({ status = error })");
    let value_of = |meta: &str, attr: &str, value: &str| -> Option<f64> {
        compare.as_array().unwrap().iter().find_map(|s| {
            let labels = s["labels"].as_array().unwrap();
            let has = |k: &str, v: &str| labels.iter().any(|l| l["key"] == k && l["value"].to_string().contains(v));
            (has("__meta_type", meta) && has(attr, value)).then(|| s["samples"].as_array().unwrap().iter().map(|x| x["value"].as_f64().unwrap()).sum())
        })
    };
    assert_eq!(value_of("selection", "name", "leaf"), Some(1.0));
    assert_eq!(value_of("baseline", "name", "root"), Some(1.0));
    assert_eq!(value_of("baseline", "name", "leaf"), None);
    assert_eq!(value_of("baseline_total", "name", "nil"), Some(3.0));
    assert_eq!(value_of("selection", "status", "error"), Some(1.0));
}

fn prom_get(state: &AppState, path: &str, query: &[(&str, &str)]) -> Value {
    let params = query
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<HashMap<_, _>>();
    let response = http::route("GET", path, &params, &api_headers(), &[], state);
    assert_eq!(response.status(), 200, "{}", response.json_body());
    response.json_body()
}

fn instant_value(state: &AppState, query: &str) -> String {
    // 2026-01-01T00:00:02Z
    let body = prom_get(
        state,
        "/api/v1/query",
        &[("query", query), ("time", "1767225602")],
    );
    body["data"]["result"][0]["value"][1]
        .as_str()
        .unwrap_or_else(|| panic!("no value for {query}: {body}"))
        .to_string()
}

#[test]
fn prometheus_instant_query_uses_latest_sample_per_series() {
    let (_dir, state) = app_with_gauge_series();
    // Latest per series first (a=2, b=4), then aggregate across series.
    assert_eq!(instant_value(&state, "max(sem_gauge)"), "4");
    assert_eq!(instant_value(&state, "min(sem_gauge)"), "2");
    assert_eq!(instant_value(&state, "sum(sem_gauge)"), "6");
    // count() is the number of series, not the number of samples.
    assert_eq!(instant_value(&state, "count(sem_gauge)"), "2");
    // A bare selector returns the newest sample, not an average over the window.
    assert_eq!(instant_value(&state, r#"sem_gauge{job="a"}"#), "2");
    assert_eq!(
        instant_value(&state, r#"max(sem_gauge{job="a"})"#),
        "2"
    );
}

#[test]
fn prometheus_range_query_includes_the_end_timestamp() {
    let (_dir, state) = app_with_gauge_series();
    let body = prom_get(
        &state,
        "/api/v1/query_range",
        &[
            ("query", r#"sem_gauge{job="a"}"#),
            ("start", "1767225600"),
            ("end", "1767225602"),
            ("step", "1"),
        ],
    );
    let values = body["data"]["result"][0]["values"].as_array().unwrap();
    let points: Vec<(i64, &str)> = values
        .iter()
        .map(|v| (v[0].as_i64().unwrap(), v[1].as_str().unwrap()))
        .collect();
    assert_eq!(
        points,
        vec![(1767225600, "1"), (1767225601, "9"), (1767225602, "2")],
        "the sample exactly at `end` must be returned"
    );
}

#[test]
fn prometheus_instant_query_falls_back_to_sum_and_returns_empty_for_no_match() {
    let (_dir, state) = app_with_metric_without_summary();
    // No gauge exists, so this resolves through the sum fallback (latest value 7).
    let body = prom_get(
        &state,
        "/api/v1/query",
        &[("query", "max(demo_payment_transactions_total)")],
    );
    assert_eq!(body["data"]["result"][0]["value"][1], "7", "{body}");

    // A selector that matches nothing is an empty vector, not a storage error.
    let body = prom_get(
        &state,
        "/api/v1/query",
        &[(
            "query",
            r#"max(demo_payment_transactions_total{job="nobody"})"#,
        )],
    );
    assert_eq!(body["data"]["result"], serde_json::json!([]), "{body}");
    let body = prom_get(&state, "/api/v1/query", &[("query", "max(no_such_metric)")]);
    assert_eq!(body["data"]["result"], serde_json::json!([]), "{body}");
}

#[test]
fn prometheus_range_query_returns_the_sample_at_each_step_not_a_bucket_aggregate() {
    let (_dir, state) = app_with_gauge_series();
    // Series `a` is 1, 9, 2 at t0, t0+1, t0+2. With step=2 the steps are t0 and t0+2, and each
    // reports the newest sample at or before it (1 and 2), not the 9 in between or an average.
    let body = prom_get(
        &state,
        "/api/v1/query_range",
        &[
            ("query", r#"sem_gauge{job="a"}"#),
            ("start", "1767225600"),
            ("end", "1767225602"),
            ("step", "2"),
        ],
    );
    let values = body["data"]["result"][0]["values"].as_array().unwrap();
    let points: Vec<(i64, &str)> = values
        .iter()
        .map(|v| (v[0].as_i64().unwrap(), v[1].as_str().unwrap()))
        .collect();
    assert_eq!(points, vec![(1767225600, "1"), (1767225602, "2")], "{body}");
}

#[test]
fn prometheus_range_query_keeps_one_series_per_service() {
    let (_dir, state) = app_with_gauge_series();
    let body = prom_get(
        &state,
        "/api/v1/query_range",
        &[
            ("query", "sem_gauge"),
            ("start", "1767225600"),
            ("end", "1767225602"),
            ("step", "1"),
        ],
    );
    let series: std::collections::BTreeMap<String, Vec<String>> = body["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| {
            (
                s["metric"]["job"].as_str().unwrap().to_string(),
                s["values"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v[1].as_str().unwrap().to_string())
                    .collect(),
            )
        })
        .collect();
    assert_eq!(series["a"], ["1", "9", "2"], "{body}");
    assert_eq!(series["b"], ["5", "4", "4"], "{body}");
}

#[test]
fn grafana_health_probes_for_loki_and_tempo_are_answered() {
    let (_dir, state) = app();
    // Loki "Save & test" runs a metric query that canardstack otherwise does not implement.
    let loki = prom_get(
        &state,
        "/loki/api/v1/query",
        &[("query", "vector(1)+vector(1)")],
    );
    assert_eq!(loki["data"]["resultType"], "vector", "{loki}");
    assert_eq!(loki["data"]["result"][0]["value"][1], "2", "{loki}");

    // Tempo "Save & test" calls /api/echo and expects the plain-text body `echo`.
    let echo = http::route(
        "GET",
        "/api/echo",
        &HashMap::new(),
        &api_headers(),
        &[],
        &state,
    );
    assert_eq!(echo.status(), 200);
    assert_eq!(echo.text_body(), "echo");
    let unauthorized = http::route(
        "GET",
        "/api/echo",
        &HashMap::new(),
        &HashMap::new(),
        &[],
        &state,
    );
    assert_eq!(unauthorized.status(), 401);
}
