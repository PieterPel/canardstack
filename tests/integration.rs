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
        assert_eq!(response["data"]["result"][0]["value"][1], "2");
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
        let trickle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(40)).await;
                if writer.write_all(b"x").await.is_err() {
                    break;
                }
            }
        });
        let mut bytes = Vec::new();
        timeout(Duration::from_secs(2), reader.read_to_end(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        trickle.abort();
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
fn prometheus_metric_discovery_falls_back_to_raw_metrics() {
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
        serde_json::json!(["demo.payment.transactions"])
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
            "__name__": "demo.payment.transactions",
            "deployment_environment": "demo",
            "service_name": "payment"
        }])
    );
}
