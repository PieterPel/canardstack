use anyhow::{bail, Context, Result};
use canardstack::{http, AppState, Config};
use reqwest::Client;
use serde_json::{json, Value};
use std::fs::{self, File};
use std::net::TcpListener as StdListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::sync::CancellationToken;

const TOKEN: &str = "external-writer-smoke-token";

struct Process {
    child: Child,
    log: PathBuf,
}

impl Process {
    fn spawn(mut command: Command, log: PathBuf) -> Result<Self> {
        let output = File::create(&log)?;
        let child = command
            .stdout(output.try_clone()?)
            .stderr(output)
            .stdin(Stdio::null())
            .spawn()?;
        Ok(Self { child, log })
    }

    async fn ready(&mut self, port: u16) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(45);
        while Instant::now() < deadline {
            if let Some(status) = self.child.try_wait()? {
                bail!(
                    "external process exited {status}: {}",
                    fs::read_to_string(&self.log)?
                );
            }
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                return Ok(());
            }
            sleep(Duration::from_millis(100)).await;
        }
        bail!(
            "listener {port} not ready: {}",
            fs::read_to_string(&self.log)?
        )
    }

    fn stop(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            Command::new("kill")
                .args(["-TERM", &self.child.id().to_string()])
                .status()?;
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while std::time::Instant::now() < deadline {
                if let Some(status) = self.child.try_wait()? {
                    anyhow::ensure!(status.success(), "external process shutdown: {status}");
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(25));
            }
            bail!(
                "external process did not stop: {}",
                fs::read_to_string(&self.log)?
            );
        }
        Ok(())
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Reader {
    state: Arc<AppState>,
    url: String,
    stop: CancellationToken,
    task: JoinHandle<Result<()>>,
}

impl Reader {
    async fn open(config: Config) -> Result<Self> {
        let state = tokio::task::spawn_blocking(move || {
            let state = AppState::new(config)?;
            state.storage.with_conn(|conn, _| {
                let version: String = conn.query_row("SELECT version()", [], |row| row.get(0))?;
                eprintln!("reader DuckDB {version}");
                let mut stmt = conn.prepare("SELECT extension_name, extension_version FROM duckdb_extensions() WHERE loaded AND extension_name IN ('ducklake', 'quack') ORDER BY extension_name")?;
                for row in stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))? {
                    eprintln!("reader extension {:?}", row?);
                }
                Ok(())
            })?;
            Ok::<_, anyhow::Error>(Arc::new(state))
        }).await??;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let stop = CancellationToken::new();
        let task = tokio::spawn(http::serve_listener(
            listener,
            state.clone(),
            stop.clone().cancelled_owned(),
        ));
        Ok(Self {
            state,
            url,
            stop,
            task,
        })
    }

    async fn close(self) -> Result<()> {
        self.stop.cancel();
        timeout(Duration::from_secs(5), self.task).await???;
        drop(self.state);
        Ok(())
    }
}

struct Writer {
    binary: PathBuf,
    root: PathBuf,
    init_sql: PathBuf,
    port: u16,
}

impl Writer {
    fn command(&self) -> Command {
        let mut command = Command::new(&self.binary);
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.root.join("home"))
            .env("DUCKDB_OTLP_TOKEN", TOKEN)
            .env("DUCKDB_OTLP_HOST", "127.0.0.1");
        command
    }

    async fn start(&self) -> Result<Process> {
        let mut command = self.command();
        command
            .args([
                "serve",
                "--mode",
                "none",
                "--host",
                "127.0.0.1",
                "--grpc",
                "0",
                "--http",
                &self.port.to_string(),
                "--catalog",
                "canardlake",
                "--init-sql",
            ])
            .arg(&self.init_sql)
            .arg("--database")
            .arg(self.root.join("writer.duckdb"))
            .env("DUCKDB_OTLP_SEAL_MAX_AGE_MS", "250");
        let mut process = Process::spawn(command, self.root.join("writer.log"))?;
        process.ready(self.port).await?;
        Ok(process)
    }

    async fn sql(&self, sql: &str) -> Result<()> {
        let mut command = self.command();
        command
            .args([
                "query",
                "--mode",
                "none",
                "--catalog",
                "canardlake",
                "--init-sql",
            ])
            .arg(&self.init_sql)
            .arg("--database")
            .arg(self.root.join("inspect.duckdb"))
            .arg(sql);
        let output = tokio::task::spawn_blocking(move || command.output()).await??;
        anyhow::ensure!(
            output.status.success(),
            "writer inspection failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        eprintln!("{}", String::from_utf8_lossy(&output.stdout));
        Ok(())
    }
}

fn free_port() -> Result<u16> {
    Ok(StdListener::bind("127.0.0.1:0")?.local_addr()?.port())
}

async fn post(client: &Client, port: u16, signal: &str, body: Value) -> Result<()> {
    let response = client
        .post(format!("http://127.0.0.1:{port}/v1/{signal}"))
        .bearer_auth(TOKEN)
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    anyhow::ensure!(status.as_u16() == 202, "OTLP {signal}: {status}: {body}");
    Ok(())
}

async fn get(client: &Client, url: &str, params: &[(&str, String)]) -> Result<Value> {
    let response = client
        .get(url)
        .bearer_auth(TOKEN)
        .header("accept", "application/json")
        .query(params)
        .send()
        .await?;
    Ok(response.json().await?)
}

fn metric_value(payload: &Value) -> Option<f64> {
    payload["data"]["result"][0]["value"][1]
        .as_str()?
        .parse()
        .ok()
}

async fn verify(
    client: &Client,
    reader: &Reader,
    service: &str,
    trace_id: &str,
    round: u32,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        let logs_url = format!("{}/loki/api/v1/query_range", reader.url);
        let metric_url = format!("{}/api/v1/query", reader.url);
        let trace_url = format!("{}/api/v2/traces/{trace_id}", reader.url);
        let logs_params = [
            ("query", format!("{{service_name=\"{service}\"}}")),
            ("limit", "100".to_owned()),
        ];
        let max_gauge = [(
            "query",
            format!("max(compat_smoke_gauge{{service_name=\"{service}\"}})"),
        )];
        let max_sum = [(
            "query",
            format!("max(compat_smoke.sum{{service_name=\"{service}\"}})"),
        )];
        // Metrics are checked by their latest value: `max()` over the newest sample per
        // series (PromQL instant semantics) only reaches 10 + round / 100 + round once that
        // round's data is visible. `count()` would be the number of series, not samples.
        let (logs, gauge_max, sum_max, trace) = tokio::try_join!(
            get(client, &logs_url, &logs_params),
            get(client, &metric_url, &max_gauge),
            get(client, &metric_url, &max_sum),
            get(client, &trace_url, &[]),
        )?;
        let mut bodies = logs["data"]["result"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|stream| stream["values"].as_array().into_iter().flatten())
            .filter_map(|row| row[1].as_str().map(str::to_owned))
            .collect::<Vec<_>>();
        bodies.sort();
        let mut expected = (1..=round)
            .map(|round| format!("round-{round}"))
            .collect::<Vec<_>>();
        expected.sort();
        if bodies == expected
            && metric_value(&gauge_max) == Some(10.0 + f64::from(round))
            && metric_value(&sum_max) == Some(100.0 + f64::from(round))
            && trace["batches"][0]["instrumentationLibrarySpans"][0]["spans"][0]["name"]
                == format!("round-{round}")
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("round {round} not readable: logs={logs}, gauge_max={gauge_max}, sum_max={sum_max}, trace={trace}");
        }
        sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
#[ignore = "requires DUCKDB_OTLP_BIN pointing at the pinned native duckdb-otlp release"]
async fn external_writer_survives_schema_changes_flush_and_restarts() -> Result<()> {
    let binary = std::env::var_os("DUCKDB_OTLP_BIN")
        .map(PathBuf::from)
        .context("set DUCKDB_OTLP_BIN")?;
    let root = tempfile::tempdir()?;
    fs::create_dir_all(root.path().join("home"))?;
    fs::create_dir_all(root.path().join("data"))?;
    let writer = Writer {
        binary,
        root: root.path().to_owned(),
        init_sql: root.path().join("init.sql"),
        port: free_port()?,
    };
    let catalog_port = free_port()?;
    fs::write(&writer.init_sql, format!(
        "INSTALL ducklake; LOAD ducklake; INSTALL quack; LOAD quack; CREATE SECRET q (TYPE quack, SCOPE 'quack:127.0.0.1:{catalog_port}', TOKEN '{TOKEN}'); ATTACH 'ducklake:quack:127.0.0.1:{catalog_port}' AS canardlake (DATA_PATH '{}'); USE canardlake;",
        root.path().join("data").display().to_string().replace('\'', "''")
    ))?;
    let mut command = writer.command();
    command
        .args([
            "serve",
            "--mode",
            "none",
            "--host",
            "127.0.0.1",
            "--grpc",
            "0",
            "--http",
            &free_port()?.to_string(),
            "--quack",
            &catalog_port.to_string(),
            "--quack-token",
            TOKEN,
            "--database",
        ])
        .arg(root.path().join("catalog.duckdb"));
    let mut catalog = Process::spawn(command, root.path().join("catalog.log"))?;
    catalog.ready(catalog_port).await?;
    let mut ingest = writer.start().await?;
    writer.sql("SELECT version() AS writer_duckdb, extension_name, extension_version FROM duckdb_extensions() WHERE loaded AND extension_name IN ('ducklake', 'quack')").await?;
    let mut config = Config::test(root.path().join("reader.duckdb"));
    config.operator.ducklake_attach_uri = Some(format!("ducklake:quack:127.0.0.1:{catalog_port}"));
    config.operator.ducklake_quack_token = Some(TOKEN.to_owned());
    config.operator.ducklake_data_path = Some(root.path().join("data").display().to_string());
    config.operator.duckdb_extension_dir = Some(root.path().join("reader-extensions"));
    config.operator.api_key = TOKEN.to_owned();
    config.operator.query_admission_wait = Duration::from_secs(2);
    let mut reader = Reader::open(config.clone()).await?;
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let nanos = chrono::Utc::now()
        .timestamp_nanos_opt()
        .context("timestamp")?;
    let service = format!("compat-{nanos}");
    for round in 1..=6_u32 {
        // One second apart and in the past (round 6 is ~1 s old), so every round has a distinct
        // sample time and "latest sample" is well defined; the store keeps microsecond precision.
        let sample_ns = nanos - (7 - i64::from(round)) * 1_000_000_000;
        let timestamp = sample_ns.to_string();
        let trace_id = format!("{nanos:030x}{round:02x}");
        let resource =
            json!({"attributes":[{"key":"service.name","value":{"stringValue":service}}]});
        let logs = json!({"resourceLogs":[{"resource":resource,"scopeLogs":[{"logRecords":[{"timeUnixNano":timestamp,"severityNumber":9,"severityText":"INFO","body":{"stringValue":format!("round-{round}")}}]}]}]});
        let metrics = json!({"resourceMetrics":[{"resource":resource,"scopeMetrics":[{"metrics":[
            {"name":"compat_smoke_gauge","gauge":{"dataPoints":[{"timeUnixNano":timestamp,"asDouble":10.0+f64::from(round)}]}},
            {"name":"compat_smoke.sum","sum":{"aggregationTemporality":2,"isMonotonic":true,"dataPoints":[{"timeUnixNano":timestamp,"asDouble":100.0+f64::from(round)}]}}
        ]}]}]});
        let traces = json!({"resourceSpans":[{"resource":resource,"scopeSpans":[{"spans":[{"traceId":trace_id,"spanId":"0123456789abcdef","name":format!("round-{round}"),"startTimeUnixNano":timestamp,"endTimeUnixNano":(sample_ns+1_000_000).to_string(),"kind":1}]}]}]});
        tokio::try_join!(
            post(&client, writer.port, "logs", logs),
            post(&client, writer.port, "metrics", metrics),
            post(&client, writer.port, "traces", traces)
        )?;
        verify(&client, &reader, &service, &trace_id, round).await?;
        match round {
            1 => writer.sql("CREATE TABLE unrelated_catalog_change (id INTEGER); INSERT INTO unrelated_catalog_change VALUES (1); SELECT 1").await?,
            2 => {
                reader.close().await?;
                reader = Reader::open(config.clone()).await?;
            }
            3 => {
                ingest = tokio::task::spawn_blocking(move || { ingest.stop()?; Ok::<_, anyhow::Error>(ingest) }).await??;
                drop(ingest);
                ingest = writer.start().await?;
            }
            4 => writer.sql("CALL ducklake_flush_inlined_data('canardlake'); SELECT 1 AS flushed").await?,
            _ => {},
        }
        verify(&client, &reader, &service, &trace_id, round).await?;
        eprintln!("round {round}: logs, gauges, sums, and spans readable");
    }
    reader.close().await?;
    tokio::task::spawn_blocking(move || {
        ingest.stop()?;
        catalog.stop()
    })
    .await??;
    Ok(())
}
