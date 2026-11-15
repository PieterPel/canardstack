use super::compat_routes::match_compat_route;
use super::parser::query_params;
use super::response::HttpResponse;
use super::router::route_admitted;
use crate::metrics::MetricName;
use crate::storage::QueryExecution;
use crate::validation::{self, ApiError};
use crate::AppState;
use axum::body::{to_bytes, Body};
use axum::extract::State;
use axum::http::{header, Request, Response};
use axum::response::IntoResponse;
use axum::Router;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::{timeout, timeout_at, Instant};
use tokio_io_timeout::TimeoutStream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_REQUEST_TARGET_BYTES: usize = 8 * 1024;
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_HEADER_COUNT: usize = 100;

#[derive(Clone)]
struct Transport {
    app: Arc<AppState>,
    operators: Arc<Semaphore>,
    stopping: CancellationToken,
    jobs: TaskTracker,
}

pub async fn serve(state: Arc<AppState>) -> anyhow::Result<()> {
    serve_until(state, std::future::pending()).await
}

pub async fn serve_until(
    state: Arc<AppState>,
    shutdown: impl Future<Output = ()>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&state.config.operator.bind).await?;
    serve_listener(listener, state, shutdown).await
}

pub async fn serve_listener(
    listener: TcpListener,
    state: Arc<AppState>,
    shutdown: impl Future<Output = ()>,
) -> anyhow::Result<()> {
    #[cfg(feature = "tls")]
    let tls = if state.config.operator.tls.enabled {
        Some(tokio_rustls::TlsAcceptor::from(crate::tls::server_config(
            &state.config.operator.tls,
        )?))
    } else {
        None
    };
    #[cfg(not(feature = "tls"))]
    if state.config.operator.tls.enabled {
        anyhow::bail!("CANARDSTACK_TLS_ENABLED=true requires --features tls");
    }

    let transport = Transport {
        app: state.clone(),
        operators: Arc::new(Semaphore::new(1)),
        stopping: CancellationToken::new(),
        jobs: TaskTracker::new(),
    };
    let router = Router::new()
        .fallback(dispatch)
        .with_state(transport.clone());
    let connections = Arc::new(Semaphore::new(
        state.config.operator.max_concurrent_connections,
    ));
    let mut tasks = JoinSet::new();
    let read_timeout = state.config.operator.socket_read_timeout;
    let write_timeout = state.config.operator.socket_write_timeout;
    tracing::info!(event = "server_listening", addr = %listener.local_addr()?);
    tokio::pin!(shutdown);
    let result = loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break Ok(()),
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(err) = result {
                    tracing::warn!(event = "http_connection_task_failed", error = %err);
                }
            }
            accepted = listener.accept() => {
                let (socket, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(err) => break Err(err.into()),
                };
                let permit = match connections.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        state.metrics.inc(MetricName::HttpConnectionErrorsTotal, &[("reason", "max_connections_exceeded")], 1);
                        drop(socket);
                        continue;
                    }
                };
                if let Err(err) = socket.set_nodelay(true) {
                    tracing::debug!(event = "socket_configuration_failed", error = %err);
                    continue;
                }
                let router = router.clone();
                let stopping = transport.stopping.clone();
                #[cfg(feature = "tls")]
                let tls = tls.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    #[cfg(feature = "tls")]
                    if let Some(tls) = tls {
                        let stream = tokio::select! {
                            _ = stopping.cancelled() => return,
                            result = timeout(read_timeout, tls.accept(socket)) => match result {
                                Ok(Ok(stream)) => stream,
                                _ => return,
                            }
                        };
                        connection(stream, router, stopping, read_timeout, write_timeout).await;
                        return;
                    }
                    connection(socket, router, stopping, read_timeout, write_timeout).await;
                });
            }
        }
    };
    drop(listener);
    transport.stopping.cancel();
    let drain_deadline = Instant::now() + SHUTDOWN_DRAIN_TIMEOUT;
    if timeout_at(drain_deadline, async {
        while tasks.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        tracing::warn!(event = "shutdown_drain_timeout");
    }
    transport.jobs.close();
    if timeout_at(drain_deadline, transport.jobs.wait())
        .await
        .is_err()
    {
        tracing::warn!(event = "blocking_query_drain_timeout");
    }
    result
}

async fn connection<I>(
    stream: I,
    router: Router,
    stopping: CancellationToken,
    read_timeout: Duration,
    write_timeout: Duration,
) where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut stream = TimeoutStream::new(stream);
    stream.set_read_timeout(Some(read_timeout));
    stream.set_write_timeout(Some(write_timeout));
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(read_timeout)
        .max_headers(MAX_HEADER_COUNT)
        .max_buf_size(MAX_HEADER_BYTES);
    let connection = builder.serve_connection(
        TokioIo::new(Box::pin(stream)),
        TowerToHyperService::new(router),
    );
    tokio::pin!(connection);
    let result = tokio::select! {
        result = &mut connection => result,
        _ = stopping.cancelled() => {
            connection.as_mut().graceful_shutdown();
            connection.await
        }
    };
    if let Err(err) = result {
        tracing::debug!(event = "http_connection_failed", error = %err);
    }
}

struct CancelOnDrop(Arc<QueryExecution>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

async fn dispatch(State(transport): State<Transport>, request: Request<Body>) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let method = parts.method.as_str().to_owned();
    let path = parts.uri.path().to_owned();
    let fail = |err| request_error(&method, &path, err);
    if transport.stopping.is_cancelled() {
        return fail(ApiError::new(
            503,
            "server_stopping",
            "server is shutting down",
        ));
    }
    if parts.uri.to_string().len() > MAX_REQUEST_TARGET_BYTES
        || parts
            .headers
            .iter()
            .map(|(name, value)| name.as_str().len() + value.len() + 4)
            .sum::<usize>()
            > MAX_HEADER_BYTES
    {
        return fail(ApiError::new(
            400,
            "headers_too_large",
            "request headers exceed configured limits",
        ));
    }
    for name in [
        header::AUTHORIZATION,
        header::HeaderName::from_static("x-api-key"),
    ] {
        if parts.headers.get_all(name).iter().count() > 1 {
            return fail(ApiError::new(
                400,
                "duplicate_header",
                "duplicate authentication header",
            ));
        }
    }
    let headers: HashMap<String, String> = match parts
        .headers
        .iter()
        .map(|(name, value)| {
            value
                .to_str()
                .map(|value| (name.as_str().to_owned(), value.to_owned()))
        })
        .collect()
    {
        Ok(headers) => headers,
        Err(_) => {
            return fail(ApiError::new(
                400,
                "invalid_header",
                "header value is not valid text",
            ))
        }
    };
    let matched = match_compat_route(&method, &path);
    if matched.is_some() || path.starts_with("/api/admin/") {
        if let Err(err) =
            validation::validate_api_key(&headers, &transport.app.config, matched.is_none())
        {
            return fail(err);
        }
    }
    let wait = transport.app.config.operator.query_admission_wait;
    let permit = if let Some(class) = matched.and_then(|matched| matched.query_class()) {
        transport
            .app
            .admission
            .reserve_query_with_wait(class, wait, &transport.app.metrics)
            .await
    } else {
        match timeout(wait, transport.operators.clone().acquire_owned()).await {
            Ok(Ok(permit)) => Ok(permit),
            _ => Err(ApiError::new(
                429,
                "operator_admission_full",
                "operator request capacity is exhausted",
            )
            .with_retry_after(1)),
        }
    };
    let permit = match permit {
        Ok(permit) => permit,
        Err(err) => return fail(err),
    };
    let body = match timeout(
        transport.app.config.operator.socket_read_timeout,
        to_bytes(body, transport.app.config.operator.max_body_bytes),
    )
    .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(err)) => return fail(body_error(&err)),
        Err(_) => {
            return fail(ApiError::new(
                408,
                "request_timeout",
                "request body deadline exceeded",
            ))
        }
    };
    let query = query_params(parts.uri.query().unwrap_or_default());
    let budget = Duration::from_secs(transport.app.config.operator.query_interactive.timeout_secs);
    let execution = Arc::new(QueryExecution::new(budget));
    let _cancel = CancelOnDrop(execution.clone());
    let mut state = transport.app.as_ref().clone();
    state.queries = state.queries.with_execution(execution);
    let method_for_job = method.clone();
    let path_for_job = path.clone();
    let task = transport.jobs.spawn_blocking(move || {
        let _permit = permit;
        route_admitted(
            &method_for_job,
            &path_for_job,
            &query,
            &headers,
            &body,
            &state,
        )
    });
    match timeout(budget, task).await {
        Ok(Ok(response)) => response.into_response(),
        Ok(Err(err)) => {
            tracing::error!(event = "request_task_failed", error = %err);
            fail(ApiError::new(
                500,
                "internal_error",
                "request execution failed",
            ))
        }
        Err(_) => fail(ApiError::new(
            503,
            "query_timeout",
            "request execution deadline exceeded",
        )),
    }
}

fn body_error(err: &axum::Error) -> ApiError {
    let mut source = Some(err as &(dyn Error + 'static));
    while let Some(err) = source {
        if err.is::<http_body_util::LengthLimitError>() {
            return ApiError::new(
                413,
                "payload_too_large",
                "request body exceeds configured limit",
            );
        }
        if err.downcast_ref::<std::io::Error>().is_some_and(|err| {
            matches!(
                err.kind(),
                std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
            )
        }) || err
            .downcast_ref::<hyper::Error>()
            .is_some_and(hyper::Error::is_timeout)
        {
            return ApiError::new(408, "request_timeout", "request body deadline exceeded");
        }
        source = err.source();
    }
    ApiError::new(400, "invalid_body", "could not read request body")
}

fn request_error(method: &str, path: &str, err: ApiError) -> Response<Body> {
    let response = if match_compat_route(method, path).is_some() {
        let status = err.status;
        let retry = err.retry_after_seconds;
        let mut response = HttpResponse::json(status, crate::compat::compat_error(err));
        if let Some(retry) = retry {
            response = response.with_retry_after(retry);
        }
        response
    } else {
        HttpResponse::from_api_error(&err)
    };
    let mut response = response.into_response();
    response.headers_mut().insert(
        header::CONNECTION,
        header::HeaderValue::from_static("close"),
    );
    response
}
