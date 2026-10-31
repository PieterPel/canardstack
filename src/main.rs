use canardstack::cli::healthcheck;
use canardstack::{http, init_logging, AppState, Config};
use std::sync::Arc;
use std::time::Duration;

fn main() -> anyhow::Result<()> {
    init_logging();
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("--version") | Some("-V") => {
            println!("canardstack {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Some("healthcheck") => healthcheck::run(args),
        Some("serve") | None => {
            let Some(options) = parse_serve_options(args)? else {
                return Ok(());
            };
            let mut config = Config::from_env()?;
            if let Some(listen) = options.listen {
                config.operator.bind = listen;
            }
            config.validate()?;
            #[cfg(not(feature = "tls"))]
            if config.operator.tls.enabled {
                anyhow::bail!("CANARDSTACK_TLS_ENABLED=true requires --features tls");
            }
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            let result = runtime.block_on(async move {
                let state = tokio::task::spawn_blocking(move || AppState::new(config)).await??;
                let shutdown = shutdown_signal()?;
                http::serve_until(Arc::new(state), shutdown).await
            });
            runtime.shutdown_timeout(Duration::from_secs(30));
            result
        }
        Some(other) => {
            anyhow::bail!("unknown command {other}; use --version, serve, or healthcheck")
        }
    }
}

fn shutdown_signal() -> std::io::Result<impl std::future::Future<Output = ()>> {
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    Ok(async move {
        #[cfg(unix)]
        tokio::select! {
            result = tokio::signal::ctrl_c() => if let Err(err) = result { tracing::error!(event = "signal_failed", error = %err); },
            _ = terminate.recv() => {},
        }
        #[cfg(not(unix))]
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::error!(event = "signal_failed", error = %err);
        }
    })
}

struct ServeOptions {
    listen: Option<String>,
}

fn parse_serve_options(
    mut args: impl Iterator<Item = String>,
) -> anyhow::Result<Option<ServeOptions>> {
    let mut listen = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => {
                listen = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("{arg} requires an address"))?,
                )
            }
            "--help" | "-h" => {
                println!("usage: canardstack serve [--listen addr]");
                return Ok(None);
            }
            other => {
                if let Some(value) = other.strip_prefix("--listen=") {
                    listen = Some(value.to_owned());
                } else {
                    anyhow::bail!(
                        "unknown serve option {other}; usage: canardstack serve [--listen addr]"
                    );
                }
            }
        }
    }
    Ok(Some(ServeOptions { listen }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_serve_options_accepts_listen() {
        for args in [
            vec!["--listen", "127.0.0.1:4320"],
            vec!["--listen=127.0.0.1:4320"],
        ] {
            let options = parse_serve_options(args.into_iter().map(str::to_owned))
                .unwrap()
                .unwrap();
            assert_eq!(options.listen.as_deref(), Some("127.0.0.1:4320"));
        }
    }

    #[test]
    fn parse_serve_options_help_is_ok_none() {
        assert!(parse_serve_options(["--help".to_owned()].into_iter())
            .unwrap()
            .is_none());
    }
}
