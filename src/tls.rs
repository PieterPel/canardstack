use crate::config::{TlsMode, TlsServerConfig};
use anyhow::{Context, Result};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::ServerConfig;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

pub(crate) fn server_config(config: &TlsServerConfig) -> Result<Arc<ServerConfig>> {
    let (certs, key) = match config.mode {
        TlsMode::File => {
            let cert_path = config
                .cert_file
                .as_ref()
                .context("TLS cert_file is required")?;
            let key_path = config
                .key_file
                .as_ref()
                .context("TLS key_file is required")?;
            let certs = rustls_pemfile::certs(&mut BufReader::new(
                File::open(cert_path)
                    .with_context(|| format!("read TLS certificate {}", cert_path.display()))?,
            ))
            .collect::<std::io::Result<Vec<_>>>()?;
            let key = rustls_pemfile::private_key(&mut BufReader::new(
                File::open(key_path)
                    .with_context(|| format!("read TLS key {}", key_path.display()))?,
            ))?
            .context("TLS key file contains no private key")?;
            (certs, key)
        }
        TlsMode::EphemeralSelfSigned => {
            tracing::warn!(
                event = "serve_tls_ephemeral_self_signed",
                "clients must explicitly trust the self-signed certificate"
            );
            let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
            (
                vec![CertificateDer::from(cert.cert.der().to_vec())],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der())),
            )
        }
    };
    let mut config =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_single_cert(certs, key)?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}
