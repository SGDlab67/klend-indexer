//! ClickHouse connection, shared by every binary that writes to the klend schema.
//!
//! Kept out of `main.rs` so the snapshot writer connects through exactly the same
//! code path as the stream indexer. The TLS branch in particular is not something
//! to reimplement per binary: it is the piece that took the 2026-08-05 debugging
//! session to get right.

use anyhow::{Context, Result};
use klickhouse::{Client, ClientOptions};

/// Connect the ClickHouse sink, plain or TLS. Hosted ClickHouse Cloud speaks
/// native-secure on :9440; the local docker sink speaks plain native on :9000.
/// `url` is `host:port`; when secure, the host part is also the TLS SNI name.
pub async fn connect_clickhouse(url: &str, secure: bool, options: ClientOptions) -> Result<Client> {
    if !secure {
        return Client::connect(url, options).await.map_err(Into::into);
    }

    // Trust anchors baked into the binary (webpki-roots), so a stripped container
    // image needs no OS cert store. The crypto backend is the ring provider
    // installed as process default at the top of `main`.
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));

    // SNI name is the host without the port. connect_tls resolves `url` for the
    // TCP connection and uses this name for the certificate check.
    let host = url.rsplit_once(':').map(|(h, _)| h).unwrap_or(url);
    let server_name = tokio_rustls::rustls::pki_types::ServerName::try_from(host.to_owned())
        .with_context(|| format!("invalid TLS server name {host:?}"))?;

    Client::connect_tls(url, options, server_name, &connector)
        .await
        .map_err(Into::into)
}

