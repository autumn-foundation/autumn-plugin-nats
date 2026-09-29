//! Tests for the client. The connection tests use a refused port, so they
//! need no server.

use super::Nats;
use crate::config::NatsConfig;
use crate::error::{ErrorKind, NatsError};

/// A config that points at a port where nothing listens.
fn refused_config() -> NatsConfig {
    let mut config = NatsConfig::default();
    config.url = "nats://127.0.0.1:1".into();
    config.connect_timeout_ms = 500;
    config
}

#[tokio::test]
async fn refused_connect_gives_connect_error() {
    let err = Nats::connect(refused_config()).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Connect);
    // The text hides the URL.
    assert!(!format!("{err}").contains("127.0.0.1"));
}

#[tokio::test]
async fn invalid_config_stops_connect() {
    let mut config = NatsConfig::default();
    config.request_timeout_ms = 0;
    let err = Nats::connect(config).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Config);
}

#[tokio::test]
async fn connect_reads_explicit_config() {
    // A bad URL fails fast instead of hanging.
    let mut config = NatsConfig::default();
    config.url = "nats://".into();
    let err = Nats::connect(config).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Config);
    assert!(matches!(err, NatsError::Config(_)));
}
