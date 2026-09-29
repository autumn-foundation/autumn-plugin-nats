//! Tests for the error type.

use http::StatusCode;

use super::{ErrorKind, NatsError};
use crate::config::ConfigError;

#[test]
fn status_codes_follow_the_contract() {
    assert_eq!(NatsError::Timeout.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(
        NatsError::connect("x").status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        NatsError::NotInstalled.status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        NatsError::JetStreamDisabled.status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        NatsError::publish("x").status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        NatsError::Config(ConfigError("bad".into())).status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[test]
fn kinds_classify_each_variant() {
    assert_eq!(NatsError::Timeout.kind(), ErrorKind::Timeout);
    assert_eq!(NatsError::connect("x").kind(), ErrorKind::Connect);
    assert_eq!(NatsError::NotInstalled.kind(), ErrorKind::NotInstalled);
    assert_eq!(NatsError::JetStreamDisabled.kind(), ErrorKind::JetStream);
    assert_eq!(NatsError::jetstream("op", "x").kind(), ErrorKind::JetStream);
    assert_eq!(NatsError::NoReplySubject.kind(), ErrorKind::NoReplySubject);
}

#[test]
fn text_hides_detail() {
    let err = NatsError::connect("nats://user:pass@host:4222 refused");
    assert_eq!(format!("{err}"), "the NATS server refused the connection");
    assert_eq!(format!("{err:?}"), "the NATS server refused the connection");
    assert_eq!(err.detail(), Some("nats://user:pass@host:4222 refused"));
}

#[test]
fn detail_is_none_without_a_message() {
    assert_eq!(NatsError::Timeout.detail(), None);
    assert_eq!(NatsError::NotInstalled.detail(), None);
    assert_eq!(NatsError::JetStreamDisabled.detail(), None);
    assert_eq!(NatsError::NoReplySubject.detail(), None);
}

#[test]
fn into_autumn_carries_the_status() {
    use super::NatsResultExt as _;
    let result: Result<(), NatsError> = Err(NatsError::Timeout);
    let err = result.or_http().unwrap_err();
    assert_eq!(format!("{err}"), "the NATS call timed out");
    assert_eq!(err.status(), StatusCode::GATEWAY_TIMEOUT);
}
