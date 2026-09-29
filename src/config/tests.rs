//! Tests for the layered `[nats]` configuration.

use std::collections::HashMap;

use autumn_web::config::Env;
use proptest::prop_assert_eq;

use super::{AuthMethod, NatsConfig, active_profile, inline_profile_names};

/// A test environment.
#[derive(Default)]
struct FakeEnv(HashMap<String, String>);

impl FakeEnv {
    fn with(mut self, key: &str, value: &str) -> Self {
        self.0.insert(key.to_owned(), value.to_owned());
        self
    }
}

impl Env for FakeEnv {
    fn var(&self, key: &str) -> Result<String, std::env::VarError> {
        self.0
            .get(key)
            .cloned()
            .ok_or(std::env::VarError::NotPresent)
    }
}

#[test]
fn default_config_validates() {
    NatsConfig::default().validate().unwrap();
}

#[test]
fn each_auth_method_reports_itself() {
    let mut config = NatsConfig::default();
    assert_eq!(config.auth_method(), AuthMethod::None);

    config.username = "app".into();
    config.password = "secret".into();
    assert_eq!(config.auth_method(), AuthMethod::UserPassword);
    config.validate().unwrap();

    let mut config = NatsConfig::default();
    config.nkey_seed = "SUABC".into();
    assert_eq!(config.auth_method(), AuthMethod::Nkey);
    config.validate().unwrap();

    let mut config = NatsConfig::default();
    config.creds_file = "/run/nats/app.creds".into();
    assert_eq!(config.auth_method(), AuthMethod::CredsFile);
    config.validate().unwrap();

    let mut config = NatsConfig::default();
    config.token = "token".into();
    assert_eq!(config.auth_method(), AuthMethod::Token);
    config.validate().unwrap();
}

#[test]
fn two_auth_methods_do_not_validate() {
    let mut config = NatsConfig::default();
    config.username = "app".into();
    config.password = "secret".into();
    config.token = "token".into();
    let err = config.validate().unwrap_err();
    assert!(err.0.contains("one auth method"), "{err}");
}

#[test]
fn username_without_password_does_not_validate() {
    let mut config = NatsConfig::default();
    config.username = "app".into();
    let err = config.validate().unwrap_err();
    assert!(err.0.contains("password"), "{err}");
}

#[test]
fn password_without_username_does_not_validate() {
    let mut config = NatsConfig::default();
    config.password = "secret".into();
    let err = config.validate().unwrap_err();
    assert!(err.0.contains("username"), "{err}");
}

#[test]
fn empty_url_does_not_validate() {
    let mut config = NatsConfig::default();
    config.url = "   ".into();
    assert!(config.validate().is_err());
}

#[test]
fn url_with_whitespace_does_not_validate() {
    let mut config = NatsConfig::default();
    config.url = "nats://local host:4222".into();
    assert!(config.validate().is_err());
}

#[test]
fn timeout_bounds_are_checked() {
    for timeout_ms in [0, 300_001] {
        let mut config = NatsConfig::default();
        config.request_timeout_ms = timeout_ms;
        assert!(config.validate().is_err(), "{timeout_ms}");
        let mut config = NatsConfig::default();
        config.connect_timeout_ms = timeout_ms;
        assert!(config.validate().is_err(), "{timeout_ms}");
    }
    let mut config = NatsConfig::default();
    config.request_timeout_ms = 300_000;
    config.connect_timeout_ms = 300_000;
    config.validate().unwrap();
}

#[test]
fn env_overrides_secrets() {
    let env = FakeEnv::default()
        .with("AUTUMN_NATS__URL", "nats://prod:4222")
        .with("AUTUMN_NATS__PASSWORD", "s3cret")
        .with("AUTUMN_NATS__USERNAME", "app")
        .with("AUTUMN_NATS__JETSTREAM_ENABLED", "true")
        .with("AUTUMN_NATS__REQUEST_TIMEOUT_MS", "1000");
    let config = NatsConfig::resolve_with_env("nats", &env).unwrap();
    assert_eq!(config.url, "nats://prod:4222");
    assert_eq!(config.password, "s3cret");
    assert!(config.jetstream_enabled);
    assert_eq!(config.request_timeout_ms, 1000);
}

#[test]
fn bad_env_bool_is_an_error() {
    let env = FakeEnv::default().with("AUTUMN_NATS__JETSTREAM_ENABLED", "yes");
    let err = NatsConfig::resolve_with_env("nats", &env).unwrap_err();
    assert!(err.0.contains("must be `true` or `false`"), "{err}");
}

#[test]
fn bad_env_int_is_an_error() {
    let env = FakeEnv::default().with("AUTUMN_NATS__REQUEST_TIMEOUT_MS", "soon");
    let err = NatsConfig::resolve_with_env("nats", &env).unwrap_err();
    assert!(err.0.contains("unsigned integer"), "{err}");
}

#[test]
fn unknown_section_is_empty_and_valid() {
    // No autumn.toml in the test working dir: the defaults apply.
    let config = NatsConfig::resolve_with_env("nats", &FakeEnv::default()).unwrap();
    assert_eq!(config, NatsConfig::default());
}

#[test]
fn profile_aliases_normalize() {
    let env = FakeEnv::default().with("AUTUMN_ENV", "production");
    assert_eq!(active_profile(&env), ("production".into(), "prod".into()));

    let env = FakeEnv::default().with("AUTUMN_ENV", "DEVELOPMENT");
    assert_eq!(active_profile(&env), ("DEVELOPMENT".into(), "dev".into()));

    let env = FakeEnv::default().with("AUTUMN_PROFILE", "staging");
    assert_eq!(active_profile(&env), ("staging".into(), "staging".into()));
}

#[test]
fn profile_defaults_follow_debug_flag() {
    let env = FakeEnv::default();
    assert_eq!(active_profile(&env), ("dev".into(), "dev".into()));

    let env = FakeEnv::default().with("AUTUMN_IS_DEBUG", "0");
    assert_eq!(active_profile(&env), ("prod".into(), "prod".into()));
}

#[test]
fn canonical_profile_name_wins_aliases() {
    assert_eq!(inline_profile_names("prod"), ["production", "prod"]);
    assert_eq!(inline_profile_names("dev"), ["development", "dev"]);
    assert_eq!(inline_profile_names("staging"), ["staging"]);
}

proptest::proptest! {
    /// Any timeout in the valid range validates; anything outside does not.
    #[test]
    fn timeout_bounds_hold(request_ms: u64, connect_ms: u64) {
        let mut config = NatsConfig::default();
        config.request_timeout_ms = request_ms;
        config.connect_timeout_ms = connect_ms;
        let request_ok = (1..=300_000).contains(&request_ms);
        let connect_ok = (1..=300_000).contains(&connect_ms);
        prop_assert_eq!(config.validate().is_ok(), request_ok && connect_ok);
    }

    /// Validation never panics on arbitrary URLs.
    #[test]
    fn url_validation_never_panics(url in ".*") {
        let mut config = NatsConfig::default();
        config.url = url;
        let _ = config.validate();
    }
}
