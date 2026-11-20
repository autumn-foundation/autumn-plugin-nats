//! Tests for the plugin builder.

use std::pin::Pin;

use autumn_web::plugin::Plugin;

use super::{JsMessage, NatsPlugin, PLUGIN_NAME};
use crate::config::NatsConfig;

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

fn noop_core(_message: async_nats::Message) -> BoxFuture {
    Box::pin(async {})
}

fn noop_js(_message: JsMessage) -> BoxFuture {
    Box::pin(async {})
}

#[test]
fn plugin_name_is_stable() {
    assert_eq!(NatsPlugin::new().name(), PLUGIN_NAME);
    assert_eq!(NatsPlugin::default().name(), PLUGIN_NAME);
}

#[test]
fn builder_chains() {
    let plugin = NatsPlugin::new()
        .config_section("nats_alt")
        .configure(|c| {
            c.url = "nats://example:4222".into();
        })
        .consumer("events", noop_core)
        .queue_consumer("work", "workers", noop_core)
        .jetstream_consumer(
            "events",
            vec!["events.>".into()],
            "app",
            "events.>",
            noop_js,
        );
    assert_eq!(plugin.name(), PLUGIN_NAME);
}

#[test]
fn resolve_applies_configure_changes() {
    use super::ConfigSource;
    let source = ConfigSource::Explicit(Box::default());
    let config = NatsPlugin::resolve(
        &source,
        vec![Box::new(|c| {
            c.url = "nats://example:4222".into();
        })],
    )
    .unwrap();
    assert_eq!(config.url, "nats://example:4222");
}

#[test]
fn resolve_rejects_invalid_explicit_config() {
    use super::ConfigSource;
    let mut config = NatsConfig::default();
    config.url = String::new();
    let source = ConfigSource::Explicit(Box::new(config));
    let err = NatsPlugin::resolve(&source, Vec::new()).unwrap_err();
    assert!(err.0.contains("url"), "{err}");
}

#[test]
fn resolve_rejects_invalid_change() {
    use super::ConfigSource;
    let source = ConfigSource::Explicit(Box::default());
    let err = NatsPlugin::resolve(
        &source,
        vec![Box::new(|c| {
            c.request_timeout_ms = 0;
        })],
    )
    .unwrap_err();
    assert!(err.0.contains("request_timeout_ms"), "{err}");
}

/// The role of a config with `lines`.
fn role(lines: &str) -> autumn_web::Role {
    let config: autumn_web::config::AutumnConfig = toml::from_str(lines).expect("config parses");
    config.resolve_role().expect("role resolves")
}

#[test]
fn the_consumer_capability_follows_the_job_workers() {
    use super::{CONSUMER_TASK, NATS_CONSUMER};
    assert_eq!(NATS_CONSUMER.name(), "nats.consumer");
    assert_eq!(NATS_CONSUMER.parent(), autumn_web::Capability::JobWorkers);
    assert_eq!(CONSUMER_TASK, "nats.consumers");
    assert!(role("role = \"combined\"").runs_plugin(NATS_CONSUMER));
    assert!(role("role = \"worker\"").runs_plugin(NATS_CONSUMER));
    assert!(!role("role = \"web\"").runs_plugin(NATS_CONSUMER));
    let off = "role = \"cron\"\n[roles.cron]\nbase = \"worker\"\n\
               capabilities = { \"nats.consumer\" = false }\n";
    assert!(!role(off).runs_plugin(NATS_CONSUMER));
}

#[tokio::test]
async fn the_consumer_task_without_a_connection_does_nothing() {
    use std::sync::Arc;

    use autumn_web::reexports::tokio_util::sync::CancellationToken;

    use super::{Shared, run_consumers};
    let shared = Arc::new(Shared::default());
    // The token is never cancelled: with no connection the task returns
    // at once.
    let done = run_consumers(
        Arc::clone(&shared),
        Arc::new(Vec::new()),
        Arc::new(Vec::new()),
        CancellationToken::new(),
    )
    .await;
    assert!(done.is_ok());
    assert_eq!(shared.consumer_count(), 0);
}
