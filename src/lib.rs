//! Autumn plugin for NATS.
//!
//! Add [`NatsPlugin`] to the app. Then use the [`Nats`] extractor in a handler.
//!
//! ```rust,no_run
//! use autumn_plugin_nats::{Nats, NatsPlugin, NatsResultExt as _};
//! use autumn_web::prelude::*;
//!
//! #[post("/events")]
//! async fn publish_event(nats: Nats, body: String) -> AutumnResult<()> {
//!     nats.publish("events", body).await.or_http()?;
//!     Ok(())
//! }
//!
//! # async fn run() {
//! autumn_web::app()
//!     .plugin(
//!         NatsPlugin::new()
//!             .configure(|c| {
//!                 c.url = "nats://localhost:4222".into();
//!             })
//!             .consumer("events", |message| async move {
//!                 tracing::info!(subject = %message.subject, "got an event");
//!             }),
//!     )
//!     .routes(routes![publish_event])
//!     .run()
//!     .await;
//! # }
//! ```
//!
//! The plugin reads `[nats]` in `autumn.toml`. See [`config`] for the keys.
//!
//! # What the plugin gives
//!
//! - [`Nats`]: async publish and subscribe, request/reply, and a flush round trip.
//! - [`JetStream`](jetstream::JetStream): streams, durable push consumers,
//!   key-value buckets and object stores. Set `jetstream_enabled = true`.
//! - Background consumers: [`NatsPlugin::consumer`], [`NatsPlugin::queue_consumer`]
//!   and [`NatsPlugin::jetstream_consumer`] run subscriptions as tasks.
//! - A readiness check and Prometheus metrics.
//!
//! # Limits
//!
//! - A publish sends at most once. Use `JetStream` for at-least-once delivery.
//! - A request waits at most `request_timeout_ms` for the reply.
//! - The error text never shows the server URL or a credential.
//!   [`NatsError::detail`] gives the full message.
//! - The plugin does not start an embedded server. Point `url` at a NATS server.
//!
//! # Shutdown
//!
//! At shutdown the plugin aborts the consumer tasks, then drains the connection:
//! it unsubscribes, flushes pending publishes and closes.

// Tests use `unwrap` and field reassignment for brevity. The strict lints stay
// on for non-test code.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::field_reassign_with_default,
        clippy::float_cmp
    )
)]

mod client;
pub mod config;
mod error;
mod health;
pub mod jetstream;
mod metrics;
mod plugin;

/// The `async-nats` crate that the plugin uses. Use it for message types in consumers.
pub use async_nats;
pub use client::Nats;
pub use config::{AuthMethod, ConfigError, NatsConfig};
pub use error::{ErrorKind, NatsError, NatsResultExt};
pub use jetstream::{JetStream, JsConsumer, JsMessage, JsStream, KvBucket, ObjectBucket};
pub use plugin::{CONSUMER_TASK, NATS_CONSUMER, NatsPlugin, PLUGIN_NAME};

/// Compiles the Rust snippets of the README as doctests.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;
