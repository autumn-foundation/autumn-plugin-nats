//! [`Nats`]: the handle that handlers, jobs and tasks use.
//!
//! # Contract
//!
//! - [`Nats::connect`] reads the credentials from the [`NatsConfig`](crate::NatsConfig).
//!   At most one auth method applies. The connect waits at most `connect_timeout_ms`.
//! - Each call fails fast after [`Nats::drain`]. The error text never shows
//!   the server URL or a credential.
//! - [`Nats::publish`] sends at most once. [`Nats::request`] waits at most
//!   `request_timeout_ms` for the reply.
//! - [`Nats::jetstream`] fails with [`NatsError::JetStreamDisabled`](crate::NatsError::JetStreamDisabled)
//!   unless `jetstream_enabled` is `true`.
//! - Clones share the connection and the counters.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::config::{AuthMethod, NatsConfig};
use crate::error::NatsError;
use crate::jetstream::JetStream;
use crate::metrics::{Metrics, Outcome};

/// A handle to the NATS connection. Clones share the connection.
///
/// Get it with the extractor in a handler, or with [`Nats::from_state`] in a
/// job or a task — see the [`plugin`](crate::plugin) module.
#[derive(Clone, Debug)]
pub struct Nats {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    client: async_nats::Client,
    jetstream: Option<async_nats::jetstream::Context>,
    config: NatsConfig,
    metrics: Arc<Metrics>,
    closed: AtomicBool,
}

impl Nats {
    /// Connects to the server with `config`.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::Connect`] if the server is unreachable or the
    /// credentials are wrong, and [`NatsError::Config`] if the configuration
    /// is not valid.
    pub async fn connect(config: NatsConfig) -> Result<Self, NatsError> {
        Self::connect_with(config, Arc::new(Metrics::default())).await
    }

    /// Connects to the server with `config` and counts calls in `metrics`.
    ///
    /// # Errors
    ///
    /// See [`connect`](Self::connect).
    pub(crate) async fn connect_with(
        config: NatsConfig,
        metrics: Arc<Metrics>,
    ) -> Result<Self, NatsError> {
        config.validate()?;
        let mut options = async_nats::ConnectOptions::new();
        match config.auth_method() {
            AuthMethod::None => {}
            AuthMethod::UserPassword => {
                options =
                    options.user_and_password(config.username.clone(), config.password.clone());
            }
            AuthMethod::Nkey => {
                options = options.nkey(config.nkey_seed.clone());
            }
            AuthMethod::CredsFile => {
                let contents =
                    std::fs::read_to_string(config.creds_file.as_str()).map_err(|err| {
                        NatsError::connect(format!("cannot read the creds file: {err}"))
                    })?;
                options = options.credentials(&contents).map_err(NatsError::connect)?;
            }
            AuthMethod::Token => {
                options = options.token(config.token.clone());
            }
        }
        if !config.name.trim().is_empty() {
            options = options.name(config.name.clone());
        }
        let max_reconnects = config
            .max_reconnects
            .map_or(usize::MAX, |n| usize::try_from(n).map_or(usize::MAX, |v| v));
        options = options
            .connection_timeout(Duration::from_millis(config.connect_timeout_ms))
            .max_reconnects(max_reconnects);
        let client = options
            .connect(config.url.as_str())
            .await
            .map_err(NatsError::connect)?;
        let jetstream = config
            .jetstream_enabled
            .then(|| async_nats::jetstream::new(client.clone()));
        Ok(Self {
            inner: Arc::new(Inner {
                client,
                jetstream,
                config,
                metrics,
                closed: AtomicBool::new(false),
            }),
        })
    }

    /// Returns the configuration that the handle connected with.
    #[must_use]
    pub fn config(&self) -> &NatsConfig {
        &self.inner.config
    }

    /// Returns the `JetStream` handle.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::JetStreamDisabled`] unless `jetstream_enabled` is `true`.
    pub fn jetstream(&self) -> Result<JetStream, NatsError> {
        self.inner
            .jetstream
            .clone()
            .map(|context| JetStream::new(context, Arc::clone(&self.inner.metrics)))
            .ok_or(NatsError::JetStreamDisabled)
    }

    /// Publishes `payload` on `subject`. The send is at most once.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::Publish`] if the send did not go through.
    pub async fn publish(
        &self,
        subject: impl async_nats::subject::ToSubject,
        payload: impl Into<Bytes>,
    ) -> Result<(), NatsError> {
        self.check_open()?;
        let result = self.inner.client.publish(subject, payload.into()).await;
        self.inner.metrics.published(match result {
            Ok(()) => Outcome::Ok,
            Err(_) => Outcome::Error,
        });
        result.map_err(NatsError::publish)
    }

    /// Publishes `value` as JSON on `subject`.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::Publish`] if the value does not serialize or the
    /// send did not go through.
    pub async fn publish_json<T: serde::Serialize + Sync>(
        &self,
        subject: impl async_nats::subject::ToSubject + Send,
        value: &T,
    ) -> Result<(), NatsError> {
        let payload = serde_json::to_vec(value)
            .map_err(|err| NatsError::publish(format!("the value is not JSON: {err}")))?;
        self.publish(subject, Bytes::from(payload)).await
    }

    /// Subscribes to `subject`. Each message arrives on the returned subscriber.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::Subscribe`] if the subscribe did not go through.
    pub async fn subscribe(
        &self,
        subject: impl async_nats::subject::ToSubject,
    ) -> Result<async_nats::Subscriber, NatsError> {
        self.check_open()?;
        self.inner
            .client
            .subscribe(subject)
            .await
            .map_err(NatsError::subscribe)
    }

    /// Subscribes to `subject` in the queue group `queue_group`. Each message
    /// goes to one member of the group.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::Subscribe`] if the subscribe did not go through.
    pub async fn queue_subscribe(
        &self,
        subject: impl async_nats::subject::ToSubject,
        queue_group: &str,
    ) -> Result<async_nats::Subscriber, NatsError> {
        self.check_open()?;
        self.inner
            .client
            .queue_subscribe(subject, queue_group.to_owned())
            .await
            .map_err(NatsError::subscribe)
    }

    /// Sends `payload` on `subject` and waits for the first reply.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::Timeout`] if no reply arrives in `request_timeout_ms`,
    /// or [`NatsError::Request`] if the round trip did not go through.
    pub async fn request(
        &self,
        subject: impl async_nats::subject::ToSubject,
        payload: impl Into<Bytes>,
    ) -> Result<async_nats::Message, NatsError> {
        self.check_open()?;
        let timeout = Duration::from_millis(self.inner.config.request_timeout_ms);
        let result =
            tokio::time::timeout(timeout, self.inner.client.request(subject, payload.into()))
                .await
                .map_err(|_| NatsError::Timeout)?
                .map_err(NatsError::request);
        self.inner.metrics.requests(match result {
            Ok(_) => Outcome::Ok,
            Err(_) => Outcome::Error,
        });
        result
    }

    /// Publishes `payload` to the reply subject of `request`.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::NoReplySubject`] if the message has no reply subject,
    /// or [`NatsError::Publish`] if the send did not go through.
    pub async fn respond(
        &self,
        request: &async_nats::Message,
        payload: impl Into<Bytes>,
    ) -> Result<(), NatsError> {
        let Some(reply) = request.reply.clone() else {
            return Err(NatsError::NoReplySubject);
        };
        self.publish(reply, payload).await
    }

    /// Flushes the connection. The server answers, so this is a round trip.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::Flush`] if the round trip did not go through.
    pub async fn flush(&self) -> Result<(), NatsError> {
        self.inner.client.flush().await.map_err(NatsError::flush)
    }

    /// Measures the round-trip time with a flush.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::Flush`] if the round trip did not go through.
    pub async fn rtt(&self) -> Result<Duration, NatsError> {
        let start = Instant::now();
        self.flush().await?;
        Ok(start.elapsed())
    }

    /// Drains the connection: unsubscribes, flushes pending publishes and closes.
    /// New calls fail after the drain starts.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::Connect`] if the drain did not finish.
    pub async fn drain(&self) -> Result<(), NatsError> {
        self.inner.closed.store(true, Ordering::SeqCst);
        self.inner.client.drain().await.map_err(NatsError::connect)
    }

    /// Counts a message that a background consumer got.
    pub(crate) fn received(&self) {
        self.inner.metrics.received();
    }

    fn check_open(&self) -> Result<(), NatsError> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(NatsError::connect("the connection is closed"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
