//! `JetStream`: streams, durable consumers, key-value buckets and object stores.
//!
//! # Contract
//!
//! - Get a [`JetStream`] from [`Nats::jetstream`](crate::Nats::jetstream).
//!   It fails unless `jetstream_enabled` is `true`.
//! - [`JetStream::publish`] waits for the server ack.
//! - [`JsStream::durable_consumer`] makes a durable push consumer. The server
//!   keeps the consumer state. Each message needs an ack; the background
//!   consumer in the [`plugin`](crate::plugin) module acks after the handler runs.
//! - [`KvBucket`] and [`ObjectBucket`] wrap the `JetStream` KV and object store.
//! - Clones share the context and the counters.

use std::sync::Arc;

use async_nats::jetstream::consumer::push;
use async_nats::jetstream::kv;
use async_nats::jetstream::object_store::ObjectMetadata;
use bytes::Bytes;
use futures_util::StreamExt;
use serde::de::DeserializeOwned;

use crate::error::NatsError;
use crate::metrics::{Metrics, Outcome};

/// The `JetStream` handle.
#[derive(Clone)]
pub struct JetStream {
    context: async_nats::jetstream::Context,
    metrics: Arc<Metrics>,
}

impl JetStream {
    pub(crate) const fn new(
        context: async_nats::jetstream::Context,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self { context, metrics }
    }

    /// Publishes `payload` on `subject` and waits for the server ack.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::JetStream`] if the publish or the ack did not go through.
    pub async fn publish(
        &self,
        subject: impl async_nats::subject::ToSubject,
        payload: impl Into<Bytes>,
    ) -> Result<async_nats::jetstream::publish::PublishAck, NatsError> {
        let ack = match self.context.publish(subject, payload.into()).await {
            Ok(pending) => pending
                .await
                .map_err(|err| NatsError::jetstream("publish_ack", err)),
            Err(err) => Err(NatsError::jetstream("publish", err)),
        };
        self.metrics.jetstream_published(match ack {
            Ok(_) => Outcome::Ok,
            Err(_) => Outcome::Error,
        });
        ack
    }

    /// Gets the stream `name`.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::JetStream`] if the stream does not exist.
    pub async fn stream(&self, name: &str) -> Result<JsStream, NatsError> {
        let stream = self
            .context
            .get_stream(name)
            .await
            .map_err(|err| NatsError::jetstream("get_stream", err))?;
        Ok(JsStream::new(
            stream,
            self.context.clone(),
            Arc::clone(&self.metrics),
        ))
    }

    /// Creates the stream `name` on `subjects`.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::JetStream`] if the server refuses the stream.
    pub async fn create_stream(
        &self,
        name: &str,
        subjects: Vec<String>,
    ) -> Result<JsStream, NatsError> {
        let config = async_nats::jetstream::stream::Config {
            name: name.to_owned(),
            subjects,
            ..Default::default()
        };
        let stream = self
            .context
            .create_stream(config)
            .await
            .map_err(|err| NatsError::jetstream("create_stream", err))?;
        Ok(JsStream::new(
            stream,
            self.context.clone(),
            Arc::clone(&self.metrics),
        ))
    }

    /// Gets the stream `name`, or creates it on `subjects` if it is missing.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::JetStream`] if the server refuses the stream.
    pub async fn get_or_create_stream(
        &self,
        name: &str,
        subjects: Vec<String>,
    ) -> Result<JsStream, NatsError> {
        let config = async_nats::jetstream::stream::Config {
            name: name.to_owned(),
            subjects,
            ..Default::default()
        };
        let stream = self
            .context
            .get_or_create_stream(config)
            .await
            .map_err(|err| NatsError::jetstream("get_or_create_stream", err))?;
        Ok(JsStream::new(
            stream,
            self.context.clone(),
            Arc::clone(&self.metrics),
        ))
    }

    /// Gets the key-value bucket `bucket`.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::JetStream`] if the bucket does not exist.
    pub async fn kv(&self, bucket: &str) -> Result<KvBucket, NatsError> {
        let store = self
            .context
            .get_key_value(bucket)
            .await
            .map_err(|err| NatsError::jetstream("get_key_value", err))?;
        Ok(KvBucket::new(store))
    }

    /// Creates the key-value bucket `bucket`.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::JetStream`] if the server refuses the bucket.
    pub async fn create_kv(&self, bucket: &str) -> Result<KvBucket, NatsError> {
        let config = kv::Config {
            bucket: bucket.to_owned(),
            ..Default::default()
        };
        let store = self
            .context
            .create_key_value(config)
            .await
            .map_err(|err| NatsError::jetstream("create_key_value", err))?;
        Ok(KvBucket::new(store))
    }

    /// Gets the object store bucket `bucket`.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::JetStream`] if the bucket does not exist.
    pub async fn object_store(&self, bucket: &str) -> Result<ObjectBucket, NatsError> {
        let store = self
            .context
            .get_object_store(bucket)
            .await
            .map_err(|err| NatsError::jetstream("get_object_store", err))?;
        Ok(ObjectBucket::new(store))
    }

    /// Creates the object store bucket `bucket`.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::JetStream`] if the server refuses the bucket.
    pub async fn create_object_store(&self, bucket: &str) -> Result<ObjectBucket, NatsError> {
        let config = async_nats::jetstream::object_store::Config {
            bucket: bucket.to_owned(),
            ..Default::default()
        };
        let store = self
            .context
            .create_object_store(config)
            .await
            .map_err(|err| NatsError::jetstream("create_object_store", err))?;
        Ok(ObjectBucket::new(store))
    }
}

/// A `JetStream` stream.
#[derive(Clone)]
pub struct JsStream {
    stream: async_nats::jetstream::stream::Stream,
    context: async_nats::jetstream::Context,
    metrics: Arc<Metrics>,
}

impl JsStream {
    pub(crate) const fn new(
        stream: async_nats::jetstream::stream::Stream,
        context: async_nats::jetstream::Context,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            stream,
            context,
            metrics,
        }
    }

    /// Makes a durable push consumer on the stream. The server keeps the
    /// consumer state under `name`; only messages on `filter_subject` arrive.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::JetStream`] if the server refuses the consumer.
    pub async fn durable_consumer(
        &self,
        name: &str,
        filter_subject: &str,
    ) -> Result<JsConsumer, NatsError> {
        let config = push::Config {
            deliver_subject: self.context.client().new_inbox(),
            durable_name: Some(name.to_owned()),
            filter_subject: filter_subject.to_owned(),
            ..Default::default()
        };
        let consumer = self
            .stream
            .get_or_create_consumer(name, config)
            .await
            .map_err(|err| NatsError::jetstream("get_or_create_consumer", err))?;
        let messages = consumer
            .messages()
            .await
            .map_err(|err| NatsError::jetstream("consumer_messages", err))?;
        Ok(JsConsumer::new(messages, Arc::clone(&self.metrics)))
    }
}

/// One message from a durable consumer.
#[derive(Clone)]
pub struct JsMessage {
    inner: async_nats::jetstream::Message,
}

impl JsMessage {
    pub(crate) const fn new(inner: async_nats::jetstream::Message) -> Self {
        Self { inner }
    }

    /// Gives the raw `JetStream` message.
    #[must_use]
    pub const fn message(&self) -> &async_nats::jetstream::Message {
        &self.inner
    }

    /// Acks the message.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::JetStream`] if the ack did not go through.
    pub async fn ack(&self) -> Result<(), NatsError> {
        self.inner
            .ack()
            .await
            .map_err(|err| NatsError::jetstream("ack", err))
    }
}

/// The message stream of a durable push consumer.
pub struct JsConsumer {
    messages: push::Messages,
    metrics: Arc<Metrics>,
}

impl JsConsumer {
    pub(crate) const fn new(messages: push::Messages, metrics: Arc<Metrics>) -> Self {
        Self { messages, metrics }
    }

    /// Waits for the next message. `None` means the stream ended.
    pub async fn next(&mut self) -> Option<Result<JsMessage, NatsError>> {
        let next = self.messages.next().await?;
        match next {
            Ok(message) => {
                self.metrics.received();
                Some(Ok(JsMessage::new(message)))
            }
            Err(err) => Some(Err(NatsError::jetstream("consumer_next", err))),
        }
    }
}

/// A `JetStream` key-value bucket.
#[derive(Clone)]
pub struct KvBucket {
    store: kv::Store,
}

impl KvBucket {
    pub(crate) const fn new(store: kv::Store) -> Self {
        Self { store }
    }

    /// Puts `value` at `key`. Returns the revision.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::JetStream`] if the put did not go through.
    pub async fn put(&self, key: &str, value: impl Into<Bytes>) -> Result<u64, NatsError> {
        self.store
            .put(key, value.into())
            .await
            .map_err(|err| NatsError::jetstream("kv_put", err))
    }

    /// Puts `value` as JSON at `key`. Returns the revision.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::JetStream`] if the value does not serialize or the
    /// put did not go through.
    pub async fn put_json<T: serde::Serialize + Sync>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<u64, NatsError> {
        let payload = serde_json::to_vec(value).map_err(|err| {
            NatsError::jetstream("kv_put_json", format!("the value is not JSON: {err}"))
        })?;
        self.put(key, Bytes::from(payload)).await
    }

    /// Gets the value at `key`. `None` means the key is missing.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::JetStream`] if the get did not go through.
    pub async fn get(&self, key: &str) -> Result<Option<Bytes>, NatsError> {
        self.store
            .get(key)
            .await
            .map_err(|err| NatsError::jetstream("kv_get", err))
    }

    /// Gets the value at `key` as JSON. `None` means the key is missing.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::JetStream`] if the get did not go through or the
    /// value is not `T`.
    pub async fn get_json<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, NatsError> {
        let Some(bytes) = self.get(key).await? else {
            return Ok(None);
        };
        let value = serde_json::from_slice(&bytes).map_err(|err| {
            NatsError::jetstream("kv_get_json", format!("the value is not JSON: {err}"))
        })?;
        Ok(Some(value))
    }

    /// Deletes `key`.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::JetStream`] if the delete did not go through.
    pub async fn delete(&self, key: &str) -> Result<(), NatsError> {
        self.store
            .delete(key)
            .await
            .map_err(|err| NatsError::jetstream("kv_delete", err))
    }
}

/// A `JetStream` object store bucket.
#[derive(Clone)]
pub struct ObjectBucket {
    store: async_nats::jetstream::object_store::ObjectStore,
}

impl ObjectBucket {
    pub(crate) const fn new(store: async_nats::jetstream::object_store::ObjectStore) -> Self {
        Self { store }
    }

    /// Puts `data` at `name`.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::JetStream`] if the put did not go through.
    pub async fn put(&self, name: &str, data: &[u8]) -> Result<(), NatsError> {
        let meta = ObjectMetadata {
            name: name.to_owned(),
            ..Default::default()
        };
        let mut reader = SliceReader(data);
        self.store
            .put(meta, &mut reader)
            .await
            .map(|_| ())
            .map_err(|err| NatsError::jetstream("object_put", err))
    }

    /// Gets the object at `name`.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::JetStream`] if the get did not go through.
    pub async fn get(&self, name: &str) -> Result<Vec<u8>, NatsError> {
        use tokio::io::AsyncReadExt as _;
        let mut object = self
            .store
            .get(name)
            .await
            .map_err(|err| NatsError::jetstream("object_get", err))?;
        let mut data = Vec::new();
        object
            .read_to_end(&mut data)
            .await
            .map_err(|err| NatsError::jetstream("object_read", err))?;
        Ok(data)
    }
}

/// An [`AsyncRead`](tokio::io::AsyncRead) over a byte slice.
struct SliceReader<'a>(&'a [u8]);

impl tokio::io::AsyncRead for SliceReader<'_> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let n = std::cmp::min(buf.remaining(), self.0.len());
        buf.put_slice(&self.0[..n]);
        self.0 = &self.0[n..];
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests;
