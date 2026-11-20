//! [`NatsPlugin`]: installs a [`Nats`](crate::Nats) handle in an Autumn app.
//!
//! # Contract
//!
//! - `build` reads the configuration. A bad configuration stops the boot in the startup hook.
//! - The startup hook connects to the server and puts the handle in the app state.
//! - Consumers that [`consumer`](NatsPlugin::consumer),
//!   [`queue_consumer`](NatsPlugin::queue_consumer) or
//!   [`jetstream_consumer`](NatsPlugin::jetstream_consumer) registered run as
//!   background tasks. The `JetStream` consumer acks each message after the handler runs.
//! - The shutdown hook aborts the consumer tasks and drains the connection.
//! - The readiness check and the metrics source use the same handle.

use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};

use autumn_web::actuator::{MetricFamily, MetricsSource};
use autumn_web::app::AppBuilder;
use autumn_web::background::PluginCapability;
use autumn_web::plugin::Plugin;
use autumn_web::reexports::tokio_util::sync::CancellationToken;
use autumn_web::{AppState, AutumnError, Capability};
use futures_util::StreamExt;

use crate::client::Nats;
use crate::config::{ConfigError, DEFAULT_SECTION, NatsConfig};
use crate::error::NatsError;
use crate::health::NatsCheck;
use crate::jetstream::JsMessage;
use crate::metrics::Metrics;

/// The plugin name in Autumn diagnostics.
pub const PLUGIN_NAME: &str = "autumn-plugin-nats";

/// The boxed future that a consumer handler returns.
///
/// The builder methods box the future for you, so handlers can just be
/// `async` blocks.
pub type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// A core NATS subscription that runs as a background task.
#[derive(Clone)]
pub(crate) struct CoreConsumer {
    subject: String,
    queue_group: Option<String>,
    handler: Arc<dyn Fn(async_nats::Message) -> BoxFuture + Send + Sync>,
}

/// A `JetStream` durable push consumer that runs as a background task.
#[derive(Clone)]
pub(crate) struct StreamConsumer {
    stream: String,
    subjects: Vec<String>,
    durable_name: String,
    filter_subject: String,
    handler: Arc<dyn Fn(JsMessage) -> BoxFuture + Send + Sync>,
}

/// The capability of the consumers.
///
/// A role runs it when it runs job workers (`combined`, `worker`). A named
/// role can turn it off:
/// `[roles.<name>] capabilities = { "nats.consumer" = false }`.
pub const NATS_CONSUMER: PluginCapability =
    PluginCapability::new("nats.consumer", Capability::JobWorkers);

/// The name of the background task that runs the consumers.
/// `/actuator/tasks` and `autumn graph` show it.
pub const CONSUMER_TASK: &str = "nats.consumers";

/// State that the plugin hooks share.
#[derive(Default)]
pub(crate) struct Shared {
    pub(crate) handle: OnceLock<Nats>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl Shared {
    /// Aborts the consumer tasks.
    fn stop_consumers(&self) {
        let tasks = self
            .tasks
            .lock()
            .map(|mut tasks| std::mem::take(&mut *tasks))
            .unwrap_or_default();
        for task in tasks {
            task.abort();
        }
    }

    /// The number of consumer tasks.
    #[cfg(test)]
    pub(crate) fn consumer_count(&self) -> usize {
        self.tasks.lock().map_or(0, |tasks| tasks.len())
    }

    /// Aborts the consumer tasks and drains the connection.
    async fn shutdown(&self) {
        self.stop_consumers();
        if let Some(nats) = self.handle.get()
            && let Err(err) = nats.drain().await
        {
            tracing::warn!("the NATS drain did not finish: {err}");
        }
    }
}

/// The metrics source: the plugin counters.
struct Source(Arc<Metrics>);

impl MetricsSource for Source {
    fn collect(&self) -> Vec<MetricFamily> {
        self.0.families()
    }
}

enum ConfigSource {
    Section(String),
    Explicit(Box<NatsConfig>),
}

type Change = Box<dyn FnOnce(&mut NatsConfig) + Send>;

/// Installs a [`Nats`](crate::Nats) handle in an Autumn app.
///
/// ```rust,no_run
/// use autumn_plugin_nats::NatsPlugin;
///
/// # async fn run() {
/// autumn_web::app()
///     .plugin(NatsPlugin::new().configure(|c| {
///         c.url = "nats://localhost:4222".into();
///         c.jetstream_enabled = true;
///     }))
///     .run()
///     .await;
/// # }
/// ```
#[must_use]
pub struct NatsPlugin {
    source: ConfigSource,
    changes: Vec<Change>,
    core_consumers: Vec<CoreConsumer>,
    stream_consumers: Vec<StreamConsumer>,
}

impl Default for NatsPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl NatsPlugin {
    /// Makes a plugin that reads `[nats]`.
    pub fn new() -> Self {
        Self {
            source: ConfigSource::Section(DEFAULT_SECTION.to_owned()),
            changes: Vec::new(),
            core_consumers: Vec::new(),
            stream_consumers: Vec::new(),
        }
    }

    /// Reads `[section]` instead of `[nats]`.
    ///
    /// An app can have one NATS plugin only. Autumn ignores a second plugin with the same name.
    pub fn config_section(mut self, section: impl Into<String>) -> Self {
        self.source = ConfigSource::Section(section.into());
        self
    }

    /// Uses `config` and reads no files or variables.
    pub fn config(mut self, config: NatsConfig) -> Self {
        self.source = ConfigSource::Explicit(Box::new(config));
        self
    }

    /// Changes the configuration after the plugin reads it.
    pub fn configure(mut self, change: impl FnOnce(&mut NatsConfig) + Send + 'static) -> Self {
        self.changes.push(Box::new(change));
        self
    }

    /// Runs `handler` on each message on `subject` as a background task.
    ///
    /// The task starts at startup and stops at shutdown. Messages run one by one, in order.
    pub fn consumer<F, Fut>(mut self, subject: impl Into<String>, handler: F) -> Self
    where
        F: Fn(async_nats::Message) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.core_consumers.push(CoreConsumer {
            subject: subject.into(),
            queue_group: None,
            handler: Arc::new(move |message| Box::pin(handler(message)) as BoxFuture),
        });
        self
    }

    /// Runs `handler` on each message on `subject` in the queue group `queue_group`.
    ///
    /// Each message goes to one member of the group. The task starts at startup
    /// and stops at shutdown.
    pub fn queue_consumer<F, Fut>(
        mut self,
        subject: impl Into<String>,
        queue_group: impl Into<String>,
        handler: F,
    ) -> Self
    where
        F: Fn(async_nats::Message) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.core_consumers.push(CoreConsumer {
            subject: subject.into(),
            queue_group: Some(queue_group.into()),
            handler: Arc::new(move |message| Box::pin(handler(message)) as BoxFuture),
        });
        self
    }

    /// Runs `handler` on each message of a durable `JetStream` push consumer.
    ///
    /// The plugin gets or creates the stream `stream` on `subjects`, then gets
    /// or creates the durable consumer `durable_name` on `filter_subject`.
    /// The server keeps the consumer state, so a restart resumes where it stopped.
    /// The plugin acks each message after the handler runs.
    ///
    /// This needs `jetstream_enabled = true`.
    pub fn jetstream_consumer<F, Fut>(
        mut self,
        stream: impl Into<String>,
        subjects: Vec<String>,
        durable_name: impl Into<String>,
        filter_subject: impl Into<String>,
        handler: F,
    ) -> Self
    where
        F: Fn(JsMessage) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.stream_consumers.push(StreamConsumer {
            stream: stream.into(),
            subjects,
            durable_name: durable_name.into(),
            filter_subject: filter_subject.into(),
            handler: Arc::new(move |message| Box::pin(handler(message)) as BoxFuture),
        });
        self
    }

    fn resolve(source: &ConfigSource, changes: Vec<Change>) -> Result<NatsConfig, ConfigError> {
        let mut config = match source {
            ConfigSource::Section(section) => NatsConfig::resolve(section)?,
            ConfigSource::Explicit(config) => (**config).clone(),
        };
        for change in changes {
            change(&mut config);
        }
        match source {
            ConfigSource::Section(section) => config.validate_section(section)?,
            ConfigSource::Explicit(_) => config.validate()?,
        }
        Ok(config)
    }
}

/// Makes a boot-stopping error.
fn boot_error(err: impl std::fmt::Display) -> AutumnError {
    AutumnError::internal_server_error_msg(format!("{PLUGIN_NAME}: {err}"))
}

impl Plugin for NatsPlugin {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed(PLUGIN_NAME)
    }

    fn build(self, app: AppBuilder) -> AppBuilder {
        let Self {
            source,
            changes,
            core_consumers,
            stream_consumers,
        } = self;
        let mut app = app;
        if let ConfigSource::Section(section) = &source {
            app = app.config_section(section.clone());
        }
        let resolved = Self::resolve(&source, changes);
        let shared = Arc::new(Shared::default());
        let metrics = Arc::new(Metrics::default());
        app = app.metrics_source("nats", Arc::new(Source(Arc::clone(&metrics))));
        if resolved.as_ref().is_ok_and(|config| config.health_check) {
            app = app.health_indicator("nats", Arc::new(NatsCheck::new(Arc::clone(&shared))));
        }
        let on_start = Arc::clone(&shared);
        let start_metrics = Arc::clone(&metrics);
        let resolved = Arc::new(resolved);
        let core_consumers = Arc::new(core_consumers);
        let stream_consumers = Arc::new(stream_consumers);
        let on_consume = Arc::clone(&shared);
        app.on_startup(move |state| {
            let shared = Arc::clone(&on_start);
            let metrics = Arc::clone(&start_metrics);
            let resolved = Arc::clone(&resolved);
            async move {
                let config = resolved.as_ref().clone().map_err(|err| boot_error(&err))?;
                let nats = Nats::connect_with(config, Arc::clone(&metrics))
                    .await
                    .map_err(|err| boot_error(&err))?;
                state.insert_extension(nats.clone());
                let _ = shared.handle.set(nats);
                tracing::info!("the NATS plugin is ready");
                Ok(())
            }
        })
        .background_task(CONSUMER_TASK, NATS_CONSUMER, move |_state, shutdown| {
            run_consumers(
                Arc::clone(&on_consume),
                Arc::clone(&core_consumers),
                Arc::clone(&stream_consumers),
                shutdown,
            )
        })
        .on_shutdown(move || {
            let shared = Arc::clone(&shared);
            async move {
                shared.shutdown().await;
            }
        })
    }
}

/// Starts the consumers and keeps them until `shutdown`. The
/// `nats.consumers` background task runs this. The framework starts it only
/// on a role that runs [`NATS_CONSUMER`]. Without a connection it does
/// nothing.
pub(crate) async fn run_consumers(
    shared: Arc<Shared>,
    core_consumers: Arc<Vec<CoreConsumer>>,
    stream_consumers: Arc<Vec<StreamConsumer>>,
    shutdown: CancellationToken,
) -> autumn_web::AutumnResult<()> {
    let Some(nats) = shared.handle.get().cloned() else {
        return Ok(());
    };
    for consumer in core_consumers.iter().cloned() {
        spawn_core_consumer(&shared, nats.clone(), consumer);
    }
    for consumer in stream_consumers.iter().cloned() {
        spawn_stream_consumer(&shared, nats.clone(), consumer);
    }
    shutdown.cancelled().await;
    shared.stop_consumers();
    Ok(())
}

/// Spawns a core subscription as a background task.
fn spawn_core_consumer(shared: &Arc<Shared>, nats: Nats, consumer: CoreConsumer) {
    let CoreConsumer {
        subject,
        queue_group,
        handler,
    } = consumer;
    let task = tokio::spawn(async move {
        let result = if let Some(group) = queue_group {
            nats.queue_subscribe(subject.clone(), &group).await
        } else {
            nats.subscribe(subject.clone()).await
        };
        let mut subscriber = match result {
            Ok(subscriber) => subscriber,
            Err(err) => {
                tracing::error!("the NATS consumer on `{subject}` did not start: {err}");
                return;
            }
        };
        tracing::info!("the NATS consumer on `{subject}` runs");
        while let Some(message) = subscriber.next().await {
            nats.received();
            handler(message).await;
        }
        tracing::info!("the NATS consumer on `{subject}` stopped");
    });
    if let Ok(mut tasks) = shared.tasks.lock() {
        tasks.push(task);
    }
}

/// Spawns a `JetStream` durable consumer as a background task.
fn spawn_stream_consumer(shared: &Arc<Shared>, nats: Nats, consumer: StreamConsumer) {
    let StreamConsumer {
        stream,
        subjects,
        durable_name,
        filter_subject,
        handler,
    } = consumer;
    let task = tokio::spawn(async move {
        let jetstream = match nats.jetstream() {
            Ok(jetstream) => jetstream,
            Err(err) => {
                tracing::error!("the JetStream consumer `{durable_name}` did not start: {err}");
                return;
            }
        };
        let stream = match jetstream.get_or_create_stream(&stream, subjects).await {
            Ok(stream) => stream,
            Err(err) => {
                tracing::error!("the JetStream consumer `{durable_name}` did not start: {err}");
                return;
            }
        };
        let mut consumer = match stream
            .durable_consumer(&durable_name, &filter_subject)
            .await
        {
            Ok(consumer) => consumer,
            Err(err) => {
                tracing::error!("the JetStream consumer `{durable_name}` did not start: {err}");
                return;
            }
        };
        tracing::info!("the JetStream consumer `{durable_name}` runs");
        while let Some(next) = consumer.next().await {
            match next {
                Ok(message) => {
                    handler(message.clone()).await;
                    if let Err(err) = message.ack().await {
                        tracing::warn!("the ack of `{durable_name}` did not go through: {err}");
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        "the JetStream consumer `{durable_name}` skipped a message: {err}"
                    );
                }
            }
        }
        tracing::info!("the JetStream consumer `{durable_name}` stopped");
    });
    if let Ok(mut tasks) = shared.tasks.lock() {
        tasks.push(task);
    }
}

impl Nats {
    /// Gets the handle from the app state, for example in a job or a task.
    #[must_use]
    pub fn from_state(state: &AppState) -> Option<Self> {
        state.extension::<Self>().map(|nats| (*nats).clone())
    }
}

impl axum::extract::FromRequestParts<AppState> for Nats {
    type Rejection = AutumnError;

    async fn from_request_parts(
        _parts: &mut http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        Self::from_state(state).ok_or_else(|| NatsError::NotInstalled.into_autumn())
    }
}

#[cfg(test)]
mod tests;
