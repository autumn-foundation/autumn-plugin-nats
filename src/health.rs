//! The readiness check.
//!
//! # Contract
//!
//! - The check is down before the plugin starts and after the shutdown.
//! - The check is up if a flush round trip answers in time. The details carry
//!   the round-trip time in milliseconds. They never show the server URL.
//! - The check waits at most 1 second, so it answers before the Autumn timeout of 2 seconds.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use autumn_web::actuator::{HealthCheckOutput, HealthIndicator};

use crate::plugin::Shared;

/// The future type of a health check.
type BoxFuture<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The longest wait of the check for the server.
pub(crate) const CHECK_WAIT: Duration = Duration::from_secs(1);

/// Checks the NATS connection with a flush round trip.
pub(crate) struct NatsCheck {
    shared: Arc<Shared>,
}

impl NatsCheck {
    pub(crate) const fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }
}

impl HealthIndicator for NatsCheck {
    fn check(&self) -> BoxFuture<'_, HealthCheckOutput> {
        Box::pin(async move {
            let Some(nats) = self.shared.handle.get() else {
                return HealthCheckOutput::down()
                    .with_details(details(&[("state", "not started")]));
            };
            let rtt = tokio::time::timeout(CHECK_WAIT, nats.rtt()).await;
            match rtt {
                Ok(Ok(rtt)) => {
                    let rtt_ms = rtt.as_millis().to_string();
                    HealthCheckOutput::up().with_details(details(&[("rtt_ms", rtt_ms.as_str())]))
                }
                _ => HealthCheckOutput::down().with_details(details(&[("state", "no round trip")])),
            }
        })
    }
}

/// Builds the details map.
fn details(pairs: &[(&str, &str)]) -> HashMap<String, serde_json::Value> {
    pairs
        .iter()
        .map(|(key, value)| {
            (
                (*key).to_owned(),
                serde_json::Value::String((*value).to_owned()),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests;
