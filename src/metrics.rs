//! Call counters. The plugin module adds them to Autumn.
//!
//! # Contract
//!
//! - `nats_published_total` counts publishes, with the label `outcome`: `ok` or `error`.
//! - `nats_received_total` counts messages that background consumers got.
//!   It has no labels.
//! - `nats_requests_total` counts request/reply round trips, with the label `outcome`.
//! - `nats_jetstream_published_total` counts `JetStream` publishes, with the label `outcome`.
//! - No metric name starts with `autumn_`. Each counter name ends with `_total`.

use std::sync::atomic::{AtomicU64, Ordering};

use autumn_web::actuator::{MetricFamily, MetricKind, MetricSample};

/// The outcome label of a counted call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    Ok,
    Error,
}

impl Outcome {
    const fn label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }
}

/// Counters for all calls of one connection.
#[derive(Debug, Default)]
pub(crate) struct Metrics {
    published: AtomicU64,
    publish_errors: AtomicU64,
    received: AtomicU64,
    requests: AtomicU64,
    request_errors: AtomicU64,
    jetstream_published: AtomicU64,
    jetstream_publish_errors: AtomicU64,
}

impl Metrics {
    pub(crate) fn published(&self, outcome: Outcome) {
        match outcome {
            Outcome::Ok => self.published.fetch_add(1, Ordering::Relaxed),
            Outcome::Error => self.publish_errors.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub(crate) fn received(&self) {
        self.received.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn requests(&self, outcome: Outcome) {
        match outcome {
            Outcome::Ok => self.requests.fetch_add(1, Ordering::Relaxed),
            Outcome::Error => self.request_errors.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub(crate) fn jetstream_published(&self, outcome: Outcome) {
        match outcome {
            Outcome::Ok => self.jetstream_published.fetch_add(1, Ordering::Relaxed),
            Outcome::Error => self
                .jetstream_publish_errors
                .fetch_add(1, Ordering::Relaxed),
        };
    }

    /// Builds the Prometheus families.
    pub(crate) fn families(&self) -> Vec<MetricFamily> {
        vec![
            outcome_family(
                "nats_published_total",
                "NATS publishes.",
                self.published.load(Ordering::Relaxed),
                self.publish_errors.load(Ordering::Relaxed),
            ),
            single(
                "nats_received_total",
                "Messages that background consumers got.",
                self.received.load(Ordering::Relaxed),
            ),
            outcome_family(
                "nats_requests_total",
                "NATS request/reply round trips.",
                self.requests.load(Ordering::Relaxed),
                self.request_errors.load(Ordering::Relaxed),
            ),
            outcome_family(
                "nats_jetstream_published_total",
                "JetStream publishes.",
                self.jetstream_published.load(Ordering::Relaxed),
                self.jetstream_publish_errors.load(Ordering::Relaxed),
            ),
        ]
    }
}

#[allow(clippy::cast_precision_loss, reason = "Prometheus values are f64")]
const fn as_f64(raw: u64) -> f64 {
    raw as f64
}

fn single(name: &str, help: &str, value: u64) -> MetricFamily {
    MetricFamily {
        name: name.to_owned(),
        help: help.to_owned(),
        kind: MetricKind::Counter,
        samples: vec![MetricSample {
            labels: Vec::new(),
            value: as_f64(value),
        }],
    }
}

fn outcome_family(name: &str, help: &str, ok: u64, error: u64) -> MetricFamily {
    MetricFamily {
        name: name.to_owned(),
        help: help.to_owned(),
        kind: MetricKind::Counter,
        samples: [Outcome::Ok, Outcome::Error]
            .into_iter()
            .zip([ok, error])
            .map(|(outcome, value)| MetricSample {
                labels: vec![("outcome".to_owned(), outcome.label().to_owned())],
                value: as_f64(value),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests;
