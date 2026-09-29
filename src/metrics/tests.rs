//! Tests for the metrics counters.

use super::{Metrics, Outcome};

#[test]
fn counters_start_at_zero() {
    let families = Metrics::default().families();
    let names: Vec<&str> = families.iter().map(|family| family.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "nats_published_total",
            "nats_received_total",
            "nats_requests_total",
            "nats_jetstream_published_total"
        ]
    );
    for family in &families {
        for sample in &family.samples {
            assert_eq!(sample.value, 0.0);
        }
    }
}

#[test]
fn outcome_labels_count_ok_and_error() {
    let metrics = Metrics::default();
    metrics.published(Outcome::Ok);
    metrics.published(Outcome::Ok);
    metrics.published(Outcome::Error);
    metrics.received();
    metrics.requests(Outcome::Error);
    metrics.jetstream_published(Outcome::Ok);

    let families = metrics.families();
    let published = &families[0];
    let outcomes: Vec<(&str, f64)> = published
        .samples
        .iter()
        .map(|sample| {
            (
                sample
                    .labels
                    .iter()
                    .find(|(key, _)| key == "outcome")
                    .map_or("", |(_, value)| value.as_str()),
                sample.value,
            )
        })
        .collect();
    assert!(outcomes.contains(&("ok", 2.0)), "{outcomes:?}");
    assert!(outcomes.contains(&("error", 1.0)), "{outcomes:?}");

    let received = &families[1];
    assert_eq!(received.samples[0].value, 1.0);

    let requests = &families[2];
    let total: f64 = requests.samples.iter().map(|sample| sample.value).sum();
    assert_eq!(total, 1.0);

    let jetstream = &families[3];
    let total: f64 = jetstream.samples.iter().map(|sample| sample.value).sum();
    assert_eq!(total, 1.0);
}
