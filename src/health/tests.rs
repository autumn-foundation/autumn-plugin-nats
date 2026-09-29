//! Tests for the readiness check.

use std::sync::Arc;

use autumn_web::actuator::{HealthIndicator, HealthStatus};

use super::{NatsCheck, details};
use crate::plugin::Shared;

#[test]
fn details_builds_a_string_map() {
    let map = details(&[("state", "not started")]);
    assert_eq!(
        map.get("state"),
        Some(&serde_json::Value::String("not started".into()))
    );
}

#[tokio::test]
async fn check_is_down_before_startup() {
    let check = NatsCheck::new(Arc::new(Shared::default()));
    let output = check.check().await;
    assert_eq!(output.status, HealthStatus::Down);
    assert_eq!(
        output.details.get("state"),
        Some(&serde_json::Value::String("not started".into()))
    );
}
