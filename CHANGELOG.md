# Changelog

All notable changes to this project follow
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Changed

- Support `autumn-web` 0.9. The plugin now needs `autumn-web` 0.9.

## [0.1.0] - 2026-09-29

### Added

- Initial release.
- `NatsPlugin` with `::new()`, `.config_section()`, `.config()`,
  `.configure()`, `.consumer()`, `.queue_consumer()` and
  `.jetstream_consumer()`.
- `Nats` handle and extractor: `connect`, `publish`, `publish_json`,
  `subscribe`, `queue_subscribe`, `request`, `respond`, `flush`, `rtt`,
  `drain`, `jetstream`, `from_state`.
- Layered `[nats]` config: defaults, `autumn.toml`, inline profiles,
  `autumn-<profile>.toml`, `AUTUMN_NATS__<KEY>` environment overrides.
  Auth: username/password, NKey seed, creds file, or token (at most one).
- JetStream: streams, durable push consumers, KV buckets, object stores.
- Readiness check (flush round trip) and Prometheus metrics.
- `examples/pubsub.rs`.
