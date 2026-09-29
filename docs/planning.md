# Planning

## Goal

NATS messaging for Autumn (lane 1: service integration), built on
`async-nats` 0.50. Shipped as a private repo under `madmax983`; Mark
promotes winners to `autumn-foundation`.

## Scope (v0.1)

- Connection management: URL, username/password, NKey seed, creds file, token.
- Async publish/subscribe, queue groups, request/reply.
- JetStream: streams, durable push consumers, KV buckets, object stores.
- `Nats` extractor for handlers; `Nats::from_state` for jobs and tasks.
- Background consumers registered on the plugin (core, queue, JetStream).
- `[nats]` config with env-var overrides for secrets.
- Health check (flush round trip) and Prometheus metrics.
- `examples/pubsub.rs`.

## Non-goals (v0.1)

- Embedded NATS server. The plugin connects to an existing server.
- NATS service API (`async-nats` `service` feature). The request/reply
  helpers cover the common case; a service-API module can come later.
- MQTT/websocket transports. TLS (`tls://`) works through the `ring` feature.

## API grounding

- Autumn patterns (`Plugin::build`, `on_startup`/`on_shutdown`,
  `HealthIndicator`, `MetricsSource`, extractor via `FromRequestParts`,
  layered config) taken from the MCP docs and from
  `autumn-foundation/autumn-plugin-rocksdb`.
- `async-nats` 0.50 APIs verified against the crate source:
  `ConnectOptions::{user_and_password, nkey, credentials, token,
  connection_timeout, max_reconnects, connect}` (there is no
  `credentials_file` in 0.50: the plugin reads the file and passes the
  contents to `credentials`),
  `Client::{publish, subscribe, queue_subscribe, request, flush, drain,
  new_inbox}`, `jetstream::{new, Context::{create_stream,
  get_or_create_stream, get_stream, create_key_value, get_key_value,
  create_object_store, get_object_store, publish}}`,
  `Stream::get_or_create_consumer` with `consumer::push::Config`,
  `Consumer::messages`, KV `Store::{put, get, delete}`, object store
  `ObjectStore::{put, get}`.

## Test strategy

- Pure unit tests per module: config layering/validation, error mapping,
  metrics, health-check states, plugin builder.
- Connection tests point at a refused port (`127.0.0.1:1`): no server needed.
- `cargo test` builds the example but does not run it; run it by hand
  against a local server before release.
