# autumn-plugin-nats

NATS messaging for the [Autumn](https://autumn-web.app) web framework:
async pub/sub, request/reply, and JetStream (streams, durable consumers,
key-value buckets, object stores).

## Quickstart

Add the plugin and publish from a handler:

```rust,no_run
use autumn_plugin_nats::{Nats, NatsPlugin, NatsResultExt as _};
use autumn_web::prelude::*;

#[post("/events")]
async fn publish_event(nats: Nats, body: String) -> AutumnResult<()> {
    nats.publish("events", body).await.or_http()?;
    Ok(())
}

# async fn run() {
autumn_web::app()
    .plugin(
        NatsPlugin::new()
            .configure(|c| {
                c.url = "nats://localhost:4222".into();
                c.jetstream_enabled = true;
            })
            // Runs as a background task: each message goes to the handler.
            .consumer("events", |message| async move {
                tracing::info!(subject = %message.subject, "got an event");
            }),
    )
    .routes(routes![publish_event])
    .run()
    .await;
# }
```

Run the example (needs a server, for example `docker run -p 4222:4222 nats:2 -js`):

```sh
cargo run --example pubsub
```

## Configuration

The plugin reads `[nats]` in `autumn.toml`. Each key also has an
`AUTUMN_NATS__<KEY>` environment override, which is where secrets belong.

```toml
[nats]
url = "nats://localhost:4222"
# One auth method at most: username/password, nkey_seed, creds_file or token.
username = "app"
password = "secret"
# nkey_seed = "SU..."
# creds_file = "/run/nats/app.creds"
# token = "..."
jetstream_enabled = true
health_check = true
request_timeout_ms = 5000
connect_timeout_ms = 5000
# max_reconnects = 10   # unset reconnects forever
# name = "my-app"       # the client name in server monitoring
```

Profiles work the same as everywhere in Autumn: `[profile.<name>.nats]`
overrides `[nats]`, and `autumn-<name>.toml` overrides both.

## What the plugin gives

- `Nats`: the extractor and the handle. `publish`, `publish_json`,
  `subscribe`, `queue_subscribe`, `request`, `respond`, `flush`, `rtt`,
  `drain`. Get it in a handler as an extractor, or in a job or task with
  `Nats::from_state(&state)`.
- `JetStream` (needs `jetstream_enabled = true`): `stream`, `create_stream`,
  `get_or_create_stream`, `publish` (waits for the server ack), `kv`,
  `create_kv`, `object_store`, `create_object_store`.
- `JsStream::durable_consumer`: a durable push consumer. The server keeps the
  consumer state, so a restart resumes where it stopped.
- `KvBucket`: `put`, `put_json`, `get`, `get_json`, `delete`.
- `ObjectBucket`: `put`, `get`.
- Background consumers: `NatsPlugin::consumer`, `NatsPlugin::queue_consumer`
  and `NatsPlugin::jetstream_consumer` run subscriptions as tasks. The
  JetStream consumer acks each message after the handler runs.
- Roles: the consumers run in the `nats.consumers` background task. Its
  capability is `nats.consumer`, with the parent `job_workers`. So
  `combined` and `worker` run the consumers, and `web` only connects. A
  named role can turn them off with
  `[roles.<name>] capabilities = { "nats.consumer" = false }`.
- A readiness check (`nats`, via a flush round trip) and Prometheus metrics
  (`nats_published_total`, `nats_received_total`, `nats_requests_total`,
  `nats_jetstream_published_total`).

Request/reply:

```rust,no_run
# use autumn_plugin_nats::{Nats, NatsResultExt as _};
# use autumn_web::prelude::*;
# async fn reply(nats: Nats) -> AutumnResult<()> {
// Ask.
let answer = nats.request("math.double", "21").await.or_http()?;
tracing::info!(reply = %String::from_utf8_lossy(&answer.payload), "got a reply");
# Ok(())
# }
# async fn serve(nats: Nats) -> AutumnResult<()> {
// Answer (for example inside a `.consumer` handler).
let mut sub = nats.subscribe("math.double").await.or_http()?;
use futures_util::StreamExt as _;
while let Some(message) = sub.next().await {
    let doubled: u64 = String::from_utf8_lossy(&message.payload).parse().unwrap_or(0) * 2;
    nats.respond(&message, doubled.to_string()).await.or_http()?;
}
# Ok(())
# }
```

JetStream:

```rust,no_run
# use autumn_plugin_nats::{Nats, NatsResultExt as _};
# use autumn_web::prelude::*;
# async fn run(nats: Nats) -> AutumnResult<()> {
let js = nats.jetstream().or_http()?;
let stream = js.get_or_create_stream("events", vec!["events.>".into()]).await.or_http()?;
js.publish("events.created", "{}").await.or_http()?; // waits for the ack

let kv = js.create_kv("sessions").await.or_http()?;
kv.put_json("user:1", &serde_json::json!({"name": "mark"})).await.or_http()?;

let mut consumer = stream.durable_consumer("app", "events.>").await.or_http()?;
while let Some(next) = consumer.next().await {
    let message = next.or_http()?;
    // ... handle it ...
    message.ack().await.or_http()?;
}
# Ok(())
# }
```

## Limits

- A core publish sends at most once. Use JetStream for at-least-once delivery.
- A request waits at most `request_timeout_ms` for the reply.
- Error text never shows the server URL or a credential; `NatsError::detail`
  gives the full message for logs you control.
- The plugin does not start an embedded server. Point `url` at a NATS server.

## Shutdown

At shutdown the plugin aborts the consumer tasks, then drains the connection:
it unsubscribes, flushes pending publishes and closes.

## Known issues

- No live-server integration test has run in CI or in this environment.
  The automated suite builds `examples/pubsub.rs` but does not run it;
  running the example needs a real NATS server at `nats://127.0.0.1:4222`.
  Core pub/sub, request/reply, and the `JetStream` wrappers are unit-tested
  with mocks only.
