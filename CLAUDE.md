# CLAUDE.md — autumn-plugin-nats

Agent guidance for working in this repo.

## Commands

```sh
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_BUILD_JOBS=2
export CARGO_TARGET_DIR=~/workspace/autumn-arena/target   # shared with the arena; never `cargo clean` it
export TMPDIR=~/workspace/.tmp-cargo

cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
cargo run --example pubsub   # needs a NATS server on localhost:4222 (or NATS_URL)
```

## Architecture

| Module | What it owns |
|---|---|
| `config` | Layered `[nats]` config: defaults → `autumn.toml` → inline profiles → `autumn-<profile>.toml` → `AUTUMN_NATS__<KEY>` env. Validation. Pure. |
| `error` | `NatsError` (thiserror) + `ErrorKind` + HTTP status mapping + `into_autumn`. Error text never shows the URL or a credential; `detail()` does. |
| `client` | `Nats`: connect (auth), publish/subscribe/request/respond/flush/rtt/drain, metrics. Clones share the connection. |
| `jetstream` | `JetStream`, `JsStream`, `JsConsumer`/`JsMessage` (durable push consumer), `KvBucket`, `ObjectBucket`. Needs `jetstream_enabled = true`. |
| `plugin` | `NatsPlugin` builder, `build()` wiring, background consumer tasks, `Nats::from_state` + the `FromRequestParts` extractor. |
| `health` | `HealthIndicator`: flush round trip, ≤1s, reports `rtt_ms`. |
| `metrics` | Atomic counters → `MetricFamily` list. The plugin registers it as the `nats` metrics source. |

## Rules

- README Rust snippets are doctests. Make them compile; use `no_run`, not `ignore`.
  `tests/doc_snippets.rs` checks the TOML snippets and the version pins in the docs.
- Ground Autumn APIs against the docs MCP (`~/workspace/skills/autumn-mcp/bin/mcp.py`), never training memory.
- Ground `async-nats` APIs against docs.rs (this repo pins async-nats 0.50; the API moved a lot since 0.3x).
- Edition 2024. No `unwrap`/`expect`/`panic`/`todo`/`unimplemented` in production code (tests may use them). `unsafe_code = forbid`.
- Docs and comments in ASD-STE100: short sentences, active voice, simple present tense.
- Unit tests live in `src/<module>/tests.rs`. Property tests where valuable.
- Connection tests use a refused port (`127.0.0.1:1`) so they need no server.
- `examples/pubsub.rs` needs a real server; it only builds in CI.
- Never log or print the server URL with credentials, the NKey seed, the token, or the creds file contents.
