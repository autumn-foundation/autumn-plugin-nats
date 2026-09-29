//! Publish a message and receive it.
//!
//! Run a NATS server first, for example with Docker:
//!
//! ```sh
//! docker run -p 4222:4222 nats:2 -js
//! ```
//!
//! Then run the example:
//!
//! ```sh
//! cargo run --example pubsub
//! ```
//!
//! Set `NATS_URL` to point at another server.

use autumn_plugin_nats::{Nats, NatsConfig};
use futures_util::StreamExt as _;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = NatsConfig::default();
    if let Ok(url) = std::env::var("NATS_URL") {
        config.url = url;
    }
    let nats = Nats::connect(config).await?;

    let mut subscriber = nats.subscribe("demo.hello").await?;
    nats.publish("demo.hello", "hello from autumn-plugin-nats")
        .await?;
    nats.flush().await?;

    let message = subscriber
        .next()
        .await
        .ok_or_else(|| std::io::Error::other("the server sent no message"))?;
    println!(
        "got on `{}`: {}",
        message.subject,
        String::from_utf8_lossy(&message.payload)
    );

    nats.drain().await?;
    Ok(())
}
