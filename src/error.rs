//! The public error type.
//!
//! # Contract
//!
//! - A NATS error keeps its kind. The error text shows the kind only.
//!   [`NatsError::detail`] gives the full message, which can hold a server URL
//!   or a credential path. Logs and `Debug` output never show `detail`.
//! - A timeout gives HTTP 504. A refused or lost connection gives 503.
//!   [`NatsError::NotInstalled`] and [`NatsError::JetStreamDisabled`] give 500.
//!   All other errors give 500.
//! - The `Debug` output is the error text. It does not show `detail`.

use autumn_web::AutumnError;
use http::StatusCode;

use crate::config::ConfigError;

/// The kind of a [`NatsError`]. It classifies the error without the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The configuration is not valid.
    Config,
    /// The plugin is not installed in the app.
    NotInstalled,
    /// The server refused the connection or the connection broke.
    Connect,
    /// A publish did not go through.
    Publish,
    /// A subscribe did not go through.
    Subscribe,
    /// A request/reply round trip did not go through.
    Request,
    /// A flush did not go through.
    Flush,
    /// A `JetStream` call did not go through, or `JetStream` is not enabled.
    JetStream,
    /// The call took longer than the timeout.
    Timeout,
    /// The message has no reply subject.
    NoReplySubject,
}

/// An error from the plugin.
///
/// The `Debug` output is the error text. It does not show `detail`.
#[derive(Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum NatsError {
    /// The configuration is not valid.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// The plugin is not installed in the app.
    #[error("the NATS plugin is not installed in this app")]
    NotInstalled,
    /// The server refused the connection or the connection broke.
    ///
    /// The text does not show `detail`, because it can hold a server URL.
    #[error("the NATS server refused the connection")]
    #[non_exhaustive]
    Connect {
        /// The full connection error message.
        detail: String,
    },
    /// A publish did not go through.
    #[error("the publish did not go through")]
    #[non_exhaustive]
    Publish {
        /// The full publish error message.
        detail: String,
    },
    /// A subscribe did not go through.
    #[error("the subscribe did not go through")]
    #[non_exhaustive]
    Subscribe {
        /// The full subscribe error message.
        detail: String,
    },
    /// A request/reply round trip did not go through.
    #[error("the request did not go through")]
    #[non_exhaustive]
    Request {
        /// The full request error message.
        detail: String,
    },
    /// A flush did not go through.
    #[error("the flush did not go through")]
    #[non_exhaustive]
    Flush {
        /// The full flush error message.
        detail: String,
    },
    /// A `JetStream` call did not go through.
    #[error("the JetStream call did not go through: {op}")]
    #[non_exhaustive]
    JetStream {
        /// The `JetStream` operation, for example `create_stream`.
        op: String,
        /// The full `JetStream` error message.
        detail: String,
    },
    /// `JetStream` is not enabled in the configuration.
    #[error("JetStream is not enabled: set `jetstream_enabled = true`")]
    JetStreamDisabled,
    /// The call took longer than the timeout.
    #[error("the NATS call timed out")]
    Timeout,
    /// The message has no reply subject.
    #[error("the message has no reply subject")]
    NoReplySubject,
}

impl NatsError {
    /// Makes a [`NatsError::Connect`] from any error. The message stays out of the error text.
    pub(crate) fn connect(detail: impl std::fmt::Display) -> Self {
        Self::Connect {
            detail: detail.to_string(),
        }
    }

    /// Makes a [`NatsError::Publish`] from any error.
    pub(crate) fn publish(detail: impl std::fmt::Display) -> Self {
        Self::Publish {
            detail: detail.to_string(),
        }
    }

    /// Makes a [`NatsError::Subscribe`] from any error.
    pub(crate) fn subscribe(detail: impl std::fmt::Display) -> Self {
        Self::Subscribe {
            detail: detail.to_string(),
        }
    }

    /// Makes a [`NatsError::Request`] from any error.
    pub(crate) fn request(detail: impl std::fmt::Display) -> Self {
        Self::Request {
            detail: detail.to_string(),
        }
    }

    /// Makes a [`NatsError::Flush`] from any error.
    pub(crate) fn flush(detail: impl std::fmt::Display) -> Self {
        Self::Flush {
            detail: detail.to_string(),
        }
    }

    /// Makes a [`NatsError::JetStream`] from any error.
    pub(crate) fn jetstream(op: impl Into<String>, detail: impl std::fmt::Display) -> Self {
        Self::JetStream {
            op: op.into(),
            detail: detail.to_string(),
        }
    }

    /// Gives the kind of the error.
    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        match self {
            Self::Config(_) => ErrorKind::Config,
            Self::NotInstalled => ErrorKind::NotInstalled,
            Self::Connect { .. } => ErrorKind::Connect,
            Self::Publish { .. } => ErrorKind::Publish,
            Self::Subscribe { .. } => ErrorKind::Subscribe,
            Self::Request { .. } => ErrorKind::Request,
            Self::Flush { .. } => ErrorKind::Flush,
            Self::JetStream { .. } | Self::JetStreamDisabled => ErrorKind::JetStream,
            Self::Timeout => ErrorKind::Timeout,
            Self::NoReplySubject => ErrorKind::NoReplySubject,
        }
    }

    /// Gives the full error message. It can hold a server URL or a credential path.
    #[must_use]
    pub fn detail(&self) -> Option<&str> {
        match self {
            Self::Connect { detail }
            | Self::Publish { detail }
            | Self::Subscribe { detail }
            | Self::Request { detail }
            | Self::Flush { detail }
            | Self::JetStream { detail, .. } => Some(detail),
            Self::Config(_)
            | Self::NotInstalled
            | Self::JetStreamDisabled
            | Self::Timeout
            | Self::NoReplySubject => None,
        }
    }

    /// Maps the error to an HTTP status code.
    #[must_use]
    pub const fn status(&self) -> StatusCode {
        match self {
            Self::Timeout => StatusCode::GATEWAY_TIMEOUT,
            Self::Connect { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::Config(_)
            | Self::NotInstalled
            | Self::Publish { .. }
            | Self::Subscribe { .. }
            | Self::Request { .. }
            | Self::Flush { .. }
            | Self::JetStream { .. }
            | Self::JetStreamDisabled
            | Self::NoReplySubject => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Converts the error into an [`AutumnError`] with the mapped status code.
    #[must_use]
    pub fn into_autumn(self) -> AutumnError {
        let status = self.status();
        AutumnError::internal_server_error(self).with_status(status)
    }
}

impl std::fmt::Debug for NatsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The Debug output is the error text. It never shows `detail`.
        write!(f, "{self}")
    }
}

/// Adds [`or_http`](NatsResultExt::or_http) to `Result<T, NatsError>`.
pub trait NatsResultExt<T> {
    /// Converts the error with [`NatsError::into_autumn`].
    ///
    /// # Errors
    ///
    /// Returns the converted error.
    fn or_http(self) -> Result<T, AutumnError>;
}

impl<T> NatsResultExt<T> for Result<T, NatsError> {
    fn or_http(self) -> Result<T, AutumnError> {
        self.map_err(NatsError::into_autumn)
    }
}

#[cfg(test)]
mod tests;
