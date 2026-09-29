//! The `[nats]` section of `autumn.toml`.
//!
//! # Contract
//!
//! Each layer overrides the layers before it:
//!
//! 1. The defaults.
//! 2. `[nats]` in `autumn.toml`.
//! 3. `[profile.<name>.nats]` in `autumn.toml`.
//! 4. `[nats]` in `autumn-<name>.toml`.
//! 5. `AUTUMN_NATS__<KEY>` variables. `AUTUMN_NATS__URL` sets `url`.
//!
//! The result must pass [`NatsConfig::validate`]. Unknown keys are errors.
//!
//! ```toml
//! [nats]
//! url = "nats://localhost:4222"
//! username = "app"
//! password = "secret"
//! jetstream_enabled = true
//! request_timeout_ms = 5000
//! ```

use std::path::{Path, PathBuf};

use autumn_web::config::Env;
use serde::{Deserialize, Serialize};

/// The default section name.
pub const DEFAULT_SECTION: &str = "nats";

/// The default server URL.
pub const DEFAULT_URL: &str = "nats://localhost:4222";

/// A configuration that is not valid.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(pub(crate) String);

/// The authentication method that the configuration selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuthMethod {
    /// No credentials. The server must allow anonymous connections.
    None,
    /// `username` and `password`.
    UserPassword,
    /// `nkey_seed`.
    Nkey,
    /// `creds_file`.
    CredsFile,
    /// `token`.
    Token,
}

/// The plugin settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
pub struct NatsConfig {
    /// The NATS server URL, for example `nats://localhost:4222` or `tls://nats:4222`.
    pub url: String,
    /// The username for username/password auth. Empty means no username auth.
    pub username: String,
    /// The password for username/password auth. Empty means no password.
    pub password: String,
    /// The `NKey` seed for `NKey` auth. Empty means no `NKey` auth.
    pub nkey_seed: String,
    /// The path of a `.creds` file for credential-file auth. Empty means none.
    pub creds_file: String,
    /// The token for token auth. Empty means no token auth.
    pub token: String,
    /// If `true`, the plugin builds the `JetStream` context at startup.
    /// `JetStream` calls fail with [`crate::NatsError::JetStreamDisabled`] otherwise.
    pub jetstream_enabled: bool,
    /// If `true`, the plugin adds a readiness check.
    pub health_check: bool,
    /// The request/reply timeout in milliseconds.
    pub request_timeout_ms: u64,
    /// The initial-connect timeout in milliseconds.
    pub connect_timeout_ms: u64,
    /// The most reconnect attempts after a disconnect. `None` reconnects forever.
    pub max_reconnects: Option<u64>,
    /// The client name that the server sees in monitoring. Empty sets no name.
    pub name: String,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            url: DEFAULT_URL.to_owned(),
            username: String::new(),
            password: String::new(),
            nkey_seed: String::new(),
            creds_file: String::new(),
            token: String::new(),
            jetstream_enabled: false,
            health_check: true,
            request_timeout_ms: 5_000,
            connect_timeout_ms: 5_000,
            max_reconnects: None,
            name: String::new(),
        }
    }
}

/// The type of a configuration leaf, for environment values.
#[derive(Clone, Copy)]
enum Kind {
    Text,
    Unsigned,
    Bool,
}

/// Each leaf key and its type.
const LEAVES: &[(&str, Kind)] = &[
    ("url", Kind::Text),
    ("username", Kind::Text),
    ("password", Kind::Text),
    ("nkey_seed", Kind::Text),
    ("creds_file", Kind::Text),
    ("token", Kind::Text),
    ("jetstream_enabled", Kind::Bool),
    ("health_check", Kind::Bool),
    ("request_timeout_ms", Kind::Unsigned),
    ("connect_timeout_ms", Kind::Unsigned),
    ("max_reconnects", Kind::Unsigned),
    ("name", Kind::Text),
];

/// The longest request timeout: five minutes.
const MAX_TIMEOUT_MS: u64 = 300_000;

impl NatsConfig {
    /// Returns the authentication method that the credential fields select.
    #[must_use]
    pub const fn auth_method(&self) -> AuthMethod {
        // The validator allows at most one of these.
        if !self.username.is_empty() {
            AuthMethod::UserPassword
        } else if !self.nkey_seed.is_empty() {
            AuthMethod::Nkey
        } else if !self.creds_file.is_empty() {
            AuthMethod::CredsFile
        } else if !self.token.is_empty() {
            AuthMethod::Token
        } else {
            AuthMethod::None
        }
    }

    /// Reads `[section]` from the app files and the environment.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] if a file is not valid TOML or a value is not valid.
    pub fn resolve(section: &str) -> Result<Self, ConfigError> {
        autumn_web::dotenv::os_env_with_dotenv().map_or_else(
            |_| Self::resolve_with_env(section, &autumn_web::config::OsEnv),
            |env| Self::resolve_with_env(section, &env),
        )
    }

    /// Reads `[section]` with `env` as the environment.
    ///
    /// # Errors
    ///
    /// See [`resolve`](Self::resolve).
    pub fn resolve_with_env(section: &str, env: &dyn Env) -> Result<Self, ConfigError> {
        let (selected, profile) = active_profile(env);
        let mut merged = toml::Table::new();
        if let Some(base) = read_toml(&config_file("autumn.toml", env))? {
            merge_section(&mut merged, base.get(section), section)?;
            for name in inline_profile_names(&profile) {
                let inline = base
                    .get("profile")
                    .and_then(|p| p.get(name))
                    .and_then(|p| p.get(section));
                merge_section(&mut merged, inline, section)?;
            }
        }
        for name in autumn_web::config::profile_override_file_lookup_names(&profile, &selected) {
            if let Some(file) = read_toml(&config_file(&format!("autumn-{name}.toml"), env))? {
                merge_section(&mut merged, file.get(section), section)?;
                break;
            }
        }
        apply_env(&mut merged, section, env)?;
        let config: Self = toml::Value::Table(merged)
            .try_into()
            .map_err(|err| ConfigError(format!("[{section}]: {err}")))?;
        config.validate_section(section)?;
        Ok(config)
    }

    /// Checks each value.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] that names the first key that is not valid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validate_section(DEFAULT_SECTION)
    }

    /// Checks each value. The errors name keys in `section`.
    pub(crate) fn validate_section(&self, section: &str) -> Result<(), ConfigError> {
        let fail = |key: &str, rule: &str| Err(ConfigError(format!("{section}.{key} {rule}")));
        if self.url.trim().is_empty() {
            return fail("url", "must be a NATS server URL");
        }
        if self.url.trim() != self.url {
            return fail("url", "must not start or end with white space");
        }
        if self.url.chars().any(char::is_whitespace) {
            return fail("url", "must not contain white space");
        }
        let Some((scheme, rest)) = self.url.split_once("://") else {
            return fail("url", "must look like nats://host:port");
        };
        let scheme_ok = matches!(scheme, "nats" | "tls" | "ws" | "wss");
        if !scheme_ok || rest.is_empty() {
            return fail("url", "must look like nats://host:port");
        }
        let mut methods = 0;
        if !self.username.is_empty() {
            methods += 1;
            if self.password.is_empty() {
                return fail("password", "must be set when username is set");
            }
        }
        if self.username.is_empty() && !self.password.is_empty() {
            return fail("username", "must be set when password is set");
        }
        if !self.nkey_seed.is_empty() {
            methods += 1;
        }
        if !self.creds_file.is_empty() {
            methods += 1;
        }
        if !self.token.is_empty() {
            methods += 1;
        }
        if methods > 1 {
            return fail(
                "username",
                "must use one auth method: username/password, nkey_seed, creds_file or token",
            );
        }
        if !(1..=MAX_TIMEOUT_MS).contains(&self.request_timeout_ms) {
            return fail(
                "request_timeout_ms",
                "must be from 1 to 300000 (five minutes)",
            );
        }
        if !(1..=MAX_TIMEOUT_MS).contains(&self.connect_timeout_ms) {
            return fail(
                "connect_timeout_ms",
                "must be from 1 to 300000 (five minutes)",
            );
        }
        Ok(())
    }
}

/// Gives the selected profile text and the normalized profile, as Autumn does.
fn active_profile(env: &dyn Env) -> (String, String) {
    let selected = ["AUTUMN_ENV", "AUTUMN_PROFILE"]
        .iter()
        .filter_map(|key| env.var(key).ok())
        .map(|value| value.trim().to_owned())
        .find(|value| !value.is_empty())
        .unwrap_or_else(|| {
            let release = env.var("AUTUMN_IS_DEBUG").is_ok_and(|v| v == "0");
            if release { "prod" } else { "dev" }.to_owned()
        });
    let profile =
        autumn_web::config::normalize_profile_name(&selected).unwrap_or_else(|| "dev".to_owned());
    (selected, profile)
}

/// The inline profile names to read, in order. The canonical name is last.
fn inline_profile_names(profile: &str) -> Vec<&str> {
    match profile {
        "prod" => vec!["production", "prod"],
        "dev" => vec!["development", "dev"],
        other => vec![other],
    }
}

/// Finds a config file in `AUTUMN_MANIFEST_DIR`, or else in the working directory.
fn config_file(name: &str, env: &dyn Env) -> PathBuf {
    env.var("AUTUMN_MANIFEST_DIR")
        .ok()
        .map(|dir| Path::new(&dir).join(name))
        .filter(|path| path.exists())
        .unwrap_or_else(|| PathBuf::from(name))
}

/// Reads a TOML file. A missing file is `None`.
fn read_toml(path: &Path) -> Result<Option<toml::Table>, ConfigError> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(ConfigError(format!(
                "cannot read {}: {err}",
                path.display()
            )));
        }
    };
    toml::from_str(&text)
        .map(Some)
        .map_err(|err| ConfigError(format!("{}: {err}", path.display())))
}

/// Merges the `[section]` table of a file into the merged table.
fn merge_section(
    into: &mut toml::Table,
    section: Option<&toml::Value>,
    name: &str,
) -> Result<(), ConfigError> {
    let Some(table) = section else { return Ok(()) };
    let toml::Value::Table(table) = table else {
        return Err(ConfigError(format!("[{name}] must be a table")));
    };
    deep_merge(into, table);
    Ok(())
}

/// Merges `layer` into `into`. Tables merge key by key; other values replace.
fn deep_merge(into: &mut toml::Table, layer: &toml::Table) {
    for (key, value) in layer {
        match (into.get_mut(key), value) {
            (Some(toml::Value::Table(into_table)), toml::Value::Table(layer_table)) => {
                deep_merge(into_table, layer_table);
            }
            _ => {
                into.insert(key.clone(), value.clone());
            }
        }
    }
}

/// Applies `AUTUMN_<SECTION>__<KEY>` variables to the merged table.
fn apply_env(into: &mut toml::Table, section: &str, env: &dyn Env) -> Result<(), ConfigError> {
    let prefix = format!("AUTUMN_{}__", section.to_ascii_uppercase());
    for (key, kind) in LEAVES {
        let name = format!("{prefix}{}", key.to_ascii_uppercase());
        let Ok(raw) = env.var(&name) else { continue };
        let value = match kind {
            Kind::Text => toml::Value::String(raw),
            Kind::Bool => match raw.trim() {
                "true" | "1" => toml::Value::Boolean(true),
                "false" | "0" => toml::Value::Boolean(false),
                _ => return Err(ConfigError(format!("{name} must be `true` or `false`"))),
            },
            Kind::Unsigned => raw
                .parse::<u64>()
                .map(|n| toml::Value::Integer(i64::try_from(n).unwrap_or(i64::MAX)))
                .map_err(|_| ConfigError(format!("{name} must be an unsigned integer")))?,
        };
        into.insert((*key).to_owned(), value);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
