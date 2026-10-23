//! Async configuration providers for node startup.
//!
//! The provider boundary is intentionally independent from any configuration
//! format. A provider can return a static [`crate::node::NodeConfig`], fetch values from a
//! control plane, or compose both approaches without blocking the startup
//! executor. The optional `toml` feature supplies a small static-file adapter.

use std::fmt;

use crate::node::NodeConfig;

/// Errors returned while resolving a configuration provider.
#[derive(Debug)]
pub enum ConfigurationError {
    /// The provider could not read its backing source.
    Io(std::io::Error),
    /// The backing source could not be decoded.
    Parse(String),
    /// The provider returned an invalid configuration value.
    Invalid(String),
}

impl fmt::Display for ConfigurationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "configuration I/O error: {error}"),
            Self::Parse(error) => write!(f, "configuration parse error: {error}"),
            Self::Invalid(error) => write!(f, "invalid configuration: {error}"),
        }
    }
}

impl std::error::Error for ConfigurationError {}

impl From<std::io::Error> for ConfigurationError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Resolve a node configuration asynchronously before startup.
///
/// Implementations may read a static file or retrieve live values from a
/// remote configuration service. The provider is resolved once by
/// [`crate::node::Node::start_with_provider`]; changing values after startup
/// is an implementation concern for callers that restart or reconfigure a
/// node explicitly.
#[allow(async_fn_in_trait)]
pub trait ConfigurationProvider: Send + Sync {
    /// Resolve the complete node configuration, retaining defaults for values
    /// the provider does not override.
    async fn resolve(&self) -> Result<NodeConfig, ConfigurationError>;
}

impl ConfigurationProvider for NodeConfig {
    async fn resolve(&self) -> Result<NodeConfig, ConfigurationError> {
        Ok(self.clone())
    }
}

/// Default static configuration path used by [`TomlConfigProvider`].
#[cfg(feature = "toml")]
pub const DEFAULT_CONFIG_PATH: &str = "actordb.toml";

/// Load scalar node settings from a TOML file while retaining programmatic
/// dependencies such as the network, store, callbacks, and placement
/// controller from [`NodeConfig::default`].
#[cfg(feature = "toml")]
#[derive(Debug, Clone)]
pub struct TomlConfigProvider {
    path: std::path::PathBuf,
}

#[cfg(feature = "toml")]
impl TomlConfigProvider {
    /// Load the conventional `actordb.toml` path.
    pub fn default_path() -> Self {
        Self::from_path(DEFAULT_CONFIG_PATH)
    }

    /// Load a caller-selected static path.
    pub fn from_path(path: impl Into<std::path::PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// The path that will be read during resolution.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

#[cfg(feature = "toml")]
#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct TomlNodeConfig {
    flush_batch_size: Option<usize>,
    flush_interval_ms: Option<u64>,
    fragment_count: Option<usize>,
    clean_interval_ms: Option<u64>,
    snapshot_interval_ms: Option<u64>,
    max_clock_skew_ms: Option<u64>,
    placement_poll_interval_ms: Option<u64>,
    placement_drain_warn_interval_ms: Option<u64>,
    default_pool: Option<TomlPoolConfig>,
    high_pool: Option<TomlPoolConfig>,
    background_pool: Option<TomlPoolConfig>,
    replication: Option<TomlReplicationConfig>,
    members: std::collections::HashMap<String, String>,
    fragment_map: std::collections::HashMap<u32, Vec<String>>,
}

#[cfg(feature = "toml")]
#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct TomlPoolConfig {
    workers: Option<usize>,
    queue_cap: Option<usize>,
    rate_limit_per_sec: Option<u32>,
}

#[cfg(feature = "toml")]
#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct TomlReplicationConfig {
    batch_size: Option<usize>,
    batch_interval_ms: Option<u64>,
    max_in_flight: Option<usize>,
    buffer_capacity: Option<usize>,
    retained_byte_capacity: Option<usize>,
    consistency_interval_ms: Option<u64>,
    reconcile_timeout_ms: Option<u64>,
}

#[cfg(feature = "toml")]
fn apply_pool(pool: &mut crate::actor::PoolConfig, values: TomlPoolConfig) {
    if let Some(value) = values.workers {
        pool.workers = value;
    }
    if let Some(value) = values.queue_cap {
        pool.queue_cap = value;
    }
    if values.rate_limit_per_sec.is_some() {
        pool.rate_limit_per_sec = values.rate_limit_per_sec;
    }
}

#[cfg(feature = "toml")]
impl ConfigurationProvider for TomlConfigProvider {
    async fn resolve(&self) -> Result<NodeConfig, ConfigurationError> {
        let source = tokio::fs::read_to_string(&self.path).await?;
        let values: TomlNodeConfig = toml::from_str(&source)
            .map_err(|error| ConfigurationError::Parse(error.to_string()))?;
        let mut config = NodeConfig::default();

        if let Some(value) = values.flush_batch_size {
            config.flush_batch_size = value;
        }
        if let Some(value) = values.flush_interval_ms {
            config.flush_interval = std::time::Duration::from_millis(value);
        }
        if let Some(value) = values.fragment_count {
            config.fragment_count = value;
        }
        if let Some(value) = values.clean_interval_ms {
            config.clean_interval = std::time::Duration::from_millis(value);
        }
        if let Some(value) = values.snapshot_interval_ms {
            config.snapshot_interval = std::time::Duration::from_millis(value);
        }
        if let Some(value) = values.max_clock_skew_ms {
            config.max_clock_skew = std::time::Duration::from_millis(value);
        }
        if let Some(value) = values.placement_poll_interval_ms {
            config.placement_poll_interval = std::time::Duration::from_millis(value);
        }
        if let Some(value) = values.placement_drain_warn_interval_ms {
            config.placement_drain_warn_interval = std::time::Duration::from_millis(value);
        }
        if let Some(values) = values.default_pool {
            apply_pool(&mut config.default_pool, values);
        }
        if let Some(values) = values.high_pool {
            apply_pool(&mut config.high_pool, values);
        }
        if let Some(values) = values.background_pool {
            apply_pool(&mut config.background_pool, values);
        }
        if let Some(values) = values.replication {
            if let Some(value) = values.batch_size {
                config.replication.batch_size = value;
            }
            if let Some(value) = values.batch_interval_ms {
                config.replication.batch_interval = std::time::Duration::from_millis(value);
            }
            if let Some(value) = values.max_in_flight {
                config.replication.max_in_flight = value;
            }
            if let Some(value) = values.buffer_capacity {
                config.replication.buffer_capacity = value;
            }
            if let Some(value) = values.retained_byte_capacity {
                config.replication.retained_byte_capacity = value;
            }
            if let Some(value) = values.consistency_interval_ms {
                config.replication.consistency_interval = std::time::Duration::from_millis(value);
            }
            if let Some(value) = values.reconcile_timeout_ms {
                config.replication.reconcile_timeout = std::time::Duration::from_millis(value);
            }
        }
        for (node_id, address) in values.members {
            let address = address.parse().map_err(|error| {
                ConfigurationError::Invalid(format!("member {node_id:?} address: {error}"))
            })?;
            config.members.insert(node_id, address);
        }
        config.fragment_map = values
            .fragment_map
            .into_iter()
            .map(|(fragment, replicas)| (crate::shard::FragmentId(fragment), replicas))
            .collect();
        Ok(config)
    }
}
