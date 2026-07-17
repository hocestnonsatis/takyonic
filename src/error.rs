//! Error types for the Takyonic storage engine.

use thiserror::Error;

/// Root error type for Takyonic operations.
#[derive(Debug, Error)]
pub enum TakyonicError {
    /// Invalid or inconsistent configuration.
    #[error("configuration error: {0}")]
    Config(String),

    /// Underlying I/O failure (WAL, SST, directory setup).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Checksum or on-disk format integrity failure.
    #[error("integrity error: {0}")]
    Integrity(String),

    /// Compaction scheduling, execution, or installation failure.
    #[error("compaction error: {0}")]
    Compaction(String),

    /// Write admission or backpressure failure.
    #[error("admission error: {0}")]
    Admission(String),

    /// Raft command, ordering, or state-machine failure.
    #[error("raft state-machine error: {0}")]
    Raft(String),

    /// Engine lifecycle / orchestration failure.
    #[error("engine error: {0}")]
    Engine(String),

    /// Network / gRPC transport failure.
    #[error("network error: {0}")]
    Network(String),

    /// Optimistic concurrency conflict — retry the transaction.
    #[error("transaction conflict: {0}")]
    Conflict(String),

    /// Catch-all for unexpected failures wrapped via [`anyhow`].
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Convenient result alias for Takyonic APIs.
pub type Result<T> = std::result::Result<T, TakyonicError>;
