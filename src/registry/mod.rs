//! OCI Distribution (Registry) Client
//!
//! Implements the OCI Distribution Specification for pulling images from:
//! - Docker Hub (docker.io)
//! - GitHub Container Registry (ghcr.io)
//! - Other OCI-compliant registries
//!
//! Features:
//! - Token-based authentication
//! - Docker credential helper support
//! - Parallel layer downloads
//! - Progress reporting
//! - Resumable downloads

mod auth;
mod client;

// Re-export only what's actively used
pub use client::{PullEvent, PullOptions, RegistryClient};

use thiserror::Error;

/// Errors that can occur during registry operations
#[derive(Error, Debug)]
pub enum RegistryError {
    #[error("HTTP error: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("Authentication failed: {0}")]
    AuthError(String),

    #[error("Manifest not found: {0}")]
    ManifestNotFound(String),

    #[error("Rate limited: retry after {0} seconds")]
    RateLimited(u64),

    #[error("Invalid response: {0}")]
    InvalidResponse(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Image error: {0}")]
    ImageError(#[from] crate::image::ImageError),
}

pub type Result<T> = std::result::Result<T, RegistryError>;
