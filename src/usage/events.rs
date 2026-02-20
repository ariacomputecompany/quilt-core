/// Usage event types and structures for billing tracking.
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UsageEventType {
    /// Container was created
    ContainerCreated,
    /// Container was started
    ContainerStarted,
    /// Container was stopped
    ContainerStopped,
    /// Container runtime sample (periodic)
    ContainerRuntime,
    /// Volume was created
    VolumeCreated,
    /// Volume was deleted
    VolumeDeleted,
    /// Network allocated
    NetworkAllocated,
}

impl std::fmt::Display for UsageEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UsageEventType::ContainerCreated => write!(f, "container_created"),
            UsageEventType::ContainerStarted => write!(f, "container_started"),
            UsageEventType::ContainerStopped => write!(f, "container_stopped"),
            UsageEventType::ContainerRuntime => write!(f, "container_runtime"),
            UsageEventType::VolumeCreated => write!(f, "volume_created"),
            UsageEventType::VolumeDeleted => write!(f, "volume_deleted"),
            UsageEventType::NetworkAllocated => write!(f, "network_allocated"),
        }
    }
}

/// Represents a single usage event for billing purposes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageEvent {
    pub tenant_id: String,
    pub event_type: UsageEventType,
    pub container_id: Option<String>,
    pub timestamp: i64,

    // Hardware metrics
    pub memory_mb: Option<i32>,
    pub cpu_percent: Option<f32>,
    pub duration_seconds: Option<i64>,

    // Additional metadata
    pub metadata: HashMap<String, String>,
}

impl UsageEvent {
    /// Create a container lifecycle event
    #[allow(dead_code)]
    pub fn container_lifecycle(
        tenant_id: String,
        event_type: UsageEventType,
        container_id: String,
        memory_mb: Option<i32>,
        cpu_percent: Option<f32>,
    ) -> Self {
        Self {
            tenant_id,
            event_type,
            container_id: Some(container_id),
            timestamp: chrono::Utc::now().timestamp(),
            memory_mb,
            cpu_percent,
            duration_seconds: None,
            metadata: HashMap::new(),
        }
    }

    /// Create a container runtime sample event (for periodic tracking)
    #[allow(dead_code)]
    pub fn container_runtime_sample(
        tenant_id: String,
        container_id: String,
        memory_mb: i32,
        cpu_percent: f32,
        duration_seconds: i64,
    ) -> Self {
        Self {
            tenant_id,
            event_type: UsageEventType::ContainerRuntime,
            container_id: Some(container_id),
            timestamp: chrono::Utc::now().timestamp(),
            memory_mb: Some(memory_mb),
            cpu_percent: Some(cpu_percent),
            duration_seconds: Some(duration_seconds),
            metadata: HashMap::new(),
        }
    }

    /// Create a volume event
    #[allow(dead_code)]
    pub fn volume_event(
        tenant_id: String,
        event_type: UsageEventType,
        volume_name: String,
    ) -> Self {
        let mut metadata = HashMap::new();
        metadata.insert("volume_name".to_string(), volume_name);

        Self {
            tenant_id,
            event_type,
            container_id: None,
            timestamp: chrono::Utc::now().timestamp(),
            memory_mb: None,
            cpu_percent: None,
            duration_seconds: None,
            metadata,
        }
    }
}
