mod aggregator;
mod events;
/// Usage tracking system for billing and analytics.
///
/// Tracks:
/// - Container lifecycle events (created, started, stopped)
/// - Hardware metrics (CPU, memory usage)
/// - Runtime duration for billing
/// - Network and volume events
mod tracker;

pub use events::{UsageEvent, UsageEventType};
pub use tracker::UsageTracker;
// UsageAggregator not exported yet - will be used when implementing background aggregation
#[allow(unused_imports)]
use aggregator::UsageAggregator;
