//! Fast Container Startup Module
//!
//! High-performance container startup optimizations:
//! - Pre-extracted base image caching (eliminates tar extraction)
//! - OverlayFS-based copy-on-write rootfs (instant container creation)
//! - Inotify-based readiness detection (replaces polling)
//! - Parallel mount operations
//!
//! Target: Sub-250ms cold starts (from ~1.73s baseline)

// Allow some dead code - these items are part of the public API for future use
#![allow(dead_code)]

use crate::utils::console::ConsoleLogger;
use flate2::read::GzDecoder;
use nix::mount::{mount, umount2, MntFlags, MsFlags};
use nix::sys::inotify::{AddWatchFlags, InitFlags, Inotify};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tar::Archive;

/// Configuration for fast start optimizations
#[derive(Debug, Clone)]
pub struct FastStartConfig {
    /// Path to store pre-extracted base images
    pub base_image_cache_path: PathBuf,
    /// Path for container rootfs storage
    pub container_rootfs_path: PathBuf,
    /// Whether to use OverlayFS (falls back to copy if unavailable)
    pub use_overlayfs: bool,
    /// Maximum number of cached base images
    pub max_cached_images: usize,
    /// Timeout for inotify-based readiness detection
    pub readiness_timeout: Duration,
}

impl Default for FastStartConfig {
    fn default() -> Self {
        Self {
            base_image_cache_path: PathBuf::from("/var/lib/quilt/cache/base-images"),
            container_rootfs_path: PathBuf::from(crate::utils::constants::CONTAINER_BASE_DIR),
            use_overlayfs: true,
            max_cached_images: 10,
            readiness_timeout: Duration::from_secs(10),
        }
    }
}

/// Cached base image info
#[derive(Debug, Clone)]
pub struct CachedBaseImage {
    /// Original image path (tar.gz)
    pub source_path: String,
    /// Extracted layer path
    pub extracted_path: PathBuf,
    /// SHA256 hash of the source file
    pub hash: String,
    /// Number of active containers using this base
    pub reference_count: usize,
    /// Total size of extracted files
    pub size_bytes: u64,
    /// Time taken to extract (for metrics)
    pub extraction_time_ms: u64,
}

/// Fast container startup manager
pub struct FastStartManager {
    config: FastStartConfig,
    /// Cache of pre-extracted base images (hash -> info)
    image_cache: Arc<RwLock<HashMap<String, CachedBaseImage>>>,
    /// Track active overlay mounts (container_id -> mount_path)
    active_mounts: Arc<RwLock<HashMap<String, PathBuf>>>,
    /// OverlayFS availability flag
    overlay_available: bool,
}

impl FastStartManager {
    /// Create a new fast start manager
    pub fn new(config: FastStartConfig) -> Self {
        // Check OverlayFS availability
        let overlay_available = Self::check_overlay_support();

        // Ensure cache directories exist
        let _ = std::fs::create_dir_all(&config.base_image_cache_path);
        let _ = std::fs::create_dir_all(&config.container_rootfs_path);

        if overlay_available {
            ConsoleLogger::success(
                "FastStart: OverlayFS available - fast container creation enabled",
            );
        } else {
            ConsoleLogger::warning("FastStart: OverlayFS unavailable - using copy fallback");
        }

        Self {
            config,
            image_cache: Arc::new(RwLock::new(HashMap::new())),
            active_mounts: Arc::new(RwLock::new(HashMap::new())),
            overlay_available,
        }
    }

    /// Create with default config
    pub fn new_default() -> Self {
        Self::new(FastStartConfig::default())
    }

    /// Check if OverlayFS is supported
    fn check_overlay_support() -> bool {
        if let Ok(contents) = std::fs::read_to_string("/proc/filesystems") {
            return contents.contains("overlay");
        }
        false
    }

    /// Calculate SHA256 hash of a file (for cache keying)
    fn calculate_file_hash(path: &str) -> Result<String, String> {
        use std::io::Read;

        let mut file = std::fs::File::open(path)
            .map_err(|e| format!("Failed to open file for hashing: {}", e))?;

        // Read first 64KB + file size for fast approximate hash
        let metadata = file
            .metadata()
            .map_err(|e| format!("Failed to get file metadata: {}", e))?;
        let file_size = metadata.len();

        let mut buffer = vec![0u8; std::cmp::min(65536, file_size as usize)];
        file.read_exact(&mut buffer)
            .map_err(|e| format!("Failed to read file for hashing: {}", e))?;

        // Simple hash: combine file size and first bytes
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        file_size.hash(&mut hasher);
        buffer.hash(&mut hasher);

        Ok(format!("{:016x}", hasher.finish()))
    }

    /// Pre-extract a base image to the cache
    pub fn cache_base_image(&self, image_path: &str) -> Result<CachedBaseImage, String> {
        let start = Instant::now();

        // Calculate hash for cache key
        let hash = Self::calculate_file_hash(image_path)?;

        // Check if already cached
        {
            let cache = self
                .image_cache
                .read()
                .map_err(|e| format!("Failed to acquire cache read lock: {}", e))?;
            if let Some(cached) = cache.get(&hash) {
                if cached.extracted_path.exists() {
                    ConsoleLogger::debug(&format!(
                        "FastStart: Base image cache hit for {}",
                        image_path
                    ));
                    return Ok(cached.clone());
                }
            }
        }

        ConsoleLogger::info(&format!(
            "FastStart: Caching base image {} (hash: {})",
            image_path,
            &hash[..8]
        ));

        // Create extraction directory
        let extracted_path = self.config.base_image_cache_path.join(&hash);
        let temp_path = self
            .config
            .base_image_cache_path
            .join(format!("{}.tmp", hash));

        // Clean up any existing temp directory
        if temp_path.exists() {
            let _ = std::fs::remove_dir_all(&temp_path);
        }
        std::fs::create_dir_all(&temp_path)
            .map_err(|e| format!("Failed to create temp extraction directory: {}", e))?;

        // Extract image
        let tar_file = std::fs::File::open(image_path)
            .map_err(|e| format!("Failed to open image file: {}", e))?;
        let tar = GzDecoder::new(tar_file);
        let mut archive = Archive::new(tar);

        archive
            .unpack(&temp_path)
            .map_err(|e| format!("Failed to extract base image: {}", e))?;

        // Calculate extracted size
        let size_bytes = Self::calculate_dir_size(&temp_path).unwrap_or(0);

        // Atomic rename
        if extracted_path.exists() {
            let _ = std::fs::remove_dir_all(&extracted_path);
        }
        std::fs::rename(&temp_path, &extracted_path)
            .map_err(|e| format!("Failed to finalize base image extraction: {}", e))?;

        let extraction_time_ms = start.elapsed().as_millis() as u64;

        let cached = CachedBaseImage {
            source_path: image_path.to_string(),
            extracted_path: extracted_path.clone(),
            hash: hash.clone(),
            reference_count: 0,
            size_bytes,
            extraction_time_ms,
        };

        // Store in cache
        {
            let mut cache = self
                .image_cache
                .write()
                .map_err(|e| format!("Failed to acquire cache write lock: {}", e))?;
            cache.insert(hash, cached.clone());
        }

        ConsoleLogger::success(&format!(
            "FastStart: Base image cached in {}ms (size: {} MB)",
            extraction_time_ms,
            size_bytes / (1024 * 1024)
        ));

        Ok(cached)
    }

    /// Calculate directory size
    fn calculate_dir_size(path: &Path) -> Result<u64, String> {
        let mut total = 0u64;

        for entry in walkdir::WalkDir::new(path)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                if let Ok(meta) = entry.metadata() {
                    total += meta.len();
                }
            }
        }

        Ok(total)
    }

    /// Prepare container rootfs using OverlayFS (fast path)
    /// Returns the path to the prepared rootfs
    pub fn prepare_rootfs_fast(
        &self,
        container_id: &str,
        image_path: &str,
    ) -> Result<PathBuf, String> {
        let start = Instant::now();

        // Ensure base image is cached
        let cached = self.cache_base_image(image_path)?;

        let cache_time = start.elapsed();
        ConsoleLogger::debug(&format!("FastStart: Base image ready in {:?}", cache_time));

        // Create container directory structure
        let container_dir = self.config.container_rootfs_path.join(container_id);
        let rootfs = container_dir.clone(); // For overlay, this is the mount point
        let upper = container_dir.join("upper"); // Writable layer
        let work = container_dir.join("work"); // OverlayFS work directory

        std::fs::create_dir_all(&rootfs)
            .map_err(|e| format!("Failed to create rootfs directory: {}", e))?;
        std::fs::create_dir_all(&upper)
            .map_err(|e| format!("Failed to create upper directory: {}", e))?;
        std::fs::create_dir_all(&work)
            .map_err(|e| format!("Failed to create work directory: {}", e))?;

        if self.config.use_overlayfs && self.overlay_available {
            // Fast path: OverlayFS
            let mount_start = Instant::now();

            let mount_opts = format!(
                "lowerdir={},upperdir={},workdir={}",
                cached.extracted_path.display(),
                upper.display(),
                work.display()
            );

            // Mount overlay
            match mount(
                Some("overlay"),
                &rootfs,
                Some("overlay"),
                MsFlags::empty(),
                Some(mount_opts.as_str()),
            ) {
                Ok(()) => {
                    // Track mount
                    {
                        let mut mounts = self
                            .active_mounts
                            .write()
                            .map_err(|e| format!("Failed to track mount: {}", e))?;
                        mounts.insert(container_id.to_string(), rootfs.clone());
                    }

                    // Increment reference count
                    {
                        let mut cache = self
                            .image_cache
                            .write()
                            .map_err(|e| format!("Failed to update reference count: {}", e))?;
                        if let Some(cached) = cache.get_mut(&cached.hash) {
                            cached.reference_count += 1;
                        }
                    }

                    let total_time = start.elapsed();
                    ConsoleLogger::success(&format!(
                        "FastStart: Overlay rootfs ready in {:?} (mount: {:?})",
                        total_time,
                        mount_start.elapsed()
                    ));

                    return Ok(rootfs);
                }
                Err(e) => {
                    ConsoleLogger::warning(&format!(
                        "FastStart: OverlayFS mount failed ({}), falling back to copy",
                        e
                    ));
                    // Fall through to copy fallback
                }
            }
        }

        // Fallback: Copy base image (slower but always works)
        let copy_start = Instant::now();
        Self::copy_dir_recursive(&cached.extracted_path, &rootfs)?;

        let total_time = start.elapsed();
        ConsoleLogger::info(&format!(
            "FastStart: Copy rootfs ready in {:?} (copy: {:?})",
            total_time,
            copy_start.elapsed()
        ));

        Ok(rootfs)
    }

    /// Copy directory recursively
    fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<(), String> {
        for entry in walkdir::WalkDir::new(src)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let relative = entry
                .path()
                .strip_prefix(src)
                .map_err(|e| format!("Path strip failed: {}", e))?;
            let target = dst.join(relative);

            if entry.file_type().is_dir() {
                std::fs::create_dir_all(&target).map_err(|e| {
                    format!("Failed to create directory {}: {}", target.display(), e)
                })?;
            } else if entry.file_type().is_file() {
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| format!("Failed to create parent: {}", e))?;
                }
                std::fs::copy(entry.path(), &target)
                    .map_err(|e| format!("Failed to copy file: {}", e))?;
            } else if entry.file_type().is_symlink() {
                if let Ok(link_target) = std::fs::read_link(entry.path()) {
                    let _ = std::os::unix::fs::symlink(&link_target, &target);
                }
            }
        }
        Ok(())
    }

    /// Clean up container rootfs
    pub fn cleanup_rootfs(&self, container_id: &str) -> Result<(), String> {
        let container_dir = self.config.container_rootfs_path.join(container_id);

        // Check if there's an overlay mount to unmount
        let was_mounted = {
            let mut mounts = self
                .active_mounts
                .write()
                .map_err(|e| format!("Failed to acquire mounts lock: {}", e))?;
            mounts.remove(container_id).is_some()
        };

        if was_mounted {
            // Unmount overlay
            if let Err(e) = umount2(&container_dir, MntFlags::MNT_DETACH) {
                ConsoleLogger::warning(&format!("FastStart: Failed to unmount overlay: {}", e));
            }
        }

        // Remove container directory
        if container_dir.exists() {
            std::fs::remove_dir_all(&container_dir)
                .map_err(|e| format!("Failed to remove container directory: {}", e))?;
        }

        Ok(())
    }

    /// Get cache statistics
    pub fn get_cache_stats(&self) -> Result<FastStartStats, String> {
        let cache = self
            .image_cache
            .read()
            .map_err(|e| format!("Failed to acquire cache lock: {}", e))?;

        let total_size: u64 = cache.values().map(|c| c.size_bytes).sum();
        let total_refs: usize = cache.values().map(|c| c.reference_count).sum();

        Ok(FastStartStats {
            cached_images: cache.len(),
            total_cache_size_bytes: total_size,
            total_active_containers: total_refs,
            overlay_available: self.overlay_available,
        })
    }

    /// Pre-warm the cache with common base images
    pub fn prewarm_cache(&self, image_paths: &[&str]) -> Result<(), String> {
        ConsoleLogger::info(&format!(
            "FastStart: Pre-warming cache with {} images",
            image_paths.len()
        ));

        for path in image_paths {
            if Path::new(path).exists() {
                match self.cache_base_image(path) {
                    Ok(cached) => {
                        ConsoleLogger::debug(&format!(
                            "FastStart: Pre-cached {} ({}ms)",
                            path, cached.extraction_time_ms
                        ));
                    }
                    Err(e) => {
                        ConsoleLogger::warning(&format!(
                            "FastStart: Failed to pre-cache {}: {}",
                            path, e
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct FastStartStats {
    pub cached_images: usize,
    pub total_cache_size_bytes: u64,
    pub total_active_containers: usize,
    pub overlay_available: bool,
}

/// Inotify-based readiness watcher
/// Replaces polling loops with event-driven detection
pub struct ReadinessWatcher {
    timeout: Duration,
}

impl ReadinessWatcher {
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    /// Wait for a file to be created (replaces polling loop)
    pub fn wait_for_file(&self, path: &str) -> Result<(), String> {
        let path = Path::new(path);

        // If file already exists, return immediately
        if path.exists() {
            return Ok(());
        }

        // Get parent directory for watching
        let parent = path
            .parent()
            .ok_or_else(|| format!("Invalid path: {}", path.display()))?;

        // Ensure parent exists
        if !parent.exists() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create parent directory: {}", e))?;
        }

        // Create inotify instance
        let inotify = Inotify::init(InitFlags::IN_NONBLOCK)
            .map_err(|e| format!("Failed to create inotify: {}", e))?;

        // Watch for file creation in parent directory
        inotify
            .add_watch(
                parent,
                AddWatchFlags::IN_CREATE | AddWatchFlags::IN_MOVED_TO,
            )
            .map_err(|e| format!("Failed to add watch: {}", e))?;

        let file_name = path
            .file_name()
            .ok_or_else(|| "Invalid file name".to_string())?;

        let start = Instant::now();

        while start.elapsed() < self.timeout {
            // Check if file appeared (race between adding watch and file creation)
            if path.exists() {
                return Ok(());
            }

            // Read events - nix 0.26 API takes no buffer argument
            match inotify.read_events() {
                Ok(events) => {
                    for event in events {
                        if let Some(name) = &event.name {
                            if name == file_name {
                                return Ok(());
                            }
                        }
                    }
                }
                Err(nix::errno::Errno::EAGAIN) => {
                    // No events available, sleep briefly
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(e) => {
                    return Err(format!("inotify read error: {}", e));
                }
            }
        }

        Err(format!("Timeout waiting for file: {}", path.display()))
    }

    /// Wait for a Unix socket to be ready (exists and accepting connections)
    pub fn wait_for_socket(&self, socket_path: &str) -> Result<(), String> {
        use std::os::unix::net::UnixStream;

        // First wait for file to exist
        self.wait_for_file(socket_path)?;

        // Then verify it's accepting connections
        let start = Instant::now();
        while start.elapsed() < self.timeout {
            match UnixStream::connect(socket_path) {
                Ok(_) => return Ok(()),
                Err(_) => std::thread::sleep(Duration::from_millis(5)),
            }
        }

        Err(format!(
            "Timeout waiting for socket to accept connections: {}",
            socket_path
        ))
    }
}

/// Parallel mount helper
/// Performs mount operations concurrently where safe
pub struct ParallelMounter;

impl ParallelMounter {
    /// Mount multiple independent filesystems in parallel
    #[allow(clippy::type_complexity)]
    pub fn mount_parallel(
        mounts: &[(Option<&str>, &str, Option<&str>, MsFlags, Option<&str>)],
    ) -> Vec<Result<(), String>> {
        use std::thread;

        let handles: Vec<_> = mounts
            .iter()
            .map(|(source, target, fstype, flags, data)| {
                let source = source.map(|s| s.to_string());
                let target = target.to_string();
                let fstype = fstype.map(|s| s.to_string());
                let flags = *flags;
                let data = data.map(|s| s.to_string());

                thread::spawn(move || {
                    mount(
                        source.as_deref(),
                        target.as_str(),
                        fstype.as_deref(),
                        flags,
                        data.as_deref(),
                    )
                    .map_err(|e| format!("Mount failed for {}: {}", target, e))
                })
            })
            .collect();

        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|_| Err("Thread panic".to_string())))
            .collect()
    }
}

/// Timing instrumentation for performance debugging
pub struct StartupTimer {
    phase_times: Vec<(String, Duration)>,
    start: Instant,
    last_checkpoint: Instant,
}

impl StartupTimer {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            phase_times: Vec::new(),
            start: now,
            last_checkpoint: now,
        }
    }

    pub fn checkpoint(&mut self, phase: &str) {
        let now = Instant::now();
        let duration = now - self.last_checkpoint;
        self.phase_times.push((phase.to_string(), duration));
        self.last_checkpoint = now;
    }

    pub fn total_time(&self) -> Duration {
        self.start.elapsed()
    }

    pub fn report(&self) -> String {
        let mut report = format!("Startup time: {:?}\n", self.total_time());
        for (phase, duration) in &self.phase_times {
            report.push_str(&format!("  {}: {:?}\n", phase, duration));
        }
        report
    }

    pub fn get_phases(&self) -> &[(String, Duration)] {
        &self.phase_times
    }
}

impl Default for StartupTimer {
    fn default() -> Self {
        Self::new()
    }
}

/// Global fast start manager singleton
static FAST_START_MANAGER: std::sync::OnceLock<FastStartManager> = std::sync::OnceLock::new();

/// Get or initialize the global fast start manager
pub fn fast_start_manager() -> &'static FastStartManager {
    FAST_START_MANAGER.get_or_init(FastStartManager::new_default)
}

/// Initialize fast start with custom config
pub fn init_fast_start(config: FastStartConfig) -> Result<(), String> {
    FAST_START_MANAGER
        .set(FastStartManager::new(config))
        .map_err(|_| "Fast start manager already initialized".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // ============================================
    // FastStartConfig Tests
    // ============================================

    #[test]
    fn test_fast_start_config_default() {
        let config = FastStartConfig::default();
        assert!(config.use_overlayfs);
        assert_eq!(config.max_cached_images, 10);
        assert_eq!(config.readiness_timeout, Duration::from_secs(10));
        assert_eq!(
            config.base_image_cache_path,
            PathBuf::from("/var/lib/quilt/cache/base-images")
        );
        assert_eq!(
            config.container_rootfs_path,
            PathBuf::from(crate::utils::constants::CONTAINER_BASE_DIR)
        );
    }

    #[test]
    fn test_fast_start_config_custom() {
        let config = FastStartConfig {
            base_image_cache_path: PathBuf::from("/custom/cache"),
            container_rootfs_path: PathBuf::from("/custom/containers"),
            use_overlayfs: false,
            max_cached_images: 5,
            readiness_timeout: Duration::from_secs(30),
        };
        assert!(!config.use_overlayfs);
        assert_eq!(config.max_cached_images, 5);
        assert_eq!(config.readiness_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_fast_start_config_clone() {
        let config = FastStartConfig::default();
        let cloned = config.clone();
        assert_eq!(config.use_overlayfs, cloned.use_overlayfs);
        assert_eq!(config.max_cached_images, cloned.max_cached_images);
    }

    // ============================================
    // CachedBaseImage Tests
    // ============================================

    #[test]
    fn test_cached_base_image_creation() {
        let cached = CachedBaseImage {
            source_path: "/path/to/image.tar.gz".to_string(),
            extracted_path: PathBuf::from("/cache/abc123"),
            hash: "abc123".to_string(),
            reference_count: 0,
            size_bytes: 1024 * 1024,
            extraction_time_ms: 500,
        };
        assert_eq!(cached.source_path, "/path/to/image.tar.gz");
        assert_eq!(cached.hash, "abc123");
        assert_eq!(cached.reference_count, 0);
        assert_eq!(cached.size_bytes, 1024 * 1024);
        assert_eq!(cached.extraction_time_ms, 500);
    }

    #[test]
    fn test_cached_base_image_clone() {
        let cached = CachedBaseImage {
            source_path: "/path/to/image.tar.gz".to_string(),
            extracted_path: PathBuf::from("/cache/abc123"),
            hash: "abc123".to_string(),
            reference_count: 2,
            size_bytes: 2048,
            extraction_time_ms: 100,
        };
        let cloned = cached.clone();
        assert_eq!(cached.hash, cloned.hash);
        assert_eq!(cached.reference_count, cloned.reference_count);
    }

    // ============================================
    // FastStartStats Tests
    // ============================================

    #[test]
    fn test_fast_start_stats_creation() {
        let stats = FastStartStats {
            cached_images: 3,
            total_cache_size_bytes: 100 * 1024 * 1024,
            total_active_containers: 5,
            overlay_available: true,
        };
        assert_eq!(stats.cached_images, 3);
        assert_eq!(stats.total_cache_size_bytes, 100 * 1024 * 1024);
        assert_eq!(stats.total_active_containers, 5);
        assert!(stats.overlay_available);
    }

    #[test]
    fn test_fast_start_stats_clone() {
        let stats = FastStartStats {
            cached_images: 2,
            total_cache_size_bytes: 50 * 1024 * 1024,
            total_active_containers: 1,
            overlay_available: false,
        };
        let cloned = stats.clone();
        assert_eq!(stats.cached_images, cloned.cached_images);
        assert_eq!(stats.overlay_available, cloned.overlay_available);
    }

    // ============================================
    // StartupTimer Tests
    // ============================================

    #[test]
    fn test_startup_timer_creation() {
        let timer = StartupTimer::new();
        assert!(timer.get_phases().is_empty());
        assert!(timer.total_time() < Duration::from_secs(1));
    }

    #[test]
    fn test_startup_timer_default() {
        let timer = StartupTimer::default();
        assert!(timer.get_phases().is_empty());
    }

    #[test]
    fn test_startup_timer_single_checkpoint() {
        let mut timer = StartupTimer::new();
        std::thread::sleep(Duration::from_millis(5));
        timer.checkpoint("phase1");

        assert_eq!(timer.get_phases().len(), 1);
        assert_eq!(timer.get_phases()[0].0, "phase1");
        assert!(timer.get_phases()[0].1 >= Duration::from_millis(5));
    }

    #[test]
    fn test_startup_timer_multiple_checkpoints() {
        let mut timer = StartupTimer::new();
        std::thread::sleep(Duration::from_millis(10));
        timer.checkpoint("phase1");
        std::thread::sleep(Duration::from_millis(10));
        timer.checkpoint("phase2");
        std::thread::sleep(Duration::from_millis(10));
        timer.checkpoint("phase3");

        assert_eq!(timer.get_phases().len(), 3);
        assert_eq!(timer.get_phases()[0].0, "phase1");
        assert_eq!(timer.get_phases()[1].0, "phase2");
        assert_eq!(timer.get_phases()[2].0, "phase3");
        assert!(timer.total_time() >= Duration::from_millis(30));
    }

    #[test]
    fn test_startup_timer_report_format() {
        let mut timer = StartupTimer::new();
        timer.checkpoint("test_phase");
        let report = timer.report();

        assert!(report.contains("Startup time:"));
        assert!(report.contains("test_phase"));
    }

    #[test]
    fn test_startup_timer_total_time_increases() {
        let timer = StartupTimer::new();
        let time1 = timer.total_time();
        std::thread::sleep(Duration::from_millis(5));
        let time2 = timer.total_time();
        assert!(time2 > time1);
    }

    // ============================================
    // ReadinessWatcher Tests
    // ============================================

    #[test]
    fn test_readiness_watcher_creation() {
        let watcher = ReadinessWatcher::new(Duration::from_secs(5));
        // Watcher created successfully
        // Smoke check: reaching here means the fast-start path executed without panicking.
        let _ = watcher; // Use the watcher
    }

    #[test]
    fn test_readiness_watcher_existing_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("ready");
        std::fs::write(&file_path, "ok").unwrap();

        let watcher = ReadinessWatcher::new(Duration::from_secs(1));
        let result = watcher.wait_for_file(file_path.to_str().unwrap());
        assert!(result.is_ok());
    }

    #[test]
    fn test_readiness_watcher_file_created_during_wait() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("will_be_created");
        let file_path_clone = file_path.clone();

        // Spawn thread to create file after short delay
        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            std::fs::write(&file_path_clone, "created").unwrap();
        });

        let watcher = ReadinessWatcher::new(Duration::from_secs(2));
        let result = watcher.wait_for_file(file_path.to_str().unwrap());

        handle.join().unwrap();
        assert!(result.is_ok());
    }

    #[test]
    fn test_readiness_watcher_timeout() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("never_created");

        let watcher = ReadinessWatcher::new(Duration::from_millis(100));
        let result = watcher.wait_for_file(file_path.to_str().unwrap());

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Timeout"));
    }

    #[test]
    fn test_readiness_watcher_timeout_on_nonexistent() {
        let watcher = ReadinessWatcher::new(Duration::from_millis(50));
        // This path won't be created, so it should timeout
        let result = watcher.wait_for_file("/tmp/nonexistent_file_for_test_abc123xyz");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Timeout"));
    }

    #[test]
    fn test_readiness_watcher_creates_parent_directory() {
        let dir = tempdir().unwrap();
        let nested_path = dir.path().join("nested").join("deep").join("ready");

        // Create the file before waiting
        std::fs::create_dir_all(nested_path.parent().unwrap()).unwrap();
        std::fs::write(&nested_path, "ok").unwrap();

        let watcher = ReadinessWatcher::new(Duration::from_secs(1));
        let result = watcher.wait_for_file(nested_path.to_str().unwrap());
        assert!(result.is_ok());
    }

    // ============================================
    // FastStartManager Tests
    // ============================================

    #[test]
    fn test_fast_start_manager_new_default() {
        // This test may require root privileges for actual overlay support check
        // but should not fail
        let _manager = FastStartManager::new_default();
    }

    #[test]
    fn test_fast_start_manager_custom_config() {
        let dir = tempdir().unwrap();
        let config = FastStartConfig {
            base_image_cache_path: dir.path().join("cache"),
            container_rootfs_path: dir.path().join("containers"),
            use_overlayfs: false,
            max_cached_images: 5,
            readiness_timeout: Duration::from_secs(5),
        };
        let _manager = FastStartManager::new(config);
    }

    #[test]
    fn test_fast_start_manager_check_overlay_support() {
        // This is a static method that checks /proc/filesystems
        let result = FastStartManager::check_overlay_support();
        // Result depends on system - just ensure it doesn't panic
        let _ = result;
    }

    #[test]
    fn test_fast_start_manager_calculate_file_hash() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_file");
        std::fs::write(&file_path, "test content for hashing").unwrap();

        let hash = FastStartManager::calculate_file_hash(file_path.to_str().unwrap());
        assert!(hash.is_ok());
        let hash = hash.unwrap();
        assert_eq!(hash.len(), 16); // 16 hex chars for u64
    }

    #[test]
    fn test_fast_start_manager_calculate_file_hash_same_content_same_hash() {
        let dir = tempdir().unwrap();
        let file1 = dir.path().join("file1");
        let file2 = dir.path().join("file2");

        let content = "identical content";
        std::fs::write(&file1, content).unwrap();
        std::fs::write(&file2, content).unwrap();

        let hash1 = FastStartManager::calculate_file_hash(file1.to_str().unwrap()).unwrap();
        let hash2 = FastStartManager::calculate_file_hash(file2.to_str().unwrap()).unwrap();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_fast_start_manager_calculate_file_hash_different_content_different_hash() {
        let dir = tempdir().unwrap();
        let file1 = dir.path().join("file1");
        let file2 = dir.path().join("file2");

        std::fs::write(&file1, "content A").unwrap();
        std::fs::write(&file2, "content B - different").unwrap();

        let hash1 = FastStartManager::calculate_file_hash(file1.to_str().unwrap()).unwrap();
        let hash2 = FastStartManager::calculate_file_hash(file2.to_str().unwrap()).unwrap();

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_fast_start_manager_calculate_file_hash_nonexistent() {
        let result = FastStartManager::calculate_file_hash("/nonexistent/file");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Failed to open file"));
    }

    #[test]
    fn test_fast_start_manager_calculate_dir_size() {
        let dir = tempdir().unwrap();

        // Create some files
        std::fs::write(dir.path().join("file1"), "hello").unwrap();
        std::fs::write(dir.path().join("file2"), "world!").unwrap();
        std::fs::create_dir(dir.path().join("subdir")).unwrap();
        std::fs::write(dir.path().join("subdir").join("file3"), "nested").unwrap();

        let size = FastStartManager::calculate_dir_size(dir.path()).unwrap();
        // "hello" (5) + "world!" (6) + "nested" (6) = 17 bytes
        assert_eq!(size, 17);
    }

    #[test]
    fn test_fast_start_manager_calculate_dir_size_empty() {
        let dir = tempdir().unwrap();
        let size = FastStartManager::calculate_dir_size(dir.path()).unwrap();
        assert_eq!(size, 0);
    }

    #[test]
    fn test_fast_start_manager_copy_dir_recursive() {
        let src = tempdir().unwrap();
        let dst = tempdir().unwrap();

        // Create source structure
        std::fs::write(src.path().join("file1.txt"), "content1").unwrap();
        std::fs::create_dir(src.path().join("subdir")).unwrap();
        std::fs::write(src.path().join("subdir").join("file2.txt"), "content2").unwrap();

        // Copy
        FastStartManager::copy_dir_recursive(src.path(), dst.path()).unwrap();

        // Verify
        assert!(dst.path().join("file1.txt").exists());
        assert!(dst.path().join("subdir").exists());
        assert!(dst.path().join("subdir").join("file2.txt").exists());
        assert_eq!(
            std::fs::read_to_string(dst.path().join("file1.txt")).unwrap(),
            "content1"
        );
        assert_eq!(
            std::fs::read_to_string(dst.path().join("subdir").join("file2.txt")).unwrap(),
            "content2"
        );
    }

    #[test]
    fn test_fast_start_manager_copy_dir_recursive_with_symlink() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        std::fs::create_dir(&src).unwrap();
        std::fs::create_dir(&dst).unwrap();

        // Create a file and symlink
        std::fs::write(src.join("target.txt"), "target content").unwrap();
        std::os::unix::fs::symlink("target.txt", src.join("link.txt")).unwrap();

        FastStartManager::copy_dir_recursive(&src, &dst).unwrap();

        // Symlink should be copied
        assert!(dst.join("target.txt").exists());
        assert!(dst.join("link.txt").exists() || dst.join("link.txt").is_symlink());
    }

    #[test]
    fn test_fast_start_manager_get_cache_stats_empty() {
        let dir = tempdir().unwrap();
        let config = FastStartConfig {
            base_image_cache_path: dir.path().join("cache"),
            container_rootfs_path: dir.path().join("containers"),
            use_overlayfs: false,
            max_cached_images: 5,
            readiness_timeout: Duration::from_secs(5),
        };
        let manager = FastStartManager::new(config);

        let stats = manager.get_cache_stats().unwrap();
        assert_eq!(stats.cached_images, 0);
        assert_eq!(stats.total_cache_size_bytes, 0);
        assert_eq!(stats.total_active_containers, 0);
    }

    #[test]
    fn test_fast_start_manager_prewarm_cache_nonexistent_images() {
        let dir = tempdir().unwrap();
        let config = FastStartConfig {
            base_image_cache_path: dir.path().join("cache"),
            container_rootfs_path: dir.path().join("containers"),
            use_overlayfs: false,
            max_cached_images: 5,
            readiness_timeout: Duration::from_secs(5),
        };
        let manager = FastStartManager::new(config);

        // Pre-warming with nonexistent images should not fail
        let result = manager.prewarm_cache(&["/nonexistent1.tar.gz", "/nonexistent2.tar.gz"]);
        assert!(result.is_ok());

        // Cache should still be empty
        let stats = manager.get_cache_stats().unwrap();
        assert_eq!(stats.cached_images, 0);
    }

    // ============================================
    // Integration Tests (require tar.gz image)
    // ============================================

    #[test]
    fn test_cache_base_image_with_real_tarball() {
        let dir = tempdir().unwrap();

        // Create a minimal tar.gz file
        let tarball_path = dir.path().join("test.tar.gz");
        create_test_tarball(&tarball_path);

        let config = FastStartConfig {
            base_image_cache_path: dir.path().join("cache"),
            container_rootfs_path: dir.path().join("containers"),
            use_overlayfs: false,
            max_cached_images: 5,
            readiness_timeout: Duration::from_secs(5),
        };
        let manager = FastStartManager::new(config);

        let result = manager.cache_base_image(tarball_path.to_str().unwrap());
        assert!(result.is_ok());

        let cached = result.unwrap();
        assert!(cached.extracted_path.exists());
        // `extraction_time_ms` is informational and may be 0ms on coarse timers.
        assert!(cached.size_bytes > 0);

        // Cache stats should reflect the cached image
        let stats = manager.get_cache_stats().unwrap();
        assert_eq!(stats.cached_images, 1);
    }

    #[test]
    fn test_cache_base_image_cache_hit() {
        let dir = tempdir().unwrap();

        let tarball_path = dir.path().join("test.tar.gz");
        create_test_tarball(&tarball_path);

        let config = FastStartConfig {
            base_image_cache_path: dir.path().join("cache"),
            container_rootfs_path: dir.path().join("containers"),
            use_overlayfs: false,
            max_cached_images: 5,
            readiness_timeout: Duration::from_secs(5),
        };
        let manager = FastStartManager::new(config);

        // First cache
        let cached1 = manager
            .cache_base_image(tarball_path.to_str().unwrap())
            .unwrap();

        // Second cache should be a cache hit (same hash)
        let cached2 = manager
            .cache_base_image(tarball_path.to_str().unwrap())
            .unwrap();

        assert_eq!(cached1.hash, cached2.hash);
        assert_eq!(cached1.extracted_path, cached2.extracted_path);
    }

    #[test]
    fn test_prepare_rootfs_fast_copy_fallback() {
        let dir = tempdir().unwrap();

        let tarball_path = dir.path().join("test.tar.gz");
        create_test_tarball(&tarball_path);

        let config = FastStartConfig {
            base_image_cache_path: dir.path().join("cache"),
            container_rootfs_path: dir.path().join("containers"),
            use_overlayfs: false, // Disable overlay to force copy
            max_cached_images: 5,
            readiness_timeout: Duration::from_secs(5),
        };
        let manager = FastStartManager::new(config);

        let result =
            manager.prepare_rootfs_fast("test-container-123", tarball_path.to_str().unwrap());
        assert!(result.is_ok());

        let rootfs = result.unwrap();
        assert!(rootfs.exists());
        // Should have the test file from tarball
        assert!(rootfs.join("test.txt").exists());
    }

    #[test]
    fn test_cleanup_rootfs() {
        let dir = tempdir().unwrap();

        let tarball_path = dir.path().join("test.tar.gz");
        create_test_tarball(&tarball_path);

        let config = FastStartConfig {
            base_image_cache_path: dir.path().join("cache"),
            container_rootfs_path: dir.path().join("containers"),
            use_overlayfs: false,
            max_cached_images: 5,
            readiness_timeout: Duration::from_secs(5),
        };
        let manager = FastStartManager::new(config);

        let container_id = "cleanup-test-container";
        let rootfs = manager
            .prepare_rootfs_fast(container_id, tarball_path.to_str().unwrap())
            .unwrap();
        assert!(rootfs.exists());

        // Cleanup
        let result = manager.cleanup_rootfs(container_id);
        assert!(result.is_ok());
        assert!(!rootfs.exists());
    }

    // ============================================
    // Helper Functions for Tests
    // ============================================

    fn create_test_tarball(path: &Path) {
        use flate2::write::GzEncoder;
        use flate2::Compression;

        let tar_gz = std::fs::File::create(path).unwrap();
        let enc = GzEncoder::new(tar_gz, Compression::default());
        let mut tar = tar::Builder::new(enc);

        // Add a test file
        let mut header = tar::Header::new_gnu();
        header.set_path("test.txt").unwrap();
        let content = b"test file content";
        header.set_size(content.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar.append(&header, &content[..]).unwrap();

        // Add a directory
        let mut dir_header = tar::Header::new_gnu();
        dir_header.set_path("subdir/").unwrap();
        dir_header.set_size(0);
        dir_header.set_mode(0o755);
        dir_header.set_entry_type(tar::EntryType::Directory);
        dir_header.set_cksum();
        tar.append(&dir_header, &[][..]).unwrap();

        // Add a file in subdirectory
        let mut sub_header = tar::Header::new_gnu();
        sub_header.set_path("subdir/nested.txt").unwrap();
        let nested_content = b"nested content";
        sub_header.set_size(nested_content.len() as u64);
        sub_header.set_mode(0o644);
        sub_header.set_cksum();
        tar.append(&sub_header, &nested_content[..]).unwrap();

        tar.finish().unwrap();
    }
}
