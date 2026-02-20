//! Layer Extraction and Overlay Filesystem Management
//!
//! Handles:
//! - Extracting compressed layer tarballs
//! - Whiteout file processing (.wh.* files)
//! - Overlay filesystem mounting for container rootfs
//! - Layer caching and deduplication

use crate::image::{ImageError, Result};
use flate2::read::GzDecoder;
use std::collections::HashMap;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use tar::Archive;

/// Information about an extracted layer (internal cache)
#[derive(Debug, Clone)]
struct LayerInfo {
    /// Path to extracted layer
    path: PathBuf,
}

/// Manages layer extraction and overlay mounting
pub struct LayerManager {
    /// Base path for layer storage
    layers_path: PathBuf,

    /// Base path for container rootfs
    containers_path: PathBuf,

    /// Cache of extracted layers (digest -> info)
    cache: RwLock<HashMap<String, LayerInfo>>,
}

impl LayerManager {
    /// Create a new layer manager
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let base = base_path.as_ref().to_path_buf();

        // Create directory structure
        let layers_path = base.join("layers");
        let containers_path = base.join("containers");

        std::fs::create_dir_all(&layers_path)?;
        std::fs::create_dir_all(&containers_path)?;

        Ok(Self {
            layers_path,
            containers_path,
            cache: RwLock::new(HashMap::new()),
        })
    }

    /// Extract a layer from compressed blob data
    pub async fn extract_layer(&self, digest: &str, blob: &[u8]) -> Result<PathBuf> {
        // Check cache
        {
            let cache = self.cache.read().map_err(|e| {
                ImageError::LayerExtractionError(format!("Failed to acquire read lock: {}", e))
            })?;
            if let Some(info) = cache.get(digest) {
                if info.path.exists() {
                    return Ok(info.path.clone());
                }
            }
        }

        // Get hash portion of digest for path
        let hash = digest
            .split(':')
            .nth(1)
            .ok_or_else(|| ImageError::LayerExtractionError("Invalid digest format".to_string()))?;

        let layer_path = self.layers_path.join(hash);

        // Skip extraction if already exists
        if layer_path.exists() {
            let info = self.scan_layer(&layer_path, digest)?;
            let mut cache = self.cache.write().map_err(|e| {
                ImageError::LayerExtractionError(format!("Failed to acquire write lock: {}", e))
            })?;
            cache.insert(digest.to_string(), info);
            return Ok(layer_path);
        }

        // Create temp directory for extraction
        let temp_path = self.layers_path.join(format!("{}.tmp", hash));
        if temp_path.exists() {
            std::fs::remove_dir_all(&temp_path)?;
        }
        std::fs::create_dir_all(&temp_path)?;

        // Detect compression and extract
        let result = if is_gzip(blob) {
            self.extract_gzip_layer(blob, &temp_path)
        } else {
            self.extract_tar_layer(blob, &temp_path)
        };

        if let Err(e) = result {
            // Clean up on failure
            let _ = std::fs::remove_dir_all(&temp_path);
            return Err(e);
        }

        // Process whiteout files
        self.process_whiteouts(&temp_path)?;

        // Atomic rename
        std::fs::rename(&temp_path, &layer_path)?;

        // Update cache
        let info = self.scan_layer(&layer_path, digest)?;
        let mut cache = self.cache.write().map_err(|e| {
            ImageError::LayerExtractionError(format!("Failed to acquire write lock: {}", e))
        })?;
        cache.insert(digest.to_string(), info);

        Ok(layer_path)
    }

    /// Extract a gzip-compressed tar layer
    fn extract_gzip_layer(&self, blob: &[u8], dest: &Path) -> Result<()> {
        let decoder = GzDecoder::new(blob);
        let mut archive = Archive::new(decoder);

        // Extract with security checks
        self.extract_archive(&mut archive, dest)
    }

    /// Extract a plain tar layer
    fn extract_tar_layer(&self, blob: &[u8], dest: &Path) -> Result<()> {
        let mut archive = Archive::new(blob);
        self.extract_archive(&mut archive, dest)
    }

    /// Extract archive entries with security checks
    fn extract_archive<R: Read>(&self, archive: &mut Archive<R>, dest: &Path) -> Result<()> {
        archive.set_preserve_permissions(true);
        archive.set_preserve_mtime(true);
        archive.set_unpack_xattrs(true);

        for entry in archive.entries().map_err(|e| {
            ImageError::LayerExtractionError(format!("Failed to read archive: {}", e))
        })? {
            let mut entry = entry.map_err(|e| {
                ImageError::LayerExtractionError(format!("Failed to read entry: {}", e))
            })?;

            let path = entry
                .path()
                .map_err(|e| ImageError::LayerExtractionError(format!("Invalid path: {}", e)))?;

            // Security: prevent path traversal
            let path_str = path.to_string_lossy();
            if path_str.contains("..") || path_str.starts_with('/') {
                continue; // Skip suspicious paths
            }

            let full_path = dest.join(&path);

            // Create parent directories
            if let Some(parent) = full_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    ImageError::LayerExtractionError(format!(
                        "Failed to create directory {}: {}",
                        parent.display(),
                        e
                    ))
                })?;
            }

            // Handle different entry types
            let entry_type = entry.header().entry_type();

            match entry_type {
                tar::EntryType::Directory => {
                    std::fs::create_dir_all(&full_path)?;
                    if let Ok(mode) = entry.header().mode() {
                        let _ = std::fs::set_permissions(
                            &full_path,
                            std::fs::Permissions::from_mode(mode),
                        );
                    }
                }
                tar::EntryType::Regular | tar::EntryType::Continuous => {
                    entry.unpack(&full_path).map_err(|e| {
                        ImageError::LayerExtractionError(format!(
                            "Failed to extract {}: {}",
                            full_path.display(),
                            e
                        ))
                    })?;
                }
                tar::EntryType::Symlink => {
                    if let Ok(Some(target)) = entry.link_name() {
                        let _ = std::os::unix::fs::symlink(&target, &full_path);
                    }
                }
                tar::EntryType::Link => {
                    if let Ok(Some(target)) = entry.link_name() {
                        let target_path = dest.join(&target);
                        let _ = std::fs::hard_link(&target_path, &full_path);
                    }
                }
                tar::EntryType::Char | tar::EntryType::Block => {
                    // Skip device nodes - require special privileges
                    continue;
                }
                tar::EntryType::Fifo => {
                    // Create named pipe
                    unsafe {
                        let c_path =
                            std::ffi::CString::new(full_path.to_string_lossy().as_bytes()).unwrap();
                        libc::mkfifo(c_path.as_ptr(), 0o644);
                    }
                }
                _ => {
                    // Unknown type - try generic unpack
                    let _ = entry.unpack(&full_path);
                }
            }
        }

        Ok(())
    }

    /// Process OCI whiteout files in extracted layer
    ///
    /// Whiteout files indicate deletions:
    /// - `.wh.filename` - delete `filename`
    /// - `.wh..wh..opq` - opaque directory (hide lower layer contents)
    fn process_whiteouts(&self, layer_path: &Path) -> Result<()> {
        let mut whiteouts = Vec::new();
        let mut opaques = Vec::new();

        // Find all whiteout files
        for entry in walkdir::WalkDir::new(layer_path)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let name = entry.file_name().to_string_lossy();

            if name == ".wh..wh..opq" {
                opaques.push(entry.path().to_path_buf());
            } else if name.starts_with(".wh.") {
                whiteouts.push(entry.path().to_path_buf());
            }
        }

        // Create opaque marker files (for overlay to read)
        for opaque_path in opaques {
            // Create trusted.overlay.opaque xattr
            let parent = opaque_path.parent().unwrap();
            let marker_path = parent.join(".opaque");
            std::fs::write(&marker_path, "")?;

            // Remove the original .wh..wh..opq file
            let _ = std::fs::remove_file(&opaque_path);
        }

        // Create whiteout device nodes (char 0:0) for overlay
        for whiteout_path in whiteouts {
            let name = whiteout_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .strip_prefix(".wh.")
                .unwrap()
                .to_string();

            let target_path = whiteout_path.parent().unwrap().join(&name);

            // Create character device (0, 0) - indicates deletion to overlay
            // This requires root, so we use a marker file as fallback
            let marker_path = whiteout_path
                .parent()
                .unwrap()
                .join(format!(".wh.{}", name));

            // Try to create char device, fall back to marker
            unsafe {
                let c_path =
                    std::ffi::CString::new(target_path.to_string_lossy().as_bytes()).unwrap();
                let result = libc::mknod(c_path.as_ptr(), libc::S_IFCHR, 0);

                if result != 0 {
                    // Fallback: keep the marker file
                    let _ = std::fs::write(&marker_path, "");
                }
            }

            // Remove original whiteout file
            let _ = std::fs::remove_file(&whiteout_path);
        }

        Ok(())
    }

    /// Create layer info for caching
    fn scan_layer(&self, path: &Path, _digest: &str) -> Result<LayerInfo> {
        Ok(LayerInfo {
            path: path.to_path_buf(),
        })
    }

    /// Mount overlay filesystem combining multiple layers
    ///
    /// Creates an overlay mount with:
    /// - Lower layers: read-only, stacked (first = bottom)
    /// - Upper layer: read-write, for container changes
    /// - Work directory: scratch space for overlay operations
    pub fn mount_overlay(&self, layers: &[PathBuf], container_id: &str) -> Result<PathBuf> {
        let container_dir = self.containers_path.join(container_id);
        let rootfs = container_dir.join("rootfs");
        let upper = container_dir.join("upper");
        let work = container_dir.join("work");

        // Create directories
        std::fs::create_dir_all(&rootfs)?;
        std::fs::create_dir_all(&upper)?;
        std::fs::create_dir_all(&work)?;

        if layers.is_empty() {
            // No layers - just use upper as rootfs
            return Ok(upper);
        }

        // For single layer without overlay support, copy to rootfs
        if layers.len() == 1 && !self.has_overlay_support() {
            self.copy_layer(&layers[0], &rootfs)?;
            return Ok(rootfs);
        }

        // Build overlay mount options
        // Lower dirs are specified from top to bottom (reverse order)
        let lower_dirs: Vec<String> = layers
            .iter()
            .rev()
            .map(|p| p.to_string_lossy().to_string())
            .collect();

        let lower_str = lower_dirs.join(":");

        let mount_opts = format!(
            "lowerdir={},upperdir={},workdir={}",
            lower_str,
            upper.display(),
            work.display()
        );

        // Mount overlay
        let result = unsafe {
            let source = std::ffi::CString::new("overlay").unwrap();
            let target = std::ffi::CString::new(rootfs.to_string_lossy().as_bytes()).unwrap();
            let fstype = std::ffi::CString::new("overlay").unwrap();
            let data = std::ffi::CString::new(mount_opts.as_bytes()).unwrap();

            libc::mount(
                source.as_ptr(),
                target.as_ptr(),
                fstype.as_ptr(),
                0,
                data.as_ptr() as *const libc::c_void,
            )
        };

        if result != 0 {
            let err = std::io::Error::last_os_error();

            // Fall back to copying if overlay mount fails
            if err.raw_os_error() == Some(libc::EPERM) || err.raw_os_error() == Some(libc::ENODEV) {
                // No overlay support - merge layers manually
                self.merge_layers(layers, &rootfs)?;
                return Ok(rootfs);
            }

            return Err(ImageError::OverlayMountError(format!(
                "Failed to mount overlay: {}",
                err
            )));
        }

        Ok(rootfs)
    }

    /// Check if overlay filesystem is supported
    fn has_overlay_support(&self) -> bool {
        // Check /proc/filesystems for overlay support
        if let Ok(contents) = std::fs::read_to_string("/proc/filesystems") {
            return contents.contains("overlay");
        }
        false
    }

    /// Copy a layer to destination (fallback for no overlay)
    fn copy_layer(&self, src: &Path, dest: &Path) -> Result<()> {
        for entry in walkdir::WalkDir::new(src)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let relative = entry.path().strip_prefix(src).unwrap();
            let target = dest.join(relative);

            if entry.file_type().is_dir() {
                std::fs::create_dir_all(&target)?;
            } else if entry.file_type().is_file() {
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::copy(entry.path(), &target)?;
            } else if entry.file_type().is_symlink() {
                let link_target = std::fs::read_link(entry.path())?;
                let _ = std::os::unix::fs::symlink(&link_target, &target);
            }
        }

        Ok(())
    }

    /// Merge multiple layers into a single directory (fallback for no overlay)
    fn merge_layers(&self, layers: &[PathBuf], dest: &Path) -> Result<()> {
        // Apply layers in order (first = bottom)
        for layer in layers {
            self.copy_layer(layer, dest)?;
        }

        Ok(())
    }
}

/// Check if data is gzip-compressed
fn is_gzip(data: &[u8]) -> bool {
    data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_is_gzip() {
        // Gzip magic bytes
        assert!(is_gzip(&[0x1f, 0x8b, 0x08]));

        // Plain data
        assert!(!is_gzip(&[0x00, 0x00, 0x00]));
    }

    #[test]
    fn test_layer_extraction() {
        let dir = tempdir().unwrap();
        let manager = LayerManager::new(dir.path()).unwrap();

        // Create a simple tar
        let mut tar_data = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut tar_data);

            let data = b"hello world";
            let mut header = tar::Header::new_gnu();
            header.set_path("test.txt").unwrap();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();

            builder.append(&header, &data[..]).unwrap();
            builder.finish().unwrap();
        }

        // Compress with gzip
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&tar_data).unwrap();
        let compressed = encoder.finish().unwrap();

        // Extract
        let rt = tokio::runtime::Runtime::new().unwrap();
        let layer_path = rt
            .block_on(manager.extract_layer("sha256:test123", &compressed))
            .unwrap();

        assert!(layer_path.join("test.txt").exists());
    }

    #[test]
    fn test_layer_caching() {
        let dir = tempdir().unwrap();
        let manager = LayerManager::new(dir.path()).unwrap();

        // Create minimal tar
        let mut tar_data = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut tar_data);
            let data = b"test";
            let mut header = tar::Header::new_gnu();
            header.set_path("file").unwrap();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append(&header, &data[..]).unwrap();
            builder.finish().unwrap();
        }

        let rt = tokio::runtime::Runtime::new().unwrap();

        // First extraction
        let path1 = rt
            .block_on(manager.extract_layer("sha256:cache_test", &tar_data))
            .unwrap();

        // Second extraction should return cached path
        let path2 = rt
            .block_on(manager.extract_layer("sha256:cache_test", &tar_data))
            .unwrap();

        assert_eq!(path1, path2);
    }
}
