use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

pub struct FileSystemUtils;

impl FileSystemUtils {
    /// Check if a path exists and is a file
    pub fn is_file<P: AsRef<Path>>(path: P) -> bool {
        path.as_ref().is_file()
    }

    /// Check if a path exists and is a directory
    pub fn is_directory<P: AsRef<Path>>(path: P) -> bool {
        path.as_ref().is_dir()
    }

    /// Check if a file is executable
    pub fn is_executable<P: AsRef<Path>>(path: P) -> bool {
        if let Ok(metadata) = fs::metadata(path.as_ref()) {
            let permissions = metadata.permissions();
            // Check if any execute bit is set (user, group, or other)
            permissions.mode() & 0o111 != 0
        } else {
            false
        }
    }

    /// Check if a path is a broken symlink
    pub fn is_broken_symlink<P: AsRef<Path>>(path: P) -> bool {
        let path_ref = path.as_ref();

        // Check if it's a symlink
        if let Ok(metadata) = fs::symlink_metadata(path_ref) {
            if metadata.file_type().is_symlink() {
                // Check if the target exists
                return !path_ref.exists();
            }
        }
        false
    }

    /// Create directories recursively with logging
    pub fn create_dir_all_with_logging<P: AsRef<Path>>(
        path: P,
        description: &str,
    ) -> Result<(), String> {
        let path_ref = path.as_ref();

        if path_ref.exists() {
            if path_ref.is_dir() {
                return Ok(()); // Directory already exists
            } else {
                return Err(format!(
                    "Path exists but is not a directory: {}",
                    path_ref.display()
                ));
            }
        }

        fs::create_dir_all(path_ref).map_err(|e| {
            format!(
                "Failed to create {} directory '{}': {}",
                description,
                path_ref.display(),
                e
            )
        })
    }

    /// Copy a file from source to destination
    pub fn copy_file<P: AsRef<Path>, Q: AsRef<Path>>(
        source: P,
        destination: Q,
    ) -> Result<(), String> {
        let src_path = source.as_ref();
        let dst_path = destination.as_ref();

        // Ensure source exists
        if !src_path.exists() {
            return Err(format!(
                "Source file does not exist: {}",
                src_path.display()
            ));
        }

        // Create parent directory if it doesn't exist
        if let Some(parent) = dst_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).map_err(|e| {
                    format!(
                        "Failed to create parent directory '{}': {}",
                        parent.display(),
                        e
                    )
                })?;
            }
        }

        // Copy the file
        fs::copy(src_path, dst_path).map_err(|e| {
            format!(
                "Failed to copy '{}' to '{}': {}",
                src_path.display(),
                dst_path.display(),
                e
            )
        })?;

        Ok(())
    }

    /// Remove a file or directory recursively
    pub fn remove_path<P: AsRef<Path>>(path: P) -> Result<(), String> {
        let path_ref = path.as_ref();

        if !path_ref.exists() {
            return Ok(()); // Nothing to remove
        }

        if path_ref.is_dir() {
            fs::remove_dir_all(path_ref)
                .map_err(|e| format!("Failed to remove directory '{}': {}", path_ref.display(), e))
        } else {
            fs::remove_file(path_ref)
                .map_err(|e| format!("Failed to remove file '{}': {}", path_ref.display(), e))
        }
    }

    /// Get file size in bytes
    pub fn get_file_size<P: AsRef<Path>>(path: P) -> Result<u64, String> {
        let metadata = fs::metadata(path.as_ref()).map_err(|e| {
            format!(
                "Failed to get metadata for '{}': {}",
                path.as_ref().display(),
                e
            )
        })?;

        Ok(metadata.len())
    }

    /// Check if a path exists
    pub fn exists<P: AsRef<Path>>(path: P) -> bool {
        path.as_ref().exists()
    }

    /// Create a file with specific content
    pub fn write_file<P: AsRef<Path>>(path: P, content: &str) -> Result<(), String> {
        fs::write(path.as_ref(), content)
            .map_err(|e| format!("Failed to write file '{}': {}", path.as_ref().display(), e))
    }

    /// Read file content as string
    pub fn read_file<P: AsRef<Path>>(path: P) -> Result<String, String> {
        fs::read_to_string(path.as_ref())
            .map_err(|e| format!("Failed to read file '{}': {}", path.as_ref().display(), e))
    }

    /// Set file permissions
    pub fn set_permissions<P: AsRef<Path>>(path: P, mode: u32) -> Result<(), String> {
        let permissions = fs::Permissions::from_mode(mode);
        fs::set_permissions(path.as_ref(), permissions).map_err(|e| {
            format!(
                "Failed to set permissions for '{}': {}",
                path.as_ref().display(),
                e
            )
        })
    }

    /// Make a file executable
    pub fn make_executable<P: AsRef<Path>>(path: P) -> Result<(), String> {
        if let Ok(metadata) = fs::metadata(path.as_ref()) {
            let mut permissions = metadata.permissions();
            // Add execute permission for owner, group, and others
            let mode = permissions.mode();
            permissions.set_mode(mode | 0o111);

            fs::set_permissions(path.as_ref(), permissions).map_err(|e| {
                format!(
                    "Failed to make file executable '{}': {}",
                    path.as_ref().display(),
                    e
                )
            })
        } else {
            Err(format!("File does not exist: {}", path.as_ref().display()))
        }
    }

    /// List directory contents
    pub fn list_dir<P: AsRef<Path>>(path: P) -> Result<Vec<PathBuf>, String> {
        let entries = fs::read_dir(path.as_ref()).map_err(|e| {
            format!(
                "Failed to read directory '{}': {}",
                path.as_ref().display(),
                e
            )
        })?;

        let mut paths = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| {
                format!(
                    "Failed to read directory entry in '{}': {}",
                    path.as_ref().display(),
                    e
                )
            })?;
            paths.push(entry.path());
        }

        Ok(paths)
    }

    /// Join path components
    pub fn join<P: AsRef<Path>, Q: AsRef<Path>>(base: P, component: Q) -> PathBuf {
        base.as_ref().join(component)
    }
}
