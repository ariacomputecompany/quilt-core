#!/bin/bash
# scripts/install-quilt.sh
# Production installation script for Quilt Container Runtime
# One-time sudo password entry, passwordless operation forever after

set -e

echo "🚀 Installing Quilt Container Runtime..."

# Check if running with sudo
if [ "$EUID" -ne 0 ]; then
    echo ""
    echo "❌ Please run with sudo: sudo ./scripts/install-quilt.sh"
    echo "   (You'll only need to enter your password once)"
    echo ""
    exit 1
fi

# Get the actual user (not root)
ACTUAL_USER="${SUDO_USER:-$USER}"
ACTUAL_HOME=$(eval echo ~$ACTUAL_USER)

echo ""
echo "📦 Installing for user: $ACTUAL_USER"
echo ""

# Build the latest release binary
echo "   🔨 Building latest release binary..."
if ! sudo -u "$ACTUAL_USER" bash -c "source $ACTUAL_HOME/.cargo/env && cargo build --release"; then
    echo ""
    echo "❌ Build failed!"
    echo "   Please fix any compilation errors and try again"
    echo ""
    exit 1
fi
echo "      ✓ Build successful"
echo ""

# 1. Stop daemon if running (to avoid "Text file busy" error)
echo "   🛑 Stopping daemon if running..."
if pgrep -f "quilt daemon" > /dev/null 2>&1; then
    pkill -f "quilt daemon" 2>/dev/null || true
    sleep 1
    # Force kill if still running
    if pgrep -f "quilt daemon" > /dev/null 2>&1; then
        pkill -9 -f "quilt daemon" 2>/dev/null || true
        sleep 0.5
    fi
    echo "      ✓ Daemon stopped"
else
    echo "      ✓ No daemon running"
fi
echo ""

# 2. Install binary to system location
echo "   📥 Installing binary to /usr/local/bin..."
cp ./target/release/quilt /usr/local/bin/quilt
chmod 755 /usr/local/bin/quilt

# 3. Create system directories with proper permissions
echo "   📁 Creating system directories..."
mkdir -p /var/run/quilt
mkdir -p /var/lib/quilt/volumes
mkdir -p /var/lib/quilt/images
chmod 755 /var/run/quilt
chmod 755 /var/lib/quilt
chmod 755 /var/lib/quilt/images
chown root:root /var/run/quilt
chown root:root /var/lib/quilt

# 2a. Generate default container image
echo "   🎨 Generating default container image..."
if [ ! -f "/var/lib/quilt/images/default.tar.gz" ]; then
    TEMP_DIR=$(mktemp -d)

    # Create basic directory structure
    mkdir -p "$TEMP_DIR"/{bin,lib,lib64,etc,proc,sys,dev,tmp,var,usr/bin,usr/lib,root}

    # Copy busybox if available
    if command -v busybox >/dev/null 2>&1; then
        cp "$(which busybox)" "$TEMP_DIR/bin/"
        cd "$TEMP_DIR/bin"
        for cmd in sh ls cat echo mkdir rm cp mv pwd ps grep sed awk tar gzip ping \
                   sleep tail head test true false which find touch chmod chown df du kill \
                   nc wget curl vi; do
            ln -sf busybox "$cmd" 2>/dev/null || true
        done
        cd - >/dev/null
    else
        # Fallback to host binaries
        for binary in sh bash ls cat echo mkdir rm cp mv pwd ps grep; do
            if command -v "$binary" >/dev/null 2>&1; then
                cp "$(which "$binary")" "$TEMP_DIR/bin/" 2>/dev/null || true
            fi
        done
    fi

    # Ensure working shell
    if [ ! -f "$TEMP_DIR/bin/sh" ] && [ -f "$TEMP_DIR/bin/bash" ]; then
        ln -sf bash "$TEMP_DIR/bin/sh"
    fi

    # Copy essential libraries
    for lib_dir in /lib /lib64 /usr/lib /usr/lib64 /lib/x86_64-linux-gnu /usr/lib/x86_64-linux-gnu; do
        if [ -d "$lib_dir" ]; then
            for lib in libc.so.* libdl.so.* libm.so.* libpthread.so.* ld-linux*.so.*; do
                if ls "$lib_dir"/$lib 1> /dev/null 2>&1; then
                    cp "$lib_dir"/$lib "$TEMP_DIR/lib/" 2>/dev/null || true
                fi
            done
        fi
    done

    # Create lib64 symlinks
    for lib in "$TEMP_DIR"/lib/ld-linux*.so.*; do
        if [ -f "$lib" ]; then
            ln -sf "../lib/$(basename "$lib")" "$TEMP_DIR/lib64/$(basename "$lib")" 2>/dev/null || true
        fi
    done

    # Create essential files
    echo "root:x:0:0:root:/root:/bin/sh" > "$TEMP_DIR/etc/passwd"
    echo "root:x:0:" > "$TEMP_DIR/etc/group"
    echo "127.0.0.1 localhost" > "$TEMP_DIR/etc/hosts"
    echo "localhost" > "$TEMP_DIR/etc/hostname"

    # Create tarball
    (cd "$TEMP_DIR" && tar czf - .) > "/var/lib/quilt/images/default.tar.gz"

    # Cleanup
    rm -rf "$TEMP_DIR"

    echo "      ✓ Default image created ($(du -h /var/lib/quilt/images/default.tar.gz | cut -f1))"
else
    echo "      ✓ Default image already exists"
fi

# 4. Configure passwordless sudo for daemon operations
echo "   🔐 Configuring passwordless daemon operations..."
cat > /etc/sudoers.d/quilt << 'EOF'
# Quilt Container Runtime - passwordless daemon management
# All users can manage the Quilt daemon without password
# Daemon itself enforces access control via gRPC

ALL ALL=(root) NOPASSWD: /usr/local/bin/quilt daemon
ALL ALL=(root) NOPASSWD: /usr/local/bin/quilt daemon *
EOF

chmod 440 /etc/sudoers.d/quilt

# Validate sudoers syntax
echo "   🔍 Validating sudoers configuration..."
if ! visudo -c -f /etc/sudoers.d/quilt >/dev/null 2>&1; then
    echo ""
    echo "❌ Sudoers configuration error!"
    rm /etc/sudoers.d/quilt
    exit 1
fi

# 5. Clean up any old PID files
echo "   🧹 Cleaning up old state..."
rm -f /var/run/quilt/quilt.pid 2>/dev/null || true
rm -f $ACTUAL_HOME/.quilt/quilt.pid 2>/dev/null || true
rm -f /run/user/*/quilt.pid 2>/dev/null || true
rm -f /tmp/quilt-*.pid 2>/dev/null || true

echo ""
echo "✅ Quilt installed successfully!"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "🎯 You can now use Quilt without sudo or passwords:"
echo ""
echo "   Start daemon:    quilt daemon --start"
echo "   Create container: quilt create --image-path ./image.tar.gz"
echo "   Check status:    quilt status"
echo "   Interactive shell: quilt shell <container>"
echo "   Stop daemon:     quilt daemon --stop"
echo ""
echo "💡 All commands work without entering passwords!"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
