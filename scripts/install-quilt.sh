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

# 1. Stop ALL quilt binaries and daemons (to avoid "Text file busy" error)
echo "   🛑 Stopping all quilt processes..."

# Kill only actual quilt binaries/daemons, not this install script
# Use -x for exact binary name match
killall -9 quilt 2>/dev/null || true
# Kill daemon processes specifically
pkill -9 -f "/quilt daemon" 2>/dev/null || true
pkill -9 -f "target/debug/quilt daemon" 2>/dev/null || true
pkill -9 -f "target/release/quilt daemon" 2>/dev/null || true
pkill -9 -f "usr/local/bin/quilt daemon" 2>/dev/null || true

# Wait for processes to die
sleep 1

# Verify all quilt daemon/binary processes are dead (exclude this install script)
if pgrep -f "quilt daemon" > /dev/null 2>&1 || pgrep -x quilt > /dev/null 2>&1; then
    echo "      ⚠ Some quilt processes still running, force killing..."
    # Nuclear option - kill by PID directly (only actual quilt binaries, not scripts)
    for pid in $(pgrep -f "quilt daemon"); do
        kill -9 "$pid" 2>/dev/null || true
    done
    for pid in $(pgrep -x quilt); do
        kill -9 "$pid" 2>/dev/null || true
    done
    sleep 1
fi

# Final verification (only check for actual quilt binaries, not this script)
REMAINING=$(pgrep -f "quilt daemon" 2>/dev/null || pgrep -x quilt 2>/dev/null || true)
if [ -n "$REMAINING" ]; then
    echo "      ❌ WARNING: Could not kill all quilt processes"
    echo "         Please manually kill these processes and try again:"
    ps -p "$REMAINING" -o pid,cmd
    exit 1
else
    echo "      ✓ All quilt processes stopped"
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

# 2a. Generate default container image with full runtimes
echo "   🎨 Generating default container image with Node.js, Bun, and Python..."
if [ ! -f "/var/lib/quilt/images/default.tar.gz" ]; then
    TEMP_DIR=$(mktemp -d)
    ARCH=$(uname -m)
    NODE_VERSION="20.10.0"

    # Create complete directory structure
    mkdir -p "$TEMP_DIR"/{bin,sbin,lib,lib64,etc,proc,sys,dev,tmp,var/log,var/tmp,usr/bin,usr/lib,usr/local/bin,root,home,opt}
    chmod 1777 "$TEMP_DIR/tmp"
    chmod 1777 "$TEMP_DIR/var/tmp"

    # Copy essential host binaries
    echo "      📦 Copying essential system binaries..."
    for binary in sh bash ls cat echo mkdir rm cp mv pwd grep sed awk tar gzip gunzip \
                  sleep head tail test true false which find touch chmod chown df du \
                  wc sort uniq cut tr expr basename dirname realpath readlink xargs env \
                  uname date whoami id hostname; do
        if command -v "$binary" >/dev/null 2>&1; then
            bin_path=$(which "$binary" 2>/dev/null)
            if [ -n "$bin_path" ] && [ -f "$bin_path" ]; then
                cp "$bin_path" "$TEMP_DIR/usr/bin/" 2>/dev/null || true
                ln -sf "/usr/bin/$binary" "$TEMP_DIR/bin/$binary" 2>/dev/null || true
            fi
        fi
    done

    # Ensure sh exists
    if [ ! -f "$TEMP_DIR/bin/sh" ] && [ -f "$TEMP_DIR/usr/bin/bash" ]; then
        ln -sf /usr/bin/bash "$TEMP_DIR/bin/sh"
    fi

    # Install Node.js
    echo "      📦 Installing Node.js v${NODE_VERSION}..."
    NODE_ARCH="x64"
    if [ "$ARCH" = "aarch64" ]; then
        NODE_ARCH="arm64"
    fi
    NODE_URL="https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${NODE_ARCH}.tar.xz"
    NODE_TAR=$(mktemp)
    if curl -fsSL "$NODE_URL" -o "$NODE_TAR" 2>/dev/null; then
        mkdir -p "$TEMP_DIR/usr/local"
        tar -xJf "$NODE_TAR" -C "$TEMP_DIR/usr/local" --strip-components=1
        ln -sf /usr/local/bin/node "$TEMP_DIR/usr/bin/node"
        ln -sf /usr/local/bin/npm "$TEMP_DIR/usr/bin/npm"
        ln -sf /usr/local/bin/npx "$TEMP_DIR/usr/bin/npx"
        echo "         ✓ Node.js installed"
        rm -f "$NODE_TAR"
    else
        echo "         ⚠ Failed to download Node.js"
    fi

    # Install Bun
    echo "      📦 Installing Bun..."
    BUN_ARCH="x64"
    if [ "$ARCH" = "aarch64" ]; then
        BUN_ARCH="aarch64"
    fi
    BUN_URL="https://github.com/oven-sh/bun/releases/latest/download/bun-linux-${BUN_ARCH}.zip"
    BUN_ZIP=$(mktemp)
    if curl -fsSL "$BUN_URL" -o "$BUN_ZIP" 2>/dev/null && command -v unzip >/dev/null 2>&1; then
        unzip -q "$BUN_ZIP" -d "$TEMP_DIR/tmp/" 2>/dev/null
        if [ -f "$TEMP_DIR/tmp/bun-linux-${BUN_ARCH}/bun" ]; then
            mv "$TEMP_DIR/tmp/bun-linux-${BUN_ARCH}/bun" "$TEMP_DIR/usr/local/bin/"
            chmod +x "$TEMP_DIR/usr/local/bin/bun"
            rm -rf "$TEMP_DIR/tmp/bun-linux-${BUN_ARCH}"
            ln -sf /usr/local/bin/bun "$TEMP_DIR/usr/bin/bun"
            ln -sf /usr/local/bin/bun "$TEMP_DIR/usr/bin/bunx"
            echo "         ✓ Bun installed"
        fi
        rm -f "$BUN_ZIP"
    else
        echo "         ⚠ Failed to download Bun"
    fi

    # Install Python
    echo "      📦 Installing Python..."
    if command -v python3 >/dev/null 2>&1; then
        PYTHON_BIN=$(which python3)
        PYTHON_VERSION=$(python3 --version 2>&1 | cut -d' ' -f2 | cut -d'.' -f1-2)
        cp "$PYTHON_BIN" "$TEMP_DIR/usr/bin/python3"
        ln -sf python3 "$TEMP_DIR/usr/bin/python"
        chmod +x "$TEMP_DIR/usr/bin/python3"

        for LIB_DIR in /usr/lib/python${PYTHON_VERSION} /usr/lib/python3; do
            if [ -d "$LIB_DIR" ]; then
                mkdir -p "$TEMP_DIR$LIB_DIR"
                cp -r "$LIB_DIR"/* "$TEMP_DIR$LIB_DIR/" 2>/dev/null || true
            fi
        done

        if command -v pip3 >/dev/null 2>&1; then
            cp "$(which pip3)" "$TEMP_DIR/usr/bin/pip3" 2>/dev/null || true
            ln -sf pip3 "$TEMP_DIR/usr/bin/pip"
        fi
        echo "         ✓ Python installed"
    fi

    # Copy all required shared libraries
    echo "      📦 Copying shared libraries..."
    LIBS_NEEDED=""
    for BIN_DIR in "$TEMP_DIR/bin" "$TEMP_DIR/usr/bin" "$TEMP_DIR/usr/local/bin"; do
        if [ -d "$BIN_DIR" ]; then
            for BINARY in "$BIN_DIR"/*; do
                if [ -x "$BINARY" ] && file "$BINARY" 2>/dev/null | grep -q "ELF"; then
                    LIBS_NEEDED="$LIBS_NEEDED $(ldd "$BINARY" 2>/dev/null | grep '=> /' | awk '{print $3}')"
                    LIBS_NEEDED="$LIBS_NEEDED $(ldd "$BINARY" 2>/dev/null | grep 'ld-linux' | awk '{print $1}')"
                fi
            done
        fi
    done

    for LIB in $(echo "$LIBS_NEEDED" | tr ' ' '\n' | sort -u); do
        if [ -f "$LIB" ]; then
            if [[ "$LIB" == */lib64/* ]]; then
                mkdir -p "$TEMP_DIR/lib64"
                cp -L "$LIB" "$TEMP_DIR/lib64/" 2>/dev/null || true
            else
                cp -L "$LIB" "$TEMP_DIR/lib/" 2>/dev/null || true
            fi
        fi
    done

    # Ensure ld-linux loader
    if [ -f /lib64/ld-linux-x86-64.so.2 ]; then
        mkdir -p "$TEMP_DIR/lib64"
        cp -L /lib64/ld-linux-x86-64.so.2 "$TEMP_DIR/lib64/" 2>/dev/null || true
    elif [ -f /lib/ld-linux-aarch64.so.1 ]; then
        cp -L /lib/ld-linux-aarch64.so.1 "$TEMP_DIR/lib/" 2>/dev/null || true
    fi

    # Copy NSS libraries
    for NSS_LIB in libnss_files libnss_dns libresolv; do
        for LIB_DIR in /lib /lib64 /usr/lib /lib/x86_64-linux-gnu /lib/aarch64-linux-gnu; do
            if ls "$LIB_DIR"/${NSS_LIB}* 2>/dev/null | head -1 >/dev/null; then
                cp -L "$LIB_DIR"/${NSS_LIB}* "$TEMP_DIR/lib/" 2>/dev/null || true
            fi
        done
    done

    # Create essential system files
    cat > "$TEMP_DIR/etc/passwd" << 'EOF'
root:x:0:0:root:/root:/bin/bash
nobody:x:65534:65534:nobody:/nonexistent:/usr/sbin/nologin
EOF

    cat > "$TEMP_DIR/etc/group" << 'EOF'
root:x:0:
nobody:x:65534:
EOF

    cat > "$TEMP_DIR/etc/hosts" << 'EOF'
127.0.0.1 localhost
::1 localhost ip6-localhost ip6-loopback
EOF

    echo "quilt-container" > "$TEMP_DIR/etc/hostname"

    cat > "$TEMP_DIR/etc/nsswitch.conf" << 'EOF'
passwd:         files
group:          files
shadow:         files
hosts:          files dns
networks:       files
protocols:      files
services:       files
ethers:         files
rpc:            files
EOF

    cat > "$TEMP_DIR/etc/resolv.conf" << 'EOF'
nameserver 8.8.8.8
nameserver 8.8.4.4
EOF

    cat > "$TEMP_DIR/etc/ld.so.conf" << 'EOF'
/lib
/lib64
/usr/lib
/usr/lib64
/usr/local/lib
EOF

    cat > "$TEMP_DIR/etc/profile" << 'EOF'
export PATH="/usr/local/bin:/usr/bin:/bin"
export HOME="/root"
export USER="root"
export SHELL="/bin/bash"
export TERM="xterm-256color"
export LANG="C.UTF-8"
export LC_ALL="C.UTF-8"
export NODE_PATH="/usr/local/lib/node_modules"
EOF

    # Make binaries executable
    chmod +x "$TEMP_DIR/bin"/* "$TEMP_DIR/usr/bin"/* 2>/dev/null || true

    # Create tarball
    echo "      📦 Creating container image..."
    (cd "$TEMP_DIR" && tar czf - .) > "/var/lib/quilt/images/default.tar.gz"

    # Cleanup
    rm -rf "$TEMP_DIR"

    echo "      ✓ Default image created ($(du -h /var/lib/quilt/images/default.tar.gz | cut -f1))"
    echo "        Includes: Node.js, Bun, Python, and essential system utilities"
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
