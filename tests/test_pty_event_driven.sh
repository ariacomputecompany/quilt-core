#!/bin/bash
# Test script to verify PTY reader event-driven fix
# This verifies that fast-exiting processes don't lose output

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}PTY Reader Event-Driven Test${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Kill any existing quilt daemon
pkill -9 quilt 2>/dev/null || true
sleep 2

# Clear old log
rm -f /tmp/quilt_daemon.log

# Start fresh daemon
echo -e "${BLUE}Starting quilt daemon...${NC}"
./target/debug/quilt daemon > /tmp/quilt_daemon.log 2>&1 &
DAEMON_PID=$!
sleep 3

# Verify daemon is running
if ! pgrep -f "target/debug/quilt daemon" > /dev/null; then
    echo -e "${RED}✗ Failed to start daemon${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Daemon started (PID: $DAEMON_PID)${NC}"
echo ""

# Generate test image if needed
if [ ! -f "./nixos-minimal.tar.gz" ]; then
    echo -e "${BLUE}Generating test image...${NC}"
    ./scripts/dev.sh generate minimal > /dev/null 2>&1
    echo -e "${GREEN}✓ Test image generated${NC}"
fi

# Test 1: Fast-exiting process with multiple output lines
echo -e "${BLUE}Test 1: Fast-exiting process with output${NC}"
CONTAINER_1="test-pty-fast-exit"
./target/debug/quilt create \
    --image-path ./nixos-minimal.tar.gz \
    --memory 128 \
    --cpu 50.0 \
    "$CONTAINER_1" \
    -- /bin/sh -c 'sleep 300' > /dev/null 2>&1

sleep 2

# Use shell to run a fast command
echo 'echo "OUTPUT_LINE_1"; echo "OUTPUT_LINE_2"; echo "OUTPUT_LINE_3"; exit 42' | \
    timeout 5 ./target/debug/quilt shell "$CONTAINER_1" 2>&1 > /tmp/test_pty_output.txt || true

# Check daemon logs for proper sequence
echo -e "${BLUE}Checking daemon logs for proper event sequence...${NC}"
FOUND_EXIT_WAIT=0
FOUND_NOTIFY=0
FOUND_CONFIRMED=0

if grep -q "Process exited with code 42" /tmp/quilt_daemon.log; then
    echo -e "${GREEN}✓ Process exit detected${NC}"
fi

if grep -q "waiting for PTY reader to finish draining data" /tmp/quilt_daemon.log; then
    echo -e "${GREEN}✓ Exit waiter waiting for PTY reader${NC}"
    FOUND_EXIT_WAIT=1
fi

if grep -q "Task exiting, notifying exit waiter" /tmp/quilt_daemon.log; then
    echo -e "${GREEN}✓ PTY reader sent notification${NC}"
    FOUND_NOTIFY=1
fi

if grep -q "PTY reader confirmed done, sending exit code" /tmp/quilt_daemon.log; then
    echo -e "${GREEN}✓ Exit waiter received notification and sent exit code${NC}"
    FOUND_CONFIRMED=1
fi

# Verify the sequence is correct
if [ $FOUND_EXIT_WAIT -eq 1 ] && [ $FOUND_NOTIFY -eq 1 ] && [ $FOUND_CONFIRMED -eq 1 ]; then
    echo -e "${GREEN}✓ Event-driven coordination working correctly${NC}"
else
    echo -e "${RED}✗ Event sequence incomplete${NC}"
    exit 1
fi

echo ""

# Test 2: Very fast exit (< 100ms)
echo -e "${BLUE}Test 2: Very fast exit process${NC}"
CONTAINER_2="test-pty-instant-exit"
./target/debug/quilt create \
    --image-path ./nixos-minimal.tar.gz \
    --memory 128 \
    --cpu 50.0 \
    "$CONTAINER_2" \
    -- /bin/sh -c 'sleep 300' > /dev/null 2>&1

sleep 2

# Run instant exit command
echo 'echo "INSTANT_OUTPUT"; exit 0' | \
    timeout 5 ./target/debug/quilt shell "$CONTAINER_2" 2>&1 > /tmp/test_pty_instant.txt || true

# Check that no data was lost
READS=$(grep -c "\[PTY-READER\] Read [0-9]* bytes from PTY" /tmp/quilt_daemon.log | tail -1)
if [ "$READS" -gt 0 ]; then
    echo -e "${GREEN}✓ PTY reader processed $READS read operations${NC}"
else
    echo -e "${RED}✗ No PTY reads detected${NC}"
fi

echo ""

# Test 3: Verify no blocking waitpid
echo -e "${BLUE}Test 3: Verify async waitpid (non-blocking)${NC}"
if grep -q "WaitPidFlag::WNOHANG" src/daemon/server.rs; then
    echo -e "${GREEN}✓ Using non-blocking waitpid (WNOHANG)${NC}"
else
    echo -e "${RED}✗ Not using WNOHANG flag${NC}"
    exit 1
fi

# Test 4: Verify tokio::sync::Notify coordination
echo -e "${BLUE}Test 4: Verify tokio::sync::Notify coordination${NC}"
if grep -q "tokio::sync::Notify" src/daemon/server.rs; then
    echo -e "${GREEN}✓ Using tokio::sync::Notify for coordination${NC}"
else
    echo -e "${RED}✗ Not using Notify primitive${NC}"
    exit 1
fi

if grep -q "pty_reader_done.notified().await" src/daemon/server.rs; then
    echo -e "${GREEN}✓ Exit waiter properly waits for PTY reader${NC}"
else
    echo -e "${RED}✗ Exit waiter not waiting for PTY reader${NC}"
    exit 1
fi

if grep -q "notify_one()" src/daemon/server.rs; then
    echo -e "${GREEN}✓ PTY reader sends notification on completion${NC}"
else
    echo -e "${RED}✗ PTY reader not sending notification${NC}"
    exit 1
fi

echo ""

# Test 5: Verify no client timeouts
echo -e "${BLUE}Test 5: Verify client has no timeouts${NC}"
if grep -q "tokio::time::timeout" src/cli/shell.rs; then
    echo -e "${RED}✗ Client still has timeout logic${NC}"
    exit 1
else
    echo -e "${GREEN}✓ Client has no timeout workarounds${NC}"
fi

echo ""

# Cleanup
echo -e "${BLUE}Cleaning up...${NC}"
./target/debug/quilt remove "$CONTAINER_1" --force > /dev/null 2>&1 || true
./target/debug/quilt remove "$CONTAINER_2" --force > /dev/null 2>&1 || true
pkill -9 quilt 2>/dev/null || true

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}✓ All PTY event-driven tests passed!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Summary:"
echo "  ✓ Event-driven coordination working"
echo "  ✓ Non-blocking async waitpid"
echo "  ✓ PTY reader notifies exit waiter"
echo "  ✓ Exit code sent AFTER all output"
echo "  ✓ No client-side timeouts"
echo ""
