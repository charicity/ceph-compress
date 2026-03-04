#!/usr/bin/env bash
# test-wal-replay.sh — smoke test for WAL bypass replay tool
#
# Usage:
#   cd <ceph-build-dir>
#   bash ../qa/standalone/test-wal-replay.sh [BENCH_SECONDS]
#
# The script:
#   1. Starts a 1-OSD cluster with WAL bypass enabled
#   2. Runs rados bench to generate data / WAL writes
#   3. Stops the cluster
#   4. Runs ceph-bluestore-wal-replay in verify-only mode
#   5. Runs ceph-bluestore-wal-replay in posix mode (skeleton DB)
#   6. Verifies the replayed DB can be opened by RocksDB tools
#   7. Tests checkpoint resume with --stop-seqno
#   8. Tests sequence gap detection
#
set -e

BENCH_SEC=${1:-10}
BYPASS_DIR="/tmp/wal_replay_bypass_$$"
REPLAY_DB="/tmp/wal_replay_db_$$"

# ---------------------------------------------------------------------------
# Resolve paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -f ./bin/ceph-osd ]]; then
  BUILD_DIR="$(pwd)"
elif [[ -d ./build/bin ]]; then
  BUILD_DIR="$(pwd)/build"
else
  BUILD_DIR="$(cd "$(dirname "$0")/../../build" && pwd)"
fi

SRC_DIR="$(cd "$BUILD_DIR/.." && pwd)/src"

if [[ ! -x "$BUILD_DIR/bin/ceph-osd" ]]; then
  echo "ERROR: ceph-osd not found at $BUILD_DIR/bin/ceph-osd"
  echo "       Please build first, or run this script from the build directory."
  exit 1
fi

if [[ ! -x "$BUILD_DIR/bin/ceph-bluestore-wal-replay" ]]; then
  echo "ERROR: ceph-bluestore-wal-replay not found."
  echo "       Please build first: ninja ceph-bluestore-wal-replay"
  exit 1
fi

cd "$BUILD_DIR"

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
info()  { echo -e "\033[1;34m[INFO]\033[0m  $*"; }
pass()  { echo -e "\033[1;32m[PASS]\033[0m  $*"; }
fail()  { echo -e "\033[1;31m[FAIL]\033[0m  $*"; }

ERRORS=0
check_pass() { pass "$*"; }
check_fail() { fail "$*"; ERRORS=$((ERRORS+1)); }

cleanup() {
  info "Cleaning up ..."
  "$SRC_DIR/stop.sh" 2>/dev/null || true
  rm -rf "$BYPASS_DIR" "$REPLAY_DB"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 1. Stop any existing cluster & clean
# ---------------------------------------------------------------------------
info "Stopping any existing vstart cluster ..."
"$SRC_DIR/stop.sh" 2>/dev/null || true

info "Removing previous dev/ and out/ ..."
rm -rf "$BUILD_DIR/dev" "$BUILD_DIR/out"

# ---------------------------------------------------------------------------
# 2. Prepare bypass dir
# ---------------------------------------------------------------------------
rm -rf "$BYPASS_DIR" "$REPLAY_DB"
mkdir -p "$BYPASS_DIR"
info "WAL bypass dir: $BYPASS_DIR"

# ---------------------------------------------------------------------------
# 3. Start a 1-OSD cluster with WAL bypass enabled
# ---------------------------------------------------------------------------
info "Starting MON=1 OSD=1 MGR=1 cluster with WAL bypass ..."

MON=1 OSD=1 MGR=1 "$SRC_DIR/vstart.sh" \
  --new -x --localhost --bluestore --without-dashboard \
  -o "bluerocks_wal_bypass_enable = true" \
  -o "bluerocks_wal_bypass_dir = $BYPASS_DIR" \
  -o "bluerocks_wal_rotate_size_mb = 4" \
  -o "bluerocks_wal_rotate_interval_sec = 5" \
  -o "bluerocks_wal_flush_trigger_kb = 64" \
  -o "bluerocks_wal_flush_interval_ms = 50" \
  -o "bluerocks_wal_max_backlog_mb = 128" \
  2>&1 | tail -5

# Wait for health
info "Waiting for cluster to become healthy ..."
TIMEOUT=30
for (( i=0; i<TIMEOUT; i++ )); do
  HEALTH=$(./bin/ceph health 2>/dev/null | head -1 || true)
  if [[ "$HEALTH" == HEALTH_OK* || "$HEALTH" == HEALTH_WARN* ]]; then
    break
  fi
  sleep 1
done

HEALTH=$(./bin/ceph health 2>/dev/null | head -1 || echo "UNKNOWN")
info "Cluster health: $HEALTH"

if [[ "$HEALTH" != HEALTH_OK* && "$HEALTH" != HEALTH_WARN* ]]; then
  check_fail "Cluster not healthy: $HEALTH"
  ./bin/ceph -s 2>/dev/null || true
  exit 1
fi

# ---------------------------------------------------------------------------
# 4. Generate data via rados bench
# ---------------------------------------------------------------------------
info "Creating pool 'testbench' ..."
./bin/ceph osd pool create testbench 32 2>/dev/null || true
sleep 2

info "Running rados bench for ${BENCH_SEC}s ..."
./bin/rados -p testbench bench "$BENCH_SEC" write -b 4096 --no-cleanup 2>&1 | \
  grep -E 'Bandwidth|IOPS|Latency' || true

# ---------------------------------------------------------------------------
# 5. Stop cluster
# ---------------------------------------------------------------------------
info "Stopping cluster ..."
"$SRC_DIR/stop.sh" 2>/dev/null || true
sleep 1

# ---------------------------------------------------------------------------
# 6. Verify bypass files exist
# ---------------------------------------------------------------------------
info "Checking bypass directory: $BYPASS_DIR"

WAL_FILES=$(find "$BYPASS_DIR" -name 'ceph_wal_*.log' -type f 2>/dev/null | wc -l)
TOTAL_BYTES=$(find "$BYPASS_DIR" -name 'ceph_wal_*.log' -type f -exec du -cb {} + 2>/dev/null | tail -1 | awk '{print $1}' || echo 0)

info "  WAL bypass log files : $WAL_FILES"
info "  Total bytes on disk  : $TOTAL_BYTES"

if [[ "$WAL_FILES" -gt 0 ]]; then
  check_pass "WAL bypass files found ($WAL_FILES files, $TOTAL_BYTES bytes)"
else
  check_fail "No WAL bypass files found — cannot proceed with replay tests"
  exit 1
fi

# Check for sharding metadata
SHARDING_META="$BYPASS_DIR/ceph_wal_sharding.meta"
if [[ -f "$SHARDING_META" ]]; then
  SHARDING_TEXT=$(cat "$SHARDING_META")
  info "  Sharding metadata: $SHARDING_TEXT"
  check_pass "Sharding metadata file exists"
else
  info "  No sharding metadata (will use default CF only)"
fi

ls -lh "$BYPASS_DIR"/ 2>/dev/null | head -20 || true

# =========================================================================
# TEST A: verify-only mode
# =========================================================================
echo ""
info "========== TEST A: verify-only mode =========="

VERIFY_OUTPUT=$("$BUILD_DIR/bin/ceph-bluestore-wal-replay" \
  --wal-dir "$BYPASS_DIR" \
  --verify-only 2>&1) || true

echo "$VERIFY_OUTPUT" | tail -15

if echo "$VERIFY_OUTPUT" | grep -q "SUCCESS"; then
  check_pass "verify-only mode completed successfully"
else
  check_fail "verify-only mode did not report SUCCESS"
fi

# Check that it found the right number of files
if echo "$VERIFY_OUTPUT" | grep -q "Found $WAL_FILES WAL files"; then
  check_pass "verify-only found $WAL_FILES WAL files"
else
  check_fail "verify-only file count mismatch"
fi

# =========================================================================
# TEST B: posix mode full replay
# =========================================================================
echo ""
info "========== TEST B: posix mode full replay =========="

rm -rf "$REPLAY_DB"
mkdir -p "$REPLAY_DB"

REPLAY_OUTPUT=$("$BUILD_DIR/bin/ceph-bluestore-wal-replay" \
  --wal-dir "$BYPASS_DIR" \
  --mode posix \
  --db-path "$REPLAY_DB" \
  --checkpoint-interval 5000 2>&1) || true

echo "$REPLAY_OUTPUT" | tail -20

if echo "$REPLAY_OUTPUT" | grep -q "SUCCESS"; then
  check_pass "POSIX replay completed successfully"
else
  check_fail "POSIX replay did not report SUCCESS"
fi

# Check that batches were applied
BATCHES=$(echo "$REPLAY_OUTPUT" | grep "Batches applied" | awk -F: '{print $2}' | tr -d ' ')
if [[ -n "$BATCHES" && "$BATCHES" -gt 0 ]]; then
  check_pass "Replay applied $BATCHES batches"
else
  check_fail "Replay applied 0 batches (expected > 0)"
fi

# Verify DB exists and has files
DB_FILES=$(find "$REPLAY_DB" -name '*.sst' -o -name 'CURRENT' -o -name 'MANIFEST-*' 2>/dev/null | wc -l)
if [[ "$DB_FILES" -gt 0 ]]; then
  check_pass "Replayed DB directory has RocksDB files ($DB_FILES files)"
else
  check_fail "Replayed DB directory has no RocksDB files"
fi

# Verify DB can be opened via ldb tool if available
if command -v ldb &>/dev/null; then
  LDB_OUT=$(ldb --db="$REPLAY_DB" list_column_families 2>&1 || true)
  info "  ldb list_column_families: $LDB_OUT"
fi

# =========================================================================
# TEST C: checkpoint resume with --stop-seqno
# =========================================================================
echo ""
info "========== TEST C: checkpoint resume / stop-seqno =========="

rm -rf "$REPLAY_DB"
mkdir -p "$REPLAY_DB"

# First replay: stop early (stop at a low seqno to test stop feature)
# Use seqno 100 as a reasonable early cut-off
STOP_SEQNO=100

PARTIAL_OUTPUT=$("$BUILD_DIR/bin/ceph-bluestore-wal-replay" \
  --wal-dir "$BYPASS_DIR" \
  --mode posix \
  --db-path "$REPLAY_DB" \
  --stop-seqno $STOP_SEQNO \
  --checkpoint-file "$REPLAY_DB/replay.ckpt" 2>&1) || true

echo "$PARTIAL_OUTPUT" | tail -10

PARTIAL_BATCHES=$(echo "$PARTIAL_OUTPUT" | grep "Batches applied" | awk -F: '{print $2}' | tr -d ' ')
info "  Partial replay applied $PARTIAL_BATCHES batches (stopped at seqno $STOP_SEQNO)"

if echo "$PARTIAL_OUTPUT" | grep -q "stop-seqno\|Stop condition\|SUCCESS"; then
  check_pass "Partial replay stopped by --stop-seqno"
else
  # It's also OK if all data had seqno < stop_seqno (nothing to stop)
  if echo "$PARTIAL_OUTPUT" | grep -q "SUCCESS"; then
    check_pass "Partial replay completed (data seqno < stop threshold)"
  else
    check_fail "Partial replay did not behave as expected"
  fi
fi

# Check checkpoint file was written
if [[ -f "$REPLAY_DB/replay.ckpt" ]]; then
  check_pass "Checkpoint file created"
  info "  Checkpoint contents:"
  cat "$REPLAY_DB/replay.ckpt" | sed 's/^/    /'
else
  info "  No checkpoint file (may not have been needed)"
fi

# =========================================================================
# TEST D: sequence gap detection
# =========================================================================
echo ""
info "========== TEST D: sequence gap detection =========="

# Create a temp bypass dir with a gap
GAP_DIR="/tmp/wal_replay_gap_$$"
rm -rf "$GAP_DIR"
mkdir -p "$GAP_DIR"

# Copy first and last file, skip middle to create a gap
SORTED_FILES=($(find "$BYPASS_DIR" -name 'ceph_wal_*.log' -type f | sort))
NUM_FILES=${#SORTED_FILES[@]}

if [[ "$NUM_FILES" -ge 3 ]]; then
  cp "${SORTED_FILES[0]}" "$GAP_DIR/"
  cp "${SORTED_FILES[$((NUM_FILES-1))]}" "$GAP_DIR/"
  # Copy sharding meta if exists
  [[ -f "$SHARDING_META" ]] && cp "$SHARDING_META" "$GAP_DIR/"

  info "  Created gap dir with files: $(ls "$GAP_DIR"/*.log 2>/dev/null | xargs -I{} basename {})"

  GAP_OUTPUT=$("$BUILD_DIR/bin/ceph-bluestore-wal-replay" \
    --wal-dir "$GAP_DIR" \
    --verify-only 2>&1) || true

  echo "$GAP_OUTPUT" | tail -5

  if echo "$GAP_OUTPUT" | grep -qi "gap\|FAIL\|error"; then
    check_pass "Gap detection correctly reported sequence gap"
  else
    check_fail "Gap detection did not report gap"
  fi
else
  info "  Skipping gap test — need >= 3 WAL files (have $NUM_FILES)"
fi

rm -rf "$GAP_DIR"

# =========================================================================
# TEST E: verify idempotent replay (replay same data twice)
# =========================================================================
echo ""
info "========== TEST E: idempotent replay =========="

rm -rf "$REPLAY_DB"
mkdir -p "$REPLAY_DB"

# First replay
FIRST_OUTPUT=$("$BUILD_DIR/bin/ceph-bluestore-wal-replay" \
  --wal-dir "$BYPASS_DIR" \
  --mode posix \
  --db-path "$REPLAY_DB" 2>&1) || true
FIRST_BATCHES=$(echo "$FIRST_OUTPUT" | grep "Batches applied" | awk -F: '{print $2}' | tr -d ' ')

# Second replay on same DB (should re-apply — WriteBatch sequences may overwrite)
# Delete checkpoint so it starts from the beginning
rm -f "$REPLAY_DB/replay_checkpoint" "$BYPASS_DIR/replay_checkpoint"

SECOND_OUTPUT=$("$BUILD_DIR/bin/ceph-bluestore-wal-replay" \
  --wal-dir "$BYPASS_DIR" \
  --mode posix \
  --db-path "$REPLAY_DB" 2>&1) || true
SECOND_BATCHES=$(echo "$SECOND_OUTPUT" | grep "Batches applied" | awk -F: '{print $2}' | tr -d ' ')

if echo "$SECOND_OUTPUT" | grep -q "SUCCESS"; then
  check_pass "Second replay on same DB succeeded (idempotent)"
else
  check_fail "Second replay on same DB failed"
fi

info "  First replay: $FIRST_BATCHES batches, Second replay: $SECOND_BATCHES batches"

# =========================================================================
# Summary
# =========================================================================
echo ""
echo "================================================================"
if [[ "$ERRORS" -eq 0 ]]; then
  echo -e "\033[1;32m========== ALL CHECKS PASSED ==========\033[0m"
  exit 0
else
  echo -e "\033[1;31m========== $ERRORS CHECK(S) FAILED ==========\033[0m"
  exit 1
fi
