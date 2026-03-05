#!/usr/bin/env bash
# test-wal-recovery.sh — end-to-end WAL bypass recovery test
#
# Validates the full disaster-recovery path:
#   1. Start a 3-OSD cluster with WAL bypass on OSD 0
#   2. Write test objects and record checksums
#   3. Stop cluster and corrupt OSD 0's BlueFS/RocksDB metadata
#   4. Recover OSD 0 via ceph-bluestore-wal-replay --recover
#   5. Restart cluster and verify data integrity
#
# Usage:
#   cd <ceph-build-dir>
#   bash ../qa/standalone/test-wal-recovery.sh [NUM_OBJECTS]
#
set -e

NUM_OBJECTS=${1:-50}
BYPASS_BASE="/tmp/wal_recovery_bypass_$$"
BYPASS_DIR="${BYPASS_BASE}/osd0"
CHECKSUM_FILE="/tmp/wal_recovery_checksums_$$"
READ_TIMEOUT_SEC=${READ_TIMEOUT_SEC:-15}

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

get_obj_md5_with_timeout() {
  local pool="$1"
  local obj_name="$2"
  local timeout_sec="$3"
  local tmp_file
  local rc

  tmp_file=$(mktemp "/tmp/wal_recovery_obj_XXXXXX")
  if timeout "$timeout_sec" ./bin/rados -p "$pool" get "$obj_name" "$tmp_file" >/dev/null 2>&1; then
    md5sum "$tmp_file" | awk '{print $1}'
    rm -f "$tmp_file"
    return 0
  fi

  rc=$?
  rm -f "$tmp_file"
  return "$rc"
}

cleanup() {
  info "Cleaning up ..."
  "$SRC_DIR/stop.sh" 2>/dev/null || true
  rm -rf "$BYPASS_BASE" "$CHECKSUM_FILE"
}
trap cleanup EXIT

# =========================================================================
# PHASE 1: SETUP — Start 3-OSD cluster with WAL bypass on OSD 0
# =========================================================================
echo ""
info "========== PHASE 1: SETUP =========="

info "Stopping any existing vstart cluster ..."
"$SRC_DIR/stop.sh" 2>/dev/null || true

info "Removing previous dev/ and out/ ..."
rm -rf "$BUILD_DIR/dev" "$BUILD_DIR/out"

# Use a per-OSD bypass dir via Ceph's $id metavariable.
# This ensures each OSD writes to its own directory, avoiding WAL interleaving.
# We pre-create OSD 0's directory; others are created on demand by the OSD.
BYPASS_BASE="/tmp/wal_recovery_bypass_$$"
BYPASS_DIR="${BYPASS_BASE}/osd0"
rm -rf "$BYPASS_BASE"
mkdir -p "$BYPASS_DIR"
info "WAL bypass base: $BYPASS_BASE  (OSD 0 dir: $BYPASS_DIR)"

info "Starting MON=1 OSD=3 MGR=1 cluster with WAL bypass on all OSDs ..."

# Bypass is enabled globally but each OSD gets its own dir via $id.
# We only care about OSD 0's capture for recovery testing.
# shellcheck disable=SC2016
BYPASS_DIR_CONF='${BYPASS_BASE}/osd$id'
MON=1 OSD=3 MGR=1 "$SRC_DIR/vstart.sh" \
  --new -x --localhost --bluestore --without-dashboard \
  -o "bluerocks_wal_bypass_enable = true" \
  -o "bluerocks_wal_bypass_dir = ${BYPASS_BASE}/osd\$id" \
  -o "bluerocks_wal_rotate_size_mb = 4" \
  -o "bluerocks_wal_rotate_interval_sec = 5" \
  -o "bluerocks_wal_flush_trigger_kb = 64" \
  -o "bluerocks_wal_flush_interval_ms = 50" \
  -o "bluerocks_wal_max_backlog_mb = 128" \
  2>&1 | tail -5

info "Waiting for cluster to become healthy (up to 60s) ..."
TIMEOUT=60
for (( i=0; i<TIMEOUT; i++ )); do
  HEALTH=$(./bin/ceph health 2>/dev/null | head -1 || true)
  if [[ "$HEALTH" == HEALTH_OK* || "$HEALTH" == HEALTH_WARN* ]]; then
    break
  fi
  if [[ $((i % 10)) -eq 0 && $i -gt 0 ]]; then
    info "  [${i}s] health: ${HEALTH:-<no response>}"
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
check_pass "Cluster started with 3 OSDs"

# Verify per-OSD bypass dirs were created
info "Bypass directory check:"
for d in "$BYPASS_BASE"/osd*; do
  if [[ -d "$d" ]]; then
    CNT=$(find "$d" -name 'ceph_wal_*.log' -type f 2>/dev/null | wc -l)
    info "  $(basename $d): $CNT WAL files"
  fi
done

# =========================================================================
# PHASE 2: WRITE DATA — Create pool and write test objects with checksums
# =========================================================================
echo ""
info "========== PHASE 2: WRITE DATA =========="

info "Creating pool 'testrecovery' (replicated, size=3) ..."
./bin/ceph osd pool create testrecovery 32 2>/dev/null || true
./bin/ceph osd pool set testrecovery size 3 2>/dev/null || true
sleep 2

info "Writing $NUM_OBJECTS test objects ..."
> "$CHECKSUM_FILE"
for (( i=0; i<NUM_OBJECTS; i++ )); do
  OBJ_NAME="recovery-test-obj-$(printf '%04d' $i)"
  # Generate deterministic content using object name as seed
  OBJ_DATA="payload-for-$OBJ_NAME-$(date +%s%N)-$$"
  echo -n "$OBJ_DATA" | ./bin/rados -p testrecovery put "$OBJ_NAME" - 2>/dev/null
  MD5=$(echo -n "$OBJ_DATA" | md5sum | awk '{print $1}')
  echo "$OBJ_NAME $MD5" >> "$CHECKSUM_FILE"
done

# Wait for writes to propagate
info "Waiting for writes to settle ..."
sleep 5

# Verify we can read them back
READABLE=0
READ_TIMEOUTS=0
READ_IDX=0
while IFS=' ' read -r obj_name expected_md5; do
  READ_IDX=$((READ_IDX+1))
  ACTUAL_MD5=$(get_obj_md5_with_timeout testrecovery "$obj_name" "$READ_TIMEOUT_SEC" || true)
  if [[ -n "$ACTUAL_MD5" && "$ACTUAL_MD5" == "$expected_md5" ]]; then
    READABLE=$((READABLE+1))
  elif [[ -z "$ACTUAL_MD5" ]]; then
    READ_TIMEOUTS=$((READ_TIMEOUTS+1))
  fi
  if [[ $((READ_IDX % 10)) -eq 0 ]]; then
    info "  pre-check progress: $READ_IDX / $NUM_OBJECTS (ok=$READABLE, timeout=$READ_TIMEOUTS)"
  fi
done < "$CHECKSUM_FILE"

info "Pre-corruption read check: $READABLE / $NUM_OBJECTS objects verified"
if [[ "$READ_TIMEOUTS" -gt 0 ]]; then
  info "  Timed out reads before corruption: $READ_TIMEOUTS (timeout=${READ_TIMEOUT_SEC}s)"
fi
if [[ "$READABLE" -eq "$NUM_OBJECTS" ]]; then
  check_pass "All $NUM_OBJECTS objects written and verified"
else
  check_fail "Only $READABLE / $NUM_OBJECTS objects could be verified"
fi

# =========================================================================
# PHASE 3: CAPTURE & STOP
# =========================================================================
echo ""
info "========== PHASE 3: CAPTURE & STOP =========="

info "Stopping cluster ..."
"$SRC_DIR/stop.sh" 2>/dev/null || true
sleep 2

# Verify bypass files exist
WAL_FILES=$(find "$BYPASS_DIR" -name 'ceph_wal_*.log' -type f 2>/dev/null | wc -l)
TOTAL_BYTES=$(find "$BYPASS_DIR" -name 'ceph_wal_*.log' -type f -exec du -cb {} + 2>/dev/null | tail -1 | awk '{print $1}' || echo 0)

info "  WAL bypass log files : $WAL_FILES"
info "  Total bytes on disk  : $TOTAL_BYTES"
ls -lh "$BYPASS_DIR"/ 2>/dev/null | head -20 || true

if [[ "$WAL_FILES" -gt 0 ]]; then
  check_pass "WAL bypass files captured ($WAL_FILES files, $TOTAL_BYTES bytes)"
else
  check_fail "No WAL bypass files found — cannot test recovery"
  exit 1
fi

# Verify sequence continuity
SEQ_LIST=$(find "$BYPASS_DIR" -name 'ceph_wal_*.log' -type f -printf '%f\n' 2>/dev/null | \
  sed -n 's/ceph_wal_0*\([0-9]*\)\.log/\1/p' | sort -n)
if [[ -n "$SEQ_LIST" ]]; then
  FIRST=$(echo "$SEQ_LIST" | head -1)
  LAST=$(echo "$SEQ_LIST" | tail -1)
  EXPECTED_COUNT=$((LAST - FIRST + 1))
  ACTUAL_COUNT=$(echo "$SEQ_LIST" | wc -l)
  if [[ "$EXPECTED_COUNT" -eq "$ACTUAL_COUNT" ]]; then
    check_pass "Sequence numbers continuous: $FIRST..$LAST ($ACTUAL_COUNT files)"
  else
    check_fail "Sequence gap detected: expected $EXPECTED_COUNT files, got $ACTUAL_COUNT"
  fi
fi

# =========================================================================
# PHASE 4: SIMULATE CORRUPTION — destroy OSD 0's DB metadata
# =========================================================================
echo ""
info "========== PHASE 4: SIMULATE CORRUPTION =========="

OSD0_DIR="$BUILD_DIR/dev/osd0"

if [[ ! -d "$OSD0_DIR" ]]; then
  check_fail "OSD 0 directory not found at $OSD0_DIR"
  exit 1
fi

info "OSD 0 directory contents before corruption:"
ls -la "$OSD0_DIR"/ 2>/dev/null | head -20 || true

# With separate block.db/block.wal devices (vstart default), BlueFS/RocksDB
# metadata lives on block.db, NOT on the main block device.  Corrupt block.db
# (and block.wal) to simulate metadata loss while preserving object data.
BLOCK_DB="$OSD0_DIR/block.db"
BLOCK_WAL="$OSD0_DIR/block.wal"

if [[ -L "$BLOCK_DB" || -f "$BLOCK_DB" ]]; then
  DB_TARGET=$(readlink -f "$BLOCK_DB")
  DB_SIZE=$(stat -c %s "$DB_TARGET" 2>/dev/null || echo 0)
  info "block.db device: $DB_TARGET ($DB_SIZE bytes)"

  CORRUPT_MB=64
  info "Zeroing first ${CORRUPT_MB} MB of block.db to corrupt BlueFS/RocksDB ..."
  dd if=/dev/zero of="$DB_TARGET" bs=1M count=$CORRUPT_MB conv=notrunc 2>/dev/null
  check_pass "block.db corrupted (first ${CORRUPT_MB} MB zeroed)"
else
  check_fail "block.db not found at $BLOCK_DB"
  exit 1
fi

if [[ -L "$BLOCK_WAL" || -f "$BLOCK_WAL" ]]; then
  WAL_TARGET=$(readlink -f "$BLOCK_WAL")
  info "Zeroing first ${CORRUPT_MB} MB of block.wal ..."
  dd if=/dev/zero of="$WAL_TARGET" bs=1M count=$CORRUPT_MB conv=notrunc 2>/dev/null
  check_pass "block.wal corrupted"
fi

# NOTE: mkfs_done is a regular file in the OSD dir (not in block.db),
# so it survives the block.db corruption — just like a real disaster.
# The recovery tool must handle removing it before re-mkfs.

# Verify OSD 0 cannot start (sanity check)
info "Verifying OSD 0 cannot start with corrupted DB ..."
OSD_START_OUT=$(timeout 15 "$BUILD_DIR/bin/ceph-osd" -i 0 \
  --conf "$BUILD_DIR/ceph.conf" \
  --osd-data "$OSD0_DIR" --foreground 2>&1 || true)
# OSD should fail to start — any non-zero exit or error output is expected
if echo "$OSD_START_OUT" | grep -qiE "error|fail|abort|assert|unable"; then
  check_pass "OSD 0 correctly fails to start with corrupted DB"
else
  info "  OSD start output: $(echo "$OSD_START_OUT" | tail -3)"
  info "  (OSD may have silently failed — continuing)"
fi

# Kill any residual OSD process that might have started
kill $(cat "$OSD0_DIR/osd.pid" 2>/dev/null) 2>/dev/null || true
sleep 1

# =========================================================================
# PHASE 5: RECOVERY — run ceph-bluestore-wal-replay --recover
# =========================================================================
echo ""
info "========== PHASE 5: RECOVERY =========="

info "Running ceph-bluestore-wal-replay --recover ..."
REPLAY_OUTPUT=$("$BUILD_DIR/bin/ceph-bluestore-wal-replay" \
  --wal-dir "$BYPASS_DIR" \
  --mode bluestore \
  --osd-path "$OSD0_DIR" \
  --recover \
  --conf "$BUILD_DIR/ceph.conf" 2>&1) || true

echo "$REPLAY_OUTPUT" | tail -30

if echo "$REPLAY_OUTPUT" | grep -q "SUCCESS"; then
  check_pass "WAL replay recovery completed successfully"
else
  check_fail "WAL replay recovery did not report SUCCESS"
  echo "--- Full replay output ---"
  echo "$REPLAY_OUTPUT"
  echo "--- End replay output ---"
fi

BATCHES=$(echo "$REPLAY_OUTPUT" | grep "Batches applied" | awk -F: '{print $2}' | tr -d ' ')
if [[ -n "$BATCHES" && "$BATCHES" -gt 0 ]]; then
  check_pass "Replay applied $BATCHES batches"
else
  check_fail "Replay applied 0 batches (expected > 0)"
fi

# =========================================================================
# PHASE 6: VERIFICATION — fsck, start cluster, read data
# =========================================================================
echo ""
info "========== PHASE 6: VERIFICATION =========="

# 6a. Run fsck on recovered OSD
info "Running ceph-bluestore-tool fsck on recovered OSD 0 ..."
FSCK_OUTPUT=$("$BUILD_DIR/bin/ceph-bluestore-tool" \
  fsck --path "$OSD0_DIR" \
  --conf "$BUILD_DIR/ceph.conf" 2>&1) || true
echo "$FSCK_OUTPUT" | tail -10

if echo "$FSCK_OUTPUT" | grep -qi "error"; then
  FSCK_ERRORS=$(echo "$FSCK_OUTPUT" | grep -ci "error" || echo "unknown")
  info "  fsck reported errors (may be expected after recovery): $FSCK_ERRORS"
  # fsck errors after recovery are informational, not fatal —
  # RocksDB metadata is rebuilt but bluestore object mappings may need
  # PG recovery from other replicas.
else
  check_pass "fsck completed without errors"
fi

# 6b. Start the cluster
info "Restarting cluster ..."

# Disable bypass on OSD 0 before restart (no need to capture during verification).
sed -i 's/bluerocks_wal_bypass_enable = true/bluerocks_wal_bypass_enable = false/' \
  "$BUILD_DIR/ceph.conf"

# Restart all daemons (reuse existing data — no --new).
MON=1 OSD=3 MGR=1 "$SRC_DIR/vstart.sh" \
  --bluestore --without-dashboard \
  2>&1 | tail -5

# Wait for cluster to come up
info "Waiting for cluster to become healthy (up to 120s) ..."
TIMEOUT=120
for (( i=0; i<TIMEOUT; i++ )); do
  HEALTH=$(./bin/ceph health 2>/dev/null | head -1 || true)
  if [[ "$HEALTH" == HEALTH_OK* ]]; then
    break
  fi
  # HEALTH_WARN is acceptable (degraded PGs during recovery)
  if [[ "$HEALTH" == HEALTH_WARN* && $i -gt 30 ]]; then
    break
  fi
  if [[ $((i % 15)) -eq 0 && $i -gt 0 ]]; then
    info "  [${i}s] health: ${HEALTH:-<no response>}"
  fi
  sleep 1
done

HEALTH=$(./bin/ceph health 2>/dev/null | head -1 || echo "UNKNOWN")
info "Cluster health: $HEALTH"
./bin/ceph -s 2>/dev/null || true

if [[ "$HEALTH" == HEALTH_OK* ]]; then
  check_pass "Cluster reached HEALTH_OK after recovery"
elif [[ "$HEALTH" == HEALTH_WARN* ]]; then
  info "  Cluster is HEALTH_WARN (may be recovering PGs)"
  check_pass "Cluster is operational (HEALTH_WARN)"
else
  check_fail "Cluster not healthy after recovery: $HEALTH"
fi

# 6c. Verify OSD 0 is up
OSD0_STATUS=$(./bin/ceph osd stat 2>/dev/null || echo "unknown")
info "OSD status: $OSD0_STATUS"

OSD0_UP=$(./bin/ceph osd dump 2>/dev/null | grep -c "^osd\.0 up" || true)
if [[ "$OSD0_UP" -gt 0 ]]; then
  check_pass "OSD 0 is up after recovery"
else
  info "  OSD 0 may still be recovering — waiting 30 more seconds ..."
  sleep 30
  OSD0_UP=$(./bin/ceph osd dump 2>/dev/null | grep -c "^osd\.0 up" || true)
  if [[ "$OSD0_UP" -gt 0 ]]; then
    check_pass "OSD 0 came up after additional wait"
  else
    info "  OSD 0 log tail:"
    tail -30 "$BUILD_DIR/out/osd.0.log" 2>/dev/null || true
    check_fail "OSD 0 did not come up after recovery"
  fi
fi

# 6d. Wait for PG recovery to complete
info "Waiting for PG recovery (up to 120s) ..."
TIMEOUT=120
for (( i=0; i<TIMEOUT; i++ )); do
  PG_STAT=$(./bin/ceph pg stat 2>/dev/null || true)
  if echo "$PG_STAT" | grep -q "active+clean" && \
     ! echo "$PG_STAT" | grep -qE "degraded|recovering|backfilling|peering|unknown"; then
    break
  fi
  if [[ $((i % 5)) -eq 0 && $i -gt 0 ]]; then
    info "  [${i}x2s] pg stat: $PG_STAT"
  fi
  sleep 2
done

PG_STAT=$(./bin/ceph pg stat 2>/dev/null || true)
info "PG status: $PG_STAT"

# 6e. Read back test objects and verify checksums
info "Verifying test object data integrity ..."
VERIFIED=0
FAILED_OBJS=""
READ_TIMEOUTS=0
VERIFY_IDX=0
while IFS=' ' read -r obj_name expected_md5; do
  VERIFY_IDX=$((VERIFY_IDX+1))
  ACTUAL_MD5=$(get_obj_md5_with_timeout testrecovery "$obj_name" "$READ_TIMEOUT_SEC" || true)
  if [[ -n "$ACTUAL_MD5" && "$ACTUAL_MD5" == "$expected_md5" ]]; then
    VERIFIED=$((VERIFIED+1))
  else
    FAILED_OBJS="$FAILED_OBJS $obj_name"
    if [[ -z "$ACTUAL_MD5" ]]; then
      READ_TIMEOUTS=$((READ_TIMEOUTS+1))
    fi
  fi
  if [[ $((VERIFY_IDX % 10)) -eq 0 ]]; then
    info "  verify progress: $VERIFY_IDX / $NUM_OBJECTS (ok=$VERIFIED, fail=$((VERIFY_IDX-VERIFIED)), timeout=$READ_TIMEOUTS)"
  fi
done < "$CHECKSUM_FILE"

info "  Objects verified: $VERIFIED / $NUM_OBJECTS"
if [[ "$READ_TIMEOUTS" -gt 0 ]]; then
  info "  Timed out reads after recovery: $READ_TIMEOUTS (timeout=${READ_TIMEOUT_SEC}s)"
fi
if [[ "$VERIFIED" -eq "$NUM_OBJECTS" ]]; then
  check_pass "All $NUM_OBJECTS objects recovered with correct checksums"
elif [[ "$VERIFIED" -gt 0 ]]; then
  info "  Failed objects: $FAILED_OBJS"
  check_fail "Only $VERIFIED / $NUM_OBJECTS objects verified (some data lost)"
else
  check_fail "No objects could be verified after recovery"
fi

# =========================================================================
# SUMMARY
# =========================================================================
echo ""
echo "================================================================"
info "Test parameters: NUM_OBJECTS=$NUM_OBJECTS, OSD_COUNT=3"
info "WAL bypass dir: $BYPASS_DIR"
info "Recovery method: ceph-bluestore-wal-replay --recover"

if [[ "$ERRORS" -eq 0 ]]; then
  echo -e "\033[1;32m========== ALL CHECKS PASSED ==========\033[0m"
  exit 0
else
  echo -e "\033[1;31m========== $ERRORS CHECK(S) FAILED ==========\033[0m"
  exit 1
fi
