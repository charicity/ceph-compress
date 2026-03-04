#!/usr/bin/env bash
# test-wal-bypass.sh — smoke test for WAL bypass capture feature
#
# Usage:
#   cd <ceph-build-dir>
#   bash ../qa/standalone/test-wal-bypass.sh [BENCH_SECONDS]
#
# The script:
#   1. Stops any running vstart cluster
#   2. Cleans up previous cluster data (dev/ out/)
#   3. Starts a fresh 1-OSD cluster with WAL bypass enabled
#   4. Runs rados bench for BENCH_SECONDS (default 10)
#   5. Stops the cluster
#   6. Verifies WAL bypass log files were produced
#
set -e

BENCH_SEC=${1:-10}
BYPASS_DIR="/tmp/wal_bypass_test_$$"

# ---------------------------------------------------------------------------
# Resolve paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# Support running from <build>/ or from repo root
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

cd "$BUILD_DIR"

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
info()  { echo -e "\033[1;34m[INFO]\033[0m  $*"; }
pass()  { echo -e "\033[1;32m[PASS]\033[0m  $*"; }
fail()  { echo -e "\033[1;31m[FAIL]\033[0m  $*"; }

cleanup() {
  info "Cleaning up bypass dir $BYPASS_DIR ..."
  rm -rf "$BYPASS_DIR"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 1. Stop any existing cluster
# ---------------------------------------------------------------------------
info "Stopping any existing vstart cluster ..."
"$SRC_DIR/stop.sh" 2>/dev/null || true

# ---------------------------------------------------------------------------
# 2. Remove previous cluster data
# ---------------------------------------------------------------------------
info "Removing previous dev/ and out/ ..."
rm -rf "$BUILD_DIR/dev" "$BUILD_DIR/out"

# ---------------------------------------------------------------------------
# 3. Prepare bypass dir
# ---------------------------------------------------------------------------
rm -rf "$BYPASS_DIR"
mkdir -p "$BYPASS_DIR"
info "WAL bypass dir: $BYPASS_DIR"

# ---------------------------------------------------------------------------
# 4. Start a 1-OSD cluster with WAL bypass enabled
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

# Verify cluster is healthy
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
  fail "Cluster not healthy: $HEALTH"
  ./bin/ceph -s 2>/dev/null || true
  exit 1
fi

# ---------------------------------------------------------------------------
# 5. Run rados bench
# ---------------------------------------------------------------------------
info "Creating pool 'testbench' ..."
./bin/ceph osd pool create testbench 32 2>/dev/null || true
sleep 2

info "Running rados bench for ${BENCH_SEC}s ..."
./bin/rados -p testbench bench "$BENCH_SEC" write -b 4096 --no-cleanup 2>&1 | \
  grep -E 'Bandwidth|IOPS|Latency' || true

# ---------------------------------------------------------------------------
# 6. Check perf counters
# ---------------------------------------------------------------------------
info "Checking perf counters on osd.0 ..."
PERF_JSON=$(./bin/ceph daemon osd.0 perf dump 2>/dev/null || echo "{}")

BYPASS_BYTES=$(echo "$PERF_JSON" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(d.get('bluestore',{}).get('wal_bypass_bytes_total',0))" 2>/dev/null || echo "0")
BYPASS_FILES=$(echo "$PERF_JSON" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(d.get('bluestore',{}).get('wal_bypass_files_total',0))" 2>/dev/null || echo "0")
BYPASS_ERRORS=$(echo "$PERF_JSON" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(d.get('bluestore',{}).get('wal_bypass_write_errors_total',0))" 2>/dev/null || echo "0")
BYPASS_DROPS=$(echo "$PERF_JSON" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(d.get('bluestore',{}).get('wal_bypass_drops_total',0))" 2>/dev/null || echo "0")

info "  wal_bypass_bytes_total  = $BYPASS_BYTES"
info "  wal_bypass_files_total  = $BYPASS_FILES"
info "  wal_bypass_write_errors = $BYPASS_ERRORS"
info "  wal_bypass_drops_total  = $BYPASS_DROPS"

# ---------------------------------------------------------------------------
# 7. Stop cluster
# ---------------------------------------------------------------------------
info "Stopping cluster ..."
"$SRC_DIR/stop.sh" 2>/dev/null || true

# ---------------------------------------------------------------------------
# 8. Verify bypass files on disk
# ---------------------------------------------------------------------------
info "Checking bypass directory: $BYPASS_DIR"

WAL_FILES=$(find "$BYPASS_DIR" -name 'ceph_wal_*.log' -type f 2>/dev/null | wc -l)
STATE_FILE=$(find "$BYPASS_DIR" -name 'ceph_wal_seq.state' -type f 2>/dev/null | wc -l)
TOTAL_BYTES=$(find "$BYPASS_DIR" -name 'ceph_wal_*.log' -type f -exec du -cb {} + 2>/dev/null | tail -1 | awk '{print $1}' || echo 0)

info "  WAL bypass log files : $WAL_FILES"
info "  Seq state file exists: $STATE_FILE"
info "  Total bytes on disk  : $TOTAL_BYTES"

# ---------------------------------------------------------------------------
# 9. Print file listing (for debugging)
# ---------------------------------------------------------------------------
info "File listing:"
ls -lh "$BYPASS_DIR"/ 2>/dev/null | head -20 || true

# ---------------------------------------------------------------------------
# 10. Verdict
# ---------------------------------------------------------------------------
echo ""
ERRORS=0

if [[ "$WAL_FILES" -gt 0 ]]; then
  pass "WAL bypass log files found ($WAL_FILES files)"
else
  fail "No WAL bypass log files found in $BYPASS_DIR"
  ERRORS=$((ERRORS+1))
fi

if [[ "$STATE_FILE" -eq 1 ]]; then
  pass "Sequence state file exists"
else
  fail "Sequence state file NOT found"
  ERRORS=$((ERRORS+1))
fi

if [[ "$TOTAL_BYTES" -gt 0 ]]; then
  pass "Bypass files contain data ($TOTAL_BYTES bytes)"
else
  fail "Bypass files are empty"
  ERRORS=$((ERRORS+1))
fi

if [[ "$BYPASS_BYTES" -gt 0 ]]; then
  pass "Perf counter wal_bypass_bytes_total > 0 ($BYPASS_BYTES)"
else
  fail "Perf counter wal_bypass_bytes_total is 0"
  ERRORS=$((ERRORS+1))
fi

if [[ "$BYPASS_ERRORS" -eq 0 ]]; then
  pass "No write errors reported"
else
  fail "Write errors reported: $BYPASS_ERRORS"
  ERRORS=$((ERRORS+1))
fi

if [[ "$BYPASS_DROPS" -eq 0 ]]; then
  pass "No data drops reported"
else
  fail "Data drops reported: $BYPASS_DROPS"
  ERRORS=$((ERRORS+1))
fi

# Verify sequence continuity: file seq numbers should be 1..N with no gaps
info "Checking sequence continuity ..."
SEQ_LIST=$(find "$BYPASS_DIR" -name 'ceph_wal_*.log' -type f -printf '%f\n' 2>/dev/null | \
  sed -n 's/ceph_wal_0*\([0-9]*\)\.log/\1/p' | sort -n)
if [[ -n "$SEQ_LIST" ]]; then
  FIRST=$(echo "$SEQ_LIST" | head -1)
  LAST=$(echo "$SEQ_LIST" | tail -1)
  EXPECTED_COUNT=$((LAST - FIRST + 1))
  ACTUAL_COUNT=$(echo "$SEQ_LIST" | wc -l)
  if [[ "$EXPECTED_COUNT" -eq "$ACTUAL_COUNT" ]]; then
    pass "Sequence numbers continuous: $FIRST..$LAST ($ACTUAL_COUNT files)"
  else
    fail "Sequence gap detected: expected $EXPECTED_COUNT files for range $FIRST..$LAST, got $ACTUAL_COUNT"
    ERRORS=$((ERRORS+1))
  fi

  # Verify state file value matches last+1
  if [[ -f "$BYPASS_DIR/ceph_wal_seq.state" ]]; then
    STATE_VAL=$(cat "$BYPASS_DIR/ceph_wal_seq.state" | tr -d '[:space:]')
    EXPECTED_NEXT=$((LAST + 1))
    if [[ "$STATE_VAL" -eq "$EXPECTED_NEXT" ]]; then
      pass "State file value ($STATE_VAL) == last_seq+1 ($EXPECTED_NEXT)"
    else
      fail "State file value ($STATE_VAL) != expected next ($EXPECTED_NEXT)"
      ERRORS=$((ERRORS+1))
    fi
  fi
fi

echo ""
if [[ "$ERRORS" -eq 0 ]]; then
  echo -e "\033[1;32m========== ALL CHECKS PASSED ==========\033[0m"
  exit 0
else
  echo -e "\033[1;31m========== $ERRORS CHECK(S) FAILED ==========\033[0m"
  exit 1
fi
