#!/bin/bash
# Readiness watchdog for the Tool Management Registry + native fabrics.
#
# The registry can wedge into an "alive but not accepting connections" state
# (its asyncio loop keeps running background tasks while the inbound listener
# is starved). Plain process-liveness supervision (launchd KeepAlive) does not
# catch that. This watchdog probes the HTTP /health endpoint and force-restarts
# the whole process group when readiness fails, so the stack is always serving.
#
# Env:
#   PORT                     registry port (default 2016)
#   HEALTH_INTERVAL          seconds between probes (default 15)
#   HEALTH_TIMEOUT           per-probe curl timeout (default 8)
#   HEALTH_FAILS             consecutive failures before restart (default 3)
#   BOOT_GRACE               seconds to wait for first healthy after a start (default 90)

set -u

cd "$(dirname "$0")/.."
ROOT="$(pwd)"

PORT="${PORT:-2016}"
HEALTH_INTERVAL="${HEALTH_INTERVAL:-15}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-8}"
HEALTH_FAILS="${HEALTH_FAILS:-3}"
BOOT_GRACE="${BOOT_GRACE:-90}"
HEALTH_URL="http://localhost:${PORT}/health"
LOG_DIR="${LOG_DIR:-$ROOT/data/logs}"
mkdir -p "$LOG_DIR"

# Stable settings for an unattended, long-lived process: no reload watcher,
# no heavy synchronous embed-sync on every backend re-announce.
export RELOAD=0
export ENABLE_ANNOUNCE_EMBED_SYNC="${ENABLE_ANNOUNCE_EMBED_SYNC:-0}"
export AUTO_START_NATIVE_MCPS="${AUTO_START_NATIVE_MCPS:-1}"
export PORT

REG_PID=""

log() { echo "[$(date -u +%FT%TZ)] $*"; }

healthy() { curl -s -m "$HEALTH_TIMEOUT" "$HEALTH_URL" -o /dev/null 2>/dev/null; }

stop_stack() {
    # macOS has no setsid, so reap by PID + process pattern + known ports.
    if [[ -n "$REG_PID" ]] && kill -0 "$REG_PID" 2>/dev/null; then
        kill "$REG_PID" 2>/dev/null
        sleep 2
        kill -9 "$REG_PID" 2>/dev/null
    fi
    pkill -9 -f "uvicorn tool_management.app:app" 2>/dev/null
    pkill -9 -f "native.integration_fabric" 2>/dev/null
    pkill -9 -f "native.finance_fabric" 2>/dev/null
    for p in "$PORT" 4001 4002 4003 4004 4009 4010; do
        lsof -ti:"$p" 2>/dev/null | xargs kill -9 2>/dev/null
    done
    sleep 1
}

start_stack() {
    stop_stack
    log "starting registry stack (RELOAD=$RELOAD EMBED_SYNC=$ENABLE_ANNOUNCE_EMBED_SYNC)"
    ./start_registry.sh >> "$LOG_DIR/registry.log" 2>&1 &
    REG_PID=$!
    log "registry pgid $REG_PID; waiting up to ${BOOT_GRACE}s for readiness"
    local waited=0
    while ! healthy; do
        sleep 3; waited=$((waited + 3))
        if (( waited >= BOOT_GRACE )); then
            log "WARN: not healthy after ${BOOT_GRACE}s; will keep probing"
            return
        fi
    done
    log "registry healthy at $HEALTH_URL"
}

trap 'log "supervisor stopping"; stop_stack; exit 0' INT TERM

start_stack
fails=0
while true; do
    sleep "$HEALTH_INTERVAL"
    if healthy; then
        fails=0
        continue
    fi
    fails=$((fails + 1))
    log "health probe failed ($fails/$HEALTH_FAILS)"
    if (( fails >= HEALTH_FAILS )); then
        log "readiness lost — restarting stack"
        start_stack
        fails=0
    fi
done
