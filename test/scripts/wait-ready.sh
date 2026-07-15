#!/usr/bin/env bash
# wait-ready.sh — Poll a health-check URL until it returns HTTP 200.
#
# Usage:
#   wait-ready.sh <url> [<label>] [<timeout_seconds>] [<interval_seconds>]
#
# Examples:
#   wait-ready.sh http://localhost:13133 "ingest-collector" 120 3
#   wait-ready.sh http://localhost:13134 "tail-sampler" 120 3

set -euo pipefail

URL="${1:?Usage: $0 <url> [label] [timeout_s] [interval_s]}"
LABEL="${2:-${URL}}"
TIMEOUT="${3:-120}"
INTERVAL="${4:-3}"

deadline=$(( $(date +%s) + TIMEOUT ))
attempt=0

echo "⏳ Waiting for ${LABEL} at ${URL} (timeout=${TIMEOUT}s, interval=${INTERVAL}s)..."

while true; do
    attempt=$(( attempt + 1 ))

    if curl -sf --connect-timeout 2 --max-time 3 "${URL}" > /dev/null 2>&1; then
        echo "✅ ${LABEL} is ready (attempt ${attempt})"
        exit 0
    fi

    now=$(date +%s)
    if (( now >= deadline )); then
        echo "❌ ${LABEL} did not become ready within ${TIMEOUT}s"
        exit 1
    fi

    remaining=$(( deadline - now ))
    echo "   ${LABEL}: not ready yet (attempt ${attempt}, ${remaining}s remaining)"
    sleep "${INTERVAL}"
done
