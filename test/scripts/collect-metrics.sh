#!/usr/bin/env bash
# collect-metrics.sh — Scrape Prometheus metrics from OTel Collectors.
#
# Usage:
#   INGEST_METRICS_URL=http://localhost:8888/metrics \
#   SAMPLER_METRICS_URL=http://localhost:8889/metrics \
#   OUTPUT_DIR=/tmp/metrics \
#   collect-metrics.sh [snapshot-label]
#
# Output files:
#   ${OUTPUT_DIR}/metrics-ingest-${label}.txt
#   ${OUTPUT_DIR}/metrics-sampler-${label}.txt
#   ${OUTPUT_DIR}/metrics-summary-${label}.txt   (key metrics only)

set -euo pipefail

LABEL="${1:-snapshot}"
INGEST_URL="${INGEST_METRICS_URL:-http://localhost:8888/metrics}"
SAMPLER_URL="${SAMPLER_METRICS_URL:-http://localhost:8889/metrics}"
OUTPUT_DIR="${OUTPUT_DIR:-.}"

mkdir -p "${OUTPUT_DIR}"

scrape() {
    local url="$1"
    local out="$2"
    if curl -sf --connect-timeout 3 --max-time 5 "${url}" > "${out}" 2>/dev/null; then
        echo "  Scraped ${url} → ${out} ($(wc -l < "${out}") lines)"
    else
        echo "  WARNING: Could not scrape ${url}" >&2
        echo "# SCRAPE FAILED: ${url}" > "${out}"
    fi
}

echo "Collecting metrics [${LABEL}]..."
scrape "${INGEST_URL}"  "${OUTPUT_DIR}/metrics-ingest-${LABEL}.txt"
scrape "${SAMPLER_URL}" "${OUTPUT_DIR}/metrics-sampler-${LABEL}.txt"

# Extract key metrics into a summary file
{
    echo "=== KEY METRICS [${LABEL}] ==="
    echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo ""

    echo "--- Tail Sampling Processor ---"
    grep -E "^otelcol_processor_tail_sampling" \
        "${OUTPUT_DIR}/metrics-sampler-${LABEL}.txt" 2>/dev/null || echo "(no tail_sampling metrics)"

    echo ""
    echo "--- Exporter send failures ---"
    grep -E "^otelcol_exporter_send_failed" \
        "${OUTPUT_DIR}/metrics-sampler-${LABEL}.txt" \
        "${OUTPUT_DIR}/metrics-ingest-${LABEL}.txt" 2>/dev/null || echo "(none)"

    echo ""
    echo "--- Queue full / refused ---"
    grep -E "^otelcol_(processor_refused|exporter_queue_size|processor_batch)" \
        "${OUTPUT_DIR}/metrics-sampler-${LABEL}.txt" \
        "${OUTPUT_DIR}/metrics-ingest-${LABEL}.txt" 2>/dev/null || echo "(none)"

    echo ""
    echo "--- Receiver accepted spans ---"
    grep -E "^otelcol_receiver_accepted_spans" \
        "${OUTPUT_DIR}/metrics-ingest-${LABEL}.txt" 2>/dev/null || echo "(none)"

    echo ""
    echo "--- Kafka receiver accepted spans ---"
    grep -E "^otelcol_receiver_accepted_spans" \
        "${OUTPUT_DIR}/metrics-sampler-${LABEL}.txt" 2>/dev/null || echo "(none)"

} > "${OUTPUT_DIR}/metrics-summary-${LABEL}.txt"

echo "Wrote ${OUTPUT_DIR}/metrics-summary-${LABEL}.txt"
cat "${OUTPUT_DIR}/metrics-summary-${LABEL}.txt"
