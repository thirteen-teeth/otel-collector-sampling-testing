# OTel Collector Tail-Based Sampling Test Harness

A reproducible GitHub Actions integration test harness that verifies OpenTelemetry
Collector **tail-based probabilistic sampling remains proportional per service**
when consuming traces from a shared raw Kafka topic.

## Table of Contents

1. [Architecture](#architecture)
2. [Trace-ID Partitioning](#trace-id-partitioning)
3. [Quick Start (local)](#quick-start-local)
4. [Workflow Usage](#workflow-usage)
5. [Cost and Resource Expectations](#cost-and-resource-expectations)
6. [Acceptance Criteria](#acceptance-criteria)
7. [Result Interpretation](#result-interpretation)
8. [Limitations](#limitations)
9. [Tuning Guide](#tuning-guide)
10. [Observed Results](#observed-results)

---

## Architecture

```
Synthetic trace generator
        │  OTLP gRPC (all traces)
        ▼
 ┌─────────────────────┐
 │  Ingest Collector   │  Writes 100% of traces to Kafka
 │  (otel-raw-ingest)  │  partition_traces_by_id: true
 └─────────┬───────────┘
           │
           ▼  Kafka topic: otel-traces-raw
 ┌─────────────────────┐
 │  Tail-Sampling      │  Probabilistic 10% sampling
 │  Collector          │  Decision based on trace ID hash
 │  (one replica)      │  decision_wait: 5s (smoke) / 30s (scale)
 └─────────┬───────────┘
           │
           ▼  Kafka topic: otel-traces-sampled
 ┌─────────────────────┐
 │  Validator          │  Reconciles raw vs sampled
 └─────────────────────┘
```

### Components

| Component | Image / Language | Purpose |
|---|---|---|
| Kafka | `confluentinc/cp-kafka:7.7.1` (KRaft) | Message bus for trace data |
| Ingest Collector | `otel/opentelemetry-collector-contrib:0.108.0` | Receives OTLP, writes raw |
| Tail-Sampling Collector | Same image | Consumes raw, samples, writes sampled |
| Generator | Python 3.12 + OTel SDK | Produces deterministic test traces |
| Validator | Python 3.12 + kafka-python | Statistical reconciliation |

### Why two Collectors?

The tail-sampling processor makes decisions **after** all spans of a trace have
arrived.  If the same Collector that receives OTLP also applies tail sampling, it
must buffer unbounded traffic.  Separating ingest and sampling:

1. Lets you scale ingest replicas independently (all write to the same Kafka topic).
2. Decouples the decision window from the OTLP receive path.
3. Gives you a clean raw topic for replay, auditing, and independent consumers.

---

## Trace-ID Partitioning

The Kafka exporter is configured with `partition_traces_by_id: true`.

This forces the exporter to use the **trace ID** as the Kafka record key, which
Kafka's default hash partitioner maps to a stable partition.  Every span that
belongs to the same trace therefore goes to the same partition.

Because the tail-sampling Collector runs as **one replica** and subscribes to all
partitions in its consumer group, all spans for each trace arrive at the same
Collector instance.  This is a **required invariant**: the tail-sampling processor
holds in-memory state per trace.  If spans for the same trace arrived at different
Collector instances, each instance would sample the trace independently on
incomplete data, breaking the proportional guarantee.

> **Scaling note:** If you add replicas to the tail-sampler consumer group,
> Kafka will rebalance so that each partition is owned by exactly one replica.
> Trace-ID partitioning guarantees that all spans for a trace remain on the same
> partition, and therefore reach the same replica.  This is the only safe way to
> scale tail sampling in this architecture.

---

## Quick Start (local)

### Prerequisites

- Docker >= 24 with Compose v2
- Python 3.12

### 1. Start infrastructure

```bash
cd test

# Smoke test profile (5 low-vol x 1k + 10 high-vol x 20k traces, 5s window)
export DECISION_WAIT=5s
export NUM_TRACES=50000
export KAFKA_GROUP_ID=otel-tail-sampler-smoke-v1
docker compose up -d
```

### 2. Install Python dependencies

```bash
pip install -r test/generator/requirements.txt
pip install -r test/validator/requirements.txt
pip install -r test/unit/requirements.txt
```

### 3. Run unit tests

```bash
python -m pytest test/unit/ -v
```

### 4. Generate traces

```bash
TEST_RUN_ID=local-001
python test/generator/generator.py \
  --run-id "${TEST_RUN_ID}" \
  --low-volume-services 5 \
  --low-volume-traces 1000 \
  --high-volume-services 10 \
  --high-volume-traces 20000 \
  --endpoint localhost:4317 \
  --output /tmp/manifest.json
```

### 5. Wait for the decision window

```bash
sleep 20   # decision_wait=5s + 15s buffer
```

### 6. Validate

```bash
python test/validator/validator.py \
  --run-id "${TEST_RUN_ID}" \
  --manifest /tmp/manifest.json \
  --output-dir /tmp/results
```

### 7. Clean up

```bash
cd test && docker compose down -v
```

---

## Workflow Usage

### Smoke test (`.github/workflows/tail-sampling-smoke.yml`)

| Trigger | Runs on |
|---|---|
| Every pull request to `main` | `ubuntu-latest` (2 CPU, 7 GB) |
| `workflow_dispatch` | `ubuntu-latest` |

**Workload:** 5 x 1,000 + 10 x 20,000 = 205,000 traces, 2-5 spans, 5s window

This test completes in 10-20 minutes and validates:
- Correct Collector configuration syntax
- Statistical proportionality of sampling per service
- No orphaned sampled traces
- No unexpected span ID duplicates

### Full-scale test (`.github/workflows/tail-sampling-scale.yml`)

| Trigger | Runs on |
|---|---|
| `workflow_dispatch` only | Configurable (default `ubuntu-latest`) |
| Weekly schedule (Sun 02:00 UTC) | Configurable |

**Default workload:** 5 x 10,000 + 10 x 2,000,000 = 20,050,000 traces

> **Important:** the 20 M-trace run requires a runner with at least 32 GB RAM
> for the tail-sampler container alone.  Use a self-hosted runner or a GitHub
> larger runner.  See [Cost and Resource Expectations](#cost-and-resource-expectations).

**Configurable workflow inputs:**

| Input | Default | Description |
|---|---|---|
| `runner_label` | `ubuntu-latest` | Runner to use |
| `low_volume_traces` | `10000` | Traces per low-vol service |
| `high_volume_traces` | `2000000` | Traces per high-vol service |
| `decision_wait` | `30s` | Tail-sampling decision window |
| `num_traces_capacity` | `3000000` | `num_traces` config value |
| `send_rate` | `1000` | Traces/sec per worker thread |
| `seed` | `42` | PRNG seed for reproducibility |

---

## Cost and Resource Expectations

### Memory sizing formula

For the tail-sampler, the key constraint is:

```
num_traces >= peak_new_traces_per_sec x decision_wait_sec x safety_factor
```

Using 10 KB/trace as a conservative planning constant:

```
memory_GiB = num_traces x 10 KB / 1024^3 x 1.3 (overhead) x 1.5 (burst)
```

| Profile | num_traces | Memory estimate | Container limit |
|---|---:|---|---|
| Smoke | 50,000 | ~1 GB | 1.25 GB |
| Full-scale | 3,000,000 | ~60 GB | 20 GB (sampler alone) |

> For the full-scale run, size the runner to have at least
> `sampler_memory + 4 GB Kafka + 2 GB ingest + 4 GB OS` = 30+ GB total.

### GitHub Actions cost

| Workflow | Runner | Est. duration | Est. cost |
|---|---|---:|---:|
| Smoke (PR) | 2-core / 7 GB | 10-20 min | $0.04-$0.08 |
| Full-scale | 8-core / 32 GB | 60-90 min | $1.20-$2.00 |
| Full-scale | 16-core / 64 GB | 45-75 min | $1.89-$3.15 |

Pricing: $0.022/min (8-core), $0.042/min (16-core), subject to change.
See [GitHub Actions billing](https://docs.github.com/en/billing/reference/actions-runner-pricing).

---

## Acceptance Criteria

### Statistical acceptance (per service and aggregate)

For each service with `n` raw traces and `p = 0.10`:

```
|sampled - n*p|  <=  5 x sqrt(n x p x (1-p))
```

Expected ranges:

| Raw traces | Expected sampled | 5-sigma range |
|---:|---:|---|
| 1,000 | 100 | ~53 - 148 |
| 10,000 | 1,000 | ~850 - 1,150 |
| 20,000 | 2,000 | ~1,788 - 2,213 |
| 2,000,000 | 200,000 | ~197,879 - 202,122 |

The probability that a correctly operating 10% sampler falls outside the 5-sigma
window is less than 1 in 3.5 million -- so a failure almost always indicates a
real problem (starvation, configuration error, or dropped-too-early traces).

### Operational checks

The validator also fails on:

| Check | Metric / assertion |
|---|---|
| Orphaned sampled traces | `sampled_ids not subset of raw_ids` |
| Dropped-too-early traces | `otelcol_processor_tail_sampling_sampling_trace_dropped_too_early > 0` |
| Sampling policy errors | `otelcol_processor_tail_sampling_sampling_policy_evaluation_error > 0` |
| Exporter send failures | `otelcol_exporter_send_failed_spans > 0` |
| No raw traces consumed | Consumer consumed zero traces for a known service |

---

## Result Interpretation

### What "proportional" means

The probabilistic tail-sampling policy hashes each **trace ID** to a value in
[0, 1) and retains the trace if the hash is below the sampling percentage.  This
is independent of:

- Which service sent the spans
- How many spans the trace has
- Traffic volume of other services

Each service therefore converges independently to approximately 10%, regardless
of whether it sends 1,000 or 2,000,000 traces.  High-volume services do **not**
crowd out low-volume ones.

### Reading the validation output

```
=== Validation PASSED ===
  Raw unique traces    : 205,000
  Sampled unique traces:  20,541
  Aggregate rate       :  10.02%  (expected 10.00%,  dev=0.08 sigma)

  PASS low-vol-svc-00   raw=  1,000  sampled=   97  rate=9.70%  dev=0.10 sigma
  PASS high-vol-svc-00  raw= 20,000  sampled=1,998  rate=9.99%  dev=0.01 sigma
  ...
```

A sigma deviation below 2 is typical; below 3 is good; exactly 5 is the boundary.
Values above 5 indicate a problem.

### If a service fails the statistical test

1. **`sampling_trace_dropped_too_early > 0`**: `num_traces` is too small for the
   decision window.  Increase `NUM_TRACES` or reduce `DECISION_WAIT` or generation rate.
2. **Orphaned sampled traces**: a bug in the pipeline routing; check Collector logs.
3. **Missing raw traces for a service**: the generator or ingest Collector had errors.
4. **Very low sigma but wrong direction**: could be hash collision bias for a specific
   seed; try a different seed.

---

## Limitations

### In-memory tail state

The tail-sampling processor holds all undecided traces in memory.  If the
Collector crashes during the `decision_wait` window:

- Kafka offsets may already have been committed (at-least-once delivery)
- The in-memory trace state is lost
- Spans that re-arrive on restart will be sampled again with a fresh decision

This means the harness validates **proportional sampling under normal operation**,
but does **not** prove crash-safe, zero-loss sampling.  For that, you would need
an experimental `tail-storage` extension or a stateful stream-processor (Kafka
Streams, Flink).

### At-least-once delivery

Both topics use `at_least_once` semantics.  The validator deduplicates trace IDs
before counting to correctly handle re-delivered messages.

### Single-replica constraint

Running more than one tail-sampler replica against a shared consumer group is
safe **only** when `partition_traces_by_id: true` is set on the ingest Kafka
exporter, ensuring all spans for a trace land on the same Kafka partition and
therefore the same sampler instance.

### Python generator throughput

The Python generator using the OTel SDK typically achieves 5,000-20,000 traces/sec
depending on CPU and gRPC throughput.  For the 20M-trace full-scale run, consider:
- Running multiple generator processes (one per service class)
- Using the `--rate` flag to pace generation to stay within `num_traces` capacity
- A future Go-based generator for higher throughput

---

## Tuning Guide

### Decision wait

| Volume | Recommended `decision_wait` |
|---|---|
| < 500k traces | 5s |
| 500k - 5M | 10-15s |
| > 5M | 30s |

Longer windows increase memory pressure.  Always verify:
`num_traces > peak_new_traces_per_sec x decision_wait_sec x 1.5`

### Trace capacity (`num_traces`)

```
num_traces = ceil(peak_traces_per_sec x decision_wait_seconds x 1.5)
```

For the smoke test (205k traces in ~30s = ~7k/s peak, 5s window):
`num_traces = ceil(7000 x 5 x 1.5) = 52,500` -- use 50,000-100,000.

### Kafka partitions

Use `KAFKA_PARTITIONS=6` for smoke tests.  For scale tests with high throughput,
12-24 partitions help distribute write load.  Partitions should be a multiple of
the number of consumer replicas (1 for this test).

### Generation rate

Keep: `rate x decision_wait < num_traces`

If `num_traces = 3,000,000` and `decision_wait = 30s`:
max rate = 100,000 traces/sec (theoretical).

Use `--rate 10000` (10k traces/sec) to leave a 10x safety margin.

---

## Observed Results

### Unit tests (run in the coding agent environment)

```
command: python -m pytest test/unit/ -v
21 tests collected, 21 passed
```

All statistical math, acceptance interval formulas, and smoke test configuration
sanity checks passed.  Full results are committed to the PR.

### Integration test status

The full Docker Compose integration test (ingest -> Kafka -> tail-sampler -> validate)
requires Docker service networking that is not available in the coding agent sandbox.

**The GitHub Actions smoke workflow (`tail-sampling-smoke.yml`) is the canonical
way to run the integration test** on a fresh `ubuntu-latest` runner with Docker.

### Projected full-scale behavior (not yet run)

| Metric | Projected value |
|---|---|
| Total raw traces | 20,050,000 |
| Expected sampled | ~2,005,000 (10.0%) |
| Low-vol service (10k traces) | ~1,000 sampled, sigma ~30 |
| High-vol service (2M traces) | ~200,000 sampled, sigma ~424 |
| Memory (tail-sampler, 30s window) | ~30 GB for 3M in-flight traces |
| Generation time (1k traces/sec/worker) | ~33 min for all 15 workers |
| Total workflow time | ~90-120 min |

> **Disclaimer:** The 20.05M-trace run has **not** been executed.  The projected
> numbers are derived from the sizing formulas above and are subject to change
> based on actual hardware performance, Kafka throughput, and network latency.
> Run the full-scale workflow on an appropriately sized self-hosted runner and
> update this section with actual observations.
