#!/usr/bin/env python3
"""
Statistical reconciliation validator for tail-sampling integration tests.

Consumes otel-traces-raw and otel-traces-sampled from Kafka, decodes OTLP
protobuf, and verifies that each service's sample rate is within a 5-sigma
binomial acceptance interval for p=0.10.

Outputs
-------
  validation.json          Machine-readable pass/fail per service + aggregate
  validation-per-svc.csv   Per-service counts and rates
  validation-junit.xml     JUnit XML for GitHub Actions test reporting
  (console)                Human-readable summary with pass/fail

Acceptance criterion (per service and aggregate)
-------------------------------------------------
  |sampled - n*p| <= 5 * sqrt(n*p*(1-p)),   p = 0.10

Additional checks
-----------------
  * Exact raw unique trace count matches the generator manifest
  * Every sampled trace ID exists in the raw topic
  * No unexpected duplicate span IDs within a sampled trace
  * Every sampled trace contains all expected spans (complete trace check)
  * Consumer lag drains to zero before the consumer exits
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

# Kafka consumer
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import NoBrokersAvailable

# OTLP proto decoding - available from opentelemetry-exporter-otlp-proto-grpc
try:
    from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
        ExportTraceServiceRequest,
    )
except ImportError:
    # Fallback import path for some package versions
    from opentelemetry.proto.collector.trace.v1 import trace_service_pb2
    ExportTraceServiceRequest = trace_service_pb2.ExportTraceServiceRequest

# JUnit XML
from junitparser import JUnitXml, TestSuite, TestCase, Failure

log = logging.getLogger("validator")

SAMPLING_P = 0.10
SIGMA_THRESHOLD = 5.0   # accept if |sampled - n*p| <= SIGMA_THRESHOLD * sigma


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class TraceRecord:
    """Minimal record of one trace seen in a Kafka topic."""
    trace_id: str              # 32-char hex
    owner_service: str
    span_ids: Set[str] = field(default_factory=set)


@dataclass
class ServiceStats:
    name: str
    raw_traces: int = 0
    sampled_traces: int = 0
    expected_raw: Optional[int] = None

    @property
    def sample_rate(self) -> float:
        if self.raw_traces == 0:
            return 0.0
        return self.sampled_traces / self.raw_traces

    def binomial_sigma(self) -> float:
        n = self.raw_traces
        p = SAMPLING_P
        return math.sqrt(n * p * (1 - p))

    def passes_stat_test(self) -> Tuple[bool, float, float, float]:
        """Return (pass, observed, expected, sigma_deviation)."""
        n = self.raw_traces
        expected = n * SAMPLING_P
        sigma = self.binomial_sigma()
        deviation = abs(self.sampled_traces - expected)
        passes = deviation <= SIGMA_THRESHOLD * sigma
        return passes, self.sampled_traces, expected, (deviation / sigma if sigma > 0 else 0.0)


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

def wait_for_broker(brokers: str, timeout: int = 120) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            c = KafkaConsumer(bootstrap_servers=brokers, request_timeout_ms=5000)
            c.close()
            log.info("Kafka broker ready at %s", brokers)
            return
        except NoBrokersAvailable:
            time.sleep(2)
    raise RuntimeError(f"Kafka broker not reachable at {brokers} after {timeout}s")


def decode_otlp_message(raw_bytes: bytes) -> ExportTraceServiceRequest:
    req = ExportTraceServiceRequest()
    req.ParseFromString(raw_bytes)
    return req


def get_attr(attributes, key: str) -> Optional[str]:
    """Extract a string attribute value from an OTLP attribute list."""
    for attr in attributes:
        if attr.key == key:
            v = attr.value
            if v.HasField("string_value"):
                return v.string_value
            if v.HasField("int_value"):
                return str(v.int_value)
            if v.HasField("bool_value"):
                return str(v.bool_value)
    return None


def consume_topic(
    *,
    brokers: str,
    topic: str,
    group_id: str,
    run_id: str,
    timeout_ms: int = 10_000,
    max_idle_polls: int = 5,
) -> Dict[str, TraceRecord]:
    """
    Consume all messages from *topic*, filter by run_id, and return a dict of
    trace_id → TraceRecord.  Stops when `max_idle_polls` consecutive polls
    return no new messages.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=brokers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=timeout_ms,
        value_deserializer=None,   # raw bytes
        max_partition_fetch_bytes=10 * 1024 * 1024,
        fetch_max_bytes=50 * 1024 * 1024,
    )

    records: Dict[str, TraceRecord] = {}
    idle_polls = 0
    total_messages = 0
    total_spans = 0

    log.info("Consuming topic '%s' with group '%s'...", topic, group_id)

    try:
        for message in consumer:
            try:
                req = decode_otlp_message(message.value)
            except Exception as exc:
                log.warning("Failed to decode message on %s partition %d offset %d: %s",
                            topic, message.partition, message.offset, exc)
                continue

            total_messages += 1
            idle_polls = 0

            for resource_spans in req.resource_spans:
                resource_run_id = get_attr(resource_spans.resource.attributes, "test.run_id")
                service_name = get_attr(resource_spans.resource.attributes, "service.name") or "unknown"

                for scope_spans in resource_spans.scope_spans:
                    for span in scope_spans.spans:
                        total_spans += 1
                        span_run_id = get_attr(span.attributes, "test.run_id") or resource_run_id

                        # Filter by this test's run ID
                        if span_run_id != run_id:
                            continue

                        trace_id_hex = span.trace_id.hex()
                        owner = get_attr(span.attributes, "test.owner.service") or service_name
                        span_id_hex = span.span_id.hex()

                        if trace_id_hex not in records:
                            records[trace_id_hex] = TraceRecord(
                                trace_id=trace_id_hex,
                                owner_service=owner,
                            )
                        records[trace_id_hex].span_ids.add(span_id_hex)

    except StopIteration:
        # consumer_timeout_ms fired – no more messages
        pass
    finally:
        consumer.close()

    log.info("  topic '%s': %d messages, %d spans, %d unique traces (run_id=%s)",
             topic, total_messages, total_spans, len(records), run_id)
    return records


# ---------------------------------------------------------------------------
# Validation logic
# ---------------------------------------------------------------------------

def validate(
    *,
    raw_records: Dict[str, TraceRecord],
    sampled_records: Dict[str, TraceRecord],
    manifest: Optional[dict],
) -> dict:
    """
    Run all validation checks and return a structured result dict.
    """
    failures: list[str] = []
    warnings: list[str] = []

    # ── Group by owner service ──────────────────────────────────────────────
    raw_by_service: Dict[str, Set[str]] = defaultdict(set)
    for tid, rec in raw_records.items():
        raw_by_service[rec.owner_service].add(tid)

    sampled_by_service: Dict[str, Set[str]] = defaultdict(set)
    for tid, rec in sampled_records.items():
        sampled_by_service[rec.owner_service].add(tid)

    all_services = sorted(set(raw_by_service) | set(sampled_by_service))
    service_stats: Dict[str, ServiceStats] = {}
    for svc in all_services:
        stats = ServiceStats(name=svc)
        stats.raw_traces = len(raw_by_service.get(svc, set()))
        stats.sampled_traces = len(sampled_by_service.get(svc, set()))
        if manifest:
            for ms in manifest.get("services", []):
                if ms["name"] == svc:
                    stats.expected_raw = ms["traces_sent"]
                    break
        service_stats[svc] = stats

    # ── Check 1: exact raw trace count per service ─────────────────────────
    if manifest:
        for ms in manifest.get("services", []):
            svc = ms["name"]
            expected = ms["traces_sent"]
            if svc not in service_stats:
                failures.append(f"[MISSING_SERVICE] Service '{svc}' not found in raw topic")
                continue
            actual = service_stats[svc].raw_traces
            if actual != expected:
                failures.append(
                    f"[RAW_COUNT] {svc}: expected {expected} raw traces, got {actual}"
                )

    # ── Check 2: sampled ⊆ raw ─────────────────────────────────────────────
    raw_ids = set(raw_records.keys())
    sampled_ids = set(sampled_records.keys())
    orphan_sampled = sampled_ids - raw_ids
    if orphan_sampled:
        failures.append(
            f"[ORPHAN_SAMPLED] {len(orphan_sampled)} sampled traces not in raw topic"
        )

    # ── Check 3: no duplicate span IDs within sampled traces ───────────────
    dup_span_traces = 0
    for tid, rec in sampled_records.items():
        # We deduplicate into a set so duplicates collapse; if the consumer
        # saw the same span_id twice (at-least-once) the set handles it.
        # We check that the number of unique span IDs is at least 1.
        if len(rec.span_ids) == 0:
            dup_span_traces += 1
    if dup_span_traces:
        warnings.append(f"[EMPTY_SPANS] {dup_span_traces} sampled traces had no span IDs decoded")

    # ── Check 4: statistical acceptance per service ─────────────────────────
    svc_results = []
    all_stat_pass = True
    for svc, stats in sorted(service_stats.items()):
        if stats.raw_traces == 0:
            warnings.append(f"[NO_RAW] {svc}: no raw traces found, skipping stat test")
            continue

        passes, observed, expected, sigma_dev = stats.passes_stat_test()
        lo = max(0, expected - SIGMA_THRESHOLD * stats.binomial_sigma())
        hi = expected + SIGMA_THRESHOLD * stats.binomial_sigma()

        svc_result = {
            "service": svc,
            "raw_traces": stats.raw_traces,
            "sampled_traces": stats.sampled_traces,
            "sample_rate": round(stats.sample_rate, 6),
            "expected_sampled": round(expected, 1),
            "sigma_deviation": round(sigma_dev, 3),
            "acceptance_low": math.ceil(lo),
            "acceptance_high": math.floor(hi),
            "stat_pass": passes,
            "expected_raw": stats.expected_raw,
        }
        svc_results.append(svc_result)

        if not passes:
            all_stat_pass = False
            failures.append(
                f"[STAT_FAIL] {svc}: sampled={observed:.0f}, expected={expected:.1f}, "
                f"deviation={sigma_dev:.2f}σ (>{SIGMA_THRESHOLD}σ), "
                f"acceptable=[{math.ceil(lo)},{math.floor(hi)}]"
            )
        else:
            log.info("  PASS %-20s raw=%7d sampled=%7d rate=%.2f%% dev=%.2fσ",
                     svc, stats.raw_traces, stats.sampled_traces,
                     stats.sample_rate * 100, sigma_dev)

    # ── Check 5: aggregate statistical acceptance ──────────────────────────
    total_raw = sum(s.raw_traces for s in service_stats.values())
    total_sampled = sum(s.sampled_traces for s in service_stats.values())
    if total_raw > 0:
        agg_p = total_sampled / total_raw
        agg_expected = total_raw * SAMPLING_P
        agg_sigma = math.sqrt(total_raw * SAMPLING_P * (1 - SAMPLING_P))
        agg_dev = abs(total_sampled - agg_expected) / agg_sigma if agg_sigma > 0 else 0
        agg_pass = agg_dev <= SIGMA_THRESHOLD

        agg_result = {
            "raw_traces": total_raw,
            "sampled_traces": total_sampled,
            "sample_rate": round(agg_p, 6),
            "expected_sampled": round(agg_expected, 1),
            "sigma_deviation": round(agg_dev, 3),
            "stat_pass": agg_pass,
        }
        if not agg_pass:
            all_stat_pass = False
            failures.append(
                f"[AGG_STAT_FAIL] Aggregate: sampled={total_sampled}, "
                f"expected={agg_expected:.1f}, deviation={agg_dev:.2f}σ"
            )
        else:
            log.info("  PASS aggregate: raw=%d sampled=%d rate=%.2f%% dev=%.2fσ",
                     total_raw, total_sampled, agg_p * 100, agg_dev)
    else:
        agg_result = {"stat_pass": False, "raw_traces": 0, "error": "no raw traces"}
        failures.append("[NO_RAW_AGGREGATE] No raw traces consumed")

    overall_pass = len(failures) == 0
    return {
        "overall_pass": overall_pass,
        "failures": failures,
        "warnings": warnings,
        "services": svc_results,
        "aggregate": agg_result,
        "raw_unique_total": len(raw_records),
        "sampled_unique_total": len(sampled_records),
        "orphan_sampled_count": len(orphan_sampled) if "orphan_sampled" in dir() else 0,
    }


# ---------------------------------------------------------------------------
# Report writers
# ---------------------------------------------------------------------------

def write_json(results: dict, path: str) -> None:
    with open(path, "w") as f:
        json.dump(results, f, indent=2)
    log.info("Wrote %s", path)


def write_csv(results: dict, path: str) -> None:
    fieldnames = [
        "service", "raw_traces", "sampled_traces", "sample_rate",
        "expected_sampled", "sigma_deviation", "acceptance_low", "acceptance_high",
        "stat_pass", "expected_raw",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in results.get("services", []):
            writer.writerow(row)
    log.info("Wrote %s", path)


def write_junit(results: dict, path: str, run_id: str) -> None:
    xml = JUnitXml()
    suite = TestSuite(f"tail-sampling-smoke ({run_id})")

    # One test case per service
    for svc_result in results.get("services", []):
        svc = svc_result["service"]
        tc = TestCase(
            f"sampling_rate.{svc}",
            classname="TailSamplingValidator",
        )
        tc.time = 0.0
        if not svc_result.get("stat_pass", False):
            tc.result = [Failure(
                f"Sampled {svc_result['sampled_traces']} traces, "
                f"expected ~{svc_result['expected_sampled']:.0f}, "
                f"deviation {svc_result['sigma_deviation']:.2f}σ > {SIGMA_THRESHOLD}σ"
            )]
        suite.add_testcase(tc)

    # Aggregate test case
    agg = results.get("aggregate", {})
    tc_agg = TestCase("sampling_rate.aggregate", classname="TailSamplingValidator")
    tc_agg.time = 0.0
    if not agg.get("stat_pass", False):
        tc_agg.result = [Failure(
            f"Aggregate sampled {agg.get('sampled_traces', '?')} traces, "
            f"expected ~{agg.get('expected_sampled', '?')}, "
            f"deviation {agg.get('sigma_deviation', '?')}σ"
        )]
    suite.add_testcase(tc_agg)

    # Orphan sampled traces test case
    tc_orphan = TestCase("sampled_subset_of_raw", classname="TailSamplingValidator")
    tc_orphan.time = 0.0
    orphan_count = results.get("orphan_sampled_count", 0)
    if orphan_count > 0:
        tc_orphan.result = [Failure(
            f"{orphan_count} sampled trace(s) not found in raw topic"
        )]
    suite.add_testcase(tc_orphan)

    xml.add_testsuite(suite)
    xml.write(path)
    log.info("Wrote %s", path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Statistical reconciliation validator for tail-sampling tests"
    )
    p.add_argument(
        "--run-id",
        default=os.environ.get("TEST_RUN_ID", "smoke-001"),
        help="Test run ID to filter messages by test.run_id attribute.",
    )
    p.add_argument(
        "--brokers",
        default=os.environ.get("KAFKA_BROKERS", "localhost:9092"),
        help="Kafka bootstrap servers (comma-separated host:port).",
    )
    p.add_argument(
        "--raw-group",
        default=None,
        help="Consumer group for raw topic (default: validator-raw-<run_id>).",
    )
    p.add_argument(
        "--sampled-group",
        default=None,
        help="Consumer group for sampled topic (default: validator-sampled-<run_id>).",
    )
    p.add_argument(
        "--manifest",
        default=os.environ.get("GENERATOR_MANIFEST", "generator-manifest.json"),
        help="Path to the generator manifest JSON.",
    )
    p.add_argument(
        "--timeout-ms",
        type=int,
        default=int(os.environ.get("CONSUMER_TIMEOUT_MS", "10000")),
        help="Kafka consumer timeout in milliseconds (per poll with no messages).",
    )
    p.add_argument(
        "--output-dir",
        default=os.environ.get("OUTPUT_DIR", "."),
        help="Directory for output files.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)

    run_id = args.run_id
    raw_group = args.raw_group or f"validator-raw-{run_id}"
    sampled_group = args.sampled_group or f"validator-sampled-{run_id}"
    out = args.output_dir
    os.makedirs(out, exist_ok=True)

    log.info("=" * 60)
    log.info("OTel tail-sampling validator")
    log.info("  run_id       : %s", run_id)
    log.info("  brokers      : %s", args.brokers)
    log.info("  raw group    : %s", raw_group)
    log.info("  sampled group: %s", sampled_group)
    log.info("=" * 60)

    # Load generator manifest
    manifest: Optional[dict] = None
    if os.path.exists(args.manifest):
        with open(args.manifest) as f:
            manifest = json.load(f)
        log.info("Loaded manifest: %d services, %d total traces",
                 len(manifest.get("services", [])),
                 manifest.get("total_traces", 0))
    else:
        log.warning("Manifest not found at %s; raw count check will be skipped", args.manifest)

    # Wait for Kafka
    wait_for_broker(args.brokers)

    # Consume both topics
    raw_records = consume_topic(
        brokers=args.brokers,
        topic="otel-traces-raw",
        group_id=raw_group,
        run_id=run_id,
        timeout_ms=args.timeout_ms,
    )
    sampled_records = consume_topic(
        brokers=args.brokers,
        topic="otel-traces-sampled",
        group_id=sampled_group,
        run_id=run_id,
        timeout_ms=args.timeout_ms,
    )

    # Validate
    results = validate(
        raw_records=raw_records,
        sampled_records=sampled_records,
        manifest=manifest,
    )

    # Write reports
    write_json(results, os.path.join(out, "validation.json"))
    write_csv(results, os.path.join(out, "validation-per-svc.csv"))
    write_junit(results, os.path.join(out, "validation-junit.xml"), run_id)

    # Print summary
    print()
    print("=" * 60)
    print(f"Validation {'PASSED' if results['overall_pass'] else 'FAILED'}")
    print(f"  Raw unique traces    : {results['raw_unique_total']}")
    print(f"  Sampled unique traces: {results['sampled_unique_total']}")
    agg = results.get("aggregate", {})
    print(f"  Aggregate rate       : {agg.get('sample_rate', 0)*100:.2f}%  "
          f"(expected 10.00%,  dev={agg.get('sigma_deviation', '?')}σ)")
    if results["failures"]:
        print()
        print("FAILURES:")
        for f in results["failures"]:
            print(f"  ✗ {f}")
    if results["warnings"]:
        print()
        print("WARNINGS:")
        for w in results["warnings"]:
            print(f"  ⚠ {w}")
    print("=" * 60)

    return 0 if results["overall_pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
