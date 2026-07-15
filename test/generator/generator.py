#!/usr/bin/env python3
"""
Deterministic OTLP trace generator for tail-sampling integration tests.

Generates complete root+child traces with:
  - Uniformly distributed unique 128-bit trace IDs (seeded PRNG, no head sampling)
  - service.name, test.run_id, test.owner.service on every span
  - Known expected span count per trace (for validation)
  - Configurable service classes, trace counts, span ranges, and send rate

Architecture
------------
Traces are sent directly to the ingest Collector via OTLP gRPC.
Each service class runs in a dedicated thread with its own TracerProvider so
there is no lock contention on the ID generator.  The generator uses a custom
IdGenerator to produce seeded, deterministic trace IDs while the child span IDs
are random (reproducible per-seed but not guessable from the trace ID).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import List

from opentelemetry import trace as otrace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.id_generator import IdGenerator
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry.trace import SpanContext, TraceFlags, NonRecordingSpan

log = logging.getLogger("generator")


# ---------------------------------------------------------------------------
# Custom ID generator that lets the caller control the next trace ID.
# NOT thread-safe: each thread must use its own instance.
# ---------------------------------------------------------------------------

class ControlledIdGenerator(IdGenerator):
    """ID generator with an injectable trace ID for the next span start."""

    def __init__(self, seed: int) -> None:
        self._rng = random.Random(seed)
        self._next_trace_id: int | None = None

    def set_next_trace_id(self, trace_id: int) -> None:
        self._next_trace_id = trace_id

    def generate_trace_id(self) -> int:
        if self._next_trace_id is not None:
            tid = self._next_trace_id
            self._next_trace_id = None
            return tid
        return self._rng.getrandbits(128) or 1  # must be non-zero

    def generate_span_id(self) -> int:
        return self._rng.getrandbits(64) or 1   # must be non-zero


# ---------------------------------------------------------------------------
# Service class descriptor
# ---------------------------------------------------------------------------

@dataclass
class ServiceClass:
    name: str
    trace_count: int
    expected_total_spans: int = 0   # filled in after generation


# ---------------------------------------------------------------------------
# Per-service trace generator (runs in its own thread)
# ---------------------------------------------------------------------------

def _generate_service(
    *,
    service_name: str,
    run_id: str,
    trace_count: int,
    span_range: tuple[int, int],
    rate_limit: float,          # max new traces/sec for this worker (0 = unlimited)
    endpoint: str,
    seed: int,
    result: dict,
    barrier: threading.Barrier,
) -> None:
    """Generate all traces for one service and store results in *result*."""

    id_gen = ControlledIdGenerator(seed)
    rng = random.Random(seed + 1)

    provider = TracerProvider(
        resource=Resource.create({
            "service.name": service_name,
        }),
        id_generator=id_gen,
        sampler=ALWAYS_ON,
    )
    exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
    provider.add_span_processor(
        BatchSpanProcessor(
            exporter,
            max_export_batch_size=256,
            max_queue_size=8192,
            export_timeout_millis=30_000,
        )
    )
    tracer = provider.get_tracer("generator")

    total_spans = 0
    sent_trace_ids: list[str] = []

    # Wait for all threads to be ready before generating (synchronised start).
    barrier.wait()

    interval = 1.0 / rate_limit if rate_limit > 0 else 0.0
    deadline = time.monotonic()

    for i in range(trace_count):
        # Deterministic unique trace ID for this service + index.
        trace_id = rng.getrandbits(128) or 1
        sent_trace_ids.append(format(trace_id, "032x"))

        num_spans = rng.randint(*span_range)
        total_spans += num_spans

        # Inject our chosen trace ID as the next one the SDK will allocate.
        id_gen.set_next_trace_id(trace_id)

        common_attrs = {
            "test.owner.service": service_name,
            "test.run_id": run_id,
            "test.span_count": num_spans,
            "test.trace_index": i,
        }

        # Root span (starts the trace)
        with tracer.start_as_current_span("root", attributes=common_attrs) as root:
            # Child spans (nested under root)
            for j in range(num_spans - 1):
                with tracer.start_as_current_span(
                    f"child-{j}",
                    attributes={**common_attrs, "test.child_index": j},
                ):
                    pass  # no artificial sleep; span timestamp is the real wall clock

        # Rate limiting: pace trace creation to avoid overwhelming the collector.
        if rate_limit > 0:
            deadline += interval
            now = time.monotonic()
            if deadline > now:
                time.sleep(deadline - now)

        if (i + 1) % 1000 == 0:
            log.info("  %s: generated %d / %d traces", service_name, i + 1, trace_count)

    # Flush all pending spans before returning.
    provider.force_flush(timeout_millis=60_000)
    provider.shutdown()

    result["service"] = service_name
    result["traces_sent"] = trace_count
    result["spans_sent"] = total_spans
    result["trace_ids"] = sent_trace_ids
    log.info("  %s: done – %d traces, %d spans", service_name, trace_count, total_spans)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Deterministic OTLP trace generator for tail-sampling tests"
    )
    p.add_argument(
        "--run-id",
        default=os.environ.get("TEST_RUN_ID", "smoke-001"),
        help="Unique identifier for this test run (propagated as test.run_id attribute).",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=int(os.environ.get("GENERATOR_SEED", "42")),
        help="PRNG seed for deterministic trace IDs.",
    )
    p.add_argument(
        "--low-volume-services",
        type=int,
        default=int(os.environ.get("LOW_VOLUME_SERVICES", "5")),
        metavar="N",
        help="Number of low-volume services.",
    )
    p.add_argument(
        "--low-volume-traces",
        type=int,
        default=int(os.environ.get("LOW_VOLUME_TRACES", "1000")),
        metavar="N",
        help="Traces per low-volume service.",
    )
    p.add_argument(
        "--high-volume-services",
        type=int,
        default=int(os.environ.get("HIGH_VOLUME_SERVICES", "10")),
        metavar="N",
        help="Number of high-volume services.",
    )
    p.add_argument(
        "--high-volume-traces",
        type=int,
        default=int(os.environ.get("HIGH_VOLUME_TRACES", "20000")),
        metavar="N",
        help="Traces per high-volume service.",
    )
    p.add_argument(
        "--min-spans",
        type=int,
        default=int(os.environ.get("MIN_SPANS", "2")),
        help="Minimum spans per trace (inclusive).",
    )
    p.add_argument(
        "--max-spans",
        type=int,
        default=int(os.environ.get("MAX_SPANS", "5")),
        help="Maximum spans per trace (inclusive).",
    )
    p.add_argument(
        "--rate",
        type=float,
        default=float(os.environ.get("SEND_RATE", "0")),
        help="Max new traces/sec per service worker (0 = unlimited).",
    )
    p.add_argument(
        "--endpoint",
        default=os.environ.get("OTLP_ENDPOINT", "localhost:4317"),
        help="OTLP gRPC endpoint (host:port, no scheme).",
    )
    p.add_argument(
        "--output",
        default=os.environ.get("GENERATOR_OUTPUT", "generator-manifest.json"),
        help="Path to write the generation manifest JSON.",
    )
    return p.parse_args(argv)


def build_service_list(args: argparse.Namespace) -> list[ServiceClass]:
    services: list[ServiceClass] = []
    for i in range(args.low_volume_services):
        services.append(ServiceClass(
            name=f"low-vol-svc-{i:02d}",
            trace_count=args.low_volume_traces,
        ))
    for i in range(args.high_volume_services):
        services.append(ServiceClass(
            name=f"high-vol-svc-{i:02d}",
            trace_count=args.high_volume_traces,
        ))
    return services


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)

    services = build_service_list(args)
    span_range = (args.min_spans, args.max_spans)
    total_traces = sum(s.trace_count for s in services)
    total_services = len(services)

    log.info("=" * 60)
    log.info("OTel tail-sampling trace generator")
    log.info("  run_id          : %s", args.run_id)
    log.info("  seed            : %d", args.seed)
    log.info("  services        : %d", total_services)
    log.info("  total traces    : %d", total_traces)
    log.info("  spans per trace : %d–%d", *span_range)
    log.info("  rate limit/svc  : %s traces/s", args.rate if args.rate > 0 else "unlimited")
    log.info("  endpoint        : %s", args.endpoint)
    log.info("=" * 60)

    # Shared barrier so all worker threads start generating simultaneously.
    barrier = threading.Barrier(total_services + 1)  # +1 for main thread

    results: list[dict] = [{} for _ in services]
    threads: list[threading.Thread] = []

    for idx, svc in enumerate(services):
        # Give each service a unique seed derived from the global seed.
        svc_seed = args.seed * 1000 + idx
        t = threading.Thread(
            target=_generate_service,
            name=f"gen-{svc.name}",
            kwargs=dict(
                service_name=svc.name,
                run_id=args.run_id,
                trace_count=svc.trace_count,
                span_range=span_range,
                rate_limit=args.rate,
                endpoint=args.endpoint,
                seed=svc_seed,
                result=results[idx],
                barrier=barrier,
            ),
            daemon=True,
        )
        t.start()
        threads.append(t)

    start_time = time.monotonic()
    barrier.wait()  # release all workers at once
    log.info("All %d service workers started simultaneously.", total_services)

    for t in threads:
        t.join()

    elapsed = time.monotonic() - start_time
    total_spans = sum(r.get("spans_sent", 0) for r in results)

    log.info("=" * 60)
    log.info("Generation complete in %.1f s", elapsed)
    log.info("  traces: %d  spans: %d  rate: %.0f traces/s",
             total_traces, total_spans, total_traces / max(elapsed, 0.001))
    log.info("=" * 60)

    # Write manifest so the validator knows what was generated.
    manifest = {
        "run_id": args.run_id,
        "seed": args.seed,
        "span_range": list(span_range),
        "sampling_percentage": 10,
        "total_traces": total_traces,
        "total_spans": total_spans,
        "elapsed_seconds": round(elapsed, 2),
        "services": [
            {
                "name": r["service"],
                "traces_sent": r["traces_sent"],
                "spans_sent": r["spans_sent"],
                "expected_sampled_low": max(0, int(r["traces_sent"] * 0.10 - 5 * (r["traces_sent"] * 0.10 * 0.90) ** 0.5)),
                "expected_sampled_high": int(r["traces_sent"] * 0.10 + 5 * (r["traces_sent"] * 0.10 * 0.90) ** 0.5 + 1),
                "trace_ids": r["trace_ids"],
            }
            for r in results
        ],
    }

    with open(args.output, "w") as f:
        json.dump(manifest, f, indent=2)
    log.info("Manifest written to %s", args.output)

    # Exit non-zero if any worker failed to produce results.
    for r in results:
        if "service" not in r:
            log.error("One or more service workers did not complete!")
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
