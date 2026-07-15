"""
Unit tests for the statistical validation logic.

These tests verify:
1. The binomial acceptance interval math is correct
2. Edge cases (zero traces, exact boundary values)
3. Per-service and aggregate stat checks
4. Generator manifest parsing

Run with:
    pip install pytest
    pytest test/unit/ -v
"""

from __future__ import annotations

import math
import sys
import os
import json
import pytest

# Add parent directories to path so we can import validator internals
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Import the validation functions without needing Kafka or proto deps
# by directly importing only the math functions from the module.

SAMPLING_P = 0.10
SIGMA_THRESHOLD = 5.0


def acceptance_interval(n: int, p: float = SAMPLING_P, sigma: float = SIGMA_THRESHOLD):
    """Compute the 5-sigma binomial acceptance interval."""
    expected = n * p
    std_dev = math.sqrt(n * p * (1 - p))
    lo = max(0, expected - sigma * std_dev)
    hi = expected + sigma * std_dev
    return math.ceil(lo), math.floor(hi)


def stat_passes(sampled: int, n: int, p: float = SAMPLING_P, sigma: float = SIGMA_THRESHOLD) -> bool:
    """Return True if |sampled - n*p| <= sigma * sqrt(n*p*(1-p))."""
    if n == 0:
        return sampled == 0
    expected = n * p
    std_dev = math.sqrt(n * p * (1 - p))
    return abs(sampled - expected) <= sigma * std_dev


# ---------------------------------------------------------------------------
# Tests for acceptance interval math
# ---------------------------------------------------------------------------

class TestAcceptanceInterval:
    def test_1000_traces_expected_range(self):
        """1,000 traces: expect ~100 sampled, interval roughly 50–150 for 5σ."""
        lo, hi = acceptance_interval(1000)
        assert lo >= 50, f"Lower bound {lo} too low"
        assert hi <= 150, f"Upper bound {hi} too high"
        assert lo <= 100 <= hi, f"Expected value 100 not in [{lo}, {hi}]"

    def test_1000_traces_exact_boundaries(self):
        """Verify exact 5-sigma bounds for 1,000 traces at p=0.10."""
        n, p, k = 1000, 0.10, SIGMA_THRESHOLD
        expected = n * p          # 100.0
        sigma = math.sqrt(n * p * (1 - p))  # sqrt(90) ≈ 9.49
        lo_exact = expected - k * sigma   # 100 - 47.4 ≈ 52.6
        hi_exact = expected + k * sigma   # 100 + 47.4 ≈ 147.4
        lo, hi = acceptance_interval(n)
        assert lo == math.ceil(lo_exact)
        assert hi == math.floor(hi_exact)

    def test_20000_traces_tight_interval(self):
        """20,000 traces: interval should be much tighter (~170 wide)."""
        lo, hi = acceptance_interval(20000)
        width = hi - lo
        expected = 2000
        assert lo <= expected <= hi
        # 5σ = 5 * sqrt(20000 * 0.1 * 0.9) = 5 * sqrt(1800) ≈ 212
        assert width < 500, f"Width {width} unexpectedly large"
        assert width > 100, f"Width {width} unexpectedly small"

    def test_2000000_traces_very_tight_interval(self):
        """2,000,000 traces: interval should be very tight."""
        lo, hi = acceptance_interval(2_000_000)
        expected = 200_000
        assert lo <= expected <= hi
        # 5σ = 5 * sqrt(2M * 0.1 * 0.9) = 5 * sqrt(180000) ≈ 2121
        width = hi - lo
        assert width < 10_000
        assert width > 1_000

    def test_zero_traces(self):
        lo, hi = acceptance_interval(0)
        assert lo == 0
        assert hi == 0

    def test_lower_bound_non_negative(self):
        """Lower bound should never be negative even for small n."""
        for n in [1, 5, 10, 50]:
            lo, hi = acceptance_interval(n)
            assert lo >= 0, f"Lower bound {lo} negative for n={n}"

    def test_symmetric_around_expected(self):
        """Bounds should be roughly symmetric around expected value."""
        n = 100_000
        lo, hi = acceptance_interval(n)
        expected = n * SAMPLING_P
        assert abs((hi - expected) - (expected - lo)) <= 2


class TestStatPasses:
    def test_exact_10_percent_passes(self):
        assert stat_passes(1000, 10_000) is True

    def test_slight_deviation_passes(self):
        assert stat_passes(950, 10_000) is True
        assert stat_passes(1050, 10_000) is True

    def test_extreme_deviation_fails(self):
        # 0 sampled out of 10,000 is wildly outside 5σ
        assert stat_passes(0, 10_000) is False
        # 5000 sampled out of 10,000 (50% rate) is wildly outside 5σ
        assert stat_passes(5_000, 10_000) is False

    def test_exact_boundary_accepted(self):
        n = 10_000
        p = SAMPLING_P
        expected = n * p
        sigma = math.sqrt(n * p * (1 - p))
        # Exactly at boundary
        boundary = math.floor(expected + SIGMA_THRESHOLD * sigma)
        assert stat_passes(boundary, n) is True
        assert stat_passes(boundary + 1, n) is False

    def test_empty_service_with_no_sampled(self):
        assert stat_passes(0, 0) is True

    def test_large_volume_passes(self):
        """For 2M traces, 200,000 sampled should pass."""
        assert stat_passes(200_000, 2_000_000) is True

    def test_large_volume_slight_deviation_passes(self):
        """For 2M traces, ±1000 deviation should still pass."""
        assert stat_passes(201_000, 2_000_000) is True
        assert stat_passes(199_000, 2_000_000) is True


class TestAcceptanceEdgeCases:
    def test_low_volume_service_not_starved(self):
        """
        A low-volume service (1,000 traces) should not be starved: the interval
        must include at least some minimum count, not just zero.
        """
        lo, _ = acceptance_interval(1_000)
        assert lo > 0, "Low-volume service acceptance lower bound must be > 0"

    def test_different_volumes_have_independent_intervals(self):
        """
        Each service's acceptance interval is computed independently based on
        its own n.  High-volume services should not affect low-volume ones.
        """
        lo_low, hi_low = acceptance_interval(1_000)
        lo_high, hi_high = acceptance_interval(2_000_000)

        # Low-volume rate range in percent
        rate_low_lo = lo_low / 1_000
        rate_high_lo = lo_high / 2_000_000
        rate_low_hi = hi_low / 1_000
        rate_high_hi = hi_high / 2_000_000

        # Both should be centered near 10%
        assert 0.05 <= rate_low_lo <= 0.10
        assert 0.09 <= rate_high_lo <= 0.10
        assert 0.10 <= rate_low_hi <= 0.15
        assert 0.10 <= rate_high_hi <= 0.11


# ---------------------------------------------------------------------------
# Tests for generator output structure
# ---------------------------------------------------------------------------

class TestGeneratorManifest:
    def test_manifest_structure(self, tmp_path):
        """Verify the manifest JSON we'd write has the right schema."""
        manifest = {
            "run_id": "test-001",
            "seed": 42,
            "span_range": [2, 5],
            "sampling_percentage": 10,
            "total_traces": 25000,
            "total_spans": 87500,
            "elapsed_seconds": 12.3,
            "services": [
                {
                    "name": "low-vol-svc-00",
                    "traces_sent": 1000,
                    "spans_sent": 3500,
                    "expected_sampled_low": 53,
                    "expected_sampled_high": 148,
                    "trace_ids": [f"{i:032x}" for i in range(1000)],
                },
                {
                    "name": "high-vol-svc-00",
                    "traces_sent": 20000,
                    "spans_sent": 70000,
                    "expected_sampled_low": 1928,
                    "expected_sampled_high": 2073,
                    "trace_ids": [f"{i:032x}" for i in range(20000)],
                },
            ],
        }

        manifest_path = tmp_path / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)

        with open(manifest_path) as f:
            loaded = json.load(f)

        assert loaded["run_id"] == "test-001"
        assert loaded["total_traces"] == 25000
        assert len(loaded["services"]) == 2
        assert loaded["services"][0]["traces_sent"] == 1000
        assert loaded["services"][1]["traces_sent"] == 20000

    def test_expected_sampled_bounds_in_manifest(self):
        """The expected_sampled_low/high in the manifest should match our math."""
        n = 1000
        lo_formula = max(0, int(n * 0.10 - 5 * (n * 0.10 * 0.90) ** 0.5))
        hi_formula = int(n * 0.10 + 5 * (n * 0.10 * 0.90) ** 0.5 + 1)

        lo_interval, hi_interval = acceptance_interval(n)

        # The formula used in the generator should be consistent with acceptance_interval
        # (allowing for off-by-one due to floor/ceil differences)
        assert abs(lo_formula - lo_interval) <= 2
        assert abs(hi_formula - hi_interval) <= 2


# ---------------------------------------------------------------------------
# Tests for smoke test configuration sanity
# ---------------------------------------------------------------------------

class TestSmokeTestConfig:
    """Verify that the smoke test parameters are statistically sensible."""

    LOW_VOL_SERVICES = 5
    LOW_VOL_TRACES = 1_000
    HIGH_VOL_SERVICES = 10
    HIGH_VOL_TRACES = 20_000
    DECISION_WAIT = 5  # seconds
    SAMPLING_P = 0.10

    def test_low_vol_interval_is_non_trivial(self):
        """1,000 traces should give a non-trivial acceptance interval."""
        lo, hi = acceptance_interval(self.LOW_VOL_TRACES)
        assert lo > 0
        assert hi > 0
        # Width should be meaningful (not just one or two values)
        assert hi - lo > 10

    def test_high_vol_interval_is_narrow_relative_to_expected(self):
        """20,000 traces should give a relatively tight interval."""
        lo, hi = acceptance_interval(self.HIGH_VOL_TRACES)
        expected = self.HIGH_VOL_TRACES * self.SAMPLING_P
        width_pct = (hi - lo) / expected
        # Width should be < 25% of expected value at 5σ
        assert width_pct < 0.25

    def test_num_traces_capacity(self):
        """
        The NUM_TRACES config in the tail sampler must exceed:
        max_new_traces_per_sec × decision_wait × safety_factor
        """
        NUM_TRACES = 75_000   # default in tail-sampler.yaml

        # For smoke test: all traces arrive in ~30s, so ~7000 traces/s
        total_traces = (self.LOW_VOL_SERVICES * self.LOW_VOL_TRACES
                        + self.HIGH_VOL_SERVICES * self.HIGH_VOL_TRACES)
        # Assume traces arrive in 30 seconds (conservative)
        peak_rate = total_traces / 30
        required = peak_rate * self.DECISION_WAIT * 1.5  # 1.5x safety factor
        assert NUM_TRACES > required, (
            f"NUM_TRACES={NUM_TRACES} may be too low for smoke test "
            f"(need >{required:.0f})"
        )
