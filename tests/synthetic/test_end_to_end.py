"""Synthetic end-to-end detection tests and benchmark assertions."""

from typing import List

import pandas as pd

from datawatch.detectors.distribution import DistributionDetector
from datawatch.detectors.null_rate import NullRateDetector
from datawatch.detectors.schema_drift import SchemaDriftDetector
from tests.synthetic.pipeline_simulator import PipelineSimulator


def _build_detectors() -> List[object]:
    """Create a full detector stack used in synthetic benchmarks."""
    return [
        SchemaDriftDetector(),
        NullRateDetector(),
        DistributionDetector(),
    ]


def _batch_has_alert(baseline_df: pd.DataFrame, current_df: pd.DataFrame) -> bool:
    """Return True when any detector flags a failed check for the batch."""
    for detector in _build_detectors():
        results = detector.detect(baseline_df, current_df)
        if any(result.passed is False for result in results):
            return True
    return False


def _evaluate_batches(batches: List[pd.DataFrame]) -> List[bool]:
    """Evaluate all batches after baseline and return per-batch alert flags."""
    baseline_df = batches[0]
    return [_batch_has_alert(baseline_df, batch) for batch in batches[1:]]


def test_clean_pipeline_no_alerts() -> None:
    """Ten healthy batches should produce zero alerting batches."""
    simulator = PipelineSimulator(seed=9001)
    batches = simulator.generate_healthy_batches(10)

    alert_flags = _evaluate_batches(batches)

    assert len(alert_flags) == 9
    assert sum(alert_flags) == 0


def test_corruption_detected_immediately() -> None:
    """Corruption starting at batch index 5 should be detected at index 5."""
    simulator = PipelineSimulator(seed=9002)
    batches = simulator.generate_corrupted_sequence(n=10, corrupt_at=5)

    alert_flags = _evaluate_batches(batches)

    assert len(alert_flags) == 9
    assert not any(alert_flags[:4])
    assert alert_flags[4] is True


def test_detection_rate_benchmark() -> None:
    """Detection benchmark on corrupted batches must be at least 90%."""
    simulator = PipelineSimulator(seed=9003)

    corrupted_sequence = simulator.generate_corrupted_sequence(n=101, corrupt_at=1)
    alert_flags = _evaluate_batches(corrupted_sequence)

    detected_batches = sum(alert_flags)
    total_corrupted_batches = len(alert_flags)
    detection_rate = detected_batches / float(total_corrupted_batches)

    print(
        "\n[Benchmark] Corrupted batches detected: {0}/{1} ({2:.2%})".format(
            detected_batches,
            total_corrupted_batches,
            detection_rate,
        )
    )

    assert detection_rate >= 0.90


def test_false_positive_rate() -> None:
    """False-positive benchmark on clean batches must stay at or below 5%."""
    simulator = PipelineSimulator(seed=9004)

    clean_sequence = simulator.generate_healthy_batches(101)
    alert_flags = _evaluate_batches(clean_sequence)

    false_positive_batches = sum(alert_flags)
    total_clean_batches = len(alert_flags)
    false_positive_rate = false_positive_batches / float(total_clean_batches)

    print(
        "\n[Benchmark] False-positive clean batches: {0}/{1} ({2:.2%})".format(
            false_positive_batches,
            total_clean_batches,
            false_positive_rate,
        )
    )

    assert false_positive_rate <= 0.05
