"""Unit tests for null-rate detection behavior."""

import numpy as np
import pandas as pd

from datawatch.detectors.base import Severity
from datawatch.detectors.null_rate import NullRateDetector


def test_detects_null_explosion() -> None:
    """A 0% to 50% null jump should be classified as CRITICAL."""
    detector = NullRateDetector()
    baseline_df = pd.DataFrame({"salary": np.ones(100)})
    current_df = baseline_df.copy(deep=True)
    current_df.loc[:49, "salary"] = np.nan

    results = detector.detect(baseline_df, current_df)

    assert len(results) == 1
    assert results[0].severity == Severity.CRITICAL
    assert results[0].passed is False
    assert results[0].score == 50.0


def test_detects_warning_threshold() -> None:
    """A 0% to 10% null jump should be classified as WARNING."""
    detector = NullRateDetector()
    baseline_df = pd.DataFrame({"salary": np.ones(100)})
    current_df = baseline_df.copy(deep=True)
    current_df.loc[:9, "salary"] = np.nan

    results = detector.detect(baseline_df, current_df)

    assert len(results) == 1
    assert results[0].severity == Severity.WARNING
    assert results[0].passed is False
    assert results[0].score == 10.0


def test_passes_within_threshold() -> None:
    """A small null-rate delta under warning threshold should remain HEALTHY."""
    detector = NullRateDetector()

    baseline_values = np.ones(100)
    baseline_values[:1] = np.nan
    baseline_df = pd.DataFrame({"salary": baseline_values})

    current_values = np.ones(100)
    current_values[:3] = np.nan
    current_df = pd.DataFrame({"salary": current_values})

    results = detector.detect(baseline_df, current_df)

    assert len(results) == 1
    assert results[0].severity == Severity.HEALTHY
    assert results[0].passed is True
    assert results[0].score == 2.0


def test_handles_all_null_baseline() -> None:
    """An all-null baseline should still produce a valid null-rate delta result."""
    detector = NullRateDetector()
    baseline_df = pd.DataFrame({"salary": [np.nan] * 100})

    current_values = [np.nan] * 90 + [1.0] * 10
    current_df = pd.DataFrame({"salary": current_values})

    results = detector.detect(baseline_df, current_df)

    assert len(results) == 1
    assert results[0].severity == Severity.WARNING
    assert results[0].passed is False
    assert results[0].score == 10.0


def test_custom_thresholds() -> None:
    """Custom warning and critical thresholds should be respected exactly."""
    detector = NullRateDetector(warning_threshold=2.0, critical_threshold=4.0)
    baseline_df = pd.DataFrame({"salary": np.ones(100)})

    warning_df = baseline_df.copy(deep=True)
    warning_df.loc[:2, "salary"] = np.nan
    warning_result = detector.detect(baseline_df, warning_df)[0]

    critical_df = baseline_df.copy(deep=True)
    critical_df.loc[:4, "salary"] = np.nan
    critical_result = detector.detect(baseline_df, critical_df)[0]

    assert warning_result.severity == Severity.WARNING
    assert warning_result.score == 3.0

    assert critical_result.severity == Severity.CRITICAL
    assert critical_result.score == 5.0
