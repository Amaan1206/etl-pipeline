"""Unit tests for distribution-shift detection and PSI calculations."""

import numpy as np
import pandas as pd

from datawatch.detectors.base import Severity
from datawatch.detectors.distribution import DistributionDetector


def test_detects_mean_shift() -> None:
    """A large mean shift should trigger a CRITICAL distribution alert."""
    rng = np.random.default_rng(301)
    baseline_df = pd.DataFrame({"metric": rng.normal(0.0, 1.0, 5000)})
    current_df = pd.DataFrame({"metric": rng.normal(2.5, 1.0, 5000)})

    detector = DistributionDetector()
    results = detector.detect(baseline_df, current_df)

    assert len(results) == 1
    assert results[0].severity == Severity.CRITICAL
    assert results[0].passed is False
    assert results[0].score >= detector.psi_critical


def test_detects_psi_warning() -> None:
    """A moderate shift with PSI in warning band should produce WARNING."""
    baseline_rng = np.random.default_rng(123)
    current_rng = np.random.default_rng(1234)

    baseline = baseline_rng.normal(0.0, 1.0, 5000)
    current = current_rng.normal(0.4, 1.0, 5000)

    detector = DistributionDetector()
    psi_value = detector._compute_psi(baseline, current)

    assert detector.psi_warning <= psi_value < detector.psi_critical

    baseline_df = pd.DataFrame({"metric": baseline})
    current_df = pd.DataFrame({"metric": current})
    results = detector.detect(baseline_df, current_df)

    assert len(results) == 1
    assert results[0].severity == Severity.WARNING
    assert results[0].passed is False


def test_passes_same_distribution() -> None:
    """Identical distributions should stay HEALTHY."""
    rng = np.random.default_rng(401)
    baseline_df = pd.DataFrame({"metric": rng.normal(0.0, 1.0, 4000)})
    current_df = baseline_df.copy(deep=True)

    detector = DistributionDetector()
    results = detector.detect(baseline_df, current_df)

    assert len(results) == 1
    assert results[0].severity == Severity.HEALTHY
    assert results[0].passed is True
    assert results[0].score == 0.0


def test_skips_non_numeric_columns() -> None:
    """Non-numeric columns should be skipped without raising exceptions."""
    baseline_df = pd.DataFrame({"category": ["A", "B", "C"] * 300})
    current_df = pd.DataFrame({"category": ["C", "B", "A"] * 300})

    detector = DistributionDetector()
    results = detector.detect(baseline_df, current_df)

    assert isinstance(results, list)
    assert len(results) == 0


def test_psi_calculation_accuracy() -> None:
    """PSI for a known shifted categorical-like distribution should be stable."""
    detector = DistributionDetector(n_bins=4)

    baseline = np.array([0.0] * 500 + [1.0] * 500 + [2.0] * 500 + [3.0] * 500)
    current = np.array([0.0] * 200 + [1.0] * 600 + [2.0] * 600 + [3.0] * 600)

    psi_value = detector._compute_psi(baseline, current)

    assert 0.15 <= psi_value <= 0.18
