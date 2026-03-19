"""Unit tests for schema drift detection."""

import pandas as pd

from datawatch.detectors.base import Severity
from datawatch.detectors.schema_drift import SchemaDriftDetector


def test_detects_added_column(sample_baseline_df: pd.DataFrame, clean_batch_df: pd.DataFrame) -> None:
    """Adding a new column should produce a CRITICAL schema drift result."""
    detector = SchemaDriftDetector()
    current_df = clean_batch_df.copy(deep=True)
    current_df["new_metric"] = 1

    results = detector.detect(sample_baseline_df, current_df)

    assert any(
        (result.column_name == "new_metric")
        and (result.severity == Severity.CRITICAL)
        and (result.passed is False)
        for result in results
    )


def test_detects_removed_column(sample_baseline_df: pd.DataFrame) -> None:
    """Removing an expected column should produce a CRITICAL schema drift result."""
    detector = SchemaDriftDetector()
    current_df = sample_baseline_df.drop(columns=["salary"])

    results = detector.detect(sample_baseline_df, current_df)

    assert any(
        (result.column_name == "salary")
        and (result.severity == Severity.CRITICAL)
        and (result.passed is False)
        for result in results
    )


def test_detects_type_change(sample_baseline_df: pd.DataFrame) -> None:
    """Changing a column dtype should produce a WARNING schema drift result."""
    detector = SchemaDriftDetector()
    current_df = sample_baseline_df.copy(deep=True)
    current_df["age"] = current_df["age"].round().astype(str)

    results = detector.detect(sample_baseline_df, current_df)

    assert any(
        (result.column_name == "age")
        and (result.severity == Severity.WARNING)
        and (result.passed is False)
        for result in results
    )


def test_passes_identical_schemas(sample_baseline_df: pd.DataFrame) -> None:
    """Identical schemas should produce no schema drift findings."""
    detector = SchemaDriftDetector()
    current_df = sample_baseline_df.copy(deep=True)

    results = detector.detect(sample_baseline_df, current_df)

    assert results == []


def test_handles_empty_dataframe() -> None:
    """Empty DataFrames with matching schemas should not raise errors."""
    detector = SchemaDriftDetector()
    baseline_df = pd.DataFrame(columns=["age", "salary", "category", "active"])
    current_df = pd.DataFrame(columns=["age", "salary", "category", "active"])

    results = detector.detect(baseline_df, current_df)

    assert isinstance(results, list)
    assert len(results) == 0
