"""
datawatch.detectors.null_rate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Compares the null (missing-value) percentage per column between a
baseline and current DataFrame.

Configurable thresholds determine whether a change in null rate
triggers a WARNING or CRITICAL alert.
"""

from typing import List

import pandas as pd

from datawatch.detectors.base import BaseDetector, DetectionResult, Severity


class NullRateDetector(BaseDetector):
    """Detect significant changes in per-column null percentages.

    Parameters
    ----------
    warning_threshold : float
        Minimum absolute change in null percentage (0–100 scale) to
        trigger a WARNING.  Default: ``5.0`` percentage points.
    critical_threshold : float
        Minimum absolute change in null percentage to trigger a
        CRITICAL alert.  Default: ``20.0`` percentage points.
    """

    def __init__(
        self,
        warning_threshold: float = 5.0,
        critical_threshold: float = 20.0,
    ) -> None:
        super().__init__(name="null_rate")
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold

    # ── Helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _null_pct(series: pd.Series) -> float:
        """Return the null percentage (0–100) for *series*.

        Returns ``0.0`` for an empty series so that empty DataFrames
        never cause division-by-zero.
        """
        if len(series) == 0:
            return 0.0
        return float(series.isna().sum() / len(series) * 100.0)

    def _classify(self, delta: float) -> Severity:
        """Map an absolute null-rate delta to a severity level."""
        if delta >= self.critical_threshold:
            return Severity.CRITICAL
        if delta >= self.warning_threshold:
            return Severity.WARNING
        return Severity.HEALTHY

    # ── Core detection logic ────────────────────────────────────────────

    def detect(
        self,
        baseline_df: pd.DataFrame,
        current_df: pd.DataFrame,
    ) -> List[DetectionResult]:
        """Compare per-column null rates between two DataFrames.

        Parameters
        ----------
        baseline_df:
            Reference DataFrame.
        current_df:
            Latest batch to compare against the baseline.

        Returns
        -------
        list[DetectionResult]
            One result per column present in **both** DataFrames.
            Columns that exist only in one DataFrame are ignored
            (schema drift is handled by :class:`SchemaDriftDetector`).
        """
        results: List[DetectionResult] = []

        common_cols = sorted(
            set(baseline_df.columns) & set(current_df.columns)
        )

        for col in common_cols:
            baseline_pct = self._null_pct(baseline_df[col])
            current_pct = self._null_pct(current_df[col])
            delta = abs(current_pct - baseline_pct)

            severity = self._classify(delta)
            passed = severity == Severity.HEALTHY

            results.append(
                DetectionResult(
                    column_name=col,
                    detector_type=self.name,
                    passed=passed,
                    severity=severity,
                    score=round(delta, 4),
                    details=(
                        f"Column '{col}' null rate: "
                        f"{baseline_pct:.2f}% → {current_pct:.2f}% "
                        f"(Δ {delta:+.2f} pp)."
                    ),
                )
            )

        return results
