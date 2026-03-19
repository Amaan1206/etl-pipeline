"""
datawatch.detectors.distribution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Statistical distribution-shift detection for numeric columns.

Two complementary tests are applied:

1. **Kolmogorov-Smirnov (KS) test** — non-parametric test of whether two
   samples were drawn from the same continuous distribution.
2. **Population Stability Index (PSI)** — quantifies how much the
   distribution of a variable has changed between two samples.

Only numeric columns are tested; non-numeric columns are silently skipped.
"""

import logging
from typing import List, Tuple

import numpy as np
import pandas as pd
from scipy import stats

from datawatch.detectors.base import BaseDetector, DetectionResult, Severity

logger = logging.getLogger(__name__)

# Small constant added to bin counts to prevent division by zero in PSI.
_EPSILON = 1e-6


class DistributionDetector(BaseDetector):
    """Detect distribution shifts in numeric columns.

    Parameters
    ----------
    ks_alpha : float
        Significance level for the KS test.  A p-value below this
        threshold triggers an alert.  Default: ``0.05``.
    psi_warning : float
        PSI value at or above which a WARNING is issued.  Default: ``0.1``.
    psi_critical : float
        PSI value at or above which a CRITICAL is issued.  Default: ``0.2``.
    n_bins : int
        Number of equal-width bins used to compute PSI.  Default: ``10``.
    """

    def __init__(
        self,
        ks_alpha: float = 0.05,
        psi_warning: float = 0.1,
        psi_critical: float = 0.2,
        n_bins: int = 10,
    ) -> None:
        super().__init__(name="distribution")
        self.ks_alpha = ks_alpha
        self.psi_warning = psi_warning
        self.psi_critical = psi_critical
        self.n_bins = n_bins

    # ── PSI Calculation ─────────────────────────────────────────────────

    def _compute_psi(
        self,
        baseline: np.ndarray,
        current: np.ndarray,
    ) -> float:
        """Compute the Population Stability Index between two 1-D arrays.

        The arrays are discretised into *n_bins* equal-width buckets
        derived from the combined range of both samples.  An epsilon is
        added to every bucket count to avoid ``log(0)`` or division by
        zero.

        Parameters
        ----------
        baseline:
            Reference sample (1-D, numeric, NaN-free).
        current:
            Latest sample (1-D, numeric, NaN-free).

        Returns
        -------
        float
            The PSI value.  Lower is better; 0.0 means identical
            distributions.
        """
        # Determine shared bin edges from the combined data range.
        combined = np.concatenate([baseline, current])
        min_val = float(np.min(combined))
        max_val = float(np.max(combined))

        # If all values are identical there is no distributional shift.
        if min_val == max_val:
            return 0.0

        bin_edges = np.linspace(min_val, max_val, self.n_bins + 1)

        # Compute proportions per bin (with epsilon to avoid zero).
        baseline_counts = np.histogram(baseline, bins=bin_edges)[0].astype(float) + _EPSILON
        current_counts = np.histogram(current, bins=bin_edges)[0].astype(float) + _EPSILON

        baseline_proportions = baseline_counts / baseline_counts.sum()
        current_proportions = current_counts / current_counts.sum()

        # PSI = Σ (current_i - baseline_i) * ln(current_i / baseline_i)
        psi = float(
            np.sum(
                (current_proportions - baseline_proportions)
                * np.log(current_proportions / baseline_proportions)
            )
        )
        return psi

    # ── Severity helpers ────────────────────────────────────────────────

    def _psi_severity(self, psi_value: float) -> Severity:
        """Classify a PSI value into a severity bucket."""
        if psi_value >= self.psi_critical:
            return Severity.CRITICAL
        if psi_value >= self.psi_warning:
            return Severity.WARNING
        return Severity.HEALTHY

    @staticmethod
    def _worst_severity(a: Severity, b: Severity) -> Severity:
        """Return the more severe of two severity levels."""
        order = {Severity.HEALTHY: 0, Severity.WARNING: 1, Severity.CRITICAL: 2}
        return a if order[a] >= order[b] else b

    # ── Core detection logic ────────────────────────────────────────────

    def _check_column(
        self,
        col: str,
        baseline_series: pd.Series,
        current_series: pd.Series,
    ) -> DetectionResult:
        """Run KS + PSI on a single numeric column and return one result.

        Parameters
        ----------
        col:
            Column name.
        baseline_series:
            Baseline values (may contain NaNs — they will be dropped).
        current_series:
            Current values (may contain NaNs — they will be dropped).

        Returns
        -------
        DetectionResult
        """
        baseline_clean = baseline_series.dropna().values.astype(float)
        current_clean = current_series.dropna().values.astype(float)

        # If either sample is empty after dropping NaNs we cannot run
        # the tests — mark the column as CRITICAL so it gets flagged.
        if len(baseline_clean) == 0 or len(current_clean) == 0:
            return DetectionResult(
                column_name=col,
                detector_type=self.name,
                passed=False,
                severity=Severity.CRITICAL,
                score=0.0,
                details=(
                    f"Column '{col}': insufficient non-null data for "
                    f"distribution comparison (baseline={len(baseline_clean)}, "
                    f"current={len(current_clean)} non-null values)."
                ),
            )

        # ── KS test ────────────────────────────────────────────────────
        ks_stat, ks_p_value = stats.ks_2samp(baseline_clean, current_clean)
        ks_alert = ks_p_value < self.ks_alpha
        ks_severity = Severity.WARNING if ks_alert else Severity.HEALTHY

        # ── PSI ─────────────────────────────────────────────────────────
        psi_value = self._compute_psi(baseline_clean, current_clean)
        psi_sev = self._psi_severity(psi_value)

        # ── Combine ─────────────────────────────────────────────────────
        combined_severity = self._worst_severity(ks_severity, psi_sev)
        passed = combined_severity == Severity.HEALTHY

        details_parts: List[str] = [
            f"Column '{col}':",
            f"KS stat={ks_stat:.4f}, p={ks_p_value:.4f}"
            + (" [ALERT]" if ks_alert else " [OK]"),
            f"PSI={psi_value:.4f}"
            + f" [{psi_sev.value}]",
        ]

        return DetectionResult(
            column_name=col,
            detector_type=self.name,
            passed=passed,
            severity=combined_severity,
            score=round(psi_value, 6),
            details=" | ".join(details_parts),
        )

    def detect(
        self,
        baseline_df: pd.DataFrame,
        current_df: pd.DataFrame,
    ) -> List[DetectionResult]:
        """Run distribution-shift detection on all shared numeric columns.

        Parameters
        ----------
        baseline_df:
            Reference DataFrame.
        current_df:
            Latest batch to compare against the baseline.

        Returns
        -------
        list[DetectionResult]
            One result per numeric column present in both DataFrames.
            Non-numeric and columns unique to one DataFrame are skipped.
        """
        results: List[DetectionResult] = []

        common_cols = sorted(
            set(baseline_df.columns) & set(current_df.columns)
        )

        for col in common_cols:
            # Only analyse numeric columns.
            if not pd.api.types.is_numeric_dtype(baseline_df[col]):
                logger.debug("Skipping non-numeric column '%s'.", col)
                continue
            if not pd.api.types.is_numeric_dtype(current_df[col]):
                logger.debug(
                    "Column '%s' changed to non-numeric in current batch; "
                    "skipping distribution check.",
                    col,
                )
                continue

            result = self._check_column(col, baseline_df[col], current_df[col])
            results.append(result)

        return results
