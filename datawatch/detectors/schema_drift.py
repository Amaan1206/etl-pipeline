"""
datawatch.detectors.schema_drift
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Detects structural changes between a baseline and current DataFrame:

* **New columns** added to the current batch.
* **Columns removed** from the current batch.
* **Data-type changes** for columns present in both.

Each change produces a separate :class:`DetectionResult`.
"""

from typing import List

import pandas as pd

from datawatch.detectors.base import BaseDetector, DetectionResult, Severity


class SchemaDriftDetector(BaseDetector):
    """Compare DataFrame schemas and flag structural drift.

    Edge cases handled:
    - Empty DataFrames (zero rows but columns may still differ).
    - All columns changed.
    - Single-column tables.
    """

    def __init__(self) -> None:
        super().__init__(name="schema_drift")

    # ── Core detection logic ────────────────────────────────────────────

    def detect(
        self,
        baseline_df: pd.DataFrame,
        current_df: pd.DataFrame,
    ) -> List[DetectionResult]:
        """Detect schema differences between *baseline_df* and *current_df*.

        Parameters
        ----------
        baseline_df:
            Reference DataFrame whose schema is considered the "expected" state.
        current_df:
            Latest batch whose schema is compared against the baseline.

        Returns
        -------
        list[DetectionResult]
            One result per detected structural change.  An empty list means
            the schemas are identical.
        """
        results: List[DetectionResult] = []

        baseline_cols = set(baseline_df.columns)
        current_cols = set(current_df.columns)

        # ── New columns (in current but not in baseline) ────────────────
        for col in sorted(current_cols - baseline_cols):
            results.append(
                DetectionResult(
                    column_name=col,
                    detector_type=self.name,
                    passed=False,
                    severity=Severity.CRITICAL,
                    score=1.0,
                    details=f"Column '{col}' was added (not present in baseline).",
                )
            )

        # ── Removed columns (in baseline but not in current) ────────────
        for col in sorted(baseline_cols - current_cols):
            results.append(
                DetectionResult(
                    column_name=col,
                    detector_type=self.name,
                    passed=False,
                    severity=Severity.CRITICAL,
                    score=1.0,
                    details=f"Column '{col}' was removed (present in baseline but missing in current).",
                )
            )

        # ── Dtype changes (columns present in both) ─────────────────────
        common_cols = sorted(baseline_cols & current_cols)
        for col in common_cols:
            baseline_dtype = str(baseline_df[col].dtype)
            current_dtype = str(current_df[col].dtype)
            if baseline_dtype != current_dtype:
                results.append(
                    DetectionResult(
                        column_name=col,
                        detector_type=self.name,
                        passed=False,
                        severity=Severity.WARNING,
                        score=0.5,
                        details=(
                            f"Column '{col}' dtype changed from "
                            f"'{baseline_dtype}' to '{current_dtype}'."
                        ),
                    )
                )

        return results
