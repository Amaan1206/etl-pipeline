"""
datawatch.core.monitor
~~~~~~~~~~~~~~~~~~~~~~~
Orchestrates all registered detectors against a baseline/current
DataFrame pair and collects the results.

Usage::

    from datawatch.core.monitor import Monitor

    monitor = Monitor()
    failed = monitor.run_check(baseline_df, current_df)
"""

import logging
from datetime import datetime
from typing import List, Optional

import pandas as pd

from datawatch.detectors.base import BaseDetector, DetectionResult

logger = logging.getLogger(__name__)


class Monitor:
    """Top-level monitoring orchestrator.

    By default all three built-in detectors are registered.  Pass a
    custom *detectors* list to override.

    Parameters
    ----------
    detectors : list[BaseDetector] | None
        Detectors to run.  If ``None``, the default set
        (SchemaDrift, NullRate, Distribution) is instantiated.
    """

    def __init__(
        self,
        detectors: Optional[List[BaseDetector]] = None,
    ) -> None:
        if detectors is not None:
            self.detectors = detectors
        else:
            # Lazy imports so that callers who supply their own list
            # are never forced to depend on scipy/numpy at import time.
            from datawatch.detectors.schema_drift import SchemaDriftDetector
            from datawatch.detectors.null_rate import NullRateDetector
            from datawatch.detectors.distribution import DistributionDetector

            self.detectors: List[BaseDetector] = [
                SchemaDriftDetector(),
                NullRateDetector(),
                DistributionDetector(),
            ]

    # ── Public API ──────────────────────────────────────────────────────

    def run_check(
        self,
        baseline_df: pd.DataFrame,
        current_df: pd.DataFrame,
    ) -> List[DetectionResult]:
        """Execute every registered detector and return **failed** results.

        Each detector is invoked inside its own ``try / except`` block so
        that a crash in one detector never prevents the others from
        running.

        Parameters
        ----------
        baseline_df:
            Reference DataFrame (the "expected" state).
        current_df:
            Latest batch DataFrame to compare against the baseline.

        Returns
        -------
        list[DetectionResult]
            Only results where ``passed is False``.
        """
        all_results: List[DetectionResult] = []
        run_start = datetime.utcnow()

        logger.info(
            "[%s] Monitor run started — %d detector(s) registered.",
            run_start.strftime("%Y-%m-%d %H:%M:%S"),
            len(self.detectors),
        )

        for detector in self.detectors:
            detector_start = datetime.utcnow()
            logger.info(
                "[%s] Running detector: %s",
                detector_start.strftime("%Y-%m-%d %H:%M:%S"),
                detector.name,
            )

            try:
                results = detector.detect(baseline_df, current_df)
                all_results.extend(results)
                logger.info(
                    "[%s] Detector '%s' finished — %d result(s).",
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    detector.name,
                    len(results),
                )
            except Exception:
                logger.exception(
                    "Detector '%s' raised an unhandled exception. "
                    "Skipping this detector and continuing.",
                    detector.name,
                )

        # Filter to only failed checks.
        failed = [r for r in all_results if not r.passed]

        logger.info(
            "[%s] Monitor run completed — %d total result(s), %d failed.",
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            len(all_results),
            len(failed),
        )

        return failed
