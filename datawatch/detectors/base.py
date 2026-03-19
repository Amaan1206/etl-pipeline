"""
datawatch.detectors.base
~~~~~~~~~~~~~~~~~~~~~~~~~
Foundation for the detection engine.

Defines the :class:`Severity` enum, the :class:`DetectionResult` data
structure returned by every detector, and the :class:`BaseDetector` abstract
base class that all concrete detectors must implement.
"""

import abc
import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import List

import pandas as pd


# ── Severity Levels ──────────────────────────────────────────────────────────


class Severity(enum.Enum):
    """Severity level assigned to each detection result."""

    HEALTHY = "HEALTHY"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


# ── Detection Result ────────────────────────────────────────────────────────


@dataclass
class DetectionResult:
    """Immutable record produced by a single detector check on one column.

    Attributes
    ----------
    column_name : str
        The column (or schema-level label) the result pertains to.
    detector_type : str
        Identifier of the detector that generated this result
        (e.g. ``"schema_drift"``, ``"null_rate"``).
    passed : bool
        ``True`` if no anomaly was found; ``False`` otherwise.
    severity : Severity
        How severe the detected issue is.
    score : float
        A numeric metric produced by the detector (e.g. PSI value,
        null-rate delta).  Interpretation depends on the detector type.
    details : str
        Human-readable explanation of the finding.
    timestamp : datetime
        When the result was created.  Defaults to *now (UTC)*.
    """

    column_name: str
    detector_type: str
    passed: bool
    severity: Severity
    score: float
    details: str
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        """Serialise the result to a plain dictionary (JSON-friendly)."""
        return {
            "column_name": self.column_name,
            "detector_type": self.detector_type,
            "passed": self.passed,
            "severity": self.severity.value,
            "score": self.score,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
        }


# ── Abstract Base Detector ──────────────────────────────────────────────────


class BaseDetector(abc.ABC):
    """Abstract interface every detector must satisfy.

    Subclasses implement :meth:`detect` to compare a *baseline*
    DataFrame against a *current* DataFrame and produce a list of
    :class:`DetectionResult` objects — one per column/check that the
    detector evaluates.
    """

    def __init__(self, name: str) -> None:
        self.name = name

    @abc.abstractmethod
    def detect(
        self,
        baseline_df: pd.DataFrame,
        current_df: pd.DataFrame,
    ) -> List[DetectionResult]:
        """Compare *baseline_df* to *current_df* and return detection results.

        Parameters
        ----------
        baseline_df:
            The reference (expected) dataset.
        current_df:
            The latest batch to compare against the baseline.

        Returns
        -------
        list[DetectionResult]
            One or more results describing the outcome of each check.
        """
        ...
