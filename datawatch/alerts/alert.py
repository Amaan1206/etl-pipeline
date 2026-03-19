"""
datawatch.alerts.alert
~~~~~~~~~~~~~~~~~~~~~~~
Core alert data model and enumerations.

An :class:`Alert` is the primary output of the monitoring pipeline.
It carries all the context needed to understand a data-quality issue
and can be serialised for storage or delivery via alert channels.
"""

import enum
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


# ── Enumerations ────────────────────────────────────────────────────────────


class AlertSeverity(enum.Enum):
    """Severity level for an alert."""

    HEALTHY = "HEALTHY"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class AlertType(enum.Enum):
    """Category of data-quality issue that triggered the alert."""

    SCHEMA_DRIFT = "SCHEMA_DRIFT"
    NULL_RATE = "NULL_RATE"
    DISTRIBUTION_SHIFT = "DISTRIBUTION_SHIFT"


# ── Mapping helpers ─────────────────────────────────────────────────────────

# Maps detector_type strings (from DetectionResult) → AlertType.
_DETECTOR_TO_ALERT_TYPE = {
    "schema_drift": AlertType.SCHEMA_DRIFT,
    "null_rate": AlertType.NULL_RATE,
    "distribution": AlertType.DISTRIBUTION_SHIFT,
}


# ── Alert dataclass ─────────────────────────────────────────────────────────


@dataclass
class Alert:
    """Represents a single data-quality alert.

    Attributes
    ----------
    id : str
        Unique identifier (auto-generated UUID4).
    pipeline_name : str
        Pipeline where the issue was detected.
    column_name : str
        Affected column, or ``""`` for table-level issues.
    alert_type : AlertType
        Category of the issue.
    severity : AlertSeverity
        How severe the issue is.
    score : float
        Numeric metric from the detector (e.g. PSI value, null-rate delta).
    details : str
        Human-readable explanation.
    timestamp : datetime
        When the alert was created (defaults to UTC now).
    acknowledged : bool
        Whether a human has acknowledged this alert.
    notes : str
        Free-text notes added by a human reviewer.
    """

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_name: str = ""
    column_name: str = ""
    alert_type: AlertType = AlertType.SCHEMA_DRIFT
    severity: AlertSeverity = AlertSeverity.WARNING
    score: float = 0.0
    details: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    acknowledged: bool = False
    notes: str = ""

    # ── Serialisation ───────────────────────────────────────────────────

    def to_dict(self) -> dict:
        """Serialise the alert to a plain dictionary (JSON-friendly)."""
        return {
            "id": self.id,
            "pipeline_name": self.pipeline_name,
            "column_name": self.column_name,
            "alert_type": self.alert_type.value,
            "severity": self.severity.value,
            "score": self.score,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
            "acknowledged": self.acknowledged,
            "notes": self.notes,
        }

    # ── Factory ─────────────────────────────────────────────────────────

    @classmethod
    def from_detection_result(
        cls,
        pipeline_name: str,
        result: "DetectionResult",
    ) -> "Alert":
        """Create an :class:`Alert` from a :class:`DetectionResult`.

        Parameters
        ----------
        pipeline_name:
            Name of the pipeline being monitored.
        result:
            A detection result produced by one of the built-in detectors.

        Returns
        -------
        Alert
            A fully-populated alert instance.
        """
        # Avoid circular import at module level.
        from datawatch.detectors.base import Severity as DetSeverity

        # Map DetectionResult.severity → AlertSeverity.
        severity_map = {
            DetSeverity.HEALTHY: AlertSeverity.HEALTHY,
            DetSeverity.WARNING: AlertSeverity.WARNING,
            DetSeverity.CRITICAL: AlertSeverity.CRITICAL,
        }

        return cls(
            pipeline_name=pipeline_name,
            column_name=result.column_name,
            alert_type=_DETECTOR_TO_ALERT_TYPE.get(
                result.detector_type, AlertType.SCHEMA_DRIFT
            ),
            severity=severity_map.get(result.severity, AlertSeverity.WARNING),
            score=result.score,
            details=result.details,
            timestamp=result.timestamp,
        )
