"""
datawatch.storage.alert_repo
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Repository for persisting and querying :class:`Alert` records in the
local SQLite database.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from datawatch.alerts.alert import Alert, AlertSeverity, AlertType
from datawatch.storage.database import Database

logger = logging.getLogger(__name__)


class AlertRepository:
    """CRUD operations for :class:`Alert` objects in SQLite.

    Parameters
    ----------
    db : Database
        An initialised :class:`Database` instance.
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    # ── Helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _row_to_alert(row) -> Alert:
        """Convert a :class:`sqlite3.Row` to an :class:`Alert`."""
        return Alert(
            id=row["id"],
            pipeline_name=row["pipeline_name"],
            column_name=row["column_name"],
            alert_type=AlertType(row["alert_type"]),
            severity=AlertSeverity(row["severity"]),
            score=float(row["score"]),
            details=row["details"],
            timestamp=datetime.fromisoformat(row["timestamp"]),
            acknowledged=bool(row["acknowledged"]),
            notes=row["notes"],
        )

    # ── Public API ──────────────────────────────────────────────────────

    def save(self, alert: Alert) -> None:
        """Persist an alert to the database.

        Parameters
        ----------
        alert:
            The alert to save.
        """
        try:
            with self._db.get_connection() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO alerts "
                    "(id, pipeline_name, column_name, alert_type, severity, "
                    " score, details, timestamp, acknowledged, notes) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        alert.id,
                        alert.pipeline_name,
                        alert.column_name,
                        alert.alert_type.value,
                        alert.severity.value,
                        alert.score,
                        alert.details,
                        alert.timestamp.isoformat(),
                        int(alert.acknowledged),
                        alert.notes,
                    ),
                )
                conn.commit()
            logger.debug("Alert %s saved.", alert.id)
        except Exception as exc:
            logger.error("Failed to save alert %s: %s", alert.id, exc)

    def get_all(self, limit: int = 100, pipeline_name: Optional[str] = None) -> List[Alert]:
        """Return the most recent alerts, newest first.

        Parameters
        ----------
        limit:
            Maximum number of alerts to return.
        pipeline_name:
            Optional pipeline filter.

        Returns
        -------
        list[Alert]
        """
        try:
            with self._db.get_connection() as conn:
                if pipeline_name:
                    rows = conn.execute(
                        "SELECT * FROM alerts WHERE pipeline_name = ? "
                        "ORDER BY timestamp DESC LIMIT ?",
                        (pipeline_name, limit),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT * FROM alerts ORDER BY timestamp DESC LIMIT ?",
                        (limit,),
                    ).fetchall()
            return [self._row_to_alert(r) for r in rows]
        except Exception as exc:
            logger.error("Failed to fetch alerts: %s", exc)
            return []

    def get_by_id(self, alert_id: str) -> Optional[Alert]:
        """Retrieve a single alert by its ID.

        Parameters
        ----------
        alert_id:
            UUID of the alert.

        Returns
        -------
        Alert | None
            The alert, or ``None`` if not found.
        """
        try:
            with self._db.get_connection() as conn:
                row = conn.execute(
                    "SELECT * FROM alerts WHERE id = ?",
                    (alert_id,),
                ).fetchone()
            if row is None:
                return None
            return self._row_to_alert(row)
        except Exception as exc:
            logger.error("Failed to fetch alert %s: %s", alert_id, exc)
            return None

    def get_by_pipeline(self, pipeline_name: str) -> List[Alert]:
        """Return all alerts for a specific pipeline, newest first.

        Parameters
        ----------
        pipeline_name:
            Pipeline identifier.

        Returns
        -------
        list[Alert]
        """
        try:
            with self._db.get_connection() as conn:
                rows = conn.execute(
                    "SELECT * FROM alerts WHERE pipeline_name = ? "
                    "ORDER BY timestamp DESC",
                    (pipeline_name,),
                ).fetchall()
            return [self._row_to_alert(r) for r in rows]
        except Exception as exc:
            logger.error("Failed to fetch alerts for '%s': %s", pipeline_name, exc)
            return []

    def acknowledge(self, alert_id: str, notes: str = "") -> bool:
        """Mark an alert as acknowledged.

        Parameters
        ----------
        alert_id:
            UUID of the alert to acknowledge.
        notes:
            Optional notes from the reviewer.

        Returns
        -------
        bool
            ``True`` if the alert was found and updated.
        """
        try:
            with self._db.get_connection() as conn:
                cursor = conn.execute(
                    "UPDATE alerts SET acknowledged = 1, notes = ? WHERE id = ?",
                    (notes, alert_id),
                )
                conn.commit()
            if cursor.rowcount > 0:
                logger.info("Alert %s acknowledged.", alert_id)
                return True
            logger.warning("Alert %s not found for acknowledgement.", alert_id)
            return False
        except Exception as exc:
            logger.error("Failed to acknowledge alert %s: %s", alert_id, exc)
            return False

    def get_stats(self, pipeline_name: Optional[str] = None) -> Dict[str, int]:
        """Return aggregate alert statistics.

        Returns
        -------
        dict
            Keys: ``total``, ``critical``, ``warning``, ``last_24h``.
        """
        stats: Dict[str, int] = {
            "total": 0,
            "critical": 0,
            "warning": 0,
            "last_24h": 0,
        }
        cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()

        try:
            with self._db.get_connection() as conn:
                if pipeline_name:
                    row = conn.execute(
                        "SELECT COUNT(*) AS cnt FROM alerts WHERE pipeline_name = ?",
                        (pipeline_name,),
                    ).fetchone()
                    stats["total"] = row["cnt"]

                    row = conn.execute(
                        "SELECT COUNT(*) AS cnt FROM alerts "
                        "WHERE pipeline_name = ? AND severity = ?",
                        (pipeline_name, "CRITICAL"),
                    ).fetchone()
                    stats["critical"] = row["cnt"]

                    row = conn.execute(
                        "SELECT COUNT(*) AS cnt FROM alerts "
                        "WHERE pipeline_name = ? AND severity = ?",
                        (pipeline_name, "WARNING"),
                    ).fetchone()
                    stats["warning"] = row["cnt"]

                    row = conn.execute(
                        "SELECT COUNT(*) AS cnt FROM alerts "
                        "WHERE pipeline_name = ? AND timestamp >= ?",
                        (pipeline_name, cutoff),
                    ).fetchone()
                    stats["last_24h"] = row["cnt"]
                else:
                    row = conn.execute("SELECT COUNT(*) AS cnt FROM alerts").fetchone()
                    stats["total"] = row["cnt"]

                    row = conn.execute(
                        "SELECT COUNT(*) AS cnt FROM alerts WHERE severity = ?",
                        ("CRITICAL",),
                    ).fetchone()
                    stats["critical"] = row["cnt"]

                    row = conn.execute(
                        "SELECT COUNT(*) AS cnt FROM alerts WHERE severity = ?",
                        ("WARNING",),
                    ).fetchone()
                    stats["warning"] = row["cnt"]

                    row = conn.execute(
                        "SELECT COUNT(*) AS cnt FROM alerts WHERE timestamp >= ?",
                        (cutoff,),
                    ).fetchone()
                    stats["last_24h"] = row["cnt"]

            return stats
        except Exception as exc:
            logger.error("Failed to compute alert stats: %s", exc)
            return stats
