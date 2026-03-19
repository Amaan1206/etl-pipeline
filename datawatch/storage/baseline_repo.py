"""
datawatch.storage.baseline_repo
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Repository for storing and retrieving per-column baseline statistics.

Baseline stats are computed from a reference DataFrame and persisted as
JSON in the ``baselines`` table.  They are later used by detectors to
compare against incoming data batches.
"""

import json
import logging
import uuid
from typing import Any, Dict, List, Optional

import pandas as pd

from datawatch.storage.database import Database

logger = logging.getLogger(__name__)


def _compute_column_stats(series: pd.Series) -> Dict[str, Any]:
    """Compute summary statistics for a single DataFrame column.

    For numeric columns: mean, std, min, max, null_rate.
    For categorical / object columns: null_rate, dtype, value_counts (top 50).

    Parameters
    ----------
    series:
        A single pandas Series.

    Returns
    -------
    dict
        JSON-serialisable statistics dictionary.
    """
    total = len(series)
    null_count = int(series.isna().sum())
    null_rate = null_count / total if total > 0 else 0.0

    stats: Dict[str, Any] = {
        "dtype": str(series.dtype),
        "count": total,
        "null_count": null_count,
        "null_rate": round(null_rate, 6),
    }

    if pd.api.types.is_numeric_dtype(series):
        clean = series.dropna()
        if len(clean) > 0:
            stats["mean"] = round(float(clean.mean()), 6)
            stats["std"] = round(float(clean.std()), 6)
            stats["min"] = float(clean.min())
            stats["max"] = float(clean.max())
        else:
            stats["mean"] = None
            stats["std"] = None
            stats["min"] = None
            stats["max"] = None
    else:
        # Categorical / object — store top value counts.
        vc = series.dropna().value_counts().head(50)
        stats["value_counts"] = {str(k): int(v) for k, v in vc.items()}

    return stats


class BaselineRepository:
    """Persist and retrieve per-column baseline statistics.

    Parameters
    ----------
    db : Database
        An initialised :class:`Database` instance.
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    # ── Public API ──────────────────────────────────────────────────────

    def save(self, pipeline_name: str, df: pd.DataFrame) -> None:
        """Compute per-column stats from *df* and store them.

        Any existing baseline for the same *pipeline_name* is replaced.

        Parameters
        ----------
        pipeline_name:
            Name of the pipeline this baseline belongs to.
        df:
            Reference DataFrame from which statistics are computed.
        """
        # Remove old baseline first.
        self.delete(pipeline_name)

        try:
            with self._db.get_connection() as conn:
                for col in df.columns:
                    col_stats = _compute_column_stats(df[col])
                    conn.execute(
                        "INSERT INTO baselines (id, pipeline_id, column_name, stats_json) "
                        "VALUES (?, ?, ?, ?)",
                        (
                            str(uuid.uuid4()),
                            pipeline_name,
                            str(col),
                            json.dumps(col_stats),
                        ),
                    )
                conn.commit()
            logger.info(
                "Baseline saved for pipeline '%s' (%d columns).",
                pipeline_name,
                len(df.columns),
            )
        except Exception as exc:
            logger.error("Failed to save baseline for '%s': %s", pipeline_name, exc)

    def get(self, pipeline_name: str) -> Dict[str, Dict[str, Any]]:
        """Retrieve stored baseline stats for a pipeline.

        Parameters
        ----------
        pipeline_name:
            Pipeline identifier.

        Returns
        -------
        dict[str, dict]
            Mapping of ``{column_name: stats_dict}``.  Returns an empty
            dict if no baseline exists.
        """
        try:
            with self._db.get_connection() as conn:
                rows = conn.execute(
                    "SELECT column_name, stats_json FROM baselines "
                    "WHERE pipeline_id = ? ORDER BY column_name",
                    (pipeline_name,),
                ).fetchall()
            return {
                row["column_name"]: json.loads(row["stats_json"])
                for row in rows
            }
        except Exception as exc:
            logger.error("Failed to get baseline for '%s': %s", pipeline_name, exc)
            return {}

    def exists(self, pipeline_name: str) -> bool:
        """Check whether a baseline has been stored for *pipeline_name*.

        Parameters
        ----------
        pipeline_name:
            Pipeline identifier.

        Returns
        -------
        bool
        """
        try:
            with self._db.get_connection() as conn:
                row = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM baselines WHERE pipeline_id = ?",
                    (pipeline_name,),
                ).fetchone()
            return row["cnt"] > 0
        except Exception as exc:
            logger.error("Failed to check baseline existence for '%s': %s", pipeline_name, exc)
            return False

    def delete(self, pipeline_name: str) -> None:
        """Remove all baseline rows for *pipeline_name*.

        Parameters
        ----------
        pipeline_name:
            Pipeline identifier.
        """
        try:
            with self._db.get_connection() as conn:
                conn.execute(
                    "DELETE FROM baselines WHERE pipeline_id = ?",
                    (pipeline_name,),
                )
                conn.commit()
            logger.debug("Baseline deleted for pipeline '%s'.", pipeline_name)
        except Exception as exc:
            logger.error("Failed to delete baseline for '%s': %s", pipeline_name, exc)

    def update_rolling(
        self,
        pipeline_name: str,
        new_df: pd.DataFrame,
        window_days: int = 7,
    ) -> None:
        """Update stored baseline stats with a weighted rolling merge.

        If no baseline exists yet, this behaves exactly like :meth:`save`.
        Existing baseline values are weighted at ``0.7`` and new batch values
        at ``0.3`` for numeric ``mean``/``std`` and ``null_rate``.
        """
        _ = window_days

        existing_stats = self.get(pipeline_name)
        if not existing_stats:
            self.save(pipeline_name=pipeline_name, df=new_df)
            return

        incoming_stats: Dict[str, Dict[str, Any]] = {}
        for col in new_df.columns:
            incoming_stats[str(col)] = _compute_column_stats(new_df[col])

        merged_stats: Dict[str, Dict[str, Any]] = {
            key: dict(value) for key, value in existing_stats.items()
        }

        for column_name, current in incoming_stats.items():
            previous = existing_stats.get(column_name)
            if previous is None:
                merged_stats[column_name] = dict(current)
                continue

            previous_dtype = str(previous.get("dtype", ""))
            current_dtype = str(current.get("dtype", ""))

            if previous_dtype != current_dtype:
                merged_stats[column_name] = dict(current)
                continue

            merged = dict(previous)
            merged["dtype"] = previous_dtype

            try:
                previous_null_rate = float(previous.get("null_rate", 0.0) or 0.0)
            except Exception:
                previous_null_rate = 0.0
            try:
                current_null_rate = float(current.get("null_rate", 0.0) or 0.0)
            except Exception:
                current_null_rate = 0.0

            merged_null_rate = (previous_null_rate * 0.7) + (current_null_rate * 0.3)
            merged["null_rate"] = round(float(merged_null_rate), 6)

            if pd.api.types.is_numeric_dtype(new_df[column_name]):
                previous_mean = previous.get("mean")
                current_mean = current.get("mean")
                if previous_mean is not None and current_mean is not None:
                    merged["mean"] = round((float(previous_mean) * 0.7) + (float(current_mean) * 0.3), 6)
                elif current_mean is not None:
                    merged["mean"] = float(current_mean)

                previous_std = previous.get("std")
                current_std = current.get("std")
                if previous_std is not None and current_std is not None:
                    merged["std"] = round((float(previous_std) * 0.7) + (float(current_std) * 0.3), 6)
                elif current_std is not None:
                    merged["std"] = float(current_std)

            merged_stats[column_name] = merged

        try:
            self.delete(pipeline_name)
            with self._db.get_connection() as conn:
                for col_name in sorted(merged_stats.keys()):
                    conn.execute(
                        "INSERT INTO baselines (id, pipeline_id, column_name, stats_json) "
                        "VALUES (?, ?, ?, ?)",
                        (
                            str(uuid.uuid4()),
                            pipeline_name,
                            str(col_name),
                            json.dumps(merged_stats[col_name]),
                        ),
                    )
                conn.commit()
            logger.info(
                "Rolling baseline updated for pipeline '%s' (%d columns).",
                pipeline_name,
                len(merged_stats),
            )
        except Exception as exc:
            logger.error("Failed to update rolling baseline for '%s': %s", pipeline_name, exc)
