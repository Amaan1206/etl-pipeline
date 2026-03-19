"""
datawatch.storage.database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Core SQLite database manager.

The database file lives at ``~/.datawatch/datawatch.db`` by default.
The parent directory is created automatically if it does not exist.
All operations use Python's built-in :mod:`sqlite3` — no ORM required.
"""

import logging
import sqlite3
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Default database location.
_DEFAULT_DB_DIR = Path.home() / ".datawatch"
_DEFAULT_DB_PATH = _DEFAULT_DB_DIR / "datawatch.db"

# ── SQL schema ──────────────────────────────────────────────────────────────

_CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS pipelines (
    id                TEXT PRIMARY KEY,
    name              TEXT NOT NULL UNIQUE,
    source_type       TEXT NOT NULL DEFAULT '',
    connection_string TEXT NOT NULL DEFAULT '',
    created_at        TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS monitors (
    id               TEXT PRIMARY KEY,
    pipeline_id      TEXT NOT NULL,
    table_name       TEXT NOT NULL DEFAULT '',
    interval_seconds INTEGER NOT NULL DEFAULT 1800,
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    last_run_at      TEXT,
    is_active        INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(id)
);

CREATE TABLE IF NOT EXISTS baselines (
    id           TEXT PRIMARY KEY,
    pipeline_id  TEXT NOT NULL,
    column_name  TEXT NOT NULL DEFAULT '',
    stats_json   TEXT NOT NULL DEFAULT '{}',
    captured_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS alerts (
    id            TEXT PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    column_name   TEXT NOT NULL DEFAULT '',
    alert_type    TEXT NOT NULL DEFAULT '',
    severity      TEXT NOT NULL DEFAULT 'WARNING',
    score         REAL NOT NULL DEFAULT 0.0,
    details       TEXT NOT NULL DEFAULT '',
    timestamp     TEXT NOT NULL DEFAULT (datetime('now')),
    acknowledged  INTEGER NOT NULL DEFAULT 0,
    notes         TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_baselines_pipeline ON baselines(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_alerts_pipeline     ON alerts(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_alerts_severity     ON alerts(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_timestamp    ON alerts(timestamp);
CREATE INDEX IF NOT EXISTS idx_monitors_pipeline   ON monitors(pipeline_id);
"""


class Database:
    """Manage the local SQLite database used by Datawatch.

    Parameters
    ----------
    db_path : str | Path | None
        Override the default database file location.  Useful for testing.
        If ``None``, defaults to ``~/.datawatch/datawatch.db``.
    """

    def __init__(self, db_path: Optional[str] = None) -> None:
        self._db_path = Path(db_path).expanduser().resolve() if db_path else _DEFAULT_DB_PATH

        # Ensure the parent directory exists.
        try:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            logger.error("Failed to create database directory: %s", exc)

        # Auto-create tables on first use.
        self.initialize()

    # ── Properties ──────────────────────────────────────────────────────

    @property
    def path(self) -> Path:
        """Return the resolved database file path."""
        return self._db_path

    # ── Connection ──────────────────────────────────────────────────────

    def get_connection(self) -> sqlite3.Connection:
        """Open a connection to the SQLite database.

        The connection has ``row_factory`` set to :class:`sqlite3.Row`
        so rows behave like dictionaries, and WAL journal mode for
        improved concurrent read performance.

        Returns
        -------
        sqlite3.Connection
        """
        conn = sqlite3.connect(str(self._db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    # ── Initialisation ──────────────────────────────────────────────────

    def initialize(self) -> None:
        """Create all required tables if they do not already exist.

        Safe to call multiple times — all statements use
        ``CREATE TABLE IF NOT EXISTS``.
        """
        try:
            with self.get_connection() as conn:
                conn.executescript(_CREATE_TABLES_SQL)
                conn.commit()
            logger.info("Database initialised at '%s'.", self._db_path)
        except sqlite3.Error as exc:
            logger.error("Database initialisation failed: %s", exc)

    # ── Health check ────────────────────────────────────────────────────

    def health_check(self) -> bool:
        """Run a trivial query to verify database connectivity.

        Returns
        -------
        bool
            ``True`` if ``SELECT 1`` succeeds.
        """
        try:
            with self.get_connection() as conn:
                conn.execute("SELECT 1")
            return True
        except Exception as exc:
            logger.error("Database health check failed: %s", exc)
            return False
