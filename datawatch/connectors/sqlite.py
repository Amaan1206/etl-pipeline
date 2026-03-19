"""
datawatch.connectors.sqlite
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Read-only connector for SQLite database files via SQLAlchemy.

Useful for local development and testing without an external database
server.  Like all connectors, it is strictly **read-only**.
"""

import logging
from pathlib import Path
from typing import Dict, Optional, Union

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from datawatch.connectors.base import BaseConnector

logger = logging.getLogger(__name__)

# Hard ceiling on rows per fetch — matches Postgres connector behaviour.
_MAX_ROWS = 100_000


class SQLiteConnector(BaseConnector):
    """Connect to a local SQLite ``.db`` file and read data.

    Parameters
    ----------
    db_path : str | Path
        Path to the SQLite database file.  The file must already exist.
    name : str
        Human-readable label for this connector instance.
    """

    def __init__(
        self,
        db_path: Union[str, Path],
        name: str = "sqlite",
    ) -> None:
        super().__init__(name=name)
        self._is_memory = str(db_path).strip() == ":memory:"
        self._db_path = (
            Path(":memory:") if self._is_memory
            else Path(db_path).expanduser().resolve()
        )
        self._engine: Optional[Engine] = None

    # ── Properties ──────────────────────────────────────────────────────

    @property
    def source_type(self) -> str:
        """Return ``'sqlite'``."""
        return "sqlite"

    # ── Internal helpers ────────────────────────────────────────────────

    def _get_engine(self) -> Engine:
        """Lazily create and cache the SQLAlchemy engine."""
        if self._engine is None:
            url = "sqlite://" if self._is_memory else f"sqlite:///{self._db_path}"
            self._engine = create_engine(url)
        return self._engine

    # ── Public API ──────────────────────────────────────────────────────

    def fetch(
        self,
        table_or_query: str,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Execute a read-only query and return the result as a DataFrame.

        Parameters
        ----------
        table_or_query:
            Either a bare table name or a full ``SELECT`` statement.
        limit:
            Row cap for this call.  Clamped to 100 000.

        Returns
        -------
        pd.DataFrame
            Query results.  Returns an empty DataFrame on error.
        """
        effective_limit = min(limit or _MAX_ROWS, _MAX_ROWS)

        stripped = table_or_query.strip()
        if stripped.upper().startswith("SELECT"):
            sql = f"SELECT * FROM ({stripped}) LIMIT {effective_limit}"
        else:
            sql = f'SELECT * FROM "{stripped}" LIMIT {effective_limit}'

        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                df = pd.read_sql(text(sql), conn)
            logger.info("Fetched %d rows from SQLite.", len(df))
            return df
        except Exception as exc:
            logger.error("SQLite fetch failed: %s", exc)
            return pd.DataFrame()

    def test_connection(self) -> bool:
        """Verify the database file exists and is queryable.

        Returns
        -------
        bool
            ``True`` if ``SELECT 1`` succeeds.
        """
        if not self._is_memory and not self._db_path.exists():
            logger.error("SQLite file does not exist: '%s'.", self._db_path)
            return False

        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("SQLite connection test passed.")
            return True
        except Exception as exc:
            logger.error("SQLite connection test failed: %s", exc)
            return False

    def get_schema(self, table: str) -> Dict[str, str]:
        """Use ``PRAGMA table_info`` to retrieve column names and types.

        Parameters
        ----------
        table:
            Table name to inspect.

        Returns
        -------
        dict[str, str]
            ``{column_name: declared_type}``.
        """
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                rows = conn.execute(
                    text(f"PRAGMA table_info(\"{table}\")")
                ).fetchall()
            # PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
            schema = {row[1]: row[2] for row in rows}
            if not schema:
                logger.warning("No columns found for table '%s'.", table)
            return schema
        except Exception as exc:
            logger.error("get_schema failed for '%s': %s", table, exc)
            return {}

    def close(self) -> None:
        """Dispose the SQLAlchemy engine."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            logger.debug("SQLite engine disposed.")
