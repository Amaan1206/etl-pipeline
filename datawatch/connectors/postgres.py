"""
datawatch.connectors.postgres
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Read-only connector for PostgreSQL databases via SQLAlchemy + psycopg2.

The connector is strictly **read-only** — it will never write, update,
insert, or delete data.  A hard row-limit of 100 000 is enforced on
every fetch to prevent accidental memory exhaustion.
"""

import logging
from typing import Dict, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from datawatch.connectors.base import BaseConnector

logger = logging.getLogger(__name__)

# Absolute ceiling on rows returned by a single fetch call.
_MAX_ROWS = 100_000


class PostgresConnector(BaseConnector):
    """Connect to a PostgreSQL database and read data.

    Parameters
    ----------
    connection_string : str
        SQLAlchemy connection URL, e.g.
        ``postgresql://user:password@host:port/database``.
    name : str
        Human-readable label for this connector instance.
    """

    def __init__(
        self,
        connection_string: str,
        name: str = "postgres",
    ) -> None:
        super().__init__(name=name)
        self._connection_string = connection_string
        self._engine: Optional[Engine] = None

    # ── Properties ──────────────────────────────────────────────────────

    @property
    def source_type(self) -> str:
        """Return ``'postgres'``."""
        return "postgres"

    # ── Internal helpers ────────────────────────────────────────────────

    def _get_engine(self) -> Engine:
        """Lazily create and cache the SQLAlchemy engine."""
        if self._engine is None:
            self._engine = create_engine(
                self._connection_string,
                pool_pre_ping=True,
            )
        return self._engine

    # ── Public API ──────────────────────────────────────────────────────

    def fetch(
        self,
        table_or_query: str,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Execute a read-only SELECT and return the result as a DataFrame.

        Parameters
        ----------
        table_or_query:
            Either a bare table name (e.g. ``"users"``) or a full
            ``SELECT`` statement.
        limit:
            Row cap for this call.  Clamped to the hard maximum of
            100 000.

        Returns
        -------
        pd.DataFrame
            Query results.  Returns an empty DataFrame on error.
        """
        effective_limit = min(limit or _MAX_ROWS, _MAX_ROWS)

        # Build the SQL statement.
        stripped = table_or_query.strip()
        if stripped.upper().startswith("SELECT"):
            sql = f"SELECT * FROM ({stripped}) AS _dw_sub LIMIT {effective_limit}"
        else:
            sql = f'SELECT * FROM "{stripped}" LIMIT {effective_limit}'

        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                df = pd.read_sql(text(sql), conn)
            logger.info("Fetched %d rows from Postgres.", len(df))
            return df
        except Exception as exc:
            logger.error("Postgres fetch failed: %s", exc)
            return pd.DataFrame()

    def test_connection(self) -> bool:
        """Run ``SELECT 1`` to verify connectivity.

        Returns
        -------
        bool
            ``True`` if the query succeeds.
        """
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Postgres connection test passed.")
            return True
        except Exception as exc:
            logger.error("Postgres connection test failed: %s", exc)
            return False

    def get_schema(self, table: str) -> Dict[str, str]:
        """Query ``information_schema.columns`` for *table* structure.

        Parameters
        ----------
        table:
            Table name to inspect.

        Returns
        -------
        dict[str, str]
            ``{column_name: data_type}``.
        """
        sql = text(
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_name = :tbl "
            "ORDER BY ordinal_position"
        )
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                rows = conn.execute(sql, {"tbl": table}).fetchall()
            schema = {row[0]: row[1] for row in rows}
            if not schema:
                logger.warning("No columns found for table '%s'.", table)
            return schema
        except Exception as exc:
            logger.error("get_schema failed for '%s': %s", table, exc)
            return {}

    def close(self) -> None:
        """Dispose the SQLAlchemy engine and release connections."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            logger.debug("Postgres engine disposed.")
