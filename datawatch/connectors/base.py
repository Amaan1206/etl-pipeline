"""
datawatch.connectors.base
~~~~~~~~~~~~~~~~~~~~~~~~~~
Abstract base class for all read-only data-source connectors.

Every connector must implement :meth:`fetch`, :meth:`test_connection`,
and :meth:`get_schema`.  Connectors are **read-only** — they never
write, update, insert, or delete data in the source system.
"""

import abc
import logging
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class BaseConnector(abc.ABC):
    """Read-only interface that every data-source connector must satisfy.

    Parameters
    ----------
    name : str
        Human-readable identifier for this connector instance
        (e.g. ``"production_pg"``).
    """

    def __init__(self, name: str) -> None:
        self.name = name

    # ── Abstract properties ─────────────────────────────────────────────

    @property
    @abc.abstractmethod
    def source_type(self) -> str:
        """Return a short label identifying the data-source type.

        Examples: ``"csv"``, ``"postgres"``, ``"sqlite"``.
        """
        ...

    # ── Abstract methods ────────────────────────────────────────────────

    @abc.abstractmethod
    def fetch(
        self,
        table_or_query: str,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Read data from the source and return it as a DataFrame.

        Parameters
        ----------
        table_or_query:
            A table name, file path, or SQL query — interpretation
            depends on the concrete connector.
        limit:
            Maximum number of rows to return.  ``None`` means no limit
            (subject to any hard caps the connector enforces).

        Returns
        -------
        pd.DataFrame
            The fetched data.  May be empty if no rows match.
        """
        ...

    @abc.abstractmethod
    def test_connection(self) -> bool:
        """Verify that the data source is reachable and readable.

        Returns
        -------
        bool
            ``True`` if the connection is healthy, ``False`` otherwise.
        """
        ...

    @abc.abstractmethod
    def get_schema(self, table: str) -> Dict[str, str]:
        """Return column names and their data types for *table*.

        Parameters
        ----------
        table:
            Table name (or file path, depending on the connector).

        Returns
        -------
        dict[str, str]
            Mapping of ``{column_name: data_type_string}``.
        """
        ...

    # ── Context-manager support ─────────────────────────────────────────

    def close(self) -> None:
        """Release any resources held by the connector.

        The default implementation is a no-op; subclasses that hold
        connections or file handles should override this.
        """

    def __enter__(self) -> "BaseConnector":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} name={self.name!r}>"
