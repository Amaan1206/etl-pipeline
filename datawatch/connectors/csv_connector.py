"""
datawatch.connectors.csv_connector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Read-only connector for CSV files (single file or directory of CSVs).

Features:
- Automatic dtype inference via pandas.
- Supports a single ``.csv`` file or a directory containing multiple CSVs
  (which are concatenated into one DataFrame).
- Gracefully handles missing files, empty files, and malformed CSVs.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd

from datawatch.connectors.base import BaseConnector

logger = logging.getLogger(__name__)


class CSVConnector(BaseConnector):
    """Read CSV files into DataFrames.

    Parameters
    ----------
    path : str | Path
        Path to a single ``.csv`` file **or** a directory containing
        one or more ``.csv`` files.
    name : str
        Human-readable label for this connector instance.
    encoding : str
        File encoding.  Default: ``"utf-8"``.
    """

    def __init__(
        self,
        path: Union[str, Path],
        name: str = "csv",
        encoding: str = "utf-8",
    ) -> None:
        super().__init__(name=name)
        self._path = Path(path).expanduser().resolve()
        self._encoding = encoding

    # ── Properties ──────────────────────────────────────────────────────

    @property
    def source_type(self) -> str:
        """Return ``'csv'``."""
        return "csv"

    # ── Helpers ─────────────────────────────────────────────────────────

    def _resolve_files(self, table_or_query: Optional[str] = None) -> List[Path]:
        """Return a list of CSV file paths to read.

        If the connector was initialised with a directory, all ``.csv``
        files within that directory are returned.  If *table_or_query*
        is given and points to a specific file, only that file is used.
        """
        target = Path(table_or_query).expanduser().resolve() if table_or_query else self._path

        if target.is_file():
            return [target]

        if target.is_dir():
            csv_files = sorted(target.glob("*.csv"))
            if not csv_files:
                logger.warning("No .csv files found in directory '%s'.", target)
            return csv_files

        # Fall back to the connector-level path.
        if self._path.is_file():
            return [self._path]
        if self._path.is_dir():
            return sorted(self._path.glob("*.csv"))

        return []

    # ── Public API ──────────────────────────────────────────────────────

    def fetch(
        self,
        table_or_query: str = "",
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Read CSV file(s) into a single DataFrame.

        Parameters
        ----------
        table_or_query:
            Optional path override.  If empty, the connector uses the
            path provided at construction time.
        limit:
            Maximum rows to return.

        Returns
        -------
        pd.DataFrame
            Concatenated DataFrame from all resolved CSV files.
            Returns an empty DataFrame on any read error.
        """
        files = self._resolve_files(table_or_query or None)

        if not files:
            logger.error("No CSV files resolved for path '%s'.", self._path)
            return pd.DataFrame()

        frames: List[pd.DataFrame] = []
        for fp in files:
            try:
                df = pd.read_csv(fp, encoding=self._encoding)
                frames.append(df)
                logger.debug("Read %d rows from '%s'.", len(df), fp)
            except pd.errors.EmptyDataError:
                logger.warning("File '%s' is empty — skipping.", fp)
            except pd.errors.ParserError as exc:
                logger.error("Malformed CSV '%s': %s — skipping.", fp, exc)
            except Exception as exc:
                logger.error("Unexpected error reading '%s': %s", fp, exc)

        if not frames:
            return pd.DataFrame()

        result = pd.concat(frames, ignore_index=True)

        if limit is not None and limit > 0:
            result = result.head(limit)

        return result

    def test_connection(self) -> bool:
        """Check that the configured path exists and is readable.

        Returns
        -------
        bool
            ``True`` if at least one CSV file can be identified.
        """
        try:
            files = self._resolve_files()
            if not files:
                logger.error("test_connection: no CSV files at '%s'.", self._path)
                return False
            # Try opening the first file to confirm readability.
            with open(files[0], "r", encoding=self._encoding) as fh:
                fh.readline()
            return True
        except Exception as exc:
            logger.error("test_connection failed: %s", exc)
            return False

    def get_schema(self, table: str = "") -> Dict[str, str]:
        """Return column names and inferred dtypes for the CSV(s).

        Parameters
        ----------
        table:
            Optional path override (same semantics as ``fetch``).

        Returns
        -------
        dict[str, str]
            ``{column_name: pandas_dtype_string}``.
        """
        try:
            # Read just the first few rows for schema inference.
            df = self.fetch(table_or_query=table, limit=5)
            return {col: str(dtype) for col, dtype in df.dtypes.items()}
        except Exception as exc:
            logger.error("get_schema failed: %s", exc)
            return {}
