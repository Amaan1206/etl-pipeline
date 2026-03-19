"""Connectors sub-package — read-only data source connectors."""

from datawatch.connectors.base import BaseConnector
from datawatch.connectors.csv_connector import CSVConnector
from datawatch.connectors.postgres import PostgresConnector
from datawatch.connectors.sqlite import SQLiteConnector

__all__ = [
    "BaseConnector",
    "CSVConnector",
    "PostgresConnector",
    "SQLiteConnector",
]
