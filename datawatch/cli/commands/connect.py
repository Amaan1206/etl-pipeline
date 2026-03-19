"""CLI command for registering and testing data source connections."""

import uuid
from enum import Enum
from typing import Tuple

import typer

from datawatch.cli.output import print_error, print_info, print_success
from datawatch.storage.database import Database


class SourceType(str, Enum):
    """Supported source types for `datawatch connect`."""

    csv = "csv"
    postgres = "postgres"
    sqlite = "sqlite"


def _build_connector(source_type: SourceType, source: str, name: str):
    """Create a connector instance for the requested source type."""
    if source_type == SourceType.csv:
        from datawatch.connectors.csv_connector import CSVConnector

        return CSVConnector(path=source, name=name)

    if source_type == SourceType.postgres:
        from datawatch.connectors.postgres import PostgresConnector

        return PostgresConnector(connection_string=source, name=name)

    if source_type == SourceType.sqlite:
        from datawatch.connectors.sqlite import SQLiteConnector

        return SQLiteConnector(db_path=source, name=name)

    raise ValueError("Unsupported source type: {0}".format(source_type.value))


def _upsert_pipeline(
    db: Database,
    name: str,
    source_type: SourceType,
    source: str,
) -> Tuple[str, bool]:
    """Insert or update a pipeline configuration row in SQLite."""
    pipeline_id = str(uuid.uuid4())
    created = False

    with db.get_connection() as conn:
        existing = conn.execute(
            "SELECT id FROM pipelines WHERE name = ?",
            (name,),
        ).fetchone()

        if existing is None:
            conn.execute(
                "INSERT INTO pipelines (id, name, source_type, connection_string) "
                "VALUES (?, ?, ?, ?)",
                (pipeline_id, name, source_type.value, source),
            )
            created = True
        else:
            pipeline_id = str(existing["id"])
            conn.execute(
                "UPDATE pipelines "
                "SET source_type = ?, connection_string = ? "
                "WHERE name = ?",
                (source_type.value, source, name),
            )

        conn.commit()

    return pipeline_id, created


def connect_command(
    source: str = typer.Option(
        ...,
        "--source",
        help="Connection string or file path for the source.",
    ),
    name: str = typer.Option(
        ...,
        "--name",
        help="Pipeline name to register in Datawatch.",
    ),
    source_type: SourceType = typer.Option(
        ...,
        "--type",
        help="Source type: csv, postgres, or sqlite.",
        case_sensitive=False,
    ),
) -> None:
    """Test a source connection and save the pipeline configuration."""
    connector = None

    try:
        print_info("Testing {0} connection for pipeline '{1}'...".format(source_type.value, name))

        connector = _build_connector(source_type=source_type, source=source, name=name)
        if not connector.test_connection():
            print_error("Connection failed. Verify source details and try again.")
            raise typer.Exit(code=1)

        db = Database()
        pipeline_id, created = _upsert_pipeline(
            db=db,
            name=name,
            source_type=source_type,
            source=source,
        )

        if created:
            print_success("Pipeline '{0}' connected and saved.".format(name))
        else:
            print_success("Pipeline '{0}' connection updated.".format(name))

        print_info("Pipeline ID: {0}".format(pipeline_id))

    except typer.Exit:
        raise
    except Exception as exc:
        print_error("Could not connect pipeline '{0}': {1}".format(name, exc))
        raise typer.Exit(code=1)
    finally:
        if connector is not None:
            try:
                connector.close()
            except Exception:
                pass
