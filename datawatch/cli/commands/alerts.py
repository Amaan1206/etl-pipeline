"""CLI subcommands for listing, inspecting, and clearing alerts."""

import typer
from rich.markup import escape
from rich.panel import Panel
from rich.table import Table

from datawatch.cli.output import console, print_alert, print_error, print_info, print_success, print_warning
from datawatch.storage.alert_repo import AlertRepository
from datawatch.storage.database import Database

app = typer.Typer(help="Inspect and manage alerts.", no_args_is_help=True)


def _severity_style(severity: str) -> str:
    """Map severity to a Rich table style name."""
    normalized = str(severity or "").upper()
    if normalized == "CRITICAL":
        return "red"
    if normalized == "WARNING":
        return "yellow"
    if normalized == "HEALTHY":
        return "green"
    return "white"


@app.command("list", help="Show recent alerts in a table.")
def list_alerts(
    limit: int = typer.Option(
        50,
        "--limit",
        min=1,
        max=1000,
        help="Maximum number of recent alerts to display.",
    ),
    output_format: str = typer.Option(
        "table",
        "--format",
        help="Output format: table or json.",
    ),
) -> None:
    """List recent alerts stored in the Datawatch database."""
    try:
        import json

        normalized_format = str(output_format).strip().lower()
        if normalized_format not in ("table", "json"):
            print_error("Invalid format '{0}'. Choose 'table' or 'json'.".format(output_format))
            raise typer.Exit(code=1)

        repo = AlertRepository(Database())
        alerts = repo.get_all(limit=limit)

        if normalized_format == "json":
            payload = [alert.to_dict() for alert in alerts]
            typer.echo(json.dumps(payload, indent=2))
            return

        if not alerts:
            print_warning("No alerts found.")
            return

        table = Table(title="Recent Alerts", show_lines=False)
        table.add_column("ID", style="cyan", no_wrap=False, min_width=36)
        table.add_column("Time", style="white", no_wrap=False, min_width=20)
        table.add_column("Pipeline", style="white", no_wrap=False, min_width=15)
        table.add_column("Column", style="white", no_wrap=False, min_width=15)
        table.add_column("Type", style="white", no_wrap=False, min_width=20)
        table.add_column("Severity", no_wrap=False, min_width=10)
        table.add_column("Score", justify="right", style="white", no_wrap=False, min_width=8)
        table.add_column("Ack", justify="center", style="white", no_wrap=False, min_width=5)

        for alert in alerts:
            severity = alert.severity.value
            table.add_row(
                alert.id,
                alert.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                alert.pipeline_name,
                alert.column_name or "-",
                alert.alert_type.value,
                "[{0}]{1}[/{0}]".format(_severity_style(severity), severity),
                "{0:.4f}".format(float(alert.score)),
                "yes" if alert.acknowledged else "no",
            )

        console.print(table)
        print_info("Displayed {0} alert(s).".format(len(alerts)))

    except Exception as exc:
        print_error("Failed to list alerts: {0}".format(exc))
        raise typer.Exit(code=1)


@app.command("inspect", help="Show full details for one alert ID.")
def inspect_alert(
    alert_id: str = typer.Argument(..., help="Unique alert ID to inspect."),
) -> None:
    """Show all fields for a single alert by ID."""
    try:
        repo = AlertRepository(Database())
        alert = repo.get_by_id(alert_id)

        if alert is None:
            print_error("Alert not found: {0}".format(alert_id))
            raise typer.Exit(code=1)

        print_alert(alert)

        details = (
            "[bold]ID:[/bold] {0}\n"
            "[bold]Pipeline:[/bold] {1}\n"
            "[bold]Column:[/bold] {2}\n"
            "[bold]Type:[/bold] {3}\n"
            "[bold]Severity:[/bold] {4}\n"
            "[bold]Score:[/bold] {5:.4f}\n"
            "[bold]Timestamp:[/bold] {6}\n"
            "[bold]Acknowledged:[/bold] {7}\n"
            "[bold]Notes:[/bold] {8}\n\n"
            "[bold]Details:[/bold]\n{9}"
        ).format(
            escape(alert.id),
            escape(alert.pipeline_name),
            escape(alert.column_name or "-"),
            escape(alert.alert_type.value),
            escape(alert.severity.value),
            float(alert.score),
            escape(alert.timestamp.strftime("%Y-%m-%d %H:%M:%S")),
            escape("yes" if alert.acknowledged else "no"),
            escape(alert.notes or "-"),
            escape(alert.details or "-"),
        )

        console.print(Panel(details, title="Alert Detail", border_style="blue"))

    except typer.Exit:
        raise
    except Exception as exc:
        print_error("Failed to inspect alert '{0}': {1}".format(alert_id, exc))
        raise typer.Exit(code=1)


@app.command("clear", help="Delete all alerts after confirmation.")
def clear_alerts() -> None:
    """Delete all stored alerts after a confirmation prompt."""
    try:
        should_clear = typer.confirm("Delete all alerts from the local database?", default=False)
        if not should_clear:
            print_info("Alert clear cancelled.")
            return

        db = Database()
        with db.get_connection() as conn:
            row = conn.execute("SELECT COUNT(*) AS cnt FROM alerts").fetchone()
            total = int(row["cnt"] or 0)
            conn.execute("DELETE FROM alerts")
            conn.commit()

        print_success("Deleted {0} alert(s).".format(total))

    except Exception as exc:
        print_error("Failed to clear alerts: {0}".format(exc))
        raise typer.Exit(code=1)
