"""CLI command for showing pipeline and alert status."""

import typer
from rich.table import Table

from datawatch.cli.output import console, print_error, print_info, print_warning
from datawatch.storage.database import Database


def status_command() -> None:
    """Show configured pipelines, last check times, and alert counts."""
    try:
        db = Database()

        query = (
            "SELECT "
            "  p.name AS pipeline_name, "
            "  p.source_type AS source_type, "
            "  lm.last_run_at AS last_run_at, "
            "  COALESCE(ac.total_alerts, 0) AS total_alerts, "
            "  COALESCE(ac.critical_alerts, 0) AS critical_alerts, "
            "  COALESCE(ac.warning_alerts, 0) AS warning_alerts "
            "FROM pipelines p "
            "LEFT JOIN ("
            "  SELECT pipeline_id, MAX(last_run_at) AS last_run_at "
            "  FROM monitors "
            "  GROUP BY pipeline_id"
            ") lm ON lm.pipeline_id = p.id "
            "LEFT JOIN ("
            "  SELECT "
            "    pipeline_name, "
            "    COUNT(*) AS total_alerts, "
            "    SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_alerts, "
            "    SUM(CASE WHEN severity = 'WARNING' THEN 1 ELSE 0 END) AS warning_alerts "
            "  FROM alerts "
            "  GROUP BY pipeline_name"
            ") ac ON ac.pipeline_name = p.name "
            "ORDER BY p.name"
        )

        with db.get_connection() as conn:
            rows = conn.execute(query).fetchall()

        if not rows:
            print_warning("No pipelines configured. Run 'datawatch connect' first.")
            return

        table = Table(title="Datawatch Pipeline Status", show_lines=False)
        table.add_column("Pipeline", style="cyan", no_wrap=True)
        table.add_column("Source", style="white", no_wrap=True)
        table.add_column("Last Check", style="white")
        table.add_column("Total Alerts", justify="right", style="white")
        table.add_column("Critical", justify="right", style="red")
        table.add_column("Warning", justify="right", style="yellow")

        for row in rows:
            table.add_row(
                str(row["pipeline_name"]),
                str(row["source_type"]),
                str(row["last_run_at"] or "Never"),
                str(int(row["total_alerts"] or 0)),
                str(int(row["critical_alerts"] or 0)),
                str(int(row["warning_alerts"] or 0)),
            )

        console.print(table)
        print_info("Status query completed for {0} pipeline(s).".format(len(rows)))

    except Exception as exc:
        print_error("Failed to load status: {0}".format(exc))
        raise typer.Exit(code=1)
