"""CLI command for continuous pipeline monitoring and alerting."""

import os
import threading
import time
import uuid
import webbrowser
from typing import Any, Dict, List, Optional

import typer
import uvicorn
from rich.table import Table

from datawatch.alerts.alert import Alert
from datawatch.cli.output import (
    console,
    print_error,
    print_info,
    print_success,
    print_warning,
)
from datawatch.storage.alert_repo import AlertRepository
from datawatch.storage.baseline_repo import BaselineRepository
from datawatch.storage.database import Database


class _DashboardServer:
    """Manage a background Uvicorn server for the dashboard."""

    def __init__(self, host: str, port: int, open_browser: bool) -> None:
        self.host = host
        self.port = port
        self.open_browser = open_browser
        self._server: Optional[uvicorn.Server] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the server in a background daemon thread."""
        config = uvicorn.Config(
            app="datawatch.server.app:app",
            host=self.host,
            port=self.port,
            log_level="warning",
        )
        self._server = uvicorn.Server(config=config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()

        if self.open_browser:
            threading.Thread(target=self._open_browser_later, daemon=True).start()

    def _open_browser_later(self) -> None:
        """Open the dashboard URL shortly after server startup."""
        time.sleep(1.5)
        try:
            webbrowser.open("http://localhost:{0}".format(self.port))
        except Exception:
            pass

    def stop(self) -> None:
        """Signal the server to stop and wait briefly for shutdown."""
        if self._server is not None:
            self._server.should_exit = True
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=5)


def _load_pipeline(db: Database, pipeline_name: str):
    """Return a configured pipeline row by name, or None if missing."""
    with db.get_connection() as conn:
        return conn.execute(
            "SELECT id, name, source_type, connection_string "
            "FROM pipelines WHERE name = ?",
            (pipeline_name,),
        ).fetchone()


def _build_connector(source_type: str, source: str, pipeline_name: str):
    """Create a source connector for monitoring."""
    source_type = source_type.lower().strip()

    if source_type == "csv":
        from datawatch.connectors.csv_connector import CSVConnector

        return CSVConnector(path=source, name=pipeline_name)

    if source_type == "postgres":
        from datawatch.connectors.postgres import PostgresConnector

        return PostgresConnector(connection_string=source, name=pipeline_name)

    if source_type == "sqlite":
        from datawatch.connectors.sqlite import SQLiteConnector

        return SQLiteConnector(db_path=source, name=pipeline_name)

    raise ValueError("Unsupported source type '{0}'.".format(source_type))


def _ensure_monitor_row(
    db: Database,
    pipeline_id: str,
    table_name: str,
    interval_seconds: int,
) -> str:
    """Insert or update a monitor row and return its ID."""
    monitor_id = str(uuid.uuid4())

    with db.get_connection() as conn:
        existing = conn.execute(
            "SELECT id FROM monitors "
            "WHERE pipeline_id = ? AND table_name = ? "
            "ORDER BY created_at DESC LIMIT 1",
            (pipeline_id, table_name),
        ).fetchone()

        if existing is None:
            conn.execute(
                "INSERT INTO monitors "
                "(id, pipeline_id, table_name, interval_seconds, is_active) "
                "VALUES (?, ?, ?, ?, 1)",
                (monitor_id, pipeline_id, table_name, interval_seconds),
            )
        else:
            monitor_id = str(existing["id"])
            conn.execute(
                "UPDATE monitors "
                "SET interval_seconds = ?, is_active = 1 "
                "WHERE id = ?",
                (interval_seconds, monitor_id),
            )

        conn.commit()

    return monitor_id


def _update_last_run(db: Database, monitor_id: str) -> None:
    """Persist the last run timestamp for a monitor row."""
    with db.get_connection() as conn:
        conn.execute(
            "UPDATE monitors SET last_run_at = datetime('now') WHERE id = ?",
            (monitor_id,),
        )
        conn.commit()


def _build_channels(alert_slack: Optional[str], alert_email: Optional[str]) -> List[Any]:
    """Build notification channels from CLI options and environment vars."""
    channels: List[Any] = []

    if alert_slack:
        try:
            from datawatch.alerts.channels.slack import SlackChannel

            channels.append(SlackChannel(webhook_url=alert_slack))
            print_info("Slack alert channel enabled.")
        except Exception as exc:
            print_warning("Slack channel disabled: {0}".format(exc))

    if alert_email:
        smtp_host = os.getenv("DATAWATCH_SMTP_HOST", "smtp.gmail.com")
        smtp_port_raw = os.getenv("DATAWATCH_SMTP_PORT", "587")
        smtp_username = (
            os.getenv("DATAWATCH_SMTP_USERNAME")
            or os.getenv("DATAWATCH_ALERT_EMAIL_FROM")
            or ""
        )
        smtp_password = (
            os.getenv("DATAWATCH_SMTP_PASSWORD")
            or os.getenv("DATAWATCH_ALERT_EMAIL_PASSWORD")
            or ""
        )

        try:
            smtp_port = int(smtp_port_raw)
        except Exception:
            smtp_port = 587

        use_ssl = smtp_port == 465

        if not smtp_username or not smtp_password:
            print_warning(
                "Email channel requested, but SMTP credentials are missing. "
                "Set DATAWATCH_SMTP_USERNAME and DATAWATCH_SMTP_PASSWORD."
            )
        else:
            try:
                from datawatch.alerts.channels.email import EmailChannel

                channels.append(
                    EmailChannel(
                        smtp_host=smtp_host,
                        smtp_port=smtp_port,
                        username=smtp_username,
                        password=smtp_password,
                        recipient=alert_email,
                        use_ssl=use_ssl,
                    )
                )
                print_info("Email alert channel enabled.")
            except Exception as exc:
                print_warning("Email channel disabled: {0}".format(exc))

    return channels


def _send_alerts(alerts: List[Alert], channels: List[Any]) -> None:
    """Persisted alerts are delivered to all configured channels."""
    for alert in alerts:
        for channel in channels:
            try:
                channel.send(alert)
            except Exception as exc:
                print_warning(
                    "Alert delivery failed via {0}: {1}".format(
                        channel.__class__.__name__,
                        exc,
                    )
                )


def _severity_rank(value: str) -> int:
    """Return numeric severity rank for sorting."""
    if value == "CRITICAL":
        return 2
    if value == "WARNING":
        return 1
    return 0


def _summarize_detector(detector_name: str, results: List[Any]) -> Dict[str, Any]:
    """Build a one-line detector summary and collect failures."""
    failures = [result for result in results if not bool(getattr(result, "passed", False))]

    if not failures:
        return {
            "detector": detector_name,
            "healthy": True,
            "severity": "HEALTHY",
            "column": "",
            "metric_label": "",
            "metric_value": "",
            "failed_results": [],
        }

    worst = max(
        failures,
        key=lambda result: (
            _severity_rank(str(getattr(getattr(result, "severity", ""), "value", getattr(result, "severity", "")))),
            float(getattr(result, "score", 0.0)),
        ),
    )

    severity = str(getattr(getattr(worst, "severity", ""), "value", getattr(worst, "severity", "UNKNOWN"))).upper()
    column_name = str(getattr(worst, "column_name", "-"))

    metric_label = "SCORE"
    if detector_name == "distribution":
        metric_label = "PSI"
    elif detector_name == "null_rate":
        metric_label = "DELTA"
    elif detector_name == "schema_drift":
        metric_label = "CHANGES"

    if metric_label == "CHANGES":
        metric_value = str(len(failures))
    else:
        try:
            metric_value = "{0:.2f}".format(float(getattr(worst, "score", 0.0)))
        except Exception:
            metric_value = "-"

    return {
        "detector": detector_name,
        "healthy": False,
        "severity": severity,
        "column": column_name,
        "metric_label": metric_label,
        "metric_value": metric_value,
        "failed_results": failures,
    }


def _print_detector_summary(summary: Dict[str, Any]) -> None:
    """Print one detector summary line in live monitoring output."""
    detector_display = summary["detector"]
    if detector_display == "schema_drift":
        detector_display = "schema"

    if summary["healthy"]:
        console.print("[green]✓[/green] {0:<12} - HEALTHY".format(detector_display))
        return

    severity = summary["severity"]
    severity_color = "red" if severity == "CRITICAL" else "yellow"

    console.print(
        "[red]✗[/red] {0:<12} - [{1}]{2}[/{1}] - {3} - {4}={5}".format(
            detector_display,
            severity_color,
            severity,
            summary["column"],
            summary["metric_label"],
            summary["metric_value"],
        )
    )


def _print_baseline_stats(pipeline: str, baseline_df: Any, baseline_stats: Dict[str, Dict[str, Any]]) -> None:
    """Print a baseline-capture summary table to the terminal."""
    print_success(
        "Baseline captured for pipeline '{0}' ({1} rows, {2} columns).".format(
            pipeline,
            len(baseline_df.index),
            len(baseline_df.columns),
        )
    )

    table = Table(title="Baseline Stats", show_lines=False)
    table.add_column("Column", style="cyan")
    table.add_column("Type", style="white")
    table.add_column("Null Rate", justify="right", style="white")

    for column_name in sorted(baseline_stats.keys()):
        stats = baseline_stats[column_name]
        dtype = str(stats.get("dtype", "-"))
        try:
            null_rate = float(stats.get("null_rate", 0.0))
            null_rate_text = "{0:.2f}%".format(null_rate * 100.0)
        except Exception:
            null_rate_text = "-"

        table.add_row(column_name, dtype, null_rate_text)

    console.print(table)


def _sleep_interval(seconds: int) -> None:
    """Sleep in one-second steps so Ctrl+C remains responsive."""
    for _ in range(seconds):
        time.sleep(1)


def monitor_command(
    pipeline: Optional[str] = typer.Option(
        None,
        "--pipeline",
        help="Pipeline name previously configured with 'datawatch connect'.",
    ),
    table: Optional[str] = typer.Option(
        None,
        "--table",
        help="Table name, SQL query target, or file path to monitor.",
    ),
    every: int = typer.Option(
        30,
        "--every",
        min=1,
        help="Monitoring interval in minutes.",
    ),
    no_ui: bool = typer.Option(
        False,
        "--no-ui",
        help="Disable dashboard server and browser launch.",
    ),
    alert_slack: Optional[str] = typer.Option(
        None,
        "--alert-slack",
        help="Optional Slack webhook URL for alert notifications.",
    ),
    alert_email: Optional[str] = typer.Option(
        None,
        "--alert-email",
        help="Optional recipient email for alert notifications.",
    ),
    demo: bool = typer.Option(
        False,
        "--demo",
        help="Run synthetic demo mode and ignore other monitor options.",
    ),
    rolling: bool = typer.Option(
        False,
        "--rolling",
        help="Update baseline after each successful check using rolling merge.",
    ),
) -> None:
    """Run continuous monitoring for a configured pipeline."""
    if demo:
        from datawatch.cli.demo import run_demo

        run_demo()
        return

    if pipeline is None or table is None:
        print_error("Error: --pipeline and --table are required unless using --demo flag")
        raise typer.Exit(code=1)

    db: Optional[Database] = None
    connector = None
    dashboard_server: Optional[_DashboardServer] = None

    try:
        db = Database()
        pipeline_row = _load_pipeline(db=db, pipeline_name=pipeline)

        if pipeline_row is None:
            print_error("Pipeline '{0}' is not configured. Run 'datawatch connect' first.".format(pipeline))
            raise typer.Exit(code=1)

        source_type = str(pipeline_row["source_type"])
        source = str(pipeline_row["connection_string"])

        connector = _build_connector(
            source_type=source_type,
            source=source,
            pipeline_name=pipeline,
        )

        print_info("Testing source connection before monitoring starts...")
        if not connector.test_connection():
            print_error("Source connection failed. Monitoring aborted.")
            raise typer.Exit(code=1)

        interval_seconds = int(every) * 60
        monitor_id = _ensure_monitor_row(
            db=db,
            pipeline_id=str(pipeline_row["id"]),
            table_name=table,
            interval_seconds=interval_seconds,
        )

        channels = _build_channels(alert_slack=alert_slack, alert_email=alert_email)

        if not no_ui:
            dashboard_server = _DashboardServer(host="127.0.0.1", port=8080, open_browser=True)
            dashboard_server.start()
            print_info("Dashboard available at http://localhost:8080")
        else:
            print_info("Dashboard disabled (--no-ui).")

        baseline_repo = BaselineRepository(db)
        alert_repo = AlertRepository(db)

        from datawatch.detectors.distribution import DistributionDetector
        from datawatch.detectors.null_rate import NullRateDetector
        from datawatch.detectors.schema_drift import SchemaDriftDetector

        detectors = [
            SchemaDriftDetector(),
            NullRateDetector(),
            DistributionDetector(),
        ]

        runtime_baseline_df = None
        print_info("Monitoring started. Press Ctrl+C to stop.")

        while True:
            timestamp = time.strftime("%H:%M:%S")
            console.print("[bold cyan][{0}] Checking {1}...[/bold cyan]".format(timestamp, pipeline))

            try:
                current_df = connector.fetch(table_or_query=table)
            except Exception as exc:
                print_error("Data fetch failed: {0}".format(exc))
                _update_last_run(db, monitor_id)
                _sleep_interval(interval_seconds)
                continue

            if current_df is None or current_df.empty:
                print_warning("No rows returned for this check.")
                _update_last_run(db, monitor_id)
                _sleep_interval(interval_seconds)
                continue

            if runtime_baseline_df is None:
                runtime_baseline_df = current_df.copy(deep=True)
                baseline_repo.save(pipeline_name=pipeline, df=runtime_baseline_df)

                baseline_stats = baseline_repo.get(pipeline)
                _print_baseline_stats(
                    pipeline=pipeline,
                    baseline_df=runtime_baseline_df,
                    baseline_stats=baseline_stats,
                )
                _update_last_run(db, monitor_id)
                _sleep_interval(interval_seconds)
                continue

            failed_results: List[Any] = []

            for detector in detectors:
                detector_results: List[Any] = []
                try:
                    detector_results = detector.detect(runtime_baseline_df, current_df)
                except Exception as exc:
                    print_error(
                        "Detector '{0}' failed: {1}".format(detector.name, exc)
                    )

                summary = _summarize_detector(detector.name, detector_results)
                _print_detector_summary(summary)

                failed_results.extend(summary["failed_results"])

            alerts_to_send: List[Alert] = []
            for result in failed_results:
                try:
                    alert = Alert.from_detection_result(
                        pipeline_name=pipeline,
                        result=result,
                    )
                    alert_repo.save(alert)
                    alerts_to_send.append(alert)
                except Exception as exc:
                    print_warning("Failed to persist alert: {0}".format(exc))

            if alerts_to_send:
                _send_alerts(alerts_to_send, channels)

                if no_ui:
                    console.print(
                        "[bold red]🚨 {0} alert fired[/bold red]".format(
                            len(alerts_to_send)
                        )
                    )
                else:
                    console.print(
                        "[bold red]🚨 {0} alert fired - view at http://localhost:8080[/bold red]".format(
                            len(alerts_to_send)
                        )
                    )
            else:
                print_success("No alerts fired.")

            if rolling:
                try:
                    baseline_repo.update_rolling(
                        pipeline_name=pipeline,
                        new_df=current_df,
                    )
                    print_success("Baseline updated (rolling window)")
                except Exception as exc:
                    print_warning("Rolling baseline update failed: {0}".format(exc))

            _update_last_run(db, monitor_id)
            _sleep_interval(interval_seconds)

    except KeyboardInterrupt:
        print_warning("Monitoring interrupted by user.")
        print_info("Goodbye.")
    except typer.Exit:
        raise
    except Exception as exc:
        print_error("Monitoring failed: {0}".format(exc))
        raise typer.Exit(code=1)
    finally:
        if dashboard_server is not None:
            try:
                dashboard_server.stop()
            except Exception:
                pass

        if connector is not None:
            try:
                connector.close()
            except Exception:
                pass
