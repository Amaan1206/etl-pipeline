"""Rich-powered terminal output helpers for the Datawatch CLI."""

from datetime import datetime
from typing import Any, Iterable

from rich import box
from rich.console import Console
from rich.markup import escape
from rich.panel import Panel
from rich.table import Table

from datawatch import __version__

console = Console()


def _severity_value(value: Any) -> str:
    """Return a normalized severity string from enum-like or raw values."""
    if value is None:
        return "UNKNOWN"
    severity = getattr(value, "value", value)
    return str(severity).upper()


def _severity_color(severity: str) -> str:
    """Map severity values to Rich color names."""
    if severity == "CRITICAL":
        return "red"
    if severity == "WARNING":
        return "yellow"
    if severity == "HEALTHY":
        return "green"
    return "cyan"


def print_success(message: str) -> None:
    """Print a success message prefixed with a green checkmark."""
    console.print("[bold green]✓[/bold green] {0}".format(escape(message)))


def print_error(message: str) -> None:
    """Print an error message prefixed with a red X."""
    console.print("[bold red]✗[/bold red] {0}".format(escape(message)))


def print_info(message: str) -> None:
    """Print an informational message prefixed with a blue info icon."""
    console.print("[bold blue]ℹ[/bold blue] {0}".format(escape(message)))


def print_warning(message: str) -> None:
    """Print a warning message prefixed with a yellow warning icon."""
    console.print("[bold yellow]⚠[/bold yellow] {0}".format(escape(message)))


def print_alert(alert: Any) -> None:
    """Print one alert in a compact severity-colored terminal format."""
    severity = _severity_value(getattr(alert, "severity", None))
    color = _severity_color(severity)

    timestamp = getattr(alert, "timestamp", "")
    if isinstance(timestamp, datetime):
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    else:
        timestamp_str = str(timestamp) if timestamp else "-"

    pipeline_name = str(getattr(alert, "pipeline_name", "-"))
    column_name = str(getattr(alert, "column_name", "-"))
    alert_type = _severity_value(getattr(alert, "alert_type", "UNKNOWN"))
    score = getattr(alert, "score", None)
    details = str(getattr(alert, "details", ""))

    try:
        score_text = "{0:.4f}".format(float(score))
    except Exception:
        score_text = "-"

    console.print(
        "[{0}] {1}[/] [bold]{2}[/bold] {3}.{4} {5} score={6} {7}".format(
            color,
            severity,
            escape(timestamp_str),
            escape(pipeline_name),
            escape(column_name),
            escape(alert_type),
            escape(score_text),
            escape(details),
        )
    )


def print_banner() -> None:
    """Print the Datawatch startup ASCII-art banner."""
    art = (
        "  ____        _            _       _       \n"
        " |  _ \\  __ _| |_ __ _  __| | __ _| |_ ___ \n"
        " | | | |/ _` | __/ _` |/ _` |/ _` | __/ __|\n"
        " | |_| | (_| | || (_| | (_| | (_| | |_\\__ \\\n"
        " |____/ \\__,_|\\__\\__,_|\\__,_|\\__,_|\\__|___/"
    )

    body = "[bold blue]{0}[/bold blue]\n[dim]version {1}[/dim]".format(art, __version__)
    console.print(Panel(body, border_style="blue", padding=(1, 2), box=box.ROUNDED))


def print_check_result(results: Iterable[Any]) -> None:
    """Render detection results as a Rich table."""
    rows = list(results or [])
    if not rows:
        print_info("No detection results to display.")
        return

    table = Table(title="Detection Results", box=box.SIMPLE_HEAVY, show_lines=False)
    table.add_column("Detector", style="cyan", no_wrap=True)
    table.add_column("Column", style="white", no_wrap=True)
    table.add_column("Severity", no_wrap=True)
    table.add_column("Score", justify="right", no_wrap=True)
    table.add_column("Status", no_wrap=True)
    table.add_column("Details", overflow="fold")

    for result in rows:
        detector = str(getattr(result, "detector_type", "-"))
        column = str(getattr(result, "column_name", "-"))
        severity = _severity_value(getattr(result, "severity", "UNKNOWN"))
        color = _severity_color(severity)

        score_raw = getattr(result, "score", None)
        try:
            score = "{0:.4f}".format(float(score_raw))
        except Exception:
            score = "-"

        passed = bool(getattr(result, "passed", False))
        status = "PASS" if passed else "FAIL"
        status_style = "green" if passed else "red"
        details = str(getattr(result, "details", ""))

        table.add_row(
            escape(detector),
            escape(column),
            "[{0}]{1}[/{0}]".format(color, escape(severity)),
            escape(score),
            "[{0}]{1}[/{0}]".format(status_style, status),
            escape(details),
        )

    console.print(table)
