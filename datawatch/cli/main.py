"""Top-level Typer application for the Datawatch CLI."""

from typing import Optional

import typer

from datawatch import __version__
from datawatch.cli.commands import alerts_app, connect_command, monitor_command, status_command
from datawatch.cli.output import print_banner

app = typer.Typer(
    name="datawatch",
    help="Datawatch command-line interface for data quality monitoring.",
    add_completion=False,
    no_args_is_help=True,
)


def _version_callback(value: Optional[bool]) -> None:
    """Print the Datawatch version and exit immediately."""
    if value:
        typer.echo("datawatch {0}".format(__version__))
        raise typer.Exit()


@app.callback()
def main(
    ctx: typer.Context,
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        help="Show the Datawatch version and exit.",
        callback=_version_callback,
        is_eager=True,
    ),
) -> None:
    """Initialize CLI execution context for every Datawatch command."""
    _ = version

    if ctx.resilient_parsing:
        return

    print_banner()


app.command("connect", help="Connect to a data source and register a pipeline.")(connect_command)
app.command("monitor", help="Run continuous monitoring for a configured pipeline.")(monitor_command)
app.command("status", help="Show configured pipeline status and alert totals.")(status_command)
app.add_typer(alerts_app, name="alerts", help="List, inspect, and clear alerts.")


if __name__ == "__main__":
    app()
