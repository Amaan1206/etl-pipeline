"""CLI command modules for Datawatch."""

from datawatch.cli.commands.alerts import app as alerts_app
from datawatch.cli.commands.connect import connect_command
from datawatch.cli.commands.monitor import monitor_command
from datawatch.cli.commands.status import status_command

__all__ = [
    "alerts_app",
    "connect_command",
    "monitor_command",
    "status_command",
]
