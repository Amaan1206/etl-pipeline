"""Alerts sub-package — alert definitions, formatting, and delivery channels."""

from datawatch.alerts.alert import Alert, AlertSeverity, AlertType
from datawatch.alerts.formatter import AlertFormatter
from datawatch.alerts.manager import AlertManager
from datawatch.alerts.channels.slack import SlackChannel
from datawatch.alerts.channels.email import EmailChannel
from datawatch.alerts.channels.discord import DiscordChannel

__all__ = [
    "Alert",
    "AlertSeverity",
    "AlertType",
    "AlertFormatter",
    "AlertManager",
    "SlackChannel",
    "EmailChannel",
    "DiscordChannel",
]
