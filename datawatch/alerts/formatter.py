"""
datawatch.alerts.formatter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Renders :class:`Alert` objects into different output formats:

* **Slack** — structured webhook payload with severity colour coding.
* **Email** — plain-text body suitable for SMTP delivery.
* **Terminal** — Rich-markup string for coloured console output.
"""

from datawatch.alerts.alert import Alert, AlertSeverity


# ── Colour / emoji maps ────────────────────────────────────────────────────

_SLACK_COLOURS = {
    AlertSeverity.HEALTHY: "#2ecc71",   # green
    AlertSeverity.WARNING: "#f1c40f",   # yellow
    AlertSeverity.CRITICAL: "#e74c3c",  # red
}

_SLACK_EMOJIS = {
    AlertSeverity.HEALTHY: ":white_check_mark:",
    AlertSeverity.WARNING: ":warning:",
    AlertSeverity.CRITICAL: ":rotating_light:",
}

_RICH_COLOURS = {
    AlertSeverity.HEALTHY: "green",
    AlertSeverity.WARNING: "yellow",
    AlertSeverity.CRITICAL: "red",
}

_DISCORD_COLOURS = {
    AlertSeverity.HEALTHY: 0x2ECC71,   # green
    AlertSeverity.WARNING: 0xF1C40F,   # yellow
    AlertSeverity.CRITICAL: 0xE74C3C,  # red
}


class AlertFormatter:
    """Format :class:`Alert` objects for different delivery channels."""

    # ── Slack ────────────────────────────────────────────────────────────

    @staticmethod
    def to_slack(alert: Alert) -> dict:
        """Return a Slack-compatible webhook JSON payload.

        Parameters
        ----------
        alert:
            The alert to format.

        Returns
        -------
        dict
            A dictionary ready to be ``POST``-ed to a Slack Incoming
            Webhook URL.
        """
        emoji = _SLACK_EMOJIS.get(alert.severity, ":question:")
        colour = _SLACK_COLOURS.get(alert.severity, "#95a5a6")

        return {
            "attachments": [
                {
                    "color": colour,
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": (
                                    f"{emoji} *[datawatch] {alert.severity.value}*\n"
                                    f"*Pipeline:* `{alert.pipeline_name}`\n"
                                    f"*Column:* `{alert.column_name}`\n"
                                    f"*Type:* {alert.alert_type.value}\n"
                                    f"*Score:* {alert.score:.4f}\n"
                                    f"*Details:* {alert.details}\n"
                                    f"*Time:* {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}"
                                ),
                            },
                        }
                    ],
                }
            ]
        }

    # ── Email ────────────────────────────────────────────────────────────

    @staticmethod
    def to_email(alert: Alert) -> str:
        """Return a plain-text email body.

        Parameters
        ----------
        alert:
            The alert to format.

        Returns
        -------
        str
            Multi-line plain-text string suitable for an email body.
        """
        lines = [
            f"[datawatch] {alert.severity.value} — {alert.pipeline_name} — {alert.column_name}",
            "",
            "=" * 60,
            f"  Alert ID    : {alert.id}",
            f"  Pipeline    : {alert.pipeline_name}",
            f"  Column      : {alert.column_name}",
            f"  Alert Type  : {alert.alert_type.value}",
            f"  Severity    : {alert.severity.value}",
            f"  Score       : {alert.score:.4f}",
            f"  Details     : {alert.details}",
            f"  Timestamp   : {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}",
            "=" * 60,
            "",
            "This alert was generated automatically by datawatch.",
        ]
        return "\n".join(lines)

    # ── Terminal (Rich markup) ──────────────────────────────────────────

    @staticmethod
    def to_terminal(alert: Alert) -> str:
        """Return a Rich-markup coloured string for console display.

        Parameters
        ----------
        alert:
            The alert to format.

        Returns
        -------
        str
            A string containing Rich markup tags.
        """
        colour = _RICH_COLOURS.get(alert.severity, "white")

        return (
            f"[bold {colour}][{alert.severity.value}][/bold {colour}] "
            f"[bold]{alert.pipeline_name}[/bold] · "
            f"{alert.column_name} · "
            f"{alert.alert_type.value} · "
            f"score={alert.score:.4f} · "
            f"{alert.details}"
        )

    # ── Discord ─────────────────────────────────────────────────────────

    @staticmethod
    def to_discord(alert: Alert) -> dict:
        """Return a Discord webhook embed payload.

        Parameters
        ----------
        alert:
            The alert to format.

        Returns
        -------
        dict
            A dictionary ready to be ``POST``-ed to a Discord webhook.
        """
        colour = _DISCORD_COLOURS.get(alert.severity, 0x95A5A6)

        return {
            "embeds": [
                {
                    "title": f"[datawatch] {alert.severity.value}",
                    "color": colour,
                    "fields": [
                        {"name": "Pipeline", "value": alert.pipeline_name, "inline": True},
                        {"name": "Column", "value": alert.column_name, "inline": True},
                        {"name": "Type", "value": alert.alert_type.value, "inline": True},
                        {"name": "Score", "value": f"{alert.score:.4f}", "inline": True},
                        {"name": "Details", "value": alert.details, "inline": False},
                    ],
                    "footer": {
                        "text": f"datawatch · {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}",
                    },
                }
            ]
        }
