"""
datawatch.alerts.manager
~~~~~~~~~~~~~~~~~~~~~~~~~~
Orchestrates alert delivery across all configured channels.

The :class:`AlertManager` accepts a list of channel instances (Slack,
Email, Discord, or any object with a ``send(alert)`` method) and fans
out each alert to every channel.  ``HEALTHY`` alerts are silently
skipped.
"""

import logging
from typing import Any, List

from rich.console import Console

from datawatch.alerts.alert import Alert, AlertSeverity
from datawatch.alerts.formatter import AlertFormatter

logger = logging.getLogger(__name__)
console = Console()


class AlertManager:
    """Fan-out delivery of alerts to one or more notification channels.

    Parameters
    ----------
    channels : list
        Channel instances.  Each channel must expose a
        ``send(alert: Alert) -> bool`` method.
    """

    def __init__(self, channels: List[Any]) -> None:
        self.channels = channels

    # ── Public API ──────────────────────────────────────────────────────

    def send_alert(self, alert: Alert) -> None:
        """Send a single alert to all configured channels.

        Alerts with severity ``HEALTHY`` are skipped.  Delivery
        failures on individual channels are logged but never propagate
        — one broken channel does not prevent delivery to the others.

        Parameters
        ----------
        alert:
            The alert to deliver.
        """
        if alert.severity == AlertSeverity.HEALTHY:
            logger.debug(
                "Skipping HEALTHY alert for %s.%s.",
                alert.pipeline_name,
                alert.column_name,
            )
            return

        # Always print to terminal.
        terminal_text = AlertFormatter.to_terminal(alert)
        console.print(terminal_text)

        for channel in self.channels:
            channel_name = channel.__class__.__name__
            try:
                success = channel.send(alert)
                if success:
                    logger.info(
                        "Alert delivered via %s (pipeline=%s, column=%s).",
                        channel_name,
                        alert.pipeline_name,
                        alert.column_name,
                    )
                else:
                    logger.warning(
                        "Alert delivery via %s returned False "
                        "(pipeline=%s, column=%s).",
                        channel_name,
                        alert.pipeline_name,
                        alert.column_name,
                    )
            except Exception as exc:
                logger.error(
                    "Alert delivery via %s raised an exception: %s. "
                    "Continuing with remaining channels.",
                    channel_name,
                    exc,
                )

    def send_all(self, alerts: List[Alert]) -> None:
        """Send multiple alerts to all configured channels.

        Parameters
        ----------
        alerts:
            A list of alerts to deliver.  ``HEALTHY`` alerts are
            automatically filtered out.
        """
        actionable = [
            a for a in alerts if a.severity != AlertSeverity.HEALTHY
        ]

        if not actionable:
            logger.info("No actionable alerts to send (all HEALTHY).")
            return

        logger.info(
            "Sending %d alert(s) across %d channel(s).",
            len(actionable),
            len(self.channels),
        )

        for alert in actionable:
            self.send_alert(alert)
