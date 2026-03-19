"""
datawatch.alerts.channels.discord
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Delivers formatted alerts to a Discord channel via webhook.
"""

import logging

import httpx

from datawatch.alerts.alert import Alert
from datawatch.alerts.formatter import AlertFormatter

logger = logging.getLogger(__name__)


class DiscordChannel:
    """Send alerts to Discord using a channel webhook URL.

    Parameters
    ----------
    webhook_url : str
        The full Discord webhook URL
        (e.g. ``https://discord.com/api/webhooks/.../...``).
    timeout : float
        HTTP request timeout in seconds.  Default: ``10.0``.
    """

    def __init__(
        self,
        webhook_url: str,
        timeout: float = 10.0,
    ) -> None:
        self.webhook_url = webhook_url
        self.timeout = timeout

    def send(self, alert: Alert) -> bool:
        """Post an alert embed to the configured Discord webhook.

        Parameters
        ----------
        alert:
            The alert to deliver.

        Returns
        -------
        bool
            ``True`` if Discord accepted the payload (HTTP 204),
            ``False`` otherwise.
        """
        payload = AlertFormatter.to_discord(alert)

        try:
            response = httpx.post(
                self.webhook_url,
                json=payload,
                timeout=self.timeout,
            )
            # Discord returns 204 No Content on success.
            if response.status_code in (200, 204):
                logger.info(
                    "Discord alert sent successfully (pipeline=%s, column=%s).",
                    alert.pipeline_name,
                    alert.column_name,
                )
                return True

            logger.error(
                "Discord webhook returned HTTP %d: %s",
                response.status_code,
                response.text[:200],
            )
            return False
        except httpx.TimeoutException:
            logger.error(
                "Discord webhook timed out after %.1fs.", self.timeout
            )
            return False
        except Exception as exc:
            logger.error("Discord send failed: %s", exc)
            return False
