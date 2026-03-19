"""
datawatch.alerts.channels.slack
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Delivers formatted alerts to a Slack channel via Incoming Webhook.
"""

import logging

import httpx

from datawatch.alerts.alert import Alert
from datawatch.alerts.formatter import AlertFormatter

logger = logging.getLogger(__name__)


class SlackChannel:
    """Send alerts to Slack using an Incoming Webhook URL.

    Parameters
    ----------
    webhook_url : str
        The full Slack Incoming Webhook URL
        (e.g. ``https://hooks.slack.com/services/T.../B.../...``).
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
        """Post an alert to the configured Slack webhook.

        Parameters
        ----------
        alert:
            The alert to deliver.

        Returns
        -------
        bool
            ``True`` if the webhook accepted the payload, ``False``
            otherwise.
        """
        payload = AlertFormatter.to_slack(alert)

        try:
            response = httpx.post(
                self.webhook_url,
                json=payload,
                timeout=self.timeout,
            )
            if response.status_code == 200:
                logger.info(
                    "Slack alert sent successfully (pipeline=%s, column=%s).",
                    alert.pipeline_name,
                    alert.column_name,
                )
                return True

            logger.error(
                "Slack webhook returned HTTP %d: %s",
                response.status_code,
                response.text[:200],
            )
            return False
        except httpx.TimeoutException:
            logger.error("Slack webhook timed out after %.1fs.", self.timeout)
            return False
        except Exception as exc:
            logger.error("Slack send failed: %s", exc)
            return False
