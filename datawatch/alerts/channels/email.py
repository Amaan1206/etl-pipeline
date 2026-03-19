"""
datawatch.alerts.channels.email
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Delivers formatted alerts via SMTP email.
"""

import logging
import smtplib
from email.mime.text import MIMEText

from datawatch.alerts.alert import Alert
from datawatch.alerts.formatter import AlertFormatter

logger = logging.getLogger(__name__)


class EmailChannel:
    """Send alerts as plain-text emails via SMTP.

    Parameters
    ----------
    smtp_host : str
        SMTP server hostname (e.g. ``"smtp.gmail.com"``).
    smtp_port : int
        SMTP server port (e.g. ``465`` for SSL, ``587`` for STARTTLS).
    username : str
        SMTP authentication username (usually the sender address).
    password : str
        SMTP authentication password or app-specific password.
    recipient : str
        Email address to deliver alerts to.
    use_ssl : bool
        If ``True`` (default), use ``SMTP_SSL``.  Set to ``False`` to
        use STARTTLS on port 587 instead.
    timeout : float
        Connection timeout in seconds.  Default: ``15.0``.
    """

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        username: str,
        password: str,
        recipient: str,
        use_ssl: bool = True,
        timeout: float = 15.0,
    ) -> None:
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.recipient = recipient
        self.use_ssl = use_ssl
        self.timeout = timeout

    def send(self, alert: Alert) -> bool:
        """Send an alert email.

        Parameters
        ----------
        alert:
            The alert to deliver.

        Returns
        -------
        bool
            ``True`` if the email was accepted by the SMTP server,
            ``False`` otherwise.
        """
        subject = (
            f"[datawatch] {alert.severity.value} — "
            f"{alert.pipeline_name} — {alert.column_name}"
        )
        body = AlertFormatter.to_email(alert)

        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = self.username
        msg["To"] = self.recipient

        try:
            if self.use_ssl:
                server = smtplib.SMTP_SSL(
                    self.smtp_host,
                    self.smtp_port,
                    timeout=self.timeout,
                )
            else:
                server = smtplib.SMTP(
                    self.smtp_host,
                    self.smtp_port,
                    timeout=self.timeout,
                )
                server.ehlo()
                server.starttls()
                server.ehlo()

            server.login(self.username, self.password)
            server.sendmail(self.username, [self.recipient], msg.as_string())
            server.quit()

            logger.info(
                "Email alert sent to %s (pipeline=%s, column=%s).",
                self.recipient,
                alert.pipeline_name,
                alert.column_name,
            )
            return True
        except smtplib.SMTPException as exc:
            logger.error("SMTP error sending alert email: %s", exc)
            return False
        except Exception as exc:
            logger.error("Email send failed: %s", exc)
            return False
