"""
datawatch.core.config
~~~~~~~~~~~~~~~~~~~~~~
Application-wide settings loaded from environment variables / .env file
using *pydantic-settings*.

Any variable can be overridden by setting the corresponding environment
variable (case-insensitive) or by adding it to a ``.env`` file in the
project root.
"""

from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central configuration object populated from the environment.

    Covers monitoring thresholds, connector defaults, alerting
    endpoints, and the dashboard server.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── General ──────────────────────────────────────────────────────────
    datawatch_env: str = "development"
    datawatch_debug: bool = True
    datawatch_log_level: str = "INFO"

    # ── Database (internal storage) ──────────────────────────────────────
    db_path: str = str(Path.home() / ".datawatch" / "datawatch.db")

    # ── Monitor ──────────────────────────────────────────────────────────
    monitor_interval_seconds: int = 1800

    # ── Null-rate detector thresholds ────────────────────────────────────
    null_rate_warning_threshold: float = 0.05
    null_rate_critical_threshold: float = 0.20

    # ── KS test threshold ────────────────────────────────────────────────
    ks_pvalue_threshold: float = 0.05

    # ── PSI thresholds ───────────────────────────────────────────────────
    psi_warning_threshold: float = 0.1
    psi_critical_threshold: float = 0.2

    # ── Dashboard / API Server ───────────────────────────────────────────
    dashboard_port: int = 8080
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # ── Alerting ─────────────────────────────────────────────────────────
    slack_webhook_url: Optional[str] = None
    alert_email: Optional[str] = None
    alert_email_smtp_host: str = "smtp.gmail.com"
    alert_email_smtp_port: int = 587
    alert_email_from: str = "alerts@example.com"
    alert_email_password: str = ""

    # ── PostgreSQL Connector ─────────────────────────────────────────────
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "datawatch"
    postgres_password: str = "changeme"
    postgres_db: str = "datawatch"

    # ── MySQL Connector ──────────────────────────────────────────────────
    mysql_host: str = "localhost"
    mysql_port: int = 3306
    mysql_user: str = "datawatch"
    mysql_password: str = "changeme"
    mysql_db: str = "datawatch"

    # ── AWS / S3 ─────────────────────────────────────────────────────────
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    aws_region: str = "us-east-1"
    s3_bucket_name: str = ""

    # ── Kafka ────────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "datawatch-events"
    kafka_group_id: str = "datawatch-consumer"

    # ── Monitoring retention ─────────────────────────────────────────────
    monitor_retention_days: int = 90


def get_settings() -> Settings:
    """Factory that returns a *Settings* instance (eases testing via DI)."""
    return Settings()
