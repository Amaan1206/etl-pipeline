# datawatch

Automated data quality monitoring for ETL pipelines

![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-3776AB)
![License MIT](https://img.shields.io/badge/License-MIT-16A34A)
![Tests Passing](https://img.shields.io/badge/Tests-Passing-22C55E)

## What it does

datawatch continuously checks pipeline outputs so data quality issues are caught before they propagate to dashboards, ML features, and downstream systems. It detects schema drift such as added, removed, or type-changed columns, and flags null rate explosions that indicate ingestion or transformation failures. It also monitors statistical distribution shifts in numeric fields using Kolmogorov-Smirnov (KS) testing and Population Stability Index (PSI). Alerts are persisted, exposed through a FastAPI API, and surfaced in both the CLI and browser dashboard.

## Quick Start

```bash
pip install datawatch
datawatch connect --source path/to/data.csv --name my_pipeline --type csv
datawatch monitor --pipeline my_pipeline --table data.csv --every 30
```

The monitor command starts the API server and opens the dashboard automatically at `http://localhost:8080`.

## How it works

### Schema Drift Detection

datawatch compares the current batch schema to the saved baseline schema and checks for column additions, removals, and dtype changes. Any structural difference is converted into a traceable alert with the affected column and drift details.

### Null Rate Monitoring

datawatch tracks per-column null ratios in the baseline and current batch, then computes the delta. Threshold-based severity assignment marks minor increases as warnings and larger jumps as critical events.

### Statistical Distribution Shift

For numeric columns, datawatch applies a KS two-sample test to detect significant shape changes and computes PSI to quantify magnitude. KS provides statistical significance, while PSI provides practical impact scoring for warning and critical decisions.

## Benchmark Results

- Detection Rate: 100% (100/100 corrupted batches detected)
- False Positive Rate: 1% (1/100 clean batches incorrectly flagged)
- Test Suite: 19/19 tests passing
- Validated on synthetic pipelines with injected corruption

## Supported Data Sources

- CSV files
- PostgreSQL
- SQLite

## Alert Channels

- Slack webhooks
- Email via Gmail SMTP
- Discord webhooks

## CLI Reference

### connect

Command:
`datawatch connect --source /absolute/path/to/data.csv --name orders_pipeline --type csv`

Options:
- `--source` required source path or connection string
- `--name` required pipeline name used by monitor and alerts
- `--type` required source type: `csv`, `postgres`, or `sqlite`

### monitor

Command:
`datawatch monitor --pipeline orders_pipeline --table orders --every 30 --alert-slack https://hooks.slack.com/services/example --alert-email ops@example.com`

Options:
- `--pipeline` required configured pipeline name
- `--table` required table name, query target, or file path
- `--every` interval in minutes, default `30`
- `--no-ui` disable dashboard server and browser launch
- `--alert-slack` optional Slack webhook URL
- `--alert-email` optional alert recipient email address

### status

Command:
`datawatch status`

Options:
- no options

### alerts

Command group:
`datawatch alerts`

Subcommands:
- `datawatch alerts list --limit 50` lists recent alerts with optional limit
- `datawatch alerts inspect 0dcd3a6b-f98d-4b08-b8d9-b0a24f7ce11d` shows full detail for one alert ID
- `datawatch alerts clear` removes all alerts after confirmation

## Configuration

`.env.example`:

```env
# Runtime mode and logging
DATAWATCH_ENV=development
DATAWATCH_DEBUG=true
DATAWATCH_LOG_LEVEL=INFO

# Internal SQLite storage path for baselines, monitors, and alerts
DB_PATH=~/.datawatch/datawatch.db

# Monitoring schedule and detector thresholds
MONITOR_INTERVAL_SECONDS=1800
NULL_RATE_WARNING_THRESHOLD=0.05
NULL_RATE_CRITICAL_THRESHOLD=0.20
KS_PVALUE_THRESHOLD=0.05
PSI_WARNING_THRESHOLD=0.10
PSI_CRITICAL_THRESHOLD=0.20
MONITOR_RETENTION_DAYS=90

# Dashboard and API server
DASHBOARD_PORT=8080
API_HOST=0.0.0.0
API_PORT=8000

# Alert routing defaults
SLACK_WEBHOOK_URL=
ALERT_EMAIL=ops@example.com
ALERT_EMAIL_SMTP_HOST=smtp.gmail.com
ALERT_EMAIL_SMTP_PORT=587
ALERT_EMAIL_FROM=alerts@example.com
ALERT_EMAIL_PASSWORD=

# PostgreSQL connector defaults
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=datawatch
POSTGRES_PASSWORD=datawatch
POSTGRES_DB=datawatch

# Optional MySQL connector settings
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=datawatch
MYSQL_PASSWORD=datawatch
MYSQL_DB=datawatch

# Optional S3 connector settings
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=us-east-1
S3_BUCKET_NAME=

# Optional Kafka connector settings
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=datawatch-events
KAFKA_GROUP_ID=datawatch-consumer

# SMTP overrides used by `datawatch monitor --alert-email`
DATAWATCH_SMTP_HOST=smtp.gmail.com
DATAWATCH_SMTP_PORT=587
DATAWATCH_SMTP_USERNAME=alerts@example.com
DATAWATCH_SMTP_PASSWORD=
DATAWATCH_ALERT_EMAIL_FROM=alerts@example.com
DATAWATCH_ALERT_EMAIL_PASSWORD=
```

## Running Tests

```bash
pytest tests/ -v
```

## License

MIT
