"""CLI command for generating markdown alert reports."""

from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import typer
from rich.markdown import Markdown

from datawatch.alerts.alert import Alert
from datawatch.cli.output import console, print_error, print_info, print_success
from datawatch.storage.alert_repo import AlertRepository
from datawatch.storage.database import Database


def _load_alerts(repo: AlertRepository, pipeline: Optional[str]) -> List[Alert]:
    """Load alerts from repository, optionally filtered by pipeline name."""
    if pipeline:
        return repo.get_by_pipeline(pipeline_name=pipeline)
    return repo.get_all(limit=1000000)


def _within_days(alerts: List[Alert], days: int) -> List[Alert]:
    """Filter alerts to only those within the requested day window."""
    cutoff = datetime.utcnow() - timedelta(days=days)
    return [alert for alert in alerts if alert.timestamp >= cutoff]


def _format_pipeline_section(alerts: List[Alert]) -> List[str]:
    """Build markdown rows for alerts grouped by pipeline."""
    if not alerts:
        return ["No alerts in selected period."]

    grouped: Dict[str, List[Alert]] = defaultdict(list)
    for alert in alerts:
        grouped[alert.pipeline_name].append(alert)

    lines: List[str] = [
        "| Pipeline | Alert Count | Most Recent Alert | Most Common Issue Type |",
        "|---|---:|---|---|",
    ]

    ordered = sorted(
        grouped.items(),
        key=lambda item: (-len(item[1]), item[0]),
    )
    for pipeline_name, pipeline_alerts in ordered:
        most_recent = max(pipeline_alerts, key=lambda alert: alert.timestamp).timestamp
        issue_counter = Counter(alert.alert_type.value for alert in pipeline_alerts)
        most_common_issue_type = issue_counter.most_common(1)[0][0]

        lines.append(
            "| {0} | {1} | {2} | {3} |".format(
                pipeline_name,
                len(pipeline_alerts),
                most_recent.isoformat(),
                most_common_issue_type,
            )
        )

    return lines


def _format_top_issues(alerts: List[Alert]) -> List[str]:
    """Build markdown list of top five column and issue combinations."""
    if not alerts:
        return ["- None"]

    counter = Counter(
        (
            (alert.column_name or "-"),
            alert.alert_type.value,
        )
        for alert in alerts
    )

    lines: List[str] = []
    for (column_name, issue_type), count in counter.most_common(5):
        lines.append("- {0} + {1}: {2}".format(column_name, issue_type, count))
    return lines


def _format_timeline(alerts: List[Alert]) -> List[str]:
    """Build markdown bullet list for the chronological alert timeline."""
    if not alerts:
        return ["- No alerts in selected period."]

    ordered = sorted(alerts, key=lambda alert: alert.timestamp)
    lines: List[str] = []

    for alert in ordered:
        lines.append(
            "- {0} | {1} | {2} | {3} | {4} | score={5:.4f} | id={6}".format(
                alert.timestamp.isoformat(),
                alert.pipeline_name,
                alert.column_name or "-",
                alert.alert_type.value,
                alert.severity.value,
                float(alert.score),
                alert.id,
            )
        )

    return lines


def _build_report(alerts: List[Alert], days: int) -> str:
    """Create the full markdown report text for the selected period."""
    generated_at = datetime.utcnow().isoformat()

    total_alerts = len(alerts)
    critical_alerts = sum(1 for alert in alerts if alert.severity.value == "CRITICAL")
    warning_alerts = sum(1 for alert in alerts if alert.severity.value == "WARNING")
    pipelines_affected = len({alert.pipeline_name for alert in alerts})

    report_lines: List[str] = [
        "# datawatch Report",
        "",
        "Generated: {0}".format(generated_at),
        "Period: Last {0} days".format(days),
        "",
        "## Summary",
        "- Total alerts: {0}".format(total_alerts),
        "- Critical: {0}".format(critical_alerts),
        "- Warning: {0}".format(warning_alerts),
        "- Pipelines affected: {0}".format(pipelines_affected),
        "",
        "## Alerts by Pipeline",
    ]
    report_lines.extend(_format_pipeline_section(alerts))
    report_lines.extend(
        [
            "",
            "## Top Issues",
        ]
    )
    report_lines.extend(_format_top_issues(alerts))
    report_lines.extend(
        [
            "",
            "## Alert Timeline",
        ]
    )
    report_lines.extend(_format_timeline(alerts))

    return "\n".join(report_lines)


def report_command(
    pipeline: Optional[str] = typer.Option(
        None,
        "--pipeline",
        help="Optional pipeline name filter.",
    ),
    days: int = typer.Option(
        7,
        "--days",
        min=1,
        help="How many days back to include in the report.",
    ),
    output: Optional[str] = typer.Option(
        None,
        "--output",
        help="Optional file path to save the report markdown.",
    ),
) -> None:
    """Generate a markdown alert report from recent alert history."""
    try:
        repo = AlertRepository(Database())
        alerts = _within_days(_load_alerts(repo=repo, pipeline=pipeline), days=days)
        report_markdown = _build_report(alerts=alerts, days=days)

        if output:
            output_path = Path(output).expanduser()
            if output_path.parent and str(output_path.parent) != ".":
                output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(report_markdown, encoding="utf-8")
            print_success("Report saved to {0}".format(output_path))
            return

        console.print(Markdown(report_markdown))
        print_info("Report generated for {0} alert(s).".format(len(alerts)))

    except Exception as exc:
        print_error("Failed to generate report: {0}".format(exc))
        raise typer.Exit(code=1)
