"""Demo mode runner for showcasing Datawatch end-to-end monitoring."""

import os
import threading
import time
import uuid
import webbrowser
from pathlib import Path
from typing import Any, List

import numpy as np
import pandas as pd
import uvicorn

from datawatch.alerts.alert import Alert
from datawatch.cli.output import print_error, print_info, print_success, print_warning
from datawatch.connectors.csv_connector import CSVConnector
from datawatch.detectors.distribution import DistributionDetector
from datawatch.detectors.null_rate import NullRateDetector
from datawatch.detectors.schema_drift import SchemaDriftDetector
from datawatch.storage.alert_repo import AlertRepository
from datawatch.storage.baseline_repo import BaselineRepository
from datawatch.storage.database import Database

_DEMO_PIPELINE = "demo_pipeline"
_DEMO_CSV_PATH = Path("/tmp/datawatch_demo_data.csv")
_DEMO_TMP_CSV_PATH = Path("/tmp/datawatch_demo_data_tmp.csv")


def _generate_demo_csv(path: Path) -> pd.DataFrame:
    """Generate a clean synthetic demo dataset and write it to CSV."""
    rng = np.random.default_rng(seed=42)
    row_count = 2000

    demo_df = pd.DataFrame(
        {
            "transaction_amount": rng.normal(150.0, 30.0, row_count),
            "user_age": rng.normal(32.0, 8.0, row_count),
            "product_category": rng.choice(
                ["electronics", "clothing", "food"],
                size=row_count,
            ),
            "is_active": rng.choice([True, False], size=row_count),
            "session_duration": rng.normal(300.0, 60.0, row_count),
        }
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    demo_df.to_csv(path, index=False)
    return demo_df


def _register_demo_pipeline(db: Database, csv_path: Path) -> None:
    """Delete and recreate the demo pipeline configuration."""
    with db.get_connection() as conn:
        existing = conn.execute(
            "SELECT id FROM pipelines WHERE name = ?",
            (_DEMO_PIPELINE,),
        ).fetchone()

        if existing is not None:
            pipeline_id = str(existing["id"])
            conn.execute("DELETE FROM monitors WHERE pipeline_id = ?", (pipeline_id,))
            conn.execute("DELETE FROM pipelines WHERE id = ?", (pipeline_id,))

        conn.execute("DELETE FROM baselines WHERE pipeline_id = ?", (_DEMO_PIPELINE,))
        conn.execute("DELETE FROM alerts WHERE pipeline_name = ?", (_DEMO_PIPELINE,))
        conn.execute(
            "INSERT INTO pipelines (id, name, source_type, connection_string) "
            "VALUES (?, ?, ?, ?)",
            (
                str(uuid.uuid4()),
                _DEMO_PIPELINE,
                "csv",
                str(csv_path),
            ),
        )
        conn.commit()


def _start_dashboard_server(port: int) -> Any:
    """Start dashboard server in the background and open browser."""
    config = uvicorn.Config(
        app="datawatch.server.app:app",
        host="127.0.0.1",
        port=port,
        log_level="warning",
    )
    server = uvicorn.Server(config=config)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()

    def _open_browser() -> None:
        """Open browser shortly after server startup."""
        time.sleep(1.5)
        try:
            webbrowser.open("http://localhost:{0}".format(port))
        except Exception:
            pass

    threading.Thread(target=_open_browser, daemon=True).start()
    return server, server_thread


def _run_monitor_loop(
    stop_event: threading.Event,
    connector: CSVConnector,
    baseline_df: pd.DataFrame,
    alert_repo: AlertRepository,
    detectors: List[Any],
) -> None:
    """Run detector checks every 60 seconds in a background thread."""
    while not stop_event.is_set():
        try:
            if _DEMO_CSV_PATH.exists() and _DEMO_CSV_PATH.stat().st_size == 0:
                if stop_event.wait(60):
                    break
                continue
        except Exception:
            pass

        _run_monitor_check(
            connector=connector,
            baseline_df=baseline_df,
            alert_repo=alert_repo,
            detectors=detectors,
        )

        if stop_event.wait(60):
            break


def _run_monitor_check(
    connector: CSVConnector,
    baseline_df: pd.DataFrame,
    alert_repo: AlertRepository,
    detectors: List[Any],
) -> None:
    """Run one monitoring check cycle against the current demo CSV."""
    try:
        current_df = connector.fetch(table_or_query=str(_DEMO_CSV_PATH))
        if current_df is not None and not current_df.empty:
            alerts_to_save: List[Alert] = []

            for detector in detectors:
                try:
                    results = detector.detect(baseline_df, current_df)
                except Exception:
                    results = []

                for result in results:
                    if bool(getattr(result, "passed", False)):
                        continue
                    alert = Alert.from_detection_result(
                        pipeline_name=_DEMO_PIPELINE,
                        result=result,
                    )
                    alerts_to_save.append(alert)

            for alert in alerts_to_save:
                alert_repo.save(alert)
    except Exception:
        pass


def _inject_corruption_after_delay(
    stop_event: threading.Event,
    connector: CSVConnector,
    baseline_df: pd.DataFrame,
    alert_repo: AlertRepository,
    detectors: List[Any],
) -> None:
    """Inject demo corruption after 60 seconds."""
    if stop_event.wait(60):
        return

    try:
        df = pd.read_csv(_DEMO_CSV_PATH)
        rng = np.random.default_rng(seed=2026)

        df["user_age"] = rng.normal(65.0, 8.0, len(df))

        if len(df) > 0:
            null_rows = int(len(df) * 0.45)
            indices = rng.choice(df.index.to_numpy(), size=null_rows, replace=False)
            df.loc[indices, "transaction_amount"] = np.nan

        df["debug_flag"] = True
        df.to_csv(_DEMO_TMP_CSV_PATH, index=False)
        os.replace(str(_DEMO_TMP_CSV_PATH), str(_DEMO_CSV_PATH))

        print_warning("Corruption injected — watch the dashboard for alerts")

        if stop_event.wait(5):
            return

        _run_monitor_check(
            connector=connector,
            baseline_df=baseline_df,
            alert_repo=alert_repo,
            detectors=detectors,
        )
    except Exception as exc:
        print_error("Failed to inject demo corruption: {0}".format(exc))


def run_demo(port: int = 8080) -> None:
    """Run an end-to-end synthetic Datawatch demo until Ctrl+C."""
    db = Database()
    baseline_repo = BaselineRepository(db)
    alert_repo = AlertRepository(db)
    stop_event = threading.Event()

    server = None
    server_thread = None
    monitor_thread = None
    corruption_thread = None
    connector = None
    interrupted = False

    try:
        print_info("datawatch demo mode — generating synthetic pipeline data...")

        _generate_demo_csv(_DEMO_CSV_PATH)
        _register_demo_pipeline(db=db, csv_path=_DEMO_CSV_PATH)

        connector = CSVConnector(path=str(_DEMO_CSV_PATH), name=_DEMO_PIPELINE)
        baseline_df = connector.fetch(table_or_query=str(_DEMO_CSV_PATH))
        baseline_repo.save(pipeline_name=_DEMO_PIPELINE, df=baseline_df)
        detectors = [
            SchemaDriftDetector(),
            NullRateDetector(),
            DistributionDetector(),
        ]

        server, server_thread = _start_dashboard_server(port=port)

        print_info("Monitoring started — dashboard at http://localhost:{0}".format(port))
        print_info("Corruption will be injected in 60 seconds...")

        monitor_thread = threading.Thread(
            target=_run_monitor_loop,
            args=(stop_event, connector, baseline_df, alert_repo, detectors),
            daemon=True,
        )
        monitor_thread.start()

        corruption_thread = threading.Thread(
            target=_inject_corruption_after_delay,
            args=(stop_event, connector, baseline_df, alert_repo, detectors),
            daemon=True,
        )
        corruption_thread.start()

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        interrupted = True
    finally:
        stop_event.set()

        if server is not None:
            try:
                server.should_exit = True
            except Exception:
                pass

        if monitor_thread is not None and monitor_thread.is_alive():
            monitor_thread.join(timeout=5)

        if corruption_thread is not None and corruption_thread.is_alive():
            corruption_thread.join(timeout=2)

        if server_thread is not None and server_thread.is_alive():
            server_thread.join(timeout=5)

        if connector is not None:
            try:
                connector.close()
            except Exception:
                pass

        try:
            if _DEMO_CSV_PATH.exists():
                _DEMO_CSV_PATH.unlink()
        except Exception:
            pass

        if interrupted:
            print_info("Demo stopped. Thank you for trying datawatch.")
