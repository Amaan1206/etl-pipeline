"""Dashboard page routes served as static HTML files."""

from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

router = APIRouter(tags=["dashboard"])

DASHBOARD_DIR = Path(__file__).resolve().parent.parent.parent / "dashboard"


def _html_file(filename: str) -> Path:
    """Resolve a dashboard HTML file and fail fast if it is missing."""
    html_path = DASHBOARD_DIR / filename
    if not html_path.is_file():
        raise HTTPException(status_code=404, detail="Dashboard page not found")
    return html_path


@router.get("/", response_class=FileResponse)
async def dashboard_index() -> FileResponse:
    """Serve the dashboard home page HTML."""
    return FileResponse(path=str(_html_file("index.html")), media_type="text/html")


@router.get("/alerts", response_class=FileResponse)
async def dashboard_alerts() -> FileResponse:
    """Serve the dashboard alerts list page HTML."""
    return FileResponse(path=str(_html_file("alerts.html")), media_type="text/html")


@router.get("/alerts/{alert_id}", response_class=FileResponse)
async def dashboard_alert_detail(alert_id: str) -> FileResponse:
    """Serve the dashboard alert detail page HTML."""
    _ = alert_id
    return FileResponse(path=str(_html_file("alert_detail.html")), media_type="text/html")
