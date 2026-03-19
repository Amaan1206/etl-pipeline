"""FastAPI application setup for the Datawatch dashboard and API."""

import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles

from datawatch.server.routes import alerts, dashboard, health, monitors
from datawatch.storage.database import Database

APP_VERSION = "0.1.0"

logger = logging.getLogger(__name__)

PACKAGE_DIR = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = PACKAGE_DIR / "dashboard"
STATIC_DIR = DASHBOARD_DIR / "static"

app = FastAPI(
    title="datawatch",
    version=APP_VERSION,
)


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)


app.state.dashboard_dir = DASHBOARD_DIR
app.state.templates_dir = DASHBOARD_DIR

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.mount("/templates", StaticFiles(directory=str(DASHBOARD_DIR)), name="templates")

app.include_router(health.router)
app.include_router(alerts.router)
app.include_router(monitors.router)
app.include_router(dashboard.router)


@app.on_event("startup")
async def startup_event() -> None:
    """Initialize storage and emit a startup log entry."""
    db = Database()
    app.state.db = db
    logger.info(
        "Datawatch server started (version=%s, db=%s)",
        APP_VERSION,
        db.path,
    )
