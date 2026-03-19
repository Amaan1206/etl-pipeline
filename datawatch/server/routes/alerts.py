"""Alert API routes for listing, reading, stats, and acknowledgement."""

from datetime import datetime
from typing import List

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from datawatch.alerts.alert import Alert
from datawatch.storage.alert_repo import AlertRepository
from datawatch.storage.database import Database

router = APIRouter(prefix="/api/alerts", tags=["alerts"])


class AlertResponse(BaseModel):
    """Public API model for a stored alert."""

    id: str
    pipeline_name: str
    column_name: str
    alert_type: str
    severity: str
    score: float
    details: str
    timestamp: datetime
    acknowledged: bool
    notes: str


class AcknowledgeAlertRequest(BaseModel):
    """Request model for acknowledging an alert."""

    notes: str = ""


class AcknowledgeAlertResponse(BaseModel):
    """Response model after alert acknowledgement."""

    status: str
    alert_id: str
    acknowledged: bool
    notes: str


class AlertStatsResponse(BaseModel):
    """Aggregate alert counts used by the dashboard."""

    total: int
    critical: int
    warning: int
    last_24h: int


def _resolve_db(request: Request) -> Database:
    """Return app-scoped database instance, creating one if missing."""
    db = getattr(request.app.state, "db", None)
    if db is None:
        db = Database()
        request.app.state.db = db
    return db


def _resolve_repo(request: Request) -> AlertRepository:
    """Build an alert repository bound to the app database."""
    return AlertRepository(_resolve_db(request))


def _to_alert_response(alert: Alert) -> AlertResponse:
    """Convert internal alert entity to the API response model."""
    return AlertResponse(
        id=alert.id,
        pipeline_name=alert.pipeline_name,
        column_name=alert.column_name,
        alert_type=alert.alert_type.value,
        severity=alert.severity.value,
        score=alert.score,
        details=alert.details,
        timestamp=alert.timestamp,
        acknowledged=alert.acknowledged,
        notes=alert.notes,
    )


@router.get("", response_model=List[AlertResponse])
async def list_alerts(
    request: Request,
    limit: int = Query(default=50, ge=1, le=500),
) -> List[AlertResponse]:
    """Return recent alerts ordered from newest to oldest."""
    repo = _resolve_repo(request)
    alerts = repo.get_all(limit=limit)
    return [_to_alert_response(alert) for alert in alerts]


@router.get("/stats", response_model=AlertStatsResponse)
async def get_alert_stats(request: Request) -> AlertStatsResponse:
    """Return aggregate statistics for all alerts."""
    repo = _resolve_repo(request)
    return AlertStatsResponse(**repo.get_stats())


@router.get("/{alert_id}", response_model=AlertResponse)
async def get_alert(alert_id: str, request: Request) -> AlertResponse:
    """Return one alert by its unique identifier."""
    repo = _resolve_repo(request)
    alert = repo.get_by_id(alert_id)
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    return _to_alert_response(alert)


@router.post("/{alert_id}/acknowledge", response_model=AcknowledgeAlertResponse)
async def acknowledge_alert(
    alert_id: str,
    payload: AcknowledgeAlertRequest,
    request: Request,
) -> AcknowledgeAlertResponse:
    """Acknowledge an alert with optional reviewer notes."""
    repo = _resolve_repo(request)
    updated = repo.acknowledge(alert_id=alert_id, notes=payload.notes)
    if not updated:
        raise HTTPException(status_code=404, detail="Alert not found")
    return AcknowledgeAlertResponse(
        status="acknowledged",
        alert_id=alert_id,
        acknowledged=True,
        notes=payload.notes,
    )
