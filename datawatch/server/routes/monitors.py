"""Monitor API routes for active monitor listings and baseline lookup."""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from datawatch.storage.baseline_repo import BaselineRepository
from datawatch.storage.database import Database

router = APIRouter(prefix="/api/monitors", tags=["monitors"])


class MonitorResponse(BaseModel):
    """API model for active monitor metadata."""

    id: str
    pipeline_id: str
    table_name: str
    interval_seconds: int
    created_at: str
    last_run_at: Optional[str] = None
    is_active: int


class PipelineBaselineResponse(BaseModel):
    """API model for baseline statistics of one pipeline."""

    pipeline_name: str
    columns: Dict[str, Dict[str, Any]]


def _resolve_db(request: Request) -> Database:
    """Return app-scoped database instance, creating one if missing."""
    db = getattr(request.app.state, "db", None)
    if db is None:
        db = Database()
        request.app.state.db = db
    return db


@router.get("", response_model=List[MonitorResponse])
async def list_monitors(request: Request) -> List[MonitorResponse]:
    """Return all active monitors stored in the database."""
    db = _resolve_db(request)
    with db.get_connection() as conn:
        rows = conn.execute(
            "SELECT id, pipeline_id, table_name, interval_seconds, "
            "created_at, last_run_at, is_active "
            "FROM monitors WHERE is_active = 1 "
            "ORDER BY created_at DESC"
        ).fetchall()
    return [MonitorResponse(**dict(row)) for row in rows]


@router.get("/{pipeline_name}/baseline", response_model=PipelineBaselineResponse)
async def get_pipeline_baseline(
    pipeline_name: str,
    request: Request,
) -> PipelineBaselineResponse:
    """Return baseline statistics for a single pipeline."""
    repo = BaselineRepository(_resolve_db(request))
    if not repo.exists(pipeline_name):
        raise HTTPException(status_code=404, detail="Baseline not found")
    return PipelineBaselineResponse(
        pipeline_name=pipeline_name,
        columns=repo.get(pipeline_name),
    )
