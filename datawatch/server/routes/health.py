"""Health-check route for server and database status."""

from fastapi import APIRouter, Request
from pydantic import BaseModel

from datawatch.storage.database import Database

APP_VERSION = "0.1.0"

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    """Health-check response payload."""

    status: str
    version: str
    db: bool


def _resolve_db(request: Request) -> Database:
    """Return app-scoped database instance, creating one if missing."""
    db = getattr(request.app.state, "db", None)
    if db is None:
        db = Database()
        request.app.state.db = db
    return db


@router.get("/health", response_model=HealthResponse)
async def health_check(request: Request) -> HealthResponse:
    """Return API and database health status."""
    db = _resolve_db(request)
    return HealthResponse(status="ok", version=APP_VERSION, db=db.health_check())
