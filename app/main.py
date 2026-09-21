"""FastAPI application: lab sensor timeline merge API."""
from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import Depends, FastAPI, Query, Request
from fastapi.responses import JSONResponse

from . import service
from .config import settings
from .database import get_db, init_db
from .models import (
    AlertHistoryResponse,
    AlertRuleIn,
    AlertRulesResponse,
    CreateSessionIn,
    CreatedAlertRuleResponse,
    CreatedSessionResponse,
    CurrentAlertsResponse,
    PullResponse,
    QuarantineResponse,
    ReadingIn,
    ReportResponse,
    SessionOut,
)
from .time_utils import TimestampError


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Database directory + schema are created as part of startup.
    init_db(app.state.db_path)
    yield


app = FastAPI(
    title="Lab Sensor Timeline Merge API",
    version="1.1.0",
    description=(
        "Out-of-order sensor reporting with per-session fixed source lists, "
        "a slowest-source watermark (minus allowed lateness), deterministic "
        "publish order, late-reading quarantine, and post-publish "
        "consecutive-threshold alerting with hysteresis."
    ),
    lifespan=lifespan,
)
app.state.db_path = settings.db_path


@app.exception_handler(service.ServiceError)
async def service_error_handler(request: Request, exc: service.ServiceError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": str(exc)})


@app.exception_handler(TimestampError)
async def timestamp_error_handler(request: Request, exc: TimestampError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.exception_handler(sqlite3.Error)
async def sqlite_error_handler(request: Request, exc: sqlite3.Error) -> JSONResponse:
    return JSONResponse(status_code=500, content={"detail": f"database error: {exc}"})


def _error(status: int, detail: Any) -> JSONResponse:
    return JSONResponse(status_code=status, content={"detail": detail})


@app.get("/health", tags=["meta"])
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/sessions", response_model=CreatedSessionResponse, status_code=201, tags=["sessions"])
def create_session(
    body: CreateSessionIn,
    conn: sqlite3.Connection = Depends(get_db),
) -> CreatedSessionResponse:
    result = service.create_session(
        conn,
        source_ids=body.source_ids,
        allowed_lateness_seconds=body.allowed_lateness_seconds,
        session_id=body.session_id,
    )
    return CreatedSessionResponse(**result)


@app.get("/sessions/{session_id}", response_model=SessionOut, tags=["sessions"])
def get_session(
    session_id: str,
    conn: sqlite3.Connection = Depends(get_db),
) -> SessionOut:
    return SessionOut(**service.get_session(conn, session_id))


@app.post(
    "/sessions/{session_id}/readings",
    response_model=ReportResponse,
    status_code=200,
    tags=["readings"],
)
async def report_readings(
    session_id: str,
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
) -> ReportResponse:
    """Batch-report readings.

    Accepts either ``{"readings": [ ... ]}`` or a bare JSON array.
    Semantic failures (bad timestamps, unknown sources) return 400;
    structural field failures return 422.
    """
    try:
        payload = await request.json()
    except Exception:
        return _error(400, "request body must be valid JSON")

    if isinstance(payload, dict) and isinstance(payload.get("readings"), list):
        raw = payload["readings"]
    elif isinstance(payload, list):
        raw = payload
    else:
        return _error(400, "body must be {'readings': [...]} or a JSON array")

    if not raw:
        return _error(400, "readings batch must not be empty")

    items: list[dict] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            return _error(400, f"reading at index {idx} must be an object")
        try:
            parsed = ReadingIn(**item)
        except Exception as exc:
            detail = exc.errors() if hasattr(exc, "errors") else str(exc)
            return _error(422, f"invalid reading at index {idx}: {detail}")
        items.append(parsed.model_dump())

    # TimestampError and ServiceError are handled by the registered handlers.
    result = service.ingest_batch(conn, session_id, items)
    return ReportResponse(**result)


@app.get(
    "/sessions/{session_id}/stream",
    response_model=PullResponse,
    tags=["readings"],
)
def pull_stream(
    session_id: str,
    limit: int = Query(100, ge=1, le=1000),
    cursor: Optional[str] = Query(None, description="Opaque next_cursor from a previous page"),
    conn: sqlite3.Connection = Depends(get_db),
) -> PullResponse:
    """Published readings only, in deterministic timeline order."""
    return PullResponse(**service.list_published(conn, session_id, limit, cursor))


@app.get(
    "/sessions/{session_id}/quarantine",
    response_model=QuarantineResponse,
    tags=["readings"],
)
def pull_quarantine(
    session_id: str,
    limit: int = Query(100, ge=1, le=1000),
    cursor: Optional[str] = Query(None, description="Opaque next_cursor from a previous page"),
    conn: sqlite3.Connection = Depends(get_db),
) -> QuarantineResponse:
    """Late readings that arrived behind the already-published watermark."""
    return QuarantineResponse(**service.list_quarantined(conn, session_id, limit, cursor))


@app.post(
    "/sessions/{session_id}/alert-rules",
    response_model=CreatedAlertRuleResponse,
    status_code=201,
    tags=["alerts"],
)
def create_alert_rule(
    session_id: str,
    body: AlertRuleIn,
    conn: sqlite3.Connection = Depends(get_db),
) -> CreatedAlertRuleResponse:
    """Create a consecutive-threshold alert rule on one source of the session.

    Unknown sources and invalid rules (bad hysteresis band, counts < 1)
    return 400. Already-published readings of the source are replayed once
    so the state machine reflects the full published timeline.
    """
    result = service.create_alert_rule(
        conn,
        session_id,
        source_id=body.source_id,
        direction=body.direction,
        enter_threshold=body.enter_threshold,
        recover_threshold=body.recover_threshold,
        enter_count=body.enter_count,
        recover_count=body.recover_count,
        rule_id=body.rule_id,
    )
    return CreatedAlertRuleResponse(**result)


@app.get(
    "/sessions/{session_id}/alert-rules",
    response_model=AlertRulesResponse,
    tags=["alerts"],
)
def list_alert_rules(
    session_id: str,
    conn: sqlite3.Connection = Depends(get_db),
) -> AlertRulesResponse:
    """Alert rule configuration of the session."""
    return AlertRulesResponse(**service.list_alert_rules(conn, session_id))


@app.get(
    "/sessions/{session_id}/alerts/current",
    response_model=CurrentAlertsResponse,
    tags=["alerts"],
)
def current_alerts(
    session_id: str,
    conn: sqlite3.Connection = Depends(get_db),
) -> CurrentAlertsResponse:
    """Live per-rule state: normal / entering / alerting / recovering."""
    return CurrentAlertsResponse(**service.get_current_alerts(conn, session_id))


@app.get(
    "/sessions/{session_id}/alerts/history",
    response_model=AlertHistoryResponse,
    tags=["alerts"],
)
def alert_history(
    session_id: str,
    limit: int = Query(100, ge=1, le=1000),
    cursor: Optional[str] = Query(None, description="Opaque next_cursor from a previous page"),
    source_id: Optional[str] = Query(None, description="Filter to one source"),
    rule_id: Optional[str] = Query(None, description="Filter to one rule"),
    conn: sqlite3.Connection = Depends(get_db),
) -> AlertHistoryResponse:
    """Alert transition events (entered / recovered) in timeline order."""
    return AlertHistoryResponse(
        **service.list_alert_events(conn, session_id, limit, cursor, source_id, rule_id)
    )
