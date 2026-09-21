"""Pydantic request/response models."""
from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


class ReadingIn(BaseModel):
    source_id: str = Field(..., min_length=1, description="Fixed sensor source declared on the session")
    seq: int = Field(..., ge=0, description="Per-source strictly increasing sequence number")
    observed_at: str = Field(..., description="Event time, ISO 8601 (UTC or with offset)")
    value: Optional[Any] = Field(None, description="Arbitrary JSON-serializable measurement payload")

    @field_validator("source_id")
    @classmethod
    def _source_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("source_id must not be blank")
        return v

    @field_validator("observed_at")
    @classmethod
    def _observed_not_blank(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("observed_at must be a non-empty ISO 8601 string")
        return v


class CreateSessionIn(BaseModel):
    source_ids: list[str] = Field(..., min_length=1, description="Fixed source list for this session")
    allowed_lateness_seconds: float = Field(
        ..., ge=0, description="How far behind the slowest source the watermark lags"
    )
    session_id: Optional[str] = Field(None, min_length=1, description="Optional caller-chosen id")

    @field_validator("source_ids")
    @classmethod
    def _sources_unique_nonblank(cls, v: list[str]) -> list[str]:
        if any(not isinstance(s, str) or not s.strip() for s in v):
            raise ValueError("source_ids must be non-empty strings")
        if len(set(v)) != len(v):
            raise ValueError("source_ids must be unique")
        return v


class ReadingOut(BaseModel):
    session_id: str
    source_id: str
    seq: int
    observed_at: str
    value: Optional[Any] = None
    published_watermark: str


class QuarantineOut(BaseModel):
    session_id: str
    source_id: str
    seq: int
    observed_at: str
    value: Optional[Any] = None
    quarantined_watermark: str
    reason: str = "behind_published_watermark"


class ReportResponse(BaseModel):
    session_id: str
    received: int
    duplicates: int
    quarantined: int
    published_now: int
    published_total: int
    buffered: int
    all_sources_reported: bool
    published_watermark: Optional[str] = None
    alerts_entered: int = 0
    alerts_recovered: int = 0


class PullResponse(BaseModel):
    session_id: str
    next_cursor: Optional[str] = None
    published_watermark: Optional[str] = None
    readings: list[ReadingOut]


class QuarantineResponse(BaseModel):
    session_id: str
    next_cursor: Optional[str] = None
    readings: list[QuarantineOut]


class SessionOut(BaseModel):
    session_id: str
    source_ids: list[str]
    allowed_lateness_seconds: float
    published_watermark: Optional[str] = None
    all_sources_reported: bool
    effective_watermark: Optional[str] = None
    created_at: str


class CreatedSessionResponse(BaseModel):
    session_id: str
    source_ids: list[str]
    allowed_lateness_seconds: float
    created_at: str


# ---------------------------------------------------------------------------
# alerting
# ---------------------------------------------------------------------------

class AlertRuleIn(BaseModel):
    """Alert rule payload.

    Value semantics (unknown source, bad hysteresis band, counts < 1) are
    validated in the service layer so they map to a clear 400, not 422.
    """

    source_id: str = Field(..., min_length=1, description="Source of the session this rule watches")
    direction: str = Field(
        "above", description="'above' alerts on high values, 'below' on low values"
    )
    enter_threshold: float = Field(..., description="Breaching side of the hysteresis band")
    recover_threshold: float = Field(..., description="Recovering side of the hysteresis band")
    enter_count: int = Field(..., description="Consecutive breaching readings required to enter")
    recover_count: int = Field(..., description="Consecutive recovering readings required to recover")
    rule_id: Optional[str] = Field(None, min_length=1, description="Optional caller-chosen id")


class AlertRuleOut(BaseModel):
    rule_id: str
    session_id: str
    source_id: str
    direction: str
    enter_threshold: float
    recover_threshold: float
    enter_count: int
    recover_count: int
    created_at: str


class CreatedAlertRuleResponse(AlertRuleOut):
    state: str
    replayed_readings: int


class AlertRulesResponse(BaseModel):
    session_id: str
    rules: list[AlertRuleOut]


class AlertStatusOut(AlertRuleOut):
    state: str
    enter_streak: int
    recover_streak: int
    last_value: Optional[float] = None
    last_observed_at: Optional[str] = None
    last_seq: Optional[int] = None
    state_changed_at: Optional[str] = None


class CurrentAlertsResponse(BaseModel):
    session_id: str
    active: int
    alerts: list[AlertStatusOut]


class AlertEventOut(BaseModel):
    rule_id: str
    source_id: str
    event: str
    value: Optional[float] = None
    observed_at: str
    seq: int
    streak: int
    recorded_at: str


class AlertHistoryResponse(BaseModel):
    session_id: str
    next_cursor: Optional[str] = None
    events: list[AlertEventOut]
