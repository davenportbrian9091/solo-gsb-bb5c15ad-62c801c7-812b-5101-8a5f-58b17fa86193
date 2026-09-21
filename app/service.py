"""Watermark timeline merge business logic.

Guarantees implemented here:

* A session fixes its source list and ``allowed_lateness_seconds`` at
  creation; reports from unknown sources are rejected.
* The watermark is defined only once *every* source has reported, as
  ``min(source max observed_at) - allowed_lateness``. It is monotonic.
* Buffered readings with ``observed_at`` strictly before the watermark are
  published exactly once, in the deterministic order
  ``(observed_at, source_id, seq)``.
* A reading arriving with ``observed_at`` strictly before the already
  published watermark can never be (re)published: it is diverted to the
  quarantine table and never mutates history.
* Re-posting the same ``(source_id, seq)`` with the same payload is an
  idempotent no-op; a different payload is a 409 conflict.
* Alert rules are consecutive-threshold state machines with hysteresis,
  maintained per source. Only readings that are definitely published feed
  them (in deterministic timeline order); buffered and quarantined readings
  never change alert state.
"""
from __future__ import annotations

import base64
import json
import math
import sqlite3
import uuid
from datetime import timedelta
from typing import Any, Optional

from .database import read_transaction, transaction, utc_now
from .time_utils import format_event_time, parse_event_time, store_event_time


class ServiceError(Exception):
    """Base class for expected service failures mapped to HTTP errors."""


class NotFoundError(ServiceError):
    status_code = 404


class BadRequestError(ServiceError):
    status_code = 400


class ConflictError(ServiceError):
    status_code = 409


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def canonical_payload(value: Any) -> str:
    """Deterministic JSON serialisation so idempotency is payload-based.

    Raises TypeError for values that cannot be represented as JSON; API
    input already went through Pydantic so this stays strict (no silent
    ``str()`` coercion).
    """
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _load_session(conn: sqlite3.Connection, session_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
    ).fetchone()
    if row is None:
        raise NotFoundError(f"session {session_id!r} not found")
    return row


def _source_maxes(conn: sqlite3.Connection, session_id: str) -> dict[str, Optional[str]]:
    rows = conn.execute(
        "SELECT source_id, max_observed_at FROM session_sources WHERE session_id = ?",
        (session_id,),
    ).fetchall()
    return {r["source_id"]: r["max_observed_at"] for r in rows}


def _compute_watermark(
    source_max: dict[str, Optional[str]], allowed_lateness_seconds: float
) -> Optional[str]:
    """Slowest-source max event time minus allowed lateness.

    Returns None while at least one source has never reported: before that
    the slowest source's event-time clock is unknown, so no reading may be
    released.
    """
    if not source_max or any(v is None for v in source_max.values()):
        return None
    slowest = parse_event_time(min(source_max.values()))  # type: ignore[arg-type]
    return store_event_time(slowest - timedelta(seconds=allowed_lateness_seconds))


def _encode_cursor(parts: tuple) -> str:
    raw = json.dumps(parts, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(cursor: str, expected: int) -> tuple:
    try:
        padding = "=" * (-len(cursor) % 4)
        parts = tuple(json.loads(base64.urlsafe_b64decode(cursor + padding)))
    except Exception as exc:  # malformed base64 / json
        raise BadRequestError("malformed cursor") from exc
    if len(parts) != expected:
        raise BadRequestError("malformed cursor")
    return parts


def _reading_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "session_id": row["session_id"],
        "source_id": row["source_id"],
        "seq": row["seq"],
        "observed_at": format_event_time(parse_event_time(row["observed_at"])),
        "value": json.loads(row["payload"]) if row["payload"] is not None else None,
        "published_watermark": format_event_time(parse_event_time(row["published_watermark"])),
    }


def _quarantine_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "session_id": row["session_id"],
        "source_id": row["source_id"],
        "seq": row["seq"],
        "observed_at": format_event_time(parse_event_time(row["observed_at"])),
        "value": json.loads(row["payload"]) if row["payload"] is not None else None,
        "quarantined_watermark": format_event_time(
            parse_event_time(row["quarantined_watermark"])
        ),
        "reason": "behind_published_watermark",
    }


# ---------------------------------------------------------------------------
# sessions
# ---------------------------------------------------------------------------

def create_session(
    conn: sqlite3.Connection,
    source_ids: list[str],
    allowed_lateness_seconds: float,
    session_id: Optional[str] = None,
) -> dict[str, Any]:
    sid = session_id or uuid.uuid4().hex
    created_at = store_event_time(utc_now())
    sources = json.dumps(source_ids)
    try:
        with transaction(conn):
            conn.execute(
                "INSERT INTO sessions (session_id, source_ids, allowed_lateness_seconds,"
                " published_watermark, created_at) VALUES (?, ?, ?, NULL, ?)",
                (sid, sources, allowed_lateness_seconds, created_at),
            )
            conn.executemany(
                "INSERT INTO session_sources (session_id, source_id, max_observed_at,"
                " first_seen_at) VALUES (?, ?, NULL, ?)",
                [(sid, s, created_at) for s in source_ids],
            )
    except sqlite3.IntegrityError as exc:
        raise ConflictError(f"session {sid!r} already exists") from exc
    return {
        "session_id": sid,
        "source_ids": source_ids,
        "allowed_lateness_seconds": allowed_lateness_seconds,
        "created_at": format_event_time(parse_event_time(created_at)),
    }


def get_session(conn: sqlite3.Connection, session_id: str) -> dict[str, Any]:
    with read_transaction(conn):
        row = _load_session(conn, session_id)
        source_max = _source_maxes(conn, session_id)
    all_reported = bool(source_max) and all(v is not None for v in source_max.values())
    effective = (
        _compute_watermark(source_max, row["allowed_lateness_seconds"])
        if all_reported
        else None
    )
    return {
        "session_id": row["session_id"],
        "source_ids": json.loads(row["source_ids"]),
        "allowed_lateness_seconds": row["allowed_lateness_seconds"],
        "published_watermark": (
            format_event_time(parse_event_time(row["published_watermark"]))
            if row["published_watermark"]
            else None
        ),
        "all_sources_reported": all_reported,
        "effective_watermark": format_event_time(parse_event_time(effective)) if effective else None,
        "created_at": format_event_time(parse_event_time(row["created_at"])),
    }


# ---------------------------------------------------------------------------
# ingestion
# ---------------------------------------------------------------------------

def ingest_batch(
    conn: sqlite3.Connection, session_id: str, readings: list[dict[str, Any]]
) -> dict[str, Any]:
    # Validate/normalise the whole batch up front: bad timestamps or
    # undumpable values reject the batch atomically (unknown sources are
    # checked against the session inside the transaction).
    parsed_times: dict[int, str] = {}
    payloads: dict[int, str] = {}
    for i, r in enumerate(readings):
        observed = parse_event_time(r["observed_at"])
        parsed_times[i] = store_event_time(observed)
        payloads[i] = canonical_payload(r["value"])

    with transaction(conn):
        session = _load_session(conn, session_id)
        allowed_sources = set(json.loads(session["source_ids"]))
        unknown = {r["source_id"] for r in readings} - allowed_sources
        if unknown:
            raise BadRequestError(
                f"unknown source_id(s) for this session: {sorted(unknown)}"
            )

        received_at = store_event_time(utc_now())
        lateness = session["allowed_lateness_seconds"]
        published_wm: Optional[str] = session["published_watermark"]

        duplicates = 0
        quarantined = 0

        for i, r in enumerate(readings):
            key = (session_id, r["source_id"], r["seq"])
            observed_store = parsed_times[i]
            payload = payloads[i]

            existing = conn.execute(
                "SELECT status, payload FROM readings"
                " WHERE session_id = ? AND source_id = ? AND seq = ?",
                key,
            ).fetchone()
            if existing is not None:
                if existing["payload"] != payload:
                    # Same key, different measurement: never silently overwrite.
                    raise ConflictError(
                        f"reading ({r['source_id']}, seq={r['seq']}) already exists"
                        " with a different payload"
                    )
                duplicates += 1
                continue

            if published_wm is not None and observed_store < published_wm:
                # Too late: the timeline up to the watermark is immutable.
                conn.execute(
                    "INSERT INTO readings (session_id, source_id, seq, observed_at,"
                    " payload, status, first_received_at, published_watermark,"
                    " quarantined_watermark)"
                    " VALUES (?, ?, ?, ?, ?, 'quarantined', ?, NULL, ?)",
                    (*key, observed_store, payload, received_at, published_wm),
                )
                quarantined += 1
                continue

            conn.execute(
                "INSERT INTO readings (session_id, source_id, seq, observed_at,"
                " payload, status, first_received_at, published_watermark,"
                " quarantined_watermark)"
                " VALUES (?, ?, ?, ?, ?, 'buffered', ?, NULL, NULL)",
                (*key, observed_store, payload, received_at),
            )
            # Advance this source's event-time clock.
            conn.execute(
                "UPDATE session_sources"
                " SET max_observed_at ="
                " CASE WHEN max_observed_at IS NULL OR ? > max_observed_at THEN ?"
                " ELSE max_observed_at END"
                " WHERE session_id = ? AND source_id = ?",
                (observed_store, observed_store, session_id, r["source_id"]),
            )

        # Recompute the watermark from the slowest source.
        source_max = _source_maxes(conn, session_id)
        all_reported = bool(source_max) and all(v is not None for v in source_max.values())
        new_wm = _compute_watermark(source_max, lateness) if all_reported else None
        published_now = 0
        alerts_entered = 0
        alerts_recovered = 0
        if new_wm is not None and (published_wm is None or new_wm > published_wm):
            cur = conn.execute(
                "UPDATE readings SET status = 'published', published_watermark = ?"
                " WHERE session_id = ? AND status = 'buffered' AND observed_at < ?",
                (new_wm, session_id, new_wm),
            )
            published_now = cur.rowcount
            conn.execute(
                "UPDATE sessions SET published_watermark = ? WHERE session_id = ?",
                (new_wm, session_id),
            )
            published_wm = new_wm
            # Post-publish alerting: only the readings that were definitely
            # published just now may advance the alert state machines.
            if published_now:
                alerts_entered, alerts_recovered = _advance_alerts(
                    conn, session_id, new_wm
                )

        published_total = conn.execute(
            "SELECT COUNT(*) AS c FROM readings WHERE session_id = ? AND status = 'published'",
            (session_id,),
        ).fetchone()["c"]
        buffered = conn.execute(
            "SELECT COUNT(*) AS c FROM readings WHERE session_id = ? AND status = 'buffered'",
            (session_id,),
        ).fetchone()["c"]

    return {
        "session_id": session_id,
        "received": len(readings),
        "duplicates": duplicates,
        "quarantined": quarantined,
        "published_now": published_now,
        "published_total": published_total,
        "buffered": buffered,
        "all_sources_reported": all_reported,
        "published_watermark": (
            format_event_time(parse_event_time(published_wm)) if published_wm else None
        ),
        "alerts_entered": alerts_entered,
        "alerts_recovered": alerts_recovered,
    }


# ---------------------------------------------------------------------------
# reads
# ---------------------------------------------------------------------------

def list_published(
    conn: sqlite3.Connection,
    session_id: str,
    limit: int,
    cursor: Optional[str] = None,
) -> dict[str, Any]:
    where = "session_id = ? AND status = 'published'"
    params: list[Any] = [session_id]
    if cursor:
        ts, src, seq = _decode_cursor(cursor, 3)
        # Keyset on the deterministic timeline order
        # (observed_at, source_id, seq).
        where += (
            " AND (observed_at > ?"
            " OR (observed_at = ? AND source_id > ?)"
            " OR (observed_at = ? AND source_id = ? AND seq > ?))"
        )
        params += [ts, ts, src, ts, src, seq]

    with read_transaction(conn):
        session = _load_session(conn, session_id)
        rows = conn.execute(
            f"SELECT * FROM readings WHERE {where}"
            " ORDER BY observed_at, source_id, seq LIMIT ?",
            (*params, limit + 1),
        ).fetchall()

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = None
    if has_more and page:
        last = page[-1]
        next_cursor = _encode_cursor((last["observed_at"], last["source_id"], last["seq"]))

    return {
        "session_id": session_id,
        "next_cursor": next_cursor,
        "published_watermark": (
            format_event_time(parse_event_time(session["published_watermark"]))
            if session["published_watermark"]
            else None
        ),
        "readings": [_reading_out(r) for r in page],
    }


def list_quarantined(
    conn: sqlite3.Connection,
    session_id: str,
    limit: int,
    cursor: Optional[str] = None,
) -> dict[str, Any]:
    where = "session_id = ? AND status = 'quarantined'"
    params: list[Any] = [session_id]
    if cursor:
        received, src, seq = _decode_cursor(cursor, 3)
        where += (
            " AND (first_received_at > ?"
            " OR (first_received_at = ? AND source_id > ?)"
            " OR (first_received_at = ? AND source_id = ? AND seq > ?))"
        )
        params += [received, received, src, received, src, seq]

    with read_transaction(conn):
        _load_session(conn, session_id)
        rows = conn.execute(
            f"SELECT * FROM readings WHERE {where}"
            " ORDER BY first_received_at, source_id, seq LIMIT ?",
            (*params, limit + 1),
        ).fetchall()

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = None
    if has_more and page:
        last = page[-1]
        next_cursor = _encode_cursor(
            (last["first_received_at"], last["source_id"], last["seq"])
        )

    return {
        "session_id": session_id,
        "next_cursor": next_cursor,
        "readings": [_quarantine_out(r) for r in page],
    }


# ---------------------------------------------------------------------------
# alerting: consecutive-threshold state machines with hysteresis
# ---------------------------------------------------------------------------

def _numeric_value(value: Any) -> Optional[float]:
    """Evaluation value of a published reading.

    Only real JSON numbers count (bools excluded); anything else — strings,
    objects, null, non-finite numbers — is non-evaluable and returns None.
    """
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        num = float(value)
        return num if math.isfinite(num) else None
    return None


def _derived_state(status: str, enter_streak: int, recover_streak: int) -> str:
    """User-facing phase derived from the machine state plus live streaks."""
    if status == "alerting":
        return "recovering" if recover_streak > 0 else "alerting"
    return "entering" if enter_streak > 0 else "normal"


def _advance_alert(
    conn: sqlite3.Connection,
    rule: sqlite3.Row,
    value: Any,
    observed_at: str,
    seq: int,
    recorded_at: str,
) -> Optional[str]:
    """Advance one rule's state machine by one published reading.

    Returns the transition event (``entered``/``recovered``) or None. A
    non-numeric value breaks any consecutive run but never transitions.
    """
    state = conn.execute(
        "SELECT * FROM alert_states WHERE rule_id = ?", (rule["rule_id"],)
    ).fetchone()
    status = state["status"]
    enter_streak = state["enter_streak"]
    recover_streak = state["recover_streak"]
    event = None

    num = _numeric_value(value)
    if num is None:
        enter_streak = 0
        recover_streak = 0
    else:
        if rule["direction"] == "above":
            breaching = num >= rule["enter_threshold"]
            recovering = num <= rule["recover_threshold"]
        else:
            breaching = num <= rule["enter_threshold"]
            recovering = num >= rule["recover_threshold"]
        # The hysteresis band (validated at rule creation) makes breaching
        # and recovering mutually exclusive.
        if status == "normal":
            if breaching:
                enter_streak += 1
                if enter_streak >= rule["enter_count"]:
                    status = "alerting"
                    event = "entered"
                    enter_streak = 0
                    recover_streak = 0
            else:
                enter_streak = 0
        else:  # alerting
            if recovering:
                recover_streak += 1
                if recover_streak >= rule["recover_count"]:
                    status = "normal"
                    event = "recovered"
                    enter_streak = 0
                    recover_streak = 0
            else:
                recover_streak = 0

    conn.execute(
        "UPDATE alert_states SET status = ?, enter_streak = ?, recover_streak = ?,"
        " last_value = ?, last_observed_at = ?, last_seq = ?, state_changed_at = ?,"
        " updated_at = ? WHERE rule_id = ?",
        (
            status,
            enter_streak,
            recover_streak,
            num,
            observed_at,
            seq,
            observed_at if event else state["state_changed_at"],
            recorded_at,
            rule["rule_id"],
        ),
    )
    if event:
        conn.execute(
            "INSERT INTO alert_events (rule_id, session_id, source_id, event, value,"
            " observed_at, seq, streak, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                rule["rule_id"],
                rule["session_id"],
                rule["source_id"],
                event,
                num,
                observed_at,
                seq,
                rule["enter_count"] if event == "entered" else rule["recover_count"],
                recorded_at,
            ),
        )
    return event


def _advance_alerts(
    conn: sqlite3.Connection, session_id: str, published_wm: str
) -> tuple[int, int]:
    """Feed the readings published under ``published_wm`` to the alert rules.

    Only readings definitely published in this batch are evaluated, in the
    deterministic timeline order ``(observed_at, source_id, seq)``; buffered
    and quarantined readings never reach the state machines. Returns the
    ``(entered, recovered)`` transition counts.
    """
    rules = conn.execute(
        "SELECT * FROM alert_rules WHERE session_id = ? ORDER BY created_at, rule_id",
        (session_id,),
    ).fetchall()
    if not rules:
        return 0, 0
    by_source: dict[str, list[sqlite3.Row]] = {}
    for rule in rules:
        by_source.setdefault(rule["source_id"], []).append(rule)

    # Watermarks are strictly increasing, so published_watermark = new_wm
    # identifies exactly the rows this batch published.
    newly = conn.execute(
        "SELECT source_id, seq, observed_at, payload FROM readings"
        " WHERE session_id = ? AND status = 'published' AND published_watermark = ?"
        " ORDER BY observed_at, source_id, seq",
        (session_id, published_wm),
    ).fetchall()

    recorded_at = store_event_time(utc_now())
    entered = 0
    recovered = 0
    for row in newly:
        value = json.loads(row["payload"]) if row["payload"] is not None else None
        for rule in by_source.get(row["source_id"], ()):
            event = _advance_alert(
                conn, rule, value, row["observed_at"], row["seq"], recorded_at
            )
            if event == "entered":
                entered += 1
            elif event == "recovered":
                recovered += 1
    return entered, recovered


def _alert_rule_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "rule_id": row["rule_id"],
        "session_id": row["session_id"],
        "source_id": row["source_id"],
        "direction": row["direction"],
        "enter_threshold": row["enter_threshold"],
        "recover_threshold": row["recover_threshold"],
        "enter_count": row["enter_count"],
        "recover_count": row["recover_count"],
        "created_at": format_event_time(parse_event_time(row["created_at"])),
    }


def _alert_event_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "rule_id": row["rule_id"],
        "source_id": row["source_id"],
        "event": row["event"],
        "value": row["value"],
        "observed_at": format_event_time(parse_event_time(row["observed_at"])),
        "seq": row["seq"],
        "streak": row["streak"],
        "recorded_at": format_event_time(parse_event_time(row["recorded_at"])),
    }


def create_alert_rule(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    source_id: str,
    direction: str,
    enter_threshold: float,
    recover_threshold: float,
    enter_count: int,
    recover_count: int,
    rule_id: Optional[str] = None,
) -> dict[str, Any]:
    with transaction(conn):
        session = _load_session(conn, session_id)
        allowed_sources = set(json.loads(session["source_ids"]))
        if source_id not in allowed_sources:
            raise BadRequestError(f"unknown source_id for this session: {source_id!r}")
        if direction not in ("above", "below"):
            raise BadRequestError("direction must be 'above' or 'below'")
        if not math.isfinite(enter_threshold) or not math.isfinite(recover_threshold):
            raise BadRequestError("thresholds must be finite numbers")
        if enter_count < 1 or recover_count < 1:
            raise BadRequestError("enter_count and recover_count must be >= 1")
        if direction == "above" and not recover_threshold < enter_threshold:
            raise BadRequestError(
                "invalid rule: for direction 'above', recover_threshold must be"
                " strictly less than enter_threshold (hysteresis band)"
            )
        if direction == "below" and not recover_threshold > enter_threshold:
            raise BadRequestError(
                "invalid rule: for direction 'below', recover_threshold must be"
                " strictly greater than enter_threshold (hysteresis band)"
            )

        rid = rule_id or uuid.uuid4().hex
        now = store_event_time(utc_now())
        try:
            conn.execute(
                "INSERT INTO alert_rules (rule_id, session_id, source_id, direction,"
                " enter_threshold, recover_threshold, enter_count, recover_count,"
                " created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    rid,
                    session_id,
                    source_id,
                    direction,
                    enter_threshold,
                    recover_threshold,
                    enter_count,
                    recover_count,
                    now,
                ),
            )
            conn.execute(
                "INSERT INTO alert_states (rule_id, session_id, source_id, status,"
                " enter_streak, recover_streak, last_value, last_observed_at,"
                " last_seq, state_changed_at, updated_at)"
                " VALUES (?, ?, ?, 'normal', 0, 0, NULL, NULL, NULL, NULL, ?)",
                (rid, session_id, source_id, now),
            )
        except sqlite3.IntegrityError as exc:
            raise ConflictError(f"alert rule {rid!r} already exists") from exc

        rule = conn.execute(
            "SELECT * FROM alert_rules WHERE rule_id = ?", (rid,)
        ).fetchone()
        # Replay the already-published stream of this source so the state
        # machine reflects the full timeline, exactly as if the rule had
        # existed from the start; later batches advance it incrementally.
        backlog = conn.execute(
            "SELECT observed_at, seq, payload FROM readings"
            " WHERE session_id = ? AND source_id = ? AND status = 'published'"
            " ORDER BY observed_at, source_id, seq",
            (session_id, source_id),
        ).fetchall()
        for row in backlog:
            value = json.loads(row["payload"]) if row["payload"] is not None else None
            _advance_alert(conn, rule, value, row["observed_at"], row["seq"], now)
        state = conn.execute(
            "SELECT * FROM alert_states WHERE rule_id = ?", (rid,)
        ).fetchone()

    out = _alert_rule_out(rule)
    out["state"] = _derived_state(
        state["status"], state["enter_streak"], state["recover_streak"]
    )
    out["replayed_readings"] = len(backlog)
    return out


def list_alert_rules(conn: sqlite3.Connection, session_id: str) -> dict[str, Any]:
    with read_transaction(conn):
        _load_session(conn, session_id)
        rows = conn.execute(
            "SELECT * FROM alert_rules WHERE session_id = ?"
            " ORDER BY created_at, rule_id",
            (session_id,),
        ).fetchall()
    return {"session_id": session_id, "rules": [_alert_rule_out(r) for r in rows]}


def get_current_alerts(conn: sqlite3.Connection, session_id: str) -> dict[str, Any]:
    with read_transaction(conn):
        _load_session(conn, session_id)
        rows = conn.execute(
            "SELECT r.rule_id, r.session_id, r.source_id, r.direction,"
            " r.enter_threshold, r.recover_threshold, r.enter_count, r.recover_count,"
            " r.created_at, s.status, s.enter_streak, s.recover_streak, s.last_value,"
            " s.last_observed_at, s.last_seq, s.state_changed_at"
            " FROM alert_rules r JOIN alert_states s ON s.rule_id = r.rule_id"
            " WHERE r.session_id = ? ORDER BY r.created_at, r.rule_id",
            (session_id,),
        ).fetchall()

    alerts = []
    for row in rows:
        alerts.append(
            {
                **_alert_rule_out(row),
                "state": _derived_state(
                    row["status"], row["enter_streak"], row["recover_streak"]
                ),
                "enter_streak": row["enter_streak"],
                "recover_streak": row["recover_streak"],
                "last_value": row["last_value"],
                "last_observed_at": (
                    format_event_time(parse_event_time(row["last_observed_at"]))
                    if row["last_observed_at"]
                    else None
                ),
                "last_seq": row["last_seq"],
                "state_changed_at": (
                    format_event_time(parse_event_time(row["state_changed_at"]))
                    if row["state_changed_at"]
                    else None
                ),
            }
        )
    return {
        "session_id": session_id,
        # "recovering" is still an active alert: recovery is not confirmed yet.
        "active": sum(1 for a in alerts if a["state"] in ("alerting", "recovering")),
        "alerts": alerts,
    }


def list_alert_events(
    conn: sqlite3.Connection,
    session_id: str,
    limit: int,
    cursor: Optional[str] = None,
    source_id: Optional[str] = None,
    rule_id: Optional[str] = None,
) -> dict[str, Any]:
    where = "session_id = ?"
    params: list[Any] = [session_id]
    if source_id is not None:
        where += " AND source_id = ?"
        params.append(source_id)
    if rule_id is not None:
        where += " AND rule_id = ?"
        params.append(rule_id)
    if cursor:
        ts, src, seq, rid = _decode_cursor(cursor, 4)
        # Keyset on the deterministic event order
        # (observed_at, source_id, seq, rule_id).
        where += (
            " AND (observed_at > ?"
            " OR (observed_at = ? AND source_id > ?)"
            " OR (observed_at = ? AND source_id = ? AND seq > ?)"
            " OR (observed_at = ? AND source_id = ? AND seq = ? AND rule_id > ?))"
        )
        params += [ts, ts, src, ts, src, seq, ts, src, seq, rid]

    with read_transaction(conn):
        _load_session(conn, session_id)
        rows = conn.execute(
            f"SELECT * FROM alert_events WHERE {where}"
            " ORDER BY observed_at, source_id, seq, rule_id LIMIT ?",
            (*params, limit + 1),
        ).fetchall()

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = None
    if has_more and page:
        last = page[-1]
        next_cursor = _encode_cursor(
            (last["observed_at"], last["source_id"], last["seq"], last["rule_id"])
        )

    return {
        "session_id": session_id,
        "next_cursor": next_cursor,
        "events": [_alert_event_out(r) for r in page],
    }
