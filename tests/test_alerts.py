"""End-to-end behaviour tests for the post-publish alerting state machine."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test.db")
    monkeypatch.setenv("SENSOR_DB_PATH", db_path)
    from app import database, main

    main.app.state.db_path = db_path
    database.init_db(db_path)
    with TestClient(main.app) as c:
        yield c


def create_session(client, sources=("a", "b"), lateness=0.0, session_id=None):
    body = {"source_ids": list(sources), "allowed_lateness_seconds": lateness}
    if session_id:
        body["session_id"] = session_id
    r = client.post("/sessions", json=body)
    assert r.status_code == 201, r.text
    return r.json()["session_id"]


def report(client, sid, readings):
    return client.post(f"/sessions/{sid}/readings", json={"readings": readings})


def ts(i):
    # i seconds after midnight (tests use i < 60).
    return f"2026-01-01T00:00:{i:02d}Z"


def push_values(client, sid, values, start=1, trailer_value=None, source="a", other="b",
                other_value=None):
    """Publish ``values`` on ``source`` in one batch, in event order.

    A trailer reading one step ahead (plus the other source) moves the
    watermark past the last real value, so exactly ``values`` (plus any
    previous trailer) are published and evaluated. The trailer itself stays
    buffered until the next push.
    """
    readings = [
        {"source_id": source, "seq": start + k, "observed_at": ts(start + k), "value": v}
        for k, v in enumerate(values)
    ]
    n = start + len(values)
    readings.append(
        {"source_id": source, "seq": n, "observed_at": ts(n), "value": trailer_value}
    )
    readings.append({"source_id": other, "seq": n, "observed_at": ts(n), "value": other_value})
    return report(client, sid, readings)


def create_rule(client, sid, **overrides):
    body = {
        "source_id": "a",
        "direction": "above",
        "enter_threshold": 10.0,
        "recover_threshold": 5.0,
        "enter_count": 2,
        "recover_count": 2,
    }
    body.update(overrides)
    return client.post(f"/sessions/{sid}/alert-rules", json=body)


def current(client, sid):
    return client.get(f"/sessions/{sid}/alerts/current").json()


def history(client, sid, **params):
    return client.get(f"/sessions/{sid}/alerts/history", params=params).json()


class TestRuleValidation:
    def test_unknown_session_404(self, client):
        assert create_rule(client, "nope").status_code == 404
        assert client.get("/sessions/nope/alert-rules").status_code == 404
        assert client.get("/sessions/nope/alerts/current").status_code == 404
        assert client.get("/sessions/nope/alerts/history").status_code == 404

    def test_unknown_source_400(self, client):
        sid = create_session(client)
        r = create_rule(client, sid, source_id="zzz")
        assert r.status_code == 400
        assert "unknown source" in r.json()["detail"]

    def test_hysteresis_band_enforced(self, client):
        sid = create_session(client)
        # above: recover must be strictly below enter
        assert create_rule(
            client, sid, enter_threshold=10, recover_threshold=10
        ).status_code == 400
        assert create_rule(
            client, sid, enter_threshold=10, recover_threshold=11
        ).status_code == 400
        # below: recover must be strictly above enter
        assert create_rule(
            client, sid, direction="below", enter_threshold=5, recover_threshold=5
        ).status_code == 400
        assert create_rule(
            client, sid, direction="below", enter_threshold=5, recover_threshold=4
        ).status_code == 400
        # valid bands are accepted
        assert create_rule(client, sid).status_code == 201
        assert create_rule(
            client, sid, direction="below", enter_threshold=5, recover_threshold=8
        ).status_code == 201

    def test_counts_and_direction_validated(self, client):
        sid = create_session(client)
        assert create_rule(client, sid, enter_count=0).status_code == 400
        assert create_rule(client, sid, recover_count=0).status_code == 400
        assert create_rule(client, sid, enter_count=-3).status_code == 400
        assert create_rule(client, sid, direction="sideways").status_code == 400

    def test_duplicate_rule_id_conflicts(self, client):
        sid = create_session(client)
        assert create_rule(client, sid, rule_id="r1").status_code == 201
        r = create_rule(client, sid, rule_id="r1")
        assert r.status_code == 409

    def test_create_and_list_rules(self, client):
        sid = create_session(client)
        r = create_rule(client, sid, rule_id="r1", enter_count=3)
        assert r.status_code == 201
        body = r.json()
        assert body["rule_id"] == "r1"
        assert body["state"] == "normal"
        assert body["replayed_readings"] == 0

        rules = client.get(f"/sessions/{sid}/alert-rules").json()["rules"]
        assert len(rules) == 1
        assert rules[0]["rule_id"] == "r1"
        assert rules[0]["enter_count"] == 3
        assert rules[0]["direction"] == "above"


class TestStateMachine:
    def test_enter_requires_consecutive_breaches(self, client):
        sid = create_session(client)
        create_rule(client, sid, enter_count=3)

        r = push_values(client, sid, [20, 20], trailer_value=20)
        assert r.json()["alerts_entered"] == 0
        (alert,) = current(client, sid)["alerts"]
        assert alert["state"] == "entering"
        assert alert["enter_streak"] == 2

        # The trailer (20 @ t3) plus one more breach complete the streak.
        r = push_values(client, sid, [20], start=4)
        assert r.json()["alerts_entered"] == 1
        (alert,) = current(client, sid)["alerts"]
        assert alert["state"] == "alerting"
        assert alert["enter_streak"] == 0
        assert alert["last_value"] == 20.0
        assert alert["state_changed_at"] == ts(3)

        events = history(client, sid)["events"]
        assert len(events) == 1
        assert events[0]["event"] == "entered"
        assert events[0]["observed_at"] == ts(3)
        assert events[0]["streak"] == 3

    def test_hysteresis_dead_band_resets_streak(self, client):
        sid = create_session(client)
        create_rule(client, sid, enter_count=2)
        # 7 sits in the dead band (5, 10): it breaks the consecutive run
        # without recovering anything.
        push_values(client, sid, [20, 7, 20])
        (alert,) = current(client, sid)["alerts"]
        assert alert["state"] == "entering"
        assert alert["enter_streak"] == 1
        assert history(client, sid)["events"] == []

    def test_recovery_requires_consecutive_recovering_readings(self, client):
        sid = create_session(client)
        create_rule(client, sid, enter_count=1, recover_count=2)

        push_values(client, sid, [20])
        assert current(client, sid)["alerts"][0]["state"] == "alerting"

        # One recovering reading: still active, now in the recovering phase.
        push_values(client, sid, [1], start=3, trailer_value=1)
        alert = current(client, sid)["alerts"][0]
        assert alert["state"] == "recovering"
        assert alert["recover_streak"] == 1
        assert current(client, sid)["active"] == 1

        # The trailer (1 @ t4) completes the recovery streak.
        r = push_values(client, sid, [1], start=5)
        assert r.json()["alerts_recovered"] == 1
        alert = current(client, sid)["alerts"][0]
        assert alert["state"] == "normal"
        assert current(client, sid)["active"] == 0

        events = history(client, sid)["events"]
        assert [e["event"] for e in events] == ["entered", "recovered"]
        assert events[1]["observed_at"] == ts(4)
        assert events[1]["streak"] == 2

    def test_breach_while_alerting_sustains_alert(self, client):
        sid = create_session(client)
        create_rule(client, sid, enter_count=1, recover_count=2)

        push_values(client, sid, [20])
        # recovering, then a new breach resets the recovery streak.
        push_values(client, sid, [1, 20, 1], start=3, trailer_value=1)
        alert = current(client, sid)["alerts"][0]
        assert alert["state"] == "recovering"
        assert alert["recover_streak"] == 1

        push_values(client, sid, [1], start=7)
        assert current(client, sid)["alerts"][0]["state"] == "normal"
        events = history(client, sid)["events"]
        assert [e["event"] for e in events] == ["entered", "recovered"]

    def test_below_direction(self, client):
        sid = create_session(client)
        create_rule(
            client, sid, direction="below", enter_threshold=5, recover_threshold=10,
            enter_count=2, recover_count=1,
        )
        push_values(client, sid, [3, 4])
        assert current(client, sid)["alerts"][0]["state"] == "alerting"
        push_values(client, sid, [11], start=4, trailer_value=11)
        # trailer 11 @ t5 is still buffered; only 11 @ t4 was evaluated
        assert current(client, sid)["alerts"][0]["state"] == "normal"
        events = history(client, sid)["events"]
        assert [e["event"] for e in events] == ["entered", "recovered"]

    def test_non_numeric_value_breaks_streak(self, client):
        sid = create_session(client)
        create_rule(client, sid, enter_count=2)
        push_values(client, sid, [20, "sensor-error", 20])
        (alert,) = current(client, sid)["alerts"]
        assert alert["state"] == "entering"
        assert alert["enter_streak"] == 1
        assert alert["last_value"] == 20.0

        # A trailing non-numeric reading resets the streak and shows as
        # last_value=None.
        push_values(client, sid, ["oops"], start=5)
        (alert,) = current(client, sid)["alerts"]
        assert alert["enter_streak"] == 0
        assert alert["last_value"] is None

    def test_multiple_rules_on_same_source_are_independent(self, client):
        sid = create_session(client)
        create_rule(client, sid, rule_id="fast", enter_count=1)
        create_rule(client, sid, rule_id="slow", enter_count=3)

        push_values(client, sid, [20, 20], trailer_value=20)
        alerts = {a["rule_id"]: a for a in current(client, sid)["alerts"]}
        assert alerts["fast"]["state"] == "alerting"
        assert alerts["slow"]["state"] == "entering"
        assert alerts["slow"]["enter_streak"] == 2


class TestOnlyPublishedReadingsAdvanceState:
    def test_buffered_readings_do_not_advance(self, client):
        sid = create_session(client)
        create_rule(client, sid, enter_count=1)
        # Watermark lands exactly on t1, so the breaching reading stays buffered.
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": ts(1), "value": 20},
            {"source_id": "b", "seq": 1, "observed_at": ts(1)},
        ])
        assert r.json()["published_now"] == 0
        (alert,) = current(client, sid)["alerts"]
        assert alert["state"] == "normal"
        assert alert["enter_streak"] == 0
        assert history(client, sid)["events"] == []

        # Advance the watermark: the same reading is published and evaluated.
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": ts(2)},
            {"source_id": "b", "seq": 2, "observed_at": ts(2)},
        ])
        assert current(client, sid)["alerts"][0]["state"] == "alerting"

    def test_quarantined_readings_do_not_change_state(self, client):
        sid = create_session(client)
        create_rule(client, sid, enter_count=1, recover_count=1)
        push_values(client, sid, [20])
        assert current(client, sid)["alerts"][0]["state"] == "alerting"

        # Late recovering reading behind the published watermark: quarantined,
        # must not recover the alert.
        r = report(client, sid, [
            {"source_id": "a", "seq": 50, "observed_at": "2025-12-31T23:59:59Z", "value": 0},
        ])
        assert r.json()["quarantined"] == 1
        assert r.json()["alerts_recovered"] == 0
        (alert,) = current(client, sid)["alerts"]
        assert alert["state"] == "alerting"
        assert alert["recover_streak"] == 0
        assert [e["event"] for e in history(client, sid)["events"]] == ["entered"]

        # A late breaching reading is equally inert.
        r = report(client, sid, [
            {"source_id": "a", "seq": 51, "observed_at": "2025-12-31T23:59:58Z", "value": 99},
        ])
        assert r.json()["quarantined"] == 1
        assert r.json()["alerts_entered"] == 0
        assert len(history(client, sid)["events"]) == 1

    def test_rule_created_after_publish_replays_stream(self, client):
        sid = create_session(client)
        push_values(client, sid, [20, 20, 1, 20], trailer_value=20)

        r = create_rule(client, sid, enter_count=2, recover_count=1)
        assert r.status_code == 201
        body = r.json()
        assert body["replayed_readings"] == 4
        # 20,20 -> entered; 1 -> recovered; 20 -> streak 1 (entering again)
        assert body["state"] == "entering"

        events = history(client, sid)["events"]
        assert [e["event"] for e in events] == ["entered", "recovered"]
        assert events[0]["observed_at"] == ts(2)
        assert events[1]["observed_at"] == ts(3)

        # And the machine keeps advancing incrementally from there: the
        # trailer (20 @ t5) plus the new breach complete the streak.
        push_values(client, sid, [20], start=6)
        assert current(client, sid)["alerts"][0]["state"] == "alerting"


class TestAlertQueries:
    def test_history_pagination_and_filters(self, client):
        sid = create_session(client)
        r = create_rule(client, sid, enter_count=1, recover_count=1, rule_id="r1")
        rid = r.json()["rule_id"]
        create_rule(client, sid, source_id="b", rule_id="r2", enter_count=1)
        # a: 5 transitions from alternating values; b readings carry a
        # breaching value but only get published once the watermark passes.
        push_values(client, sid, [20, 1, 20, 1, 20], other_value=20)
        # Flush the buffered b reading (and one more a value) so r2 fires too.
        push_values(client, sid, [1], start=7, other_value=20)

        events = []
        cursor = None
        for _ in range(10):
            page = history(client, sid, limit=2, **({"cursor": cursor} if cursor else {}))
            events.extend(page["events"])
            cursor = page["next_cursor"]
            if not cursor:
                break
        assert len(events) == 7
        keys = [(e["observed_at"], e["source_id"], e["seq"], e["rule_id"]) for e in events]
        assert keys == sorted(keys)
        a_events = [e for e in events if e["rule_id"] == rid]
        assert [e["event"] for e in a_events] == [
            "entered", "recovered", "entered", "recovered", "entered", "recovered"
        ]

        # Filters narrow the result set.
        only_b = history(client, sid, source_id="b")["events"]
        assert len(only_b) == 1 and only_b[0]["event"] == "entered"
        only_r1 = history(client, sid, rule_id=rid)["events"]
        assert len(only_r1) == 6
        assert history(client, sid, source_id="b", rule_id=rid)["events"] == []

    def test_bad_cursor_400(self, client):
        sid = create_session(client)
        r = client.get(f"/sessions/{sid}/alerts/history", params={"cursor": "%%%garbage"})
        assert r.status_code == 400

    def test_current_alerts_empty_without_rules(self, client):
        sid = create_session(client)
        body = current(client, sid)
        assert body == {"session_id": sid, "active": 0, "alerts": []}
