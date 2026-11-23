import json

import pytest

from app.consumer import ALERT, SAVED, handle_message
from app.db import create_tables, get_connection

GOOD = {
    "sensor_id": "sensor-1",
    "timestamp": "2026-11-23T10:00:00+00:00",
    "pressure_bar": 50.0,
    "temperature_c": 20.0,
}


@pytest.fixture
def conn():
    """A connection with fresh tables. Every change is rolled back after the test."""
    conn = get_connection()
    conn.execute("DROP TABLE IF EXISTS readings")
    conn.execute("DROP TABLE IF EXISTS alerts")
    create_tables(conn)
    yield conn
    conn.rollback()
    conn.close()


def count(conn, table):
    return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def alert_reasons(conn):
    return [r[0] for r in conn.execute("SELECT reason FROM alerts").fetchall()]


def test_good_reading_is_saved(conn):
    assert handle_message(conn, json.dumps(GOOD).encode()) == SAVED

    assert count(conn, "readings") == 2  # one row per metric
    assert count(conn, "alerts") == 0


def test_invalid_json_raises_alert(conn):
    assert handle_message(conn, b"{not json") == ALERT

    assert alert_reasons(conn) == ["not valid JSON"]
    assert count(conn, "readings") == 0


def test_bytes_that_are_not_text_raise_alert(conn):
    assert handle_message(conn, b"\xff\xfe") == ALERT

    assert alert_reasons(conn) == ["not valid JSON"]


def test_invalid_reading_raises_one_alert_with_all_problems(conn):
    bad = {**GOOD, "pressure_bar": None, "temperature_c": 999.0}

    assert handle_message(conn, json.dumps(bad).encode()) == ALERT

    reasons = alert_reasons(conn)
    assert len(reasons) == 1
    assert "pressure_bar is missing; temperature_c is out of range" in reasons[0]
    assert count(conn, "readings") == 0


def test_json_that_is_not_an_object_raises_alert(conn):
    assert handle_message(conn, b"[1, 2]") == ALERT

    assert alert_reasons(conn) == ["reading is not a dict"]


def test_duplicate_raises_alert(conn):
    raw = json.dumps(GOOD).encode()
    handle_message(conn, raw)

    assert handle_message(conn, raw) == ALERT

    assert alert_reasons(conn) == ["duplicate"]
    assert count(conn, "readings") == 2  # the second copy was not saved
