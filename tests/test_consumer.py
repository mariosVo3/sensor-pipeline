import json

import pytest

from app.consumer import ALERT, SAVED, handle_message, run
from app.db import create_tables, get_connection

GOOD = {
    "sensor_id": "sensor-1",
    "timestamp": "2026-11-23T10:00:00+00:00",
    "pressure_bar": 50.0,
    "temperature_c": 20.0,
}


@pytest.fixture
def conn():
    """A connection with fresh tables (dropped and recreated for each test)."""
    conn = get_connection()
    conn.execute("DROP TABLE IF EXISTS readings")
    conn.execute("DROP TABLE IF EXISTS alerts")
    create_tables(conn)
    conn.commit()  # so a rollback inside handle_message keeps the tables
    yield conn
    conn.rollback()
    conn.close()


def count(conn, table):
    return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def alert_reasons(conn):
    return [r[0] for r in conn.execute("SELECT reason FROM alerts").fetchall()]


def test_good_reading_is_saved(conn):
    assert handle_message(conn, json.dumps(GOOD).encode()) == SAVED

    rows = conn.execute(
        "SELECT sensor_id, metric, value FROM readings ORDER BY metric"
    ).fetchall()
    assert rows == [
        ("sensor-1", "pressure_bar", 50.0),
        ("sensor-1", "temperature_c", 20.0),
    ]
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


def test_duplicate_caught_by_database_raises_alert(conn, monkeypatch):
    # Simulate a race: the pre-check says "new", but the database finds a duplicate
    raw = json.dumps(GOOD).encode()
    handle_message(conn, raw)
    monkeypatch.setattr("app.consumer.reading_exists", lambda *args: False)

    assert handle_message(conn, raw) == ALERT

    assert alert_reasons(conn) == ["duplicate"]
    assert count(conn, "readings") == 2


def test_naive_timestamp_raises_alert(conn):
    bad = {**GOOD, "timestamp": "2026-11-23T10:00:00"}

    assert handle_message(conn, json.dumps(bad).encode()) == ALERT

    assert alert_reasons(conn) == ["timestamp has no timezone"]
    assert count(conn, "readings") == 0


def test_not_a_number_values_raise_alert(conn):
    raw = b'{"sensor_id": "s", "timestamp": "2026-11-23T10:00:00+00:00", "pressure_bar": "5", "temperature_c": NaN}'

    assert handle_message(conn, raw) == ALERT

    assert "pressure_bar is not a number; temperature_c is NaN" in alert_reasons(conn)[0]


def test_database_rejection_becomes_alert(conn):
    bad = {**GOOD, "sensor_id": "bad\u0000id"}  # passes validation, Postgres refuses

    assert handle_message(conn, json.dumps(bad).encode()) == ALERT

    assert alert_reasons(conn) == ["database rejected message"]
    assert conn.execute("SELECT sensor_id FROM alerts").fetchone()[0] == "bad\ufffdid"
    assert count(conn, "readings") == 0
    # The connection is still usable afterwards
    assert handle_message(conn, json.dumps(GOOD).encode()) == SAVED


def test_raw_nul_byte_alert_is_saved(conn):
    assert handle_message(conn, b"{\x00") == ALERT

    assert alert_reasons(conn) == ["not valid JSON"]


class FakeMessage:
    def __init__(self, value=None, error=None):
        self._value = value
        self._error = error

    def value(self):
        return self._value

    def error(self):
        return self._error


class FakeConsumer:
    """poll() returns the given messages, then raises KeyboardInterrupt."""

    def __init__(self, messages):
        self.messages = list(messages)
        self.committed = []

    def commit(self, message):
        self.committed.append(message)

    def poll(self, timeout):
        if not self.messages:
            raise KeyboardInterrupt
        return self.messages.pop(0)


def test_run_counts_commits_and_skips_kafka_errors(conn):
    good = json.dumps(GOOD).encode()
    consumer = FakeConsumer(
        [
            None,
            FakeMessage(error="boom"),
            FakeMessage(good),
            FakeMessage(good),  # duplicate
            FakeMessage(b"nope"),
        ]
    )

    assert run(consumer, conn) == {SAVED: 1, ALERT: 2}

    assert count(conn, "readings") == 2
    assert count(conn, "alerts") == 2
    assert len(consumer.committed) == 3  # not committed for the Kafka error


def test_connection_errors_are_not_swallowed(conn, monkeypatch):
    import psycopg

    def boom(*args):
        raise psycopg.OperationalError("connection lost")

    monkeypatch.setattr("app.consumer.save_reading", boom)

    with pytest.raises(psycopg.OperationalError):
        handle_message(conn, json.dumps(GOOD).encode())


def test_huge_integer_raises_alert(conn):
    raw = ('{"sensor_id": "s", "timestamp": "2026-11-23T10:00:00+00:00", '
           '"pressure_bar": 1' + "0" * 400 + ', "temperature_c": 20}').encode()
    assert handle_message(conn, raw) == ALERT
    assert "pressure_bar is out of range" in alert_reasons(conn)[0]


def test_deeply_nested_json_raises_alert(conn):
    assert handle_message(conn, b"[" * 100000) == ALERT
    assert alert_reasons(conn) == ["not valid JSON"]


def test_empty_message_raises_alert(conn):
    assert handle_message(conn, None) == ALERT
    assert alert_reasons(conn) == ["empty message"]


def test_unexpected_error_becomes_alert(conn, monkeypatch):
    def boom(reading):
        raise RuntimeError("surprise")

    monkeypatch.setattr("app.consumer.validate_reading", boom)

    assert handle_message(conn, json.dumps(GOOD).encode()) == ALERT
    assert alert_reasons(conn) == ["could not process message: RuntimeError"]
    assert count(conn, "readings") == 0


def test_interface_errors_are_not_swallowed(conn, monkeypatch):
    import psycopg

    def boom(reading):
        raise psycopg.InterfaceError("connection closed")

    monkeypatch.setattr("app.consumer.validate_reading", boom)

    with pytest.raises(psycopg.InterfaceError):
        handle_message(conn, json.dumps(GOOD).encode())
