import time
from datetime import datetime, timezone

import psycopg
import pytest

from app.db import (
    create_tables,
    get_connection,
    reading_exists,
    save_alert,
    save_reading,
)

MEASURED_AT = datetime(2026, 11, 23, 10, 0, tzinfo=timezone.utc)


@pytest.fixture
def conn():
    """A database connection that starts without the readings table
    and undoes every change after the test."""
    conn = get_connection()
    # Without this, a table left over from `python -m app.db` would hide a broken create_tables()
    conn.execute("DROP TABLE IF EXISTS readings")
    conn.execute("DROP TABLE IF EXISTS alerts")
    # No commit on purpose: this connection sees its own drops, and the rollback undoes them
    yield conn
    conn.rollback()
    conn.close()


def test_connection_works(conn):
    result = conn.execute("SELECT 1").fetchone()

    assert result == (1,)


def test_create_tables_makes_readings_table(conn):
    create_tables(conn)

    rows = conn.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'readings'
        ORDER BY ordinal_position
        """
    ).fetchall()

    assert rows == [
        ("id", "bigint"),
        ("sensor_id", "text"),
        ("metric", "text"),
        ("value", "double precision"),
        ("timestamp", "timestamp with time zone"),
    ]


def test_create_tables_twice_is_safe(conn):
    # No assert needed: the test fails if the second call raises an error
    create_tables(conn)
    create_tables(conn)


def test_can_save_and_read_a_reading(conn):
    create_tables(conn)

    conn.execute(
        "INSERT INTO readings (sensor_id, metric, value, timestamp) VALUES (%s, %s, %s, %s)",
        ("sensor-1", "pressure_bar", 3.2, MEASURED_AT),
    )
    row = conn.execute(
        "SELECT sensor_id, metric, value, timestamp FROM readings WHERE sensor_id = %s",
        ("sensor-1",),
    ).fetchone()

    assert row == ("sensor-1", "pressure_bar", 3.2, MEASURED_AT)


def test_create_tables_makes_alerts_table(conn):
    create_tables(conn)

    rows = conn.execute(
        """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'alerts'
        ORDER BY ordinal_position
        """
    ).fetchall()

    assert rows == [
        ("id", "bigint", "NO"),
        ("sensor_id", "text", "YES"),
        ("reason", "text", "NO"),
        ("raw", "text", "NO"),
        ("created_at", "timestamp with time zone", "NO"),
    ]


def test_create_tables_makes_readings_index(conn):
    create_tables(conn)

    rows = conn.execute(
        "SELECT indexname FROM pg_indexes "
        "WHERE tablename = 'readings' AND indexname = 'readings_sensor_id_timestamp_idx'"
    ).fetchall()

    assert len(rows) == 1


def test_save_reading_makes_two_rows(conn):
    create_tables(conn)
    reading = {
        "sensor_id": "sensor-1",
        # The producer sends the timestamp as an ISO string
        "timestamp": MEASURED_AT.isoformat(),
        "pressure_bar": 3.2,
        "temperature_c": 21.5,
    }

    save_reading(conn, reading)
    rows = conn.execute(
        "SELECT sensor_id, metric, value, timestamp FROM readings ORDER BY metric"
    ).fetchall()

    assert rows == [
        ("sensor-1", "pressure_bar", 3.2, MEASURED_AT),
        ("sensor-1", "temperature_c", 21.5, MEASURED_AT),
    ]


def test_save_reading_with_missing_key_saves_nothing(conn):
    create_tables(conn)
    reading = {"sensor_id": "sensor-1", "timestamp": MEASURED_AT, "pressure_bar": 3.2}

    with pytest.raises(KeyError):
        save_reading(conn, reading)

    assert conn.execute("SELECT count(*) FROM readings").fetchone() == (0,)


def test_save_alert_stores_a_row(conn):
    create_tables(conn)

    save_alert(conn, "sensor-1", "pressure missing", '{"sensor_id": "sensor-1"}')
    row = conn.execute("SELECT sensor_id, reason, raw FROM alerts").fetchone()

    assert row == ("sensor-1", "pressure missing", '{"sensor_id": "sensor-1"}')


def test_save_alert_allows_missing_sensor_id(conn):
    create_tables(conn)

    save_alert(conn, None, "not JSON", "garbage")
    row = conn.execute("SELECT sensor_id, raw FROM alerts").fetchone()

    assert row == (None, "garbage")


def test_reading_exists(conn):
    create_tables(conn)
    save_reading(
        conn,
        {
            "sensor_id": "sensor-1",
            # A datetime also works; reading_exists() is tested with one too
            "timestamp": MEASURED_AT,
            "pressure_bar": 3.2,
            "temperature_c": 21.5,
        },
    )

    assert reading_exists(conn, "sensor-1", MEASURED_AT) is True
    assert reading_exists(conn, "sensor-2", MEASURED_AT) is False
    assert (
        reading_exists(conn, "sensor-1", datetime(2026, 1, 1, tzinfo=timezone.utc))
        is False
    )


def test_get_connection_fails_fast_when_database_is_down(monkeypatch):
    # Nothing listens on port 1, so connecting must fail quickly instead of hanging
    monkeypatch.setattr("app.db.DATABASE_URL", "postgresql://u:p@localhost:1/x")
    start = time.monotonic()
    with pytest.raises(psycopg.OperationalError):
        get_connection()
    assert time.monotonic() - start < 10
