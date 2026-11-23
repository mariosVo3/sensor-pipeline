from datetime import datetime, timezone

import pytest

from app.db import create_tables, get_connection


@pytest.fixture
def conn():
    """A database connection that undoes every change after the test."""
    conn = get_connection()
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
        WHERE table_name = 'readings'
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
    create_tables(conn)
    create_tables(conn)


def test_can_save_and_read_a_reading(conn):
    create_tables(conn)
    measured_at = datetime(2026, 11, 23, 10, 0, tzinfo=timezone.utc)

    conn.execute(
        "INSERT INTO readings (sensor_id, metric, value, timestamp) VALUES (%s, %s, %s, %s)",
        ("sensor-1", "pressure", 3.2, measured_at),
    )
    row = conn.execute(
        "SELECT sensor_id, metric, value, timestamp FROM readings WHERE sensor_id = %s",
        ("sensor-1",),
    ).fetchone()

    assert row == ("sensor-1", "pressure", 3.2, measured_at)
