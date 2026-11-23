import os
from datetime import datetime

import psycopg

# Where to find the database. The default matches docker-compose.yml (local dev only).
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://sensors:sensors@localhost:5432/sensors"
)

# One row per sensor reading, e.g. ("sensor-1", "pressure", 3.2, 2026-11-23 10:00 UTC)
CREATE_READINGS_TABLE = """
CREATE TABLE IF NOT EXISTS readings (
    id BIGSERIAL PRIMARY KEY,
    sensor_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
)
"""

# Messages that could not be used, kept with the original text so we can look at them later
CREATE_ALERTS_TABLE = """
CREATE TABLE IF NOT EXISTS alerts (
    id BIGSERIAL PRIMARY KEY,
    sensor_id TEXT,
    reason TEXT NOT NULL,
    raw TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

# Makes "does this sensor and timestamp exist?" lookups fast
CREATE_READINGS_INDEX = """
CREATE INDEX IF NOT EXISTS readings_sensor_id_timestamp_idx
ON readings (sensor_id, timestamp)
"""

# The database refuses a second row for the same sensor, metric and timestamp,
# even if two consumers try to insert it at the same moment
CREATE_READINGS_UNIQUE_INDEX = """
CREATE UNIQUE INDEX IF NOT EXISTS readings_sensor_id_metric_timestamp_key
ON readings (sensor_id, metric, timestamp)
"""

# The two numbers in a producer reading that we store as rows
METRICS = ["pressure_bar", "temperature_c"]


def get_connection() -> psycopg.Connection:
    """Open a new connection to PostgreSQL (fails after DATABASE_CONNECT_TIMEOUT seconds)."""
    # Give up after a few seconds instead of waiting forever if the database is down
    # DATABASE_URL is read once when the module is imported; the timeout is read on every call
    raw = os.environ.get("DATABASE_CONNECT_TIMEOUT", "5")
    try:
        timeout = int(raw)
    except ValueError:
        raise ValueError(
            f"DATABASE_CONNECT_TIMEOUT must be a whole number of seconds, got {raw!r}"
        ) from None
    if timeout < 1:
        # libpq treats 0 or less as "wait forever", which is the hang we want to avoid
        raise ValueError(f"DATABASE_CONNECT_TIMEOUT must be 1 or more, got {timeout}")
    return psycopg.connect(DATABASE_URL, connect_timeout=timeout)


def create_tables(conn: psycopg.Connection) -> None:
    """Create the tables and the index if they do not exist yet."""
    conn.execute(CREATE_READINGS_TABLE)
    conn.execute(CREATE_ALERTS_TABLE)
    conn.execute(CREATE_READINGS_INDEX)
    conn.execute(CREATE_READINGS_UNIQUE_INDEX)


def save_reading(conn: psycopg.Connection, reading: dict) -> bool:
    """Save one producer reading as two rows: one per metric.

    Returns True if the rows were saved, or False if the reading was a duplicate
    (the unique index blocked an insert, so nothing new was stored for it).

    The caller must check the reading first (keys present, numbers valid) and call
    save_alert() for bad input. A missing key raises KeyError before anything is inserted.

    Nothing is committed here. Both rows are in one transaction, so they are saved
    together or not at all, but the caller must call conn.commit() (or use
    `with get_connection() as conn`) or the rows are lost.
    """
    # Read every value first so a missing key cannot leave only one row inserted
    rows = [
        (reading["sensor_id"], metric, reading[metric], reading["timestamp"])
        for metric in METRICS
    ]
    inserted = 0
    for row in rows:
        # ON CONFLICT DO NOTHING skips a duplicate instead of raising an error
        cursor = conn.execute(
            "INSERT INTO readings (sensor_id, metric, value, timestamp) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            row,
        )
        inserted += cursor.rowcount  # 1 if inserted, 0 if skipped
    return inserted == len(rows)


def save_alert(
    conn: psycopg.Connection, sensor_id: str | None, reason: str, raw: str
) -> None:
    """Save a message that could not be used. `raw` is the original message.

    Like save_reading(), this does not commit.
    """
    conn.execute(
        "INSERT INTO alerts (sensor_id, reason, raw) VALUES (%s, %s, %s)",
        (sensor_id, reason, raw),
    )


def reading_exists(
    conn: psycopg.Connection, sensor_id: str, timestamp: datetime | str
) -> bool:
    """True if readings already has a row for this sensor and timestamp."""
    row = conn.execute(
        "SELECT 1 FROM readings WHERE sensor_id = %s AND timestamp = %s LIMIT 1",
        (sensor_id, timestamp),
    ).fetchone()
    return row is not None


def get_readings(conn: psycopg.Connection, sensor_id: str, limit: int) -> list[dict]:
    """Newest readings for one sensor, newest first. Unknown sensor gives an empty list."""
    rows = conn.execute(
        "SELECT metric, value, timestamp FROM readings "
        "WHERE sensor_id = %s ORDER BY timestamp DESC, id DESC LIMIT %s",
        (sensor_id, limit),
    ).fetchall()
    return [
        {"metric": metric, "value": value, "timestamp": timestamp}
        for metric, value, timestamp in rows
    ]


def get_alerts(conn: psycopg.Connection, limit: int) -> list[dict]:
    """Newest alerts, newest first."""
    rows = conn.execute(
        "SELECT sensor_id, reason, raw, created_at FROM alerts "
        "ORDER BY created_at DESC, id DESC LIMIT %s",
        (limit,),
    ).fetchall()
    return [
        {"sensor_id": sensor_id, "reason": reason, "raw": raw, "created_at": created_at}
        for sensor_id, reason, raw, created_at in rows
    ]


if __name__ == "__main__":
    # `python -m app.db` creates the tables once
    with get_connection() as conn:
        create_tables(conn)
    print("Tables are ready")
