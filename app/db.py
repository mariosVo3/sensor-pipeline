import os

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

# The two numbers in a producer reading that we store as rows
METRICS = ["pressure_bar", "temperature_c"]


def get_connection() -> psycopg.Connection:
    """Open a new connection to PostgreSQL."""
    return psycopg.connect(DATABASE_URL)


def create_tables(conn: psycopg.Connection) -> None:
    """Create the tables and the index if they do not exist yet."""
    conn.execute(CREATE_READINGS_TABLE)
    conn.execute(CREATE_ALERTS_TABLE)
    conn.execute(CREATE_READINGS_INDEX)


def save_reading(conn: psycopg.Connection, reading: dict) -> None:
    """Save one producer reading as two rows: one per metric."""
    for metric in METRICS:
        conn.execute(
            "INSERT INTO readings (sensor_id, metric, value, timestamp) VALUES (%s, %s, %s, %s)",
            (reading["sensor_id"], metric, reading[metric], reading["timestamp"]),
        )


def save_alert(
    conn: psycopg.Connection, sensor_id: str | None, reason: str, raw: str
) -> None:
    """Save a message that could not be used. `raw` is the original message."""
    conn.execute(
        "INSERT INTO alerts (sensor_id, reason, raw) VALUES (%s, %s, %s)",
        (sensor_id, reason, raw),
    )


def reading_exists(conn: psycopg.Connection, sensor_id: str, timestamp) -> bool:
    """True if readings already has a row for this sensor and timestamp."""
    row = conn.execute(
        "SELECT 1 FROM readings WHERE sensor_id = %s AND timestamp = %s LIMIT 1",
        (sensor_id, timestamp),
    ).fetchone()
    return row is not None


if __name__ == "__main__":
    # `python -m app.db` creates the tables once
    with get_connection() as conn:
        create_tables(conn)
    print("Tables are ready")
