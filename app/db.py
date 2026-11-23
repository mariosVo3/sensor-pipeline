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


def get_connection() -> psycopg.Connection:
    """Open a new connection to PostgreSQL."""
    return psycopg.connect(DATABASE_URL)


def create_tables(conn: psycopg.Connection) -> None:
    """Create the readings table if it does not exist yet."""
    conn.execute(CREATE_READINGS_TABLE)


if __name__ == "__main__":
    # `python -m app.db` creates the tables once
    with get_connection() as conn:
        create_tables(conn)
    print("Tables are ready")
