from collections.abc import Iterator
from typing import Annotated

import psycopg
from fastapi import Depends, FastAPI, Query

from app.db import get_alerts, get_connection, get_readings

# The app version lives here, in one place
VERSION = "0.1.0"

app = FastAPI(title="Sensor Pipeline")


@app.get("/health")
def health() -> dict[str, str]:
    """Simple check that the API is running."""
    return {"status": "ok"}


@app.get("/version")
def version() -> dict[str, str]:
    """Return the app version."""
    return {"version": VERSION}


def get_db() -> Iterator[psycopg.Connection]:
    """Open a database connection for one request and close it afterwards."""
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()


# Annotated attaches extra info to a type. These aliases are shortcuts used by both endpoints:
# Limit = an int that must be 1 to 1000 (the default of 100 is set in each function signature)
# Db = a psycopg connection that FastAPI opens per request through get_db
Limit = Annotated[int, Query(ge=1, le=1000)]
Db = Annotated[psycopg.Connection, Depends(get_db)]


@app.get("/sensors/{sensor_id}/readings")
def sensor_readings(sensor_id: str, conn: Db, limit: Limit = 100) -> list[dict]:
    """Newest readings for one sensor, newest first.

    `limit` counts rows, not readings. Each reading is stored as two rows (one per metric),
    so limit=100 returns about 50 readings, and limit=1 can return half a reading.
    """
    return get_readings(conn, sensor_id, limit)


@app.get("/alerts")
def alerts(conn: Db, limit: Limit = 100) -> list[dict]:
    """Newest alerts, newest first."""
    return get_alerts(conn, limit)
