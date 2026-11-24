from collections.abc import Iterator
from pathlib import Path
from typing import Annotated

import psycopg
from fastapi import Depends, FastAPI, Query
from fastapi.responses import FileResponse

from app.db import get_alerts, get_connection, get_readings, get_sensors

# The app version lives here, in one place
VERSION = "0.1.0"

app = FastAPI(title="Sensor Pipeline")

# The dashboard page lives next to this file
DASHBOARD_FILE = Path(__file__).parent / "static" / "dashboard.html"


@app.get("/", include_in_schema=False)
def dashboard() -> FileResponse:
    """The live dashboard page."""
    return FileResponse(DASHBOARD_FILE)


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


@app.get("/sensors")
def sensors(conn: Db) -> list[str]:
    """Sensor ids that have readings, sorted."""
    return get_sensors(conn)


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
