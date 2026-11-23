from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app.db import create_tables, get_connection, save_alert, save_reading
from app.main import app, get_db


@pytest.fixture
def conn():
    """A clean database connection; every change is rolled back after the test."""
    conn = get_connection()
    conn.execute("DROP TABLE IF EXISTS readings")
    conn.execute("DROP TABLE IF EXISTS alerts")
    create_tables(conn)
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def client(conn):
    # Make the endpoints use the test connection, so they see the uncommitted rows
    app.dependency_overrides[get_db] = lambda: conn
    yield TestClient(app)
    app.dependency_overrides.clear()


def add_reading(conn, sensor_id, hour):
    save_reading(
        conn,
        {
            "sensor_id": sensor_id,
            "pressure_bar": 42.1,
            "temperature_c": 20.5,
            "timestamp": datetime(2026, 11, 23, hour, 0, tzinfo=timezone.utc),
        },
    )


def test_readings_newest_first(client, conn):
    add_reading(conn, "sensor-1", 9)
    add_reading(conn, "sensor-1", 10)
    add_reading(conn, "sensor-2", 11)

    response = client.get("/sensors/sensor-1/readings")

    assert response.status_code == 200
    timestamps = [r["timestamp"] for r in response.json()]
    assert len(timestamps) == 4
    assert timestamps[0] == "2026-11-23T10:00:00Z"
    assert timestamps[-1] == "2026-11-23T09:00:00Z"
    assert {"metric", "value", "timestamp"} == set(response.json()[0])


def test_readings_limit(client, conn):
    add_reading(conn, "sensor-1", 9)

    response = client.get("/sensors/sensor-1/readings?limit=1")

    assert len(response.json()) == 1


def test_readings_unknown_sensor_is_empty_list(client):
    response = client.get("/sensors/nope/readings")

    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.parametrize("limit", [0, 1001, "abc"])
def test_bad_limit_is_rejected(client, limit):
    assert client.get(f"/alerts?limit={limit}").status_code == 422


def test_alerts_newest_first(client, conn):
    save_alert(conn, "sensor-1", "first", "raw-1")
    save_alert(conn, None, "second", "raw-2")

    response = client.get("/alerts")

    assert response.status_code == 200
    body = response.json()
    assert [a["reason"] for a in body] == ["second", "first"]
    assert body[0]["sensor_id"] is None
    assert body[1]["raw"] == "raw-1"
    assert "created_at" in body[0]


def test_alerts_limit(client, conn):
    save_alert(conn, None, "a", "x")
    save_alert(conn, None, "b", "y")

    assert len(client.get("/alerts?limit=1").json()) == 1
