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
    values = {r["metric"]: r["value"] for r in response.json()}
    assert values == {"pressure_bar": 42.1, "temperature_c": 20.5}


def test_readings_limit_keeps_newest_rows(client, conn):
    for hour in (9, 10, 11):
        add_reading(conn, "sensor-1", hour)

    response = client.get("/sensors/sensor-1/readings?limit=2")

    # limit counts rows: 2 rows = both metrics of the newest reading
    body = response.json()
    assert len(body) == 2
    assert {r["timestamp"] for r in body} == {"2026-11-23T11:00:00Z"}


def test_readings_limit_one_returns_one_row(client, conn):
    add_reading(conn, "sensor-1", 9)
    add_reading(conn, "sensor-1", 10)

    body = client.get("/sensors/sensor-1/readings?limit=1").json()

    assert len(body) == 1
    assert body[0]["timestamp"] == "2026-11-23T10:00:00Z"


def test_readings_limit_1000_is_accepted(client):
    assert client.get("/sensors/sensor-1/readings?limit=1000").status_code == 200


def test_alerts_empty_list(client):
    response = client.get("/alerts")

    assert response.status_code == 200
    assert response.json() == []


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

    body = client.get("/alerts?limit=1").json()
    assert len(body) == 1
    assert body[0]["reason"] == "b"


def test_alerts_limit_1000_is_accepted(client):
    assert client.get("/alerts?limit=1000").status_code == 200


def test_sensors_sorted_and_distinct(client, conn):
    add_reading(conn, "sensor-2", 9)
    add_reading(conn, "sensor-1", 9)
    add_reading(conn, "sensor-1", 10)

    response = client.get("/sensors")

    assert response.status_code == 200
    assert response.json() == ["sensor-1", "sensor-2"]


def test_sensors_empty_list(client):
    assert client.get("/sensors").json() == []


def test_dashboard_page(client):
    response = client.get("/")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert 'id="pressure-chart"' in response.text
    assert 'id="temperature-chart"' in response.text
    assert 'id="alerts-table"' in response.text
