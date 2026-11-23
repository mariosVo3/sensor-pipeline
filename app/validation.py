"""Check one sensor reading and say what is wrong with it."""

import math
from datetime import datetime

from app.producer import PRESSURE_RANGE_BAR, TEMPERATURE_RANGE_C


def check_number(reading: dict, name: str, low_high: tuple[float, float]) -> list[str]:
    """Check one numeric field: it must be there, be a number and be in range."""
    value = reading.get(name)
    if value is None:
        return [f"{name} is missing"]
    # bool is a kind of int in Python, but True/False is not a real measurement
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return [f"{name} is not a number"]
    if math.isnan(value):
        return [f"{name} is NaN"]
    low, high = low_high
    if not low <= value <= high:
        return [f"{name} is out of range ({low} to {high})"]
    return []


def validate_reading(reading: dict) -> list[str]:
    """Return a list of problems. An empty list means the reading is good."""
    problems = []

    # sensor_id must be a non-empty string
    if not reading.get("sensor_id"):
        problems.append("sensor_id is missing")

    # timestamp must be a string that is a valid ISO 8601 date
    timestamp = reading.get("timestamp")
    if timestamp is None:
        problems.append("timestamp is missing")
    else:
        try:
            datetime.fromisoformat(timestamp)
        except (TypeError, ValueError):
            problems.append("timestamp is not a valid ISO 8601 date")

    problems += check_number(reading, "pressure_bar", PRESSURE_RANGE_BAR)
    problems += check_number(reading, "temperature_c", TEMPERATURE_RANGE_C)
    return problems
