import pytest

from app.validation import validate_reading

SENSOR_ID_PROBLEM = "sensor_id is missing or not a non-empty string"


def good_reading() -> dict:
    return {
        "sensor_id": "sensor-1",
        "timestamp": "2026-11-23T10:00:00+00:00",
        "pressure_bar": 42.1,
        "temperature_c": 21.5,
    }


def test_good_reading_has_no_problems():
    assert validate_reading(good_reading()) == []


def test_missing_sensor_id():
    reading = good_reading()
    del reading["sensor_id"]
    assert validate_reading(reading) == [SENSOR_ID_PROBLEM]


def test_empty_sensor_id():
    reading = good_reading()
    reading["sensor_id"] = ""
    assert validate_reading(reading) == [SENSOR_ID_PROBLEM]


def test_missing_timestamp():
    reading = good_reading()
    del reading["timestamp"]
    assert validate_reading(reading) == ["timestamp is missing"]


def test_invalid_timestamp():
    reading = good_reading()
    reading["timestamp"] = "yesterday"
    assert validate_reading(reading) == ["timestamp is not a valid ISO 8601 date"]


def test_timestamp_that_is_not_a_string():
    reading = good_reading()
    reading["timestamp"] = 12345
    assert validate_reading(reading) == ["timestamp is not a valid ISO 8601 date"]


def test_missing_pressure():
    reading = good_reading()
    del reading["pressure_bar"]
    assert validate_reading(reading) == ["pressure_bar is missing"]


def test_none_temperature():
    reading = good_reading()
    reading["temperature_c"] = None
    assert validate_reading(reading) == ["temperature_c is missing"]


def test_pressure_not_a_number():
    reading = good_reading()
    reading["pressure_bar"] = "high"
    assert validate_reading(reading) == ["pressure_bar is not a number"]


def test_boolean_is_not_a_number():
    reading = good_reading()
    reading["temperature_c"] = True
    assert validate_reading(reading) == ["temperature_c is not a number"]


def test_nan_pressure():
    reading = good_reading()
    reading["pressure_bar"] = float("nan")
    assert validate_reading(reading) == ["pressure_bar is NaN"]


def test_pressure_out_of_range():
    reading = good_reading()
    reading["pressure_bar"] = 500.0
    assert validate_reading(reading) == ["pressure_bar is out of range (20.0 to 80.0)"]


def test_temperature_out_of_range():
    reading = good_reading()
    reading["temperature_c"] = 999.0
    assert validate_reading(reading) == [
        "temperature_c is out of range (-10.0 to 60.0)"
    ]


def test_several_problems_are_all_reported():
    assert validate_reading({}) == [
        SENSOR_ID_PROBLEM,
        "timestamp is missing",
        "pressure_bar is missing",
        "temperature_c is missing",
    ]


@pytest.mark.parametrize("sensor_id", ["   ", 123, ["a"]])
def test_bad_sensor_id(sensor_id):
    reading = good_reading()
    reading["sensor_id"] = sensor_id
    assert validate_reading(reading) == [SENSOR_ID_PROBLEM]


@pytest.mark.parametrize("value", [None, [], "text"])
def test_non_dict_reading(value):
    assert validate_reading(value) == ["reading is not a dict"]


@pytest.mark.parametrize(
    "pressure, temperature", [(20.0, -10.0), (80.0, 60.0), (50, 20)]
)
def test_boundaries_and_ints_are_accepted(pressure, temperature):
    reading = good_reading()
    reading["pressure_bar"] = pressure
    reading["temperature_c"] = temperature
    assert validate_reading(reading) == []


def test_below_range_values():
    reading = good_reading()
    reading["pressure_bar"] = 19.9
    reading["temperature_c"] = -10.1
    assert validate_reading(reading) == [
        "pressure_bar is out of range (20.0 to 80.0)",
        "temperature_c is out of range (-10.0 to 60.0)",
    ]


@pytest.mark.parametrize("value", [float("inf"), float("-inf")])
def test_infinity_is_out_of_range(value):
    reading = good_reading()
    reading["pressure_bar"] = value
    assert validate_reading(reading) == ["pressure_bar is out of range (20.0 to 80.0)"]


def test_huge_integer_is_out_of_range():
    reading = good_reading()
    reading["pressure_bar"] = 10**400
    assert validate_reading(reading)[0].startswith("pressure_bar is out of range")
