from app.validation import validate_reading


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
    assert validate_reading(reading) == ["sensor_id is missing"]


def test_empty_sensor_id():
    reading = good_reading()
    reading["sensor_id"] = ""
    assert validate_reading(reading) == ["sensor_id is missing"]


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
    assert len(validate_reading({})) == 4
