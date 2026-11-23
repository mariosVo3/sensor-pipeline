import json
import random
from datetime import datetime

import pytest

import app.producer as producer_module
from app.producer import (
    PRESSURE_RANGE_BAR,
    SENSOR_IDS,
    TEMPERATURE_RANGE_C,
    make_reading,
    parse_args,
    report_delivery,
    run,
    send_reading,
)


class FakeProducer:
    """Stands in for the Kafka producer and remembers what it was given."""

    def __init__(self):
        self.messages = []

    def produce(self, topic, key, value, on_delivery):
        self.messages.append({"topic": topic, "key": key, "value": value})

    def poll(self, timeout):
        return 0


def test_make_reading_has_values_in_normal_range():
    reading = make_reading("sensor-1", random.Random(42), bad_rate=0)

    assert reading["sensor_id"] == "sensor-1"
    assert PRESSURE_RANGE_BAR[0] <= reading["pressure_bar"] <= PRESSURE_RANGE_BAR[1]
    assert TEMPERATURE_RANGE_C[0] <= reading["temperature_c"] <= TEMPERATURE_RANGE_C[1]
    assert datetime.fromisoformat(reading["timestamp"]).tzinfo is not None


def test_make_reading_can_be_broken_on_purpose():
    rng = random.Random(42)

    readings = [make_reading("sensor-1", rng, bad_rate=1) for _ in range(20)]

    missing = [r for r in readings if r["pressure_bar"] is None]
    out_of_range = [r for r in readings if r["temperature_c"] == 999.0]
    # Every reading is broken, and both kinds of breakage show up
    assert len(missing) + len(out_of_range) == len(readings)
    assert missing
    assert out_of_range


def test_send_reading_sends_json_keyed_by_sensor():
    producer = FakeProducer()
    reading = make_reading("sensor-2", random.Random(42), bad_rate=0)

    send_reading(producer, "sensor-readings", reading)

    [message] = producer.messages
    assert message["topic"] == "sensor-readings"
    assert message["key"] == "sensor-2"
    assert json.loads(message["value"]) == reading


def test_run_sends_count_readings_one_sensor_after_another():
    producer = FakeProducer()

    sent = run(producer, "sensor-readings", count=4, interval=0, rng=random.Random(42))

    assert sent == 4
    keys = [message["key"] for message in producer.messages]
    assert keys == [SENSOR_IDS[0], SENSOR_IDS[1], SENSOR_IDS[2], SENSOR_IDS[0]]


def test_run_does_not_sleep_after_the_last_reading(monkeypatch):
    sleeps = []
    monkeypatch.setattr(producer_module.time, "sleep", sleeps.append)

    run(FakeProducer(), "sensor-readings", count=3, interval=5, rng=random.Random(42))

    assert sleeps == [5, 5]


def test_report_delivery_counts_failures(monkeypatch):
    monkeypatch.setattr(producer_module, "failed_deliveries", 0)

    report_delivery(None, None)
    report_delivery("broker down", None)

    assert producer_module.failed_deliveries == 1


@pytest.mark.parametrize(
    "argv", [["--count", "0"], ["--count", "-1"], ["--interval", "-1"]]
)
def test_parse_args_rejects_bad_values(argv):
    with pytest.raises(SystemExit):
        parse_args(argv)


def test_parse_args_defaults():
    args = parse_args([])

    assert args.count is None
    assert args.interval == 1.0
