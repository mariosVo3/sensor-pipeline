"""Fake sensor producer.

Makes up pressure and temperature readings for a few sensors and sends
them to a Kafka topic as JSON.

Run it (start Kafka first with `docker compose up kafka`):

    python -m app.producer --count 10 --interval 0.5
"""

import argparse
import json
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import KafkaError, Message, Producer

TOPIC = os.getenv("KAFKA_TOPIC", "sensor-readings")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

SENSOR_IDS = ["sensor-1", "sensor-2", "sensor-3"]

# Normal ranges for a pipeline sensor
PRESSURE_RANGE_BAR = (20.0, 80.0)
TEMPERATURE_RANGE_C = (-10.0, 60.0)

# Share of readings we break on purpose, so the consumer has something to catch
BAD_READING_RATE = 0.05

Reading = dict[str, str | float | None]


def make_reading(
    sensor_id: str, rng: random.Random, bad_rate: float = BAD_READING_RATE
) -> Reading:
    """Make one fake reading. Sometimes it is broken on purpose."""
    reading: Reading = {
        "sensor_id": sensor_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pressure_bar": round(rng.uniform(*PRESSURE_RANGE_BAR), 2),
        "temperature_c": round(rng.uniform(*TEMPERATURE_RANGE_C), 2),
    }
    if rng.random() < bad_rate:
        if rng.random() < 0.5:
            reading["pressure_bar"] = None  # missing value
        else:
            reading["temperature_c"] = 999.0  # out of range
    return reading


class DeliveryReport:
    """Counts the messages Kafka told us it could not store."""

    def __init__(self) -> None:
        self.failed = 0

    def __call__(self, err: KafkaError | None, msg: Message) -> None:
        """Called by Kafka once it has stored the message, or failed to."""
        if err is not None:
            self.failed += 1
            print(f"delivery failed: {err}")


def send_reading(
    producer: Producer, topic: str, reading: Reading, report: DeliveryReport
) -> None:
    """Send one reading to Kafka as JSON. The sensor id is the message key."""
    producer.produce(
        topic,
        key=str(reading["sensor_id"]),
        value=json.dumps(reading),
        on_delivery=report,
    )
    # Let the client run delivery callbacks for messages already sent
    producer.poll(0)


def run(
    producer: Producer,
    topic: str,
    count: int | None,
    interval: float,
    rng: random.Random,
    report: DeliveryReport,
) -> int:
    """Send `count` readings (forever if None), one sensor after another."""
    sent = 0
    while count is None or sent < count:
        sensor_id = SENSOR_IDS[sent % len(SENSOR_IDS)]
        reading = make_reading(sensor_id, rng)
        send_reading(producer, topic, reading, report)
        print(f"sent {reading}")
        sent += 1
        # No need to wait after the last reading
        if count is None or sent < count:
            time.sleep(interval)
    return sent


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Read --count and --interval from the command line and check them."""
    parser = argparse.ArgumentParser(description="Send fake sensor readings to Kafka.")
    parser.add_argument(
        "--count", type=int, default=None, help="how many readings (default: forever)"
    )
    parser.add_argument(
        "--interval", type=float, default=1.0, help="seconds between readings"
    )
    args = parser.parse_args(argv)
    if args.count is not None and args.count < 1:
        parser.error("--count must be at least 1")
    if args.interval < 0:
        parser.error("--interval cannot be negative")
    return args


def main() -> None:
    args = parse_args()

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    report = DeliveryReport()
    try:
        run(producer, TOPIC, args.count, args.interval, random.Random(), report)
    except KeyboardInterrupt:
        print("stopping")
    finally:
        # Wait until Kafka has every message we handed over.
        # flush() returns how many are still not delivered.
        undelivered = producer.flush(10)

    if undelivered or report.failed:
        # SystemExit with a message prints it and exits with code 1
        raise SystemExit(
            f"not delivered: {undelivered} still waiting, {report.failed} failed"
        )


if __name__ == "__main__":
    main()
