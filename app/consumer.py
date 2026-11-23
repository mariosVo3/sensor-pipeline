"""Sensor consumer.

Reads readings from a Kafka topic, checks each one and saves it to
PostgreSQL. Bad or duplicate messages are saved as alerts instead.

Run it (start Kafka and PostgreSQL first, and run `python -m app.db` once):

    python -m app.consumer
"""

import json

import psycopg
from confluent_kafka import Consumer

from app.db import get_connection, reading_exists, save_alert, save_reading
from app.producer import BOOTSTRAP_SERVERS, TOPIC
from app.validation import validate_reading

# What handle_message() returns, so the caller can count each outcome
SAVED = "saved"
ALERT = "alert"


def clean(text: str) -> str:
    """Postgres TEXT cannot hold NUL characters, so replace them."""
    return text.replace("\x00", "\ufffd")


def handle_message(conn, raw: bytes) -> str:
    """Check one message and save it as a reading or as an alert.

    Returns SAVED or ALERT. The caller commits. If the database rejects the
    message, the open transaction is rolled back (so call this once per
    transaction) and the message is saved as an alert instead.
    """
    try:
        return _handle(conn, raw)
    except (psycopg.DataError, psycopg.IntegrityError):
        # Python and Postgres can disagree on what is valid (e.g. odd timestamps).
        # Other errors (e.g. a lost connection) are not the message's fault, so
        # they are not caught here and stop the consumer.
        conn.rollback()
        text = clean(raw.decode("utf-8", errors="replace"))
        try:
            sensor_id = _sensor_id(_parse(raw))
        except (ValueError, RecursionError):
            sensor_id = None
        save_alert(conn, sensor_id, "database rejected message", text)
        return ALERT


def _parse(raw: bytes):
    """Parse JSON. Raises ValueError (or RecursionError for very deep nesting)."""
    return json.loads(raw)


def _sensor_id(reading) -> str | None:
    """The sensor id of a parsed message, or None if it has no usable one."""
    sensor_id = reading.get("sensor_id") if isinstance(reading, dict) else None
    return clean(sensor_id) if isinstance(sensor_id, str) else None


def _handle(conn, raw: bytes) -> str:
    # Keep the original text for alerts (replace bytes that are not valid text)
    text = clean(raw.decode("utf-8", errors="replace"))

    # 1. Is it JSON at all?
    try:
        reading = _parse(raw)
    except (ValueError, RecursionError):  # ValueError also covers bad UTF-8
        save_alert(conn, None, "not valid JSON", text)
        return ALERT

    # Only keep the sensor id for the alert if it is really a string
    sensor_id = _sensor_id(reading)

    # 2. Is the content valid?
    problems = validate_reading(reading)
    if problems:
        save_alert(conn, sensor_id, "; ".join(problems), text)
        return ALERT

    # 3. Did we already store this sensor and timestamp?
    if reading_exists(conn, reading["sensor_id"], reading["timestamp"]):
        save_alert(conn, sensor_id, "duplicate", text)
        return ALERT

    # 4. All good
    save_reading(conn, reading)
    return SAVED


def run(consumer: Consumer, conn) -> dict[str, int]:
    """Handle messages until Ctrl+C. Returns how many were saved and alerted."""
    counts = {SAVED: 0, ALERT: 0}
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue  # nothing new yet
            if msg.error():
                print(f"kafka error: {msg.error()}")
                continue
            outcome = handle_message(conn, msg.value())
            # Commit after every message so one bad message cannot undo the others
            conn.commit()
            # Only now tell Kafka the message is done (at-least-once delivery)
            consumer.commit(message=msg)
            counts[outcome] += 1
    except KeyboardInterrupt:
        print("stopping")
    return counts


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "sensor-consumer",
            "auto.offset.reset": "earliest",  # start from the first message
            "enable.auto.commit": False,  # we commit offsets after the DB commit
        }
    )
    consumer.subscribe([TOPIC])
    conn = get_connection()
    try:
        counts = run(consumer, conn)
    finally:
        consumer.close()
        conn.close()
    print(f"saved {counts[SAVED]} readings, raised {counts[ALERT]} alerts")


if __name__ == "__main__":
    main()
