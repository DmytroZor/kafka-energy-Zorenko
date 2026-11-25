"""Shared local lab configuration; keep host/port configurable per environment."""
import os
from datetime import datetime, timezone

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CASSANDRA = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
P1_TOPIC = os.getenv("P1_TOPIC", "power-station-data-zorenko")


def ts_utc(milliseconds: int) -> datetime:
    return datetime.fromtimestamp(int(milliseconds) / 1000.0, tz=timezone.utc)


def bucket_hour(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def utc_now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)
