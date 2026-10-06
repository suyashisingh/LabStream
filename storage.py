import hashlib
import json
from contextlib import contextmanager
from datetime import datetime, timezone
import psycopg
from pymongo import MongoClient
from config import Config

SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
  id BIGSERIAL PRIMARY KEY, instrument_id TEXT NOT NULL, metric TEXT NOT NULL,
  value DOUBLE PRECISION NOT NULL, unit TEXT NOT NULL, observed_at TIMESTAMPTZ NOT NULL,
  raw_payload_id TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (instrument_id, metric, observed_at)
);
CREATE TABLE IF NOT EXISTS audit_events (
  id BIGSERIAL PRIMARY KEY, event_type TEXT NOT NULL, instrument_id TEXT,
  reason TEXT, payload_sha256 TEXT NOT NULL, kafka_topic TEXT, kafka_partition INTEGER,
  kafka_offset BIGINT, occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS readings_instrument_time_idx ON readings (instrument_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS audit_events_occurred_idx ON audit_events (occurred_at DESC);
"""

@contextmanager
def postgres():
    with psycopg.connect(Config.POSTGRES_DSN) as connection:
        yield connection

def initialize_schema():
    with postgres() as connection:
        connection.execute(SCHEMA)

def digest(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()

def archive_raw(payload: dict, status: str, reason: str | None = None) -> str:
    document = {"payload": payload, "status": status, "reason": reason, "received_at": datetime.now(timezone.utc)}
    return str(MongoClient(Config.MONGO_URI).labstream.raw_telemetry.insert_one(document).inserted_id)

def audit(event_type: str, payload: dict, message, reason: str | None = None):
    with postgres() as connection:
        connection.execute("INSERT INTO audit_events (event_type, instrument_id, reason, payload_sha256, kafka_topic, kafka_partition, kafka_offset) VALUES (%s,%s,%s,%s,%s,%s,%s)",
          (event_type, payload.get("instrument_id"), reason, digest(payload), message.topic(), message.partition(), message.offset()))

def save_reading(data: dict, raw_id: str):
    with postgres() as connection:
        connection.execute("INSERT INTO readings (instrument_id, metric, value, unit, observed_at, raw_payload_id) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (instrument_id, metric, observed_at) DO NOTHING",
          (data["instrument_id"], data["metric"], data["value"], data["unit"], data["observed_at"], raw_id))
