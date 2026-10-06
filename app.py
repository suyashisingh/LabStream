import json
from flask import Flask, jsonify, request
from confluent_kafka import Producer
from config import Config
from schema import Telemetry
from storage import initialize_schema, postgres

app = Flask(__name__)

def producer():
    return Producer({"bootstrap.servers": Config.KAFKA_BOOTSTRAP_SERVERS, "client.id": "labstream-api"})

@app.post("/v1/telemetry")
def ingest():
    telemetry = Telemetry.model_validate(request.get_json(force=True))
    client = producer()
    client.produce(Config.RAW_TOPIC, key=telemetry.instrument_id, value=telemetry.model_dump_json())
    client.flush(5)
    return jsonify({"status": "accepted", "topic": Config.RAW_TOPIC}), 202

@app.get("/v1/readings")
def readings():
    instrument_id = request.args.get("instrument_id")
    with postgres() as connection:
        rows = connection.execute("SELECT instrument_id, metric, value, unit, observed_at FROM readings WHERE (%s IS NULL OR instrument_id = %s) ORDER BY observed_at DESC LIMIT 500", (instrument_id, instrument_id)).fetchall()
    return jsonify([{"instrument_id": r[0], "metric": r[1], "value": r[2], "unit": r[3], "observed_at": r[4].isoformat()} for r in rows])

@app.get("/v1/audit-events")
def audit_events():
    with postgres() as connection:
        rows = connection.execute("SELECT event_type, instrument_id, reason, payload_sha256, occurred_at FROM audit_events ORDER BY occurred_at DESC LIMIT 200").fetchall()
    return jsonify([{"event_type": r[0], "instrument_id": r[1], "reason": r[2], "payload_sha256": r[3], "occurred_at": r[4].isoformat()} for r in rows])

@app.get("/health/live")
def live(): return jsonify(status="ok")

@app.get("/health/ready")
def ready():
    try:
        initialize_schema()
        producer().list_topics(timeout=3)
        return jsonify(status="ready")
    except Exception as error:
        return jsonify(status="not_ready", error=str(error)), 503

if __name__ == "__main__":
    initialize_schema()
    app.run(host="0.0.0.0", port=5000)
