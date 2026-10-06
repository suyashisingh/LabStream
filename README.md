# LabStream

LabStream is a production-minded lab telemetry pipeline. Instruments publish raw JSON readings to Kafka; a consumer parses and validates them, retains the original payload in MongoDB, stores normalized readings and immutable audit events in PostgreSQL, and exposes processed data through a Flask REST API.

## Architecture

```text
Instrument/sensor -> Flask ingest API -> Kafka (lab.telemetry.raw)
                                          |
                                  telemetry consumer
                                   /                  \
                         PostgreSQL                  MongoDB
                  normalized readings + audit     raw payload archive
                                          |
                                  Flask query API
```

Kafka topics are intentionally separated into `lab.telemetry.raw`, `lab.telemetry.validated`, and `lab.telemetry.invalid`, making invalid data observable without blocking valid traffic.

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

Submit a reading:

```bash
curl -X POST http://localhost:5000/v1/telemetry \
  -H "Content-Type: application/json" \
  -d '{"instrument_id":"incubator-7","metric":"temperature_c","value":37.2,"unit":"C","observed_at":"2026-10-06T10:00:00Z","payload":{"firmware":"2.4"}}'
```

Check readiness and query normalized data:

```bash
curl http://localhost:5000/health/ready
curl http://localhost:5000/v1/readings?instrument_id=incubator-7
curl http://localhost:5000/v1/audit-events
```

## Local development

```bash
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
pytest
```

The API accepts telemetry and publishes it to Kafka. `consumer.py` is a separately deployable worker. Required infrastructure connection values are documented in `.env.example`.

## Deployment

`k8s/` includes namespace, secrets/config template, API Deployment/Service, and a consumer Deployment. Build images into Amazon ECR, update `IMAGE_TAG` in `k8s/kustomization.yaml`, then apply with `kubectl apply -k k8s/` (or deploy through the included GitHub Actions workflow). In AWS, use managed Kafka/MSK, RDS PostgreSQL, and DocumentDB-compatible MongoDB or MongoDB Atlas; inject their endpoints through Kubernetes Secrets.

## Design notes

- **Validation:** Pydantic validates required fields, ISO-8601 timestamps, numeric values, and metric-specific reasonable ranges.
- **Auditability:** every accepted or rejected message creates an append-only `audit_events` row containing an event type, reason, Kafka offset, and payload digest.
- **Idempotency:** `(instrument_id, observed_at, metric)` is unique in PostgreSQL; duplicate Kafka deliveries become a no-op normalized write while still being audited.
- **Health checks:** `/health/live` verifies process availability, `/health/ready` tests PostgreSQL and Kafka metadata. Docker and Kubernetes probe these endpoints.
- **CI/CD:** GitHub Actions tests, builds, scans the image, pushes to ECR, and applies the Kubernetes overlay when AWS deployment secrets are configured.
