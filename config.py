import os


class Config:
    POSTGRES_DSN = "host={host} port={port} dbname={db} user={user} password={password}".format(
        host=os.getenv("POSTGRES_HOST", "localhost"), port=os.getenv("POSTGRES_PORT", "5432"),
        db=os.getenv("POSTGRES_DB", "labstream"), user=os.getenv("POSTGRES_USER", "labstream"),
        password=os.getenv("POSTGRES_PASSWORD", "change-me"))
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/labstream")
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    RAW_TOPIC = os.getenv("KAFKA_RAW_TOPIC", "lab.telemetry.raw")
    VALIDATED_TOPIC = os.getenv("KAFKA_VALIDATED_TOPIC", "lab.telemetry.validated")
    INVALID_TOPIC = os.getenv("KAFKA_INVALID_TOPIC", "lab.telemetry.invalid")
    CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "labstream-normalizer")
