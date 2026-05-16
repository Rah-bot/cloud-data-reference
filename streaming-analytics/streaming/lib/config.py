"""Shared configuration for streaming jobs."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
    schema_registry: str = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    events_topic: str = os.getenv("EVENTS_TOPIC", "vehicle.events")
    alerts_topic: str = os.getenv("ALERTS_TOPIC", "vehicle.alerts")
    max_offsets_per_trigger: int = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "500000"))


@dataclass(frozen=True)
class SnowflakeConfig:
    url: str = os.getenv("SNOWFLAKE_URL", "")
    user: str = os.getenv("SNOWFLAKE_USER", "")
    password: str = os.getenv("SNOWFLAKE_PASSWORD", "")
    role: str = os.getenv("SNOWFLAKE_ROLE", "FLEET_INGEST")
    warehouse: str = os.getenv("SNOWFLAKE_WH", "FLEET_INGEST_WH")
    database: str = os.getenv("SNOWFLAKE_DB", "FLEET")
    schema: str = os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS")


@dataclass(frozen=True)
class StorageConfig:
    cold_path: str = os.getenv("COLD_PATH", "s3://fleet-stream-cold/vehicle_events/")
    checkpoint_root: str = os.getenv("CHECKPOINT_ROOT", "s3://fleet-stream-checkpoints/")


KAFKA = KafkaConfig()
SNOW = SnowflakeConfig()
STORAGE = StorageConfig()
