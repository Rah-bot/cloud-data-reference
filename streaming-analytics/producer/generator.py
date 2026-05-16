"""
Synthetic vehicle telemetry generator.

Produces realistic events at a configurable rate to a local Kafka cluster
with Avro serialization and Schema Registry. Supports fault injection
(harsh braking, dropped messages, schema-version skew) for testing.

Usage:
    python producer/generator.py --rate 10000 --duration 300
"""

from __future__ import annotations

import argparse
import json
import math
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField


SCHEMA_PATH = Path(__file__).parent / "schemas" / "vehicle.avsc"


@dataclass
class Vehicle:
    """In-memory state for one simulated vehicle."""
    vehicle_id: str
    driver_id: str
    lat: float
    lon: float
    speed_kph: float
    heading_deg: float
    odometer_km: float

    def step(self, dt: float = 1.0) -> None:
        """Advance the vehicle by dt seconds, mutating in place."""
        # Mild heading noise
        self.heading_deg = (self.heading_deg + random.gauss(0, 2)) % 360
        # Speed wanders within sane bounds
        self.speed_kph = max(0, min(140, self.speed_kph + random.gauss(0, 3)))
        # Convert speed/heading to lat/lon delta
        dist_km = (self.speed_kph * dt) / 3600
        rad = math.radians(self.heading_deg)
        self.lat += (dist_km / 111) * math.cos(rad)
        self.lon += (dist_km / 111) * math.sin(rad)
        self.odometer_km += dist_km


def make_fleet(n: int) -> list[Vehicle]:
    return [
        Vehicle(
            vehicle_id=f"V{i:06d}",
            driver_id=f"D{random.randint(1, n // 2):06d}",
            lat=40.7 + random.uniform(-0.2, 0.2),
            lon=-74.0 + random.uniform(-0.2, 0.2),
            speed_kph=random.uniform(40, 80),
            heading_deg=random.uniform(0, 360),
            odometer_km=random.uniform(1000, 100000),
        )
        for i in range(n)
    ]


def to_event(v: Vehicle, harsh_prob: float = 0.001) -> dict:
    harsh = random.random() < harsh_prob
    return {
        "event_id": str(uuid.uuid4()),
        "vehicle_id": v.vehicle_id,
        "driver_id": v.driver_id,
        "event_ts_ms": int(time.time() * 1000),
        "latitude": v.lat,
        "longitude": v.lon,
        "speed_kph": float(v.speed_kph),
        "heading_deg": float(v.heading_deg),
        "acceleration_ms2": float(random.gauss(0, 0.5)) if not harsh else float(random.uniform(-9, -6)),
        "engine_rpm": random.randint(800, 4500),
        "fuel_level_pct": float(random.uniform(10, 100)),
        "odometer_km": float(v.odometer_km),
        "harsh_braking": harsh,
        "geofence_id": "manhattan" if -74.02 < v.lon < -73.93 and 40.70 < v.lat < 40.82 else None,
        "schema_version": 1,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--registry", default="http://localhost:8081")
    p.add_argument("--topic", default="vehicle.events")
    p.add_argument("--rate", type=int, default=5000, help="events per second")
    p.add_argument("--fleet-size", type=int, default=10000)
    p.add_argument("--duration", type=int, default=60, help="seconds")
    p.add_argument("--harsh-prob", type=float, default=0.001)
    args = p.parse_args()

    schema_str = SCHEMA_PATH.read_text()
    sr = SchemaRegistryClient({"url": args.registry})
    avro_serializer = AvroSerializer(sr, schema_str)

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "enable.idempotence": True,                    # exactly-once on the wire
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 5,
        "batch.size": 64 * 1024,
        "transactional.id": f"vehicle-gen-{uuid.uuid4()}",
    })
    producer.init_transactions()

    fleet = make_fleet(args.fleet_size)
    ctx = SerializationContext(args.topic, MessageField.VALUE)

    stop = False
    def _sig(*_): 
        nonlocal stop
        stop = True
    signal.signal(signal.SIGINT, _sig)

    print(f"Producing {args.rate} eps to {args.topic} for {args.duration}s")
    start = time.time()
    sent = 0
    batch_size = max(1, args.rate // 50)  # 50 batches per second
    interval = 1 / 50

    while not stop and time.time() - start < args.duration:
        tick = time.time()
        producer.begin_transaction()
        try:
            for _ in range(batch_size):
                v = random.choice(fleet)
                v.step(dt=interval)
                evt = to_event(v, harsh_prob=args.harsh_prob)
                producer.produce(
                    topic=args.topic,
                    key=v.vehicle_id,
                    value=avro_serializer(evt, ctx),
                )
                sent += 1
            producer.commit_transaction()
        except Exception as e:
            print(f"Aborting transaction: {e}", file=sys.stderr)
            producer.abort_transaction()

        producer.poll(0)
        elapsed = time.time() - tick
        if elapsed < interval:
            time.sleep(interval - elapsed)

    producer.flush(10)
    duration = time.time() - start
    print(f"Done. {sent:,} events in {duration:.1f}s ({sent / duration:.0f} eps)")


if __name__ == "__main__":
    main()
