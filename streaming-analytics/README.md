# Real-Time Streaming Analytics — Kafka × Spark × Snowflake

Production-grade streaming pipeline showing exactly-once delivery, schema evolution, windowed aggregations, and a dual-sink hot/warm/cold architecture.

![Architecture](docs/architecture.svg)

## What this demonstrates

- **Exactly-once delivery** end-to-end (Kafka idempotent producer + Spark checkpointing + Snowflake transactional ingest)
- **Schema evolution** via Avro + Schema Registry with backward-compatibility CI gates
- **Stateful streaming** — tumbling windows, watermarks, late-data side outputs
- **Backpressure & autoscaling** — `maxOffsetsPerTrigger`, Spark dynamic allocation
- **Observability** — Prometheus metrics, Grafana dashboards, structured logs
- **Replayability** — Kafka retention + Delta cold archive enables full backfill
- **Chaos testing** — broker kill, network partition, slow consumer

## The scenario

50K vehicle telemetry events/sec across:

1. **Hot path** — harsh-braking + geofence alerts to a Kafka topic in under 2s
2. **Warm path** — KPI aggregations to Snowflake via Snowpipe Streaming for BI (~10s latency)
3. **Cold path** — raw event archive to Delta on S3 for ML and replay

## Quick start (5 minutes)

```bash
# Boot full stack: Kafka + Schema Registry + Spark + Prometheus + Grafana
docker compose up -d

# Generate synthetic traffic at 10K eps
python producer/generator.py --rate 10000 --duration 300

# Submit alerts job
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  streaming/jobs/alerts_job.py

# Watch metrics
open http://localhost:3000  # Grafana, admin/admin
```

## Repo structure

```
.
├── producer/                 # Synthetic event generator + Avro schemas
├── streaming/
│   ├── jobs/                 # Spark Structured Streaming apps
│   └── lib/                  # Shared config, metrics, helpers
├── snowflake/                # DDL + Snowpipe Streaming setup
├── monitoring/               # Prometheus + Grafana
├── docs/                     # Architecture, delivery guarantees, runbook
└── tests/                    # Unit + property + chaos tests
```

## Delivery guarantees — the short version

| Stage | Mechanism | Guarantee |
|---|---|---|
| Producer → Kafka | Idempotent producer + transactions | Exactly-once write |
| Kafka → Spark | Checkpointed offsets + watermarks | At-least-once read |
| Spark → Snowflake | Snowpipe Streaming channels + commit tokens | Exactly-once delivery |
| Spark → Delta cold | Idempotent writer + transactional log | Exactly-once delivery |

End-to-end exactly-once is achieved because every sink dedupes on `(event_id, ingest_offset)` and the source is idempotent. See [docs/delivery-guarantees.md](docs/delivery-guarantees.md) for the full argument.

## Operating notes

- **Watermark = 30s** — late events beyond this go to a side topic for offline reconciliation
- **State TTL = 1h** on aggregation jobs — keeps RocksDB state bounded
- **Cluster sizing** — sustained 50K eps fits comfortably on 8 cores; CPU is the constraint
- **Cost** — ~$0.18/hr for the streaming infra at this scale on AWS

## License

MIT
