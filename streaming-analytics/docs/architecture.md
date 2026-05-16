# Architecture

## Hot / Warm / Cold split

```mermaid
flowchart LR
    P[Producer\nidempotent + transactional] -->|Avro| K1[(Kafka\nvehicle.events)]
    SR[(Schema Registry)] -.-> P
    SR -.-> S1
    SR -.-> S2
    SR -.-> S3

    K1 --> S1[Spark: alerts_job]
    K1 --> S2[Spark: kpi_aggregations]
    K1 --> S3[Spark: raw_archive]

    S1 -->|JSON| K2[(Kafka\nvehicle.alerts)]
    S2 -->|Snowpipe Streaming| SF[(Snowflake\nFLEET_KPI_MIN)]
    S3 -->|Delta append| CL[(Delta on S3\n90-day archive)]

    K2 --> Alert[Ops alerting]
    SF --> BI[BI dashboards]
    CL --> ML[ML training\n+ replay]
```

## Why three sinks?

| Path | Latency target | Use case | Why this technology |
|---|---|---|---|
| Hot — Kafka | < 2s p99 | Operational alerts | Sub-second propagation; consumers already on Kafka |
| Warm — Snowflake | ~10s | BI / executive dashboards | Cheap analytics, easy joins to dims, Snowpipe Streaming |
| Cold — Delta on S3 | 60s | ML training + incident replay | Cheapest storage; replayable; columnar reads |

## Why Spark Structured Streaming?

- **Same engine for batch and streaming** — code reuse with the lakehouse project
- **Exactly-once via checkpointing** when paired with idempotent sinks
- **RocksDB state backend** for bounded state on stateful aggregations
- **Native Kafka + Delta + Snowflake connectors**

Flink would be a stronger choice for sub-100ms latency or millions-of-keys aggregations; for this workload, Spark's throughput-per-dollar wins.

## Why Avro + Schema Registry?

- Compact wire format (~30% smaller than JSON for this payload)
- Schema-evolution rules enforced server-side
- Schemas versioned and reviewable in PRs
- Compatible with downstream tools (kSQL, Snowflake Kafka connector, Flink)

## Scaling characteristics

- **Linear**: throughput scales with partition count up to ~250 partitions
- **State**: bounded by `(distinct keys × state TTL)`; current config uses ~2 GB RocksDB per executor at 50K eps
- **Bottleneck**: at sustained 100K+ eps, the bottleneck becomes Snowpipe Streaming channel count, not Spark

## Runbook quick links

- [Delivery guarantees](delivery-guarantees.md)
- [Operational runbook](runbook.md)
