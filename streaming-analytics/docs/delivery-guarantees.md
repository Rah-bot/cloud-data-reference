# Delivery Guarantees — The Argument

End-to-end **exactly-once** delivery is achieved by composing per-hop guarantees with idempotent sinks. Here's the full chain.

## The chain

```
Producer ──▶ Kafka ──▶ Spark Structured Streaming ──▶ Snowflake
                                                    └▶ Delta (cold)
                                                    └▶ Kafka (alerts)
```

## Per-hop analysis

### Hop 1: Producer → Kafka

- **Idempotent producer** (`enable.idempotence=true`): each `(producerId, sequenceNumber)` is deduped by the broker.
- **Transactions** (`transactional.id` set, `begin/commit_transaction`): a batch of records either fully commits or fully aborts atomically across partitions.
- **`acks=all`**: write returns only after replication to the full ISR.

**Guarantee:** exactly-once write to Kafka, even with retries.

### Hop 2: Kafka → Spark Structured Streaming

- Spark commits offsets only after the microbatch's sink write succeeds (via the streaming checkpoint).
- A crash mid-microbatch causes the next run to re-read the same offsets.

**Guarantee:** at-least-once read, with deterministic ordering within a partition.

### Hop 3: Spark → Snowflake (warm path)

- Snowpipe Streaming uses **channels** with monotonically increasing **offset tokens**.
- Snowflake will refuse a write with an offset token ≤ the last committed token for that channel.
- Spark commits the streaming checkpoint only after Snowflake acknowledges the offset.

**Guarantee:** exactly-once delivery, because any duplicate batch from a Spark retry is silently rejected by Snowflake's offset-token contract.

### Hop 4: Spark → Delta (cold path)

- Delta Lake writes are atomic via its transactional log.
- Spark's streaming sink uses a deterministic `batch_id` per microbatch; Delta's sink dedupes on batch_id.
- A failed batch is replayed with the same batch_id → Delta no-ops the duplicate.

**Guarantee:** exactly-once delivery.

### Hop 5: Spark → Kafka (alerts)

- The alerts sink uses Spark's `kafka` writer with a checkpointed offset commit.
- Downstream alert consumers dedupe on `(vehicle_id, alert_type, event_id)` — already enforced by the upstream key choice.
- For consumers that need strict exactly-once, use Kafka's transactional consumer with `isolation.level=read_committed`.

**Guarantee:** at-least-once write, but downstream dedupe achieves exactly-once *effect*.

## What can still go wrong

| Failure | What happens | Mitigation |
|---|---|---|
| Snowflake unavailable >24h | Stream lags, checkpoint backs up | Cold path is source of truth — backfill from Delta when SF returns |
| Schema-incompatible event | Avro deserialization fails | Schema Registry CI gate; bad events routed to DLQ topic |
| Late events past 30s watermark | Excluded from KPI aggregates | Captured in cold archive; nightly reconciliation job updates `FLEET_KPI_MIN` |
| Spark driver OOM | Job restarts from last checkpoint | Cluster sized with headroom; alert if processing time > batch interval |

## How to verify

The `tests/test_alerts.py` property test asserts that for any sequence of input events containing N harsh-braking flags, the alerts topic contains *exactly* N records with `alert_type='HARSH_BRAKING'` after replay, regardless of injected failures.
