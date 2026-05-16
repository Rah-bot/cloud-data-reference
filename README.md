# Data Platform Blueprints

Three reference implementations covering the full data-architect remit:

| Project | What it shows |
|---|---|
| [01-lakehouse-medallion](01-lakehouse-medallion/) | Bronze/Silver/Gold on Delta + dbt, SCD2, Unity Catalog |
| [02-streaming-analytics](02-streaming-analytics/) | Kafka × Spark × Snowflake with exactly-once delivery |
| [03-governance-framework](03-governance-framework/) | Policy-as-code, PII classification, GDPR/HIPAA mappings |

Each directory is self-contained — its own README, docker-compose, CI, and tests.

## Why these three?

Modern data architecture sits at the intersection of **modeling** (lakehouse), **pipelines** (streaming), and **governance** (compliance). Most portfolios cover one. These cover all three at production grade.
