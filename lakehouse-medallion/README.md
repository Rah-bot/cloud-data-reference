# Enterprise Lakehouse — Medallion Architecture

End-to-end lakehouse implementing the Bronze/Silver/Gold medallion pattern on Databricks + Delta Lake, with SCD Type 2 dimensions, dbt marts, Airflow orchestration, Great Expectations data quality, and Unity Catalog governance.

![Architecture](docs/architecture.svg)

## What this demonstrates

- **Ingestion** — Auto Loader streaming + CDC patterns into Bronze (raw, append-only Delta)
- **Modeling** — SCD Type 2 dimensions with MERGE INTO, surrogate keys, conformed facts
- **Marts** — dbt project building a star schema in Gold
- **Orchestration** — Airflow DAG with quality gates between layers
- **Quality** — Great Expectations suites blocking promotion on failure
- **Governance** — Unity Catalog grants, PII tagging, column masking via dynamic views
- **IaC** — Terraform for workspace, clusters, catalogs, and S3 buckets
- **CI** — Lint, unit tests, dbt build, and GE validation on every PR

## Quick start

```bash
# 1. Install Python deps
pip install -r requirements.txt

# 2. Run unit tests
pytest tests/

# 3. Build the dbt project (requires Databricks creds in ~/.dbt/profiles.yml)
cd transforms/gold/dbt && dbt build

# 4. Provision infra (requires AWS + Databricks creds)
cd infra/terraform && terraform init && terraform apply
```

## Repo structure

```
.
├── ingestion/          # Bronze layer — Auto Loader + CDC
├── transforms/
│   ├── silver/         # PySpark transforms, SCD2 logic
│   └── gold/dbt/       # dbt project for marts
├── governance/         # Great Expectations suites
├── orchestration/      # Airflow DAGs
├── infra/terraform/    # Workspace, catalogs, storage
├── docs/               # Architecture, data model, runbook
└── tests/              # Unit + integration tests
```

## Data model

See [docs/data-model.md](docs/data-model.md) for the full ERD. Quick summary:

**Silver (3NF, conformed):** `customer`, `product`, `order`, `order_line`, `inventory_snapshot`

**Gold (star schema):**
- Facts: `fact_sales_daily`, `fact_inventory_daily`, `fact_returns`
- Dims: `dim_customer` (SCD2), `dim_product` (SCD2), `dim_date`, `dim_store`

## Architecture decisions

| Decision | Choice | Why |
|---|---|---|
| Storage format | Delta Lake | ACID, time-travel, MERGE, broad ecosystem support |
| File layout | Partition + Z-ORDER | Partition by date, Z-ORDER on high-cardinality join keys |
| Ingestion | Auto Loader | Schema inference + evolution + checkpointing built in |
| SCD2 | MERGE INTO + audit cols | Idempotent, replayable, no orphaned versions |
| Governance | Unity Catalog | Column-level lineage + masking + RBAC native |
| Quality gates | GE in Airflow | Blocks promotion Bronze→Silver→Gold on suite failure |

## Performance & cost

Tested on a 4-node autoscale (i3.xlarge) Databricks cluster:

| Layer | Volume | Wall time | Cost/run |
|---|---|---|---|
| Bronze (streaming) | 50M rows/day | continuous | ~$8/day |
| Silver (batch) | 50M rows | 12 min | ~$1.50 |
| Gold (dbt) | 200 models | 18 min | ~$2.20 |

## License

MIT
