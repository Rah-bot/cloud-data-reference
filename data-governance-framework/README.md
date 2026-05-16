# Data Governance & Quality Framework

An automated governance layer that sits on top of any cloud lakehouse and produces a single audit-ready system of record for **cataloguing, column-level lineage, PII/PHI classification, data quality, and access policy**.

![Architecture](docs/architecture.svg)

## What this demonstrates

- **Policy-as-code** — one YAML file generates Snowflake masking + row-access DDL and Unity Catalog grants
- **Automated PII/PHI classification** using regex + ML, with results pushed back to the catalog as tags
- **Column-level lineage** built from dbt manifests and Airflow DAG metadata
- **Quality gates** wired into Airflow — a failed Great Expectations suite blocks downstream promotion
- **Compliance evidence** — GDPR / HIPAA / CCPA mappings with auto-generated audit reports
- **Pluggable catalog** — OpenMetadata in the default stack; abstraction allows swapping to DataHub, Collibra, or Alation

## Quick start

```bash
# Boot OpenMetadata + Airflow + Postgres locally
docker compose up -d

# Run catalog ingestion (requires Snowflake/Databricks creds)
python ingestion/snowflake_metadata.py

# Apply policy-as-code to your environment
python policy/generators/snowflake.py --apply policy/policies.yaml

# Verify
pytest tests/
```

## Repo structure

```
.
├── ingestion/            # Metadata pullers per source platform
├── classification/       # PII/PHI rule + ML classifier; pushes tags to catalog
├── quality/              # Great Expectations suites + Airflow operator
├── policy/
│   ├── policies.yaml     # Single source of truth for sensitivity + access
│   └── generators/       # YAML → Snowflake DDL + Unity Catalog grants
├── dags/                 # Airflow DAGs: catalog refresh, classify, quality gate
├── compliance/           # GDPR/HIPAA/CCPA checklists + audit templates
├── docs/                 # Architecture, policy spec, how-to
└── tests/
```

## The headline feature: policy-as-code

Define dataset sensitivity and access policy once, in YAML:

```yaml
domain: clinical
owner: data-platform@example.com
datasets:
  - name: snowflake.prod.patient_visits
    sensitivity: PHI
    retention_days: 2555
    pii_columns: [patient_name, dob, mrn]
    access:
      - role: analyst_clinical
        masking: hash
      - role: data_scientist
        masking: tokenize
      - role: physician
        masking: none
        condition: "current_user_dept() = visit_dept"
```

The generator turns that into Snowflake masking policies, row-access policies, and `GRANT` statements — applied via Terraform, audited via git history, and reversible.

## Compliance coverage

| Regulation | Articles / sections | How this framework helps |
|---|---|---|
| **GDPR** | Art. 25 (privacy by design), 30 (records of processing), 32 (security) | Auto-classification, lineage, retention policy enforcement |
| **HIPAA** | §164.312 (technical safeguards) | Access controls, audit trail, encryption-in-transit assertions |
| **CCPA** | §1798.100 (right to know) | Catalog of all PI per consumer; data flow lineage for DSARs |

See [compliance/](compliance/) for full checklists.

## License

MIT
