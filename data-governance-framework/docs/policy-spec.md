# Policy Spec — `policies.yaml`

The canonical file `policy/policies.yaml` is the source of truth for data sensitivity, retention, ownership, and access. This document explains its schema and the contracts the generators rely on.

## Top-level

```yaml
version: 1
domains:
  - name: <string>
    owner: <email>
    contact_slack: <string>          # optional
    datasets:
      - ...
```

`version` is bumped when a backward-incompatible change is made; generators refuse to process unknown versions.

## Dataset

```yaml
- name: <fully.qualified.table.name>
  description: <string>
  sensitivity: PII | PHI | PCI | INTERNAL | PUBLIC
  retention_days: <int>
  pii_columns: [<col>, <col>, ...]
  regulated_under: [HIPAA, GDPR, CCPA, PCI_DSS, SOX]    # any subset
  access:
    - role: <string>
      masking: hash | tokenize | none
      condition: <SQL boolean expression, optional>
```

### `sensitivity`

| Value | Meaning | Default mask |
|---|---|---|
| `PUBLIC` | No restriction | `none` |
| `INTERNAL` | Employee-only | `none` |
| `PII` | Personally identifiable | `hash` |
| `PHI` | Protected health info | `hash` |
| `PCI` | Card / financial data | `tokenize` |

### `masking`

- `hash` — SHA-256 of the value with a per-environment salt; one-way
- `tokenize` — replace with a token; reversible by the tokenization service for authorized callers
- `none` — pass the raw value through; only allowed when paired with a `condition` or when the role is in an exception list

### `condition`

A SQL boolean expression evaluated at query time. It receives the row in scope and can call session functions like `current_user_dept()`. Returning `FALSE` causes the row to be filtered out.

### `retention_days`

Drives the lifecycle job that deletes rows older than the threshold. The generator refuses values that conflict with the most-restrictive regulation listed in `regulated_under` (e.g. HIPAA requires 6+ years for most records).

## What the generators produce

| Generator | Output |
|---|---|
| `policy/generators/snowflake.py` | `CREATE MASKING POLICY` + `CREATE ROW ACCESS POLICY` + `ALTER TABLE ... SET MASKING POLICY` + `GRANT SELECT` |
| `policy/generators/unity_catalog.py` | `CREATE FUNCTION` (mask UDFs) + `ALTER TABLE ... SET MASK` + `ALTER TABLE ... SET TAGS` + `GRANT SELECT` |
| (planned) `policy/generators/openmetadata.py` | Tags, ownership, classifications via OpenMetadata REST |

## Validation rules enforced before generation

1. Every dataset listed in `policies.yaml` must exist in the catalog (verified via OpenMetadata)
2. Every column in `pii_columns` must exist on the dataset
3. Roles referenced under `access` must exist in the IdP
4. `retention_days` must be ≥ the floor implied by `regulated_under`
5. At least one `access` entry per dataset (otherwise default-deny would orphan the data)
6. Mutually exclusive masking modes per (dataset, role) — no conflicting rules

Validation is run in CI before the generated DDL is allowed to merge.

## Lifecycle

```
policies.yaml ──► CI validate ──► generators ──► DDL artifacts ──► Terraform apply ──► warehouse enforces
                       │                              │
                       └─ block on error              └─ git history = audit log
```

## Adding a new dataset (5-minute version)

1. Add a stanza under the appropriate domain
2. Run `python policy/generators/snowflake.py policy/policies.yaml --out /tmp/out.sql` locally
3. Review the diff
4. Open PR; CI validates and posts the generated DDL diff as a comment
5. Merge; the apply job picks it up on next run
