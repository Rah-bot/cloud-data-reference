# GDPR Compliance Checklist

How this framework supports specific GDPR articles. None of this constitutes legal advice; it documents technical controls that contribute to compliance.

## Article 5 — Principles relating to processing

| Principle | Control in this framework |
|---|---|
| Lawfulness, fairness, transparency | Consent flags surfaced in `policies.yaml` `condition:` clauses (e.g. `marketing_consent = TRUE`) |
| Purpose limitation | Domain ownership recorded per dataset; cross-domain access blocked by default |
| Data minimisation | Column-level access policies — analysts see hashes, not raw PII |
| Accuracy | Great Expectations suites enforce schema + range validity |
| Storage limitation | `retention_days:` enforced via lifecycle jobs (see `dags/retention.py`) |
| Integrity & confidentiality | Mask UDFs ensure rest-state masking even for privileged read paths |
| Accountability | All policy changes captured in git; audit-log table tracks who applied what |

## Article 25 — Data protection by design and by default

- Default deny: any dataset not present in `policies.yaml` has no `GRANT SELECT`
- New columns are auto-classified by the classifier and tagged before exposure
- Privileged roles must pass the row-access predicate even when masking is `none`

## Article 30 — Records of processing activities

The framework produces a machine-readable processing register from `policies.yaml`:

```bash
python compliance/generate_ropa.py policy/policies.yaml > ropa.csv
```

Columns: dataset, controller, purpose, categories of data, recipients, retention, transfer locations.

## Article 32 — Security of processing

- Encryption-at-rest assumed via cloud-native storage (S3/ADLS/GCS)
- Encryption-in-transit assertions checked during catalog ingestion
- Column masking is enforced at the warehouse layer — not in the BI tool — so it survives tool swaps
- Quarterly access reviews driven by `compliance/access_review.py`

## Article 17 — Right to erasure

- `policies.yaml` records the natural-key column for each PII dataset
- `compliance/dsar_erasure.py` traverses lineage from any starting table and emits `DELETE` statements for downstream copies
- Erasure log retained 5 years per regulator guidance

## Article 33 — Breach notification

- `dags/access_anomaly.py` flags anomalous read patterns (volume, unusual roles, off-hours)
- Alerts to Slack and PagerDuty with the data subject categories impacted

## Gaps that still need humans

- Lawful-basis determination per processing activity
- DPIA execution for high-risk processing
- Vendor due diligence and DPA contract management
- Public-facing privacy notice maintenance

These are flagged in `compliance/audit_report_template.html` for the DPO to fill in.
