# HIPAA Compliance Checklist

This framework supports the HIPAA Security Rule's technical safeguards (§164.312). Administrative and physical safeguards are not in scope of a data platform — they're org policies.

## §164.312(a) — Access control

| Requirement | Control |
|---|---|
| Unique user identification | OIDC sign-on via Okta / Azure AD; service-principal pattern documented |
| Emergency access procedure | Break-glass role with mandatory PagerDuty incident + audit trail |
| Automatic logoff | Snowflake `CLIENT_SESSION_KEEP_ALIVE = FALSE` + session timeout policy |
| Encryption and decryption | Cloud-native at-rest encryption + masking UDFs at column level |

## §164.312(b) — Audit controls

- Every catalog mutation logged to an immutable audit table
- Snowflake `ACCESS_HISTORY` and Databricks `system.access` queried hourly into the compliance lake
- Anomalous-access detection runs daily and writes incidents to the audit log

## §164.312(c) — Integrity

- Great Expectations suites prevent invalid data from being promoted
- Delta Lake's transactional log provides tamper-evidence for cold archive
- Checksums on all inter-system transfers

## §164.312(d) — Person or entity authentication

- Federated SSO required for all human users
- Service principals rotate credentials via secrets manager
- MFA enforced at the IdP level

## §164.312(e) — Transmission security

- TLS 1.2+ between all components (verified during catalog ingestion)
- VPC endpoints / Private Link for warehouse <-> compute traffic
- No PHI in logs (enforced by structured logging schema and DLP scanner)

## PHI data inventory (per §164.514 De-Identification)

Every PHI dataset declares its identifiers in `policies.yaml`:

```yaml
- name: snowflake.prod.patient_visits
  sensitivity: PHI
  pii_columns: [patient_name, dob, mrn, address]
  regulated_under: [HIPAA, GDPR]
```

The framework can produce a Safe Harbor view by applying maximum masking to all 18 identifiers — see `compliance/de_identify.py`.

## Business Associate considerations

- Subcontractor/processor list maintained in `compliance/baa_register.yaml`
- Cross-border transfer flags raised by the policy generator when a dataset's regional residency conflicts with destination workspace
