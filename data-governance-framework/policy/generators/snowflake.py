"""
Generate Snowflake masking + row-access policies and GRANT statements
from the canonical policies.yaml.

Output is idempotent DDL — safe to re-run. Apply via Terraform or
`snowsql -f generated.sql`.

Pattern: one masking policy per (sensitivity, mask_type) pair; one row-access
policy per dataset that has a `condition`. Policies are *applied* to columns
via ALTER TABLE.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import yaml


logger = logging.getLogger(__name__)

MASKING_BODIES = {
    "hash": (
        "CASE WHEN IS_ROLE_IN_SESSION('{role}') "
        "THEN SHA2(val, 256) ELSE '<masked>' END"
    ),
    "tokenize": (
        "CASE WHEN IS_ROLE_IN_SESSION('{role}') "
        "THEN external_tokenize(val) ELSE '<masked>' END"
    ),
    "none": "val",
}


def render_masking_policy(role: str, mask_type: str, sensitivity: str) -> str:
    body = MASKING_BODIES[mask_type].format(role=role)
    policy_name = f"MASK_{sensitivity}_{role}_{mask_type}".upper()
    return (
        f"CREATE OR REPLACE MASKING POLICY {policy_name}\n"
        f"AS (val STRING) RETURNS STRING ->\n"
        f"  {body};\n"
    )


def render_row_access_policy(dataset: str, role: str, condition: str) -> str:
    safe = dataset.replace(".", "_").upper()
    policy_name = f"ROW_ACCESS_{safe}_{role}".upper()
    return (
        f"CREATE OR REPLACE ROW ACCESS POLICY {policy_name}\n"
        f"AS (visit_dept STRING) RETURNS BOOLEAN ->\n"
        f"  CASE WHEN IS_ROLE_IN_SESSION('{role}') THEN ({condition}) ELSE FALSE END;\n"
    )


def render_dataset(domain: dict, dataset: dict) -> str:
    parts = []
    fqn = dataset["name"]
    parts.append(f"\n-- ==== {fqn} ({dataset['sensitivity']}) =====")

    # Masking policies per (role, mask_type)
    for ac in dataset["access"]:
        parts.append(render_masking_policy(ac["role"], ac["masking"], dataset["sensitivity"]))

    # Apply masking to each PII column
    for col in dataset["pii_columns"]:
        for ac in dataset["access"]:
            pname = f"MASK_{dataset['sensitivity']}_{ac['role']}_{ac['masking']}".upper()
            parts.append(
                f"ALTER TABLE {fqn} MODIFY COLUMN {col} "
                f"SET MASKING POLICY {pname};"
            )

    # Row access (where condition is present)
    for ac in dataset["access"]:
        if ac.get("condition"):
            parts.append(render_row_access_policy(fqn, ac["role"], ac["condition"]))
            safe = fqn.replace(".", "_").upper()
            pname = f"ROW_ACCESS_{safe}_{ac['role']}".upper()
            # NB: condition above uses visit_dept; in production the generator
            # would inspect referenced columns to build the correct signature.
            parts.append(
                f"ALTER TABLE {fqn} ADD ROW ACCESS POLICY {pname} ON (visit_dept);"
            )

    # Grants
    parts.append("")
    for ac in dataset["access"]:
        parts.append(f"GRANT SELECT ON TABLE {fqn} TO ROLE {ac['role'].upper()};")

    return "\n".join(parts)


def render_all(policies: dict) -> str:
    lines = [
        "-- Auto-generated from policy/policies.yaml. Do not edit by hand.",
        "-- Re-run the generator instead.",
        "",
    ]
    for domain in policies["domains"]:
        lines.append(f"\n-- ============ DOMAIN: {domain['name']} ============")
        for dataset in domain["datasets"]:
            lines.append(render_dataset(domain, dataset))
    return "\n".join(lines) + "\n"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("policies", type=Path, help="path to policies.yaml")
    p.add_argument("--out", type=Path, default=Path("generated_policies.sql"))
    args = p.parse_args()

    raw = yaml.safe_load(args.policies.read_text())
    out_sql = render_all(raw)
    args.out.write_text(out_sql)
    logger.info("Wrote %d bytes to %s", len(out_sql), args.out)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
