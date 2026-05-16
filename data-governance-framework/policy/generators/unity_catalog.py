"""
Generate Databricks Unity Catalog grants and column masks from the canonical
policies.yaml.

Pattern:
    - One column-mask SQL UDF per (sensitivity, mask_type)
    - ALTER TABLE ... ALTER COLUMN ... SET MASK to apply
    - GRANT SELECT to roles
    - SET TAG ('sensitivity' = 'PHI') on tables and PII columns
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import yaml


logger = logging.getLogger(__name__)


MASK_UDFS = {
    "hash": (
        "CREATE OR REPLACE FUNCTION main.governance.mask_hash(val STRING)\n"
        "RETURNS STRING\n"
        "RETURN CASE\n"
        "  WHEN is_account_group_member('{role}') THEN sha2(val, 256)\n"
        "  ELSE '<masked>'\n"
        "END;\n"
    ),
    "tokenize": (
        "CREATE OR REPLACE FUNCTION main.governance.mask_tokenize(val STRING)\n"
        "RETURNS STRING\n"
        "RETURN CASE\n"
        "  WHEN is_account_group_member('{role}') THEN external_tokenize(val)\n"
        "  ELSE '<masked>'\n"
        "END;\n"
    ),
}


def render_dataset(dataset: dict) -> list[str]:
    lines: list[str] = []
    fqn = dataset["name"].replace("snowflake.", "main.")  # naive remap for demo

    lines.append(f"\n-- {fqn} ({dataset['sensitivity']})")

    # Tag the table
    lines.append(
        f"ALTER TABLE {fqn} SET TAGS "
        f"('sensitivity' = '{dataset['sensitivity']}', "
        f"'retention_days' = '{dataset['retention_days']}');"
    )

    # Tag PII columns
    for col in dataset["pii_columns"]:
        lines.append(
            f"ALTER TABLE {fqn} ALTER COLUMN {col} SET TAGS ('pii' = 'true');"
        )

    # Apply mask UDFs
    for ac in dataset["access"]:
        if ac["masking"] in MASK_UDFS:
            udf_name = f"main.governance.mask_{ac['masking']}"
            for col in dataset["pii_columns"]:
                lines.append(
                    f"ALTER TABLE {fqn} ALTER COLUMN {col} SET MASK {udf_name};"
                )

    # Grants
    for ac in dataset["access"]:
        lines.append(
            f"GRANT SELECT ON TABLE {fqn} TO `{ac['role']}`;"
        )

    return lines


def render_udfs(policies: dict) -> list[str]:
    lines: list[str] = ["-- Mask UDFs"]
    seen = set()
    for domain in policies["domains"]:
        for dataset in domain["datasets"]:
            for ac in dataset["access"]:
                key = (ac["masking"], ac["role"])
                if key in seen or ac["masking"] not in MASK_UDFS:
                    continue
                seen.add(key)
                lines.append(MASK_UDFS[ac["masking"]].format(role=ac["role"]))
    return lines


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("policies", type=Path)
    p.add_argument("--out", type=Path, default=Path("generated_uc_grants.sql"))
    args = p.parse_args()

    raw = yaml.safe_load(args.policies.read_text())

    out: list[str] = [
        "-- Auto-generated from policy/policies.yaml.",
        "CREATE SCHEMA IF NOT EXISTS main.governance;",
        "",
    ]
    out.extend(render_udfs(raw))
    for domain in raw["domains"]:
        out.append(f"\n-- ==== domain: {domain['name']} ====")
        for dataset in domain["datasets"]:
            out.extend(render_dataset(dataset))

    args.out.write_text("\n".join(out) + "\n")
    logger.info("Wrote %s", args.out)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
