"""
Tests for the policy generators.

We verify the *structure* of the generated DDL — exact whitespace and SQL
dialect details are tested by integration. Here we assert:
    - one masking policy per (role, mask_type) combination
    - PII columns get ALTER TABLE statements
    - GRANTs are issued for every access role
    - row-access policies appear iff a `condition` is configured
    - YAML round-trips without losing fields
"""

import textwrap

import pytest
import yaml

from policy.generators.snowflake import (
    render_all as render_snowflake,
    render_dataset as render_snowflake_dataset,
)
from policy.generators.unity_catalog import (
    render_dataset as render_uc_dataset,
)


SAMPLE_POLICIES = {
    "version": 1,
    "domains": [
        {
            "name": "clinical",
            "owner": "x@example.com",
            "datasets": [
                {
                    "name": "snowflake.prod.patient_visits",
                    "description": "Visits",
                    "sensitivity": "PHI",
                    "retention_days": 2555,
                    "pii_columns": ["mrn", "patient_name"],
                    "regulated_under": ["HIPAA"],
                    "access": [
                        {"role": "analyst_clinical", "masking": "hash"},
                        {"role": "physician", "masking": "none",
                         "condition": "current_user_dept() = visit_dept"},
                    ],
                }
            ],
        }
    ],
}


# ---------------------------------------------------------------------------
# Snowflake generator
# ---------------------------------------------------------------------------

def test_snowflake_masking_policy_per_role():
    sql = render_snowflake(SAMPLE_POLICIES)
    assert "MASK_PHI_ANALYST_CLINICAL_HASH" in sql
    assert "MASK_PHI_PHYSICIAN_NONE" in sql


def test_snowflake_alter_table_for_each_pii_column():
    sql = render_snowflake(SAMPLE_POLICIES)
    assert "ALTER TABLE snowflake.prod.patient_visits MODIFY COLUMN mrn" in sql
    assert "ALTER TABLE snowflake.prod.patient_visits MODIFY COLUMN patient_name" in sql


def test_snowflake_grants_issued_per_access_role():
    sql = render_snowflake(SAMPLE_POLICIES)
    assert "GRANT SELECT ON TABLE snowflake.prod.patient_visits TO ROLE ANALYST_CLINICAL;" in sql
    assert "GRANT SELECT ON TABLE snowflake.prod.patient_visits TO ROLE PHYSICIAN;" in sql


def test_snowflake_row_access_only_when_condition_present():
    sql = render_snowflake(SAMPLE_POLICIES)
    # physician has condition; analyst_clinical does not
    assert "ROW_ACCESS_SNOWFLAKE_PROD_PATIENT_VISITS_PHYSICIAN" in sql
    assert "ROW_ACCESS_SNOWFLAKE_PROD_PATIENT_VISITS_ANALYST_CLINICAL" not in sql


def test_snowflake_no_condition_means_no_row_access():
    no_cond_policy = {
        "version": 1,
        "domains": [{
            "name": "d",
            "owner": "x@example.com",
            "datasets": [{
                "name": "snowflake.prod.t",
                "description": "",
                "sensitivity": "PII",
                "retention_days": 365,
                "pii_columns": ["email"],
                "regulated_under": ["GDPR"],
                "access": [{"role": "analyst", "masking": "hash"}],
            }],
        }],
    }
    sql = render_snowflake(no_cond_policy)
    assert "ROW ACCESS POLICY" not in sql


# ---------------------------------------------------------------------------
# Unity Catalog generator
# ---------------------------------------------------------------------------

def test_uc_tags_sensitivity_and_retention():
    dataset = SAMPLE_POLICIES["domains"][0]["datasets"][0]
    lines = render_uc_dataset(dataset)
    out = "\n".join(lines)
    assert "'sensitivity' = 'PHI'" in out
    assert "'retention_days' = '2555'" in out


def test_uc_marks_pii_columns():
    dataset = SAMPLE_POLICIES["domains"][0]["datasets"][0]
    lines = render_uc_dataset(dataset)
    out = "\n".join(lines)
    assert "ALTER TABLE main.prod.patient_visits ALTER COLUMN mrn SET TAGS ('pii' = 'true');" in out
    assert "ALTER TABLE main.prod.patient_visits ALTER COLUMN patient_name SET TAGS ('pii' = 'true');" in out


def test_uc_applies_mask_udf_only_for_supported_mask_types():
    dataset = SAMPLE_POLICIES["domains"][0]["datasets"][0]
    lines = render_uc_dataset(dataset)
    out = "\n".join(lines)
    # hash is a supported mask type → MASK clause emitted
    assert "SET MASK main.governance.mask_hash" in out


# ---------------------------------------------------------------------------
# YAML invariants
# ---------------------------------------------------------------------------

def test_sample_policies_yaml_round_trips():
    dumped = yaml.safe_dump(SAMPLE_POLICIES)
    reloaded = yaml.safe_load(dumped)
    assert reloaded == SAMPLE_POLICIES
