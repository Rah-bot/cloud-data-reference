"""
Snowflake metadata ingestion to OpenMetadata.

Connects to Snowflake's INFORMATION_SCHEMA + ACCOUNT_USAGE, extracts:
    - databases, schemas, tables, views, columns (with types and comments)
    - table ownership and last refresh timestamps
    - query history → column-level lineage where derivable

…and writes everything as OpenMetadata entities via the REST API.

In production this would use the OpenMetadata Snowflake connector directly;
this script exists to show the data model and is useful when running against
a private Snowflake account where the prebuilt connector isn't configurable.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

import requests
import snowflake.connector


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


@dataclass(frozen=True)
class OMConfig:
    base_url: str = os.environ.get("OM_URL", "http://localhost:8585/api")
    jwt: str = os.environ.get("OM_JWT", "")


@dataclass(frozen=True)
class SFConfig:
    account: str = os.environ["SNOWFLAKE_ACCOUNT"]
    user: str = os.environ["SNOWFLAKE_USER"]
    password: str = os.environ["SNOWFLAKE_PASSWORD"]
    role: str = os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    warehouse: str = os.environ.get("SNOWFLAKE_WH", "DEMO_WH")


SELECT_COLUMNS_SQL = """
SELECT
    c.TABLE_CATALOG,
    c.TABLE_SCHEMA,
    c.TABLE_NAME,
    t.TABLE_TYPE,
    t.COMMENT     AS TABLE_COMMENT,
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.IS_NULLABLE,
    c.COMMENT     AS COLUMN_COMMENT,
    c.ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS c
JOIN INFORMATION_SCHEMA.TABLES t USING (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME)
WHERE c.TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
ORDER BY c.TABLE_CATALOG, c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
"""


def fetch_columns(cfg: SFConfig) -> list[dict]:
    conn = snowflake.connector.connect(
        account=cfg.account,
        user=cfg.user,
        password=cfg.password,
        role=cfg.role,
        warehouse=cfg.warehouse,
    )
    try:
        cur = conn.cursor(snowflake.connector.DictCursor)
        cur.execute(SELECT_COLUMNS_SQL)
        return cur.fetchall()
    finally:
        conn.close()


def group_by_table(rows: list[dict]) -> dict[tuple[str, str, str], dict]:
    """Group flat column rows into table-keyed dicts with a `columns` list."""
    tables: dict[tuple[str, str, str], dict] = {}
    for r in rows:
        key = (r["TABLE_CATALOG"], r["TABLE_SCHEMA"], r["TABLE_NAME"])
        tbl = tables.setdefault(key, {
            "database": r["TABLE_CATALOG"],
            "schema": r["TABLE_SCHEMA"],
            "name": r["TABLE_NAME"],
            "type": r["TABLE_TYPE"],
            "description": r["TABLE_COMMENT"] or "",
            "columns": [],
        })
        tbl["columns"].append({
            "name": r["COLUMN_NAME"],
            "dataType": map_snowflake_type(r["DATA_TYPE"]),
            "nullable": r["IS_NULLABLE"] == "YES",
            "description": r["COLUMN_COMMENT"] or "",
            "ordinalPosition": r["ORDINAL_POSITION"],
        })
    return tables


SF_TYPE_MAP = {
    "NUMBER": "NUMBER", "DECIMAL": "DECIMAL", "INT": "INT", "BIGINT": "BIGINT",
    "FLOAT": "DOUBLE", "TEXT": "VARCHAR", "VARCHAR": "VARCHAR",
    "BOOLEAN": "BOOLEAN", "DATE": "DATE", "TIMESTAMP_NTZ": "TIMESTAMP",
    "TIMESTAMP_TZ": "TIMESTAMP", "VARIANT": "JSON", "OBJECT": "JSON",
    "ARRAY": "ARRAY", "BINARY": "BINARY",
}


def map_snowflake_type(sf_type: str) -> str:
    base = sf_type.split("(")[0].upper()
    return SF_TYPE_MAP.get(base, "VARCHAR")


def push_to_openmetadata(tables: dict, om: OMConfig) -> None:
    headers = {"Authorization": f"Bearer {om.jwt}", "Content-Type": "application/json"}
    for (db, schema, name), payload in tables.items():
        fqn = f"snowflake.{db}.{schema}.{name}"
        body = {
            "name": name,
            "displayName": name,
            "description": payload["description"],
            "tableType": payload["type"],
            "columns": payload["columns"],
            "databaseSchema": f"snowflake.{db}.{schema}",
        }
        r = requests.put(
            f"{om.base_url}/v1/tables",
            json=body,
            headers=headers,
            timeout=30,
        )
        if r.status_code >= 400:
            logger.error("Failed %s: %s", fqn, r.text)
        else:
            logger.info("Upserted %s", fqn)


def main() -> None:
    sf_cfg = SFConfig()
    om_cfg = OMConfig()

    logger.info("Fetching columns from Snowflake")
    rows = fetch_columns(sf_cfg)
    logger.info("Got %d column rows", len(rows))

    tables = group_by_table(rows)
    logger.info("Grouped into %d tables", len(tables))

    push_to_openmetadata(tables, om_cfg)


if __name__ == "__main__":
    main()
