"""
codegen.py — PySpark Notebook Code Generator
=============================================
Generates complete, runnable PySpark notebook source files (as Python strings)
for the Bronze, Silver, and Gold medallion layers from parsed Informatica
mapping metadata.

Each generated notebook includes:
- Full import block
- dbutils.widgets parameter setup
- Databricks Secrets fetching
- Layer-appropriate transformation logic
- Error handling (try/except with quarantine writes)
- Structured logging
- SCD Type 2 support (Silver/Gold)
- dbutils.notebook.exit("SUCCESS|{row_count}")

Usage
-----
    from src.migration.codegen import NotebookCodeGenerator
    from src.migration.parser import PowerMartParser

    parser  = PowerMartParser("export.xml")
    repo    = parser.parse()
    gen     = NotebookCodeGenerator()

    bronze_nb  = gen.generate_bronze(repo.mappings[0])
    silver_nb  = gen.generate_silver(repo.mappings[0])
    gold_nb    = gen.generate_gold(repo.mappings[0])

    with open("nb_bronze_orders_ingest.py", "w") as f:
        f.write(bronze_nb)
"""

from __future__ import annotations

import logging
import re
import textwrap
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from src.migration.converter import MedallionLayer, TransformationConverter
from src.migration.parser import MappingDef, TransformationDef

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CATALOG = "cder_prod"
SECRET_SCOPE = "cder-secrets"
SECRET_JDBC_URL = "oracle-jdbc-url"
SECRET_JDBC_USER = "oracle-user"
SECRET_JDBC_PASSWORD = "oracle-password"
ORACLE_DRIVER = "oracle.jdbc.OracleDriver"
JDBC_FETCHSIZE = 10000
JDBC_NUM_PARTITIONS = 8

_INDENT = "    "  # 4-space indent


# ---------------------------------------------------------------------------
# Config dataclass
# ---------------------------------------------------------------------------


@dataclass
class CodegenConfig:
    """
    Configuration that controls how notebooks are generated.

    Attributes
    ----------
    catalog:
        Unity Catalog catalog name (default: ``cder_prod``).
    secret_scope:
        Databricks secret scope name.
    oracle_driver:
        JDBC driver class name for Oracle.
    jdbc_fetchsize:
        Number of rows fetched per JDBC round-trip.
    jdbc_num_partitions:
        Number of parallel JDBC partitions.
    enable_scd2:
        When True, Silver/Gold notebooks include SCD Type 2 merge logic.
    broadcast_threshold_mb:
        Tables smaller than this (in MB) get a broadcast hint.
    quarantine_schema:
        Schema where quarantine tables are written.
    """

    catalog: str = CATALOG
    secret_scope: str = SECRET_SCOPE
    oracle_driver: str = ORACLE_DRIVER
    jdbc_fetchsize: int = JDBC_FETCHSIZE
    jdbc_num_partitions: int = JDBC_NUM_PARTITIONS
    enable_scd2: bool = True
    broadcast_threshold_mb: int = 200
    quarantine_schema: str = "silver"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    extra_spark_conf: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _snake(name: str) -> str:
    """Convert a mapping/table name to snake_case."""
    s = re.sub(r"[^A-Za-z0-9]+", "_", name)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower().strip("_")


def _nb_name(layer: str, subject: str, action: str) -> str:
    """Generate a canonical notebook name: nb_{layer}_{subject}_{action}."""
    return f"nb_{layer}_{_snake(subject)}_{action}"


def _indent(code: str, levels: int = 1) -> str:
    """Indent every line of ``code`` by ``levels * 4`` spaces."""
    prefix = _INDENT * levels
    return textwrap.indent(code, prefix)


def _source_table_name(mapping: MappingDef) -> str:
    """Best-guess Oracle source table name from a mapping."""
    if mapping.sources:
        src = mapping.sources[0]
        if src.owner:
            return f"{src.owner}.{src.name}"
        return src.name
    return mapping.name


def _target_table_name(mapping: MappingDef) -> str:
    """Best-guess target table name from a mapping."""
    if mapping.targets:
        return mapping.targets[0].name
    return mapping.name


def _subject(mapping: MappingDef) -> str:
    """Derive a short subject/entity name from the mapping name."""
    return _snake(mapping.name)


def _pk_columns(mapping: MappingDef) -> List[str]:
    """
    Attempt to derive primary key column names from the mapping's target
    field definitions. Falls back to ``["id"]`` if none are tagged.
    """
    pks: List[str] = []
    for tgt in mapping.targets:
        for f in tgt.fields:
            if "PRIMARY" in f.key_type.upper():
                pks.append(f.name.lower())
    return pks if pks else ["id"]


def _has_scd2(mapping: MappingDef) -> bool:
    """Return True if the mapping contains any Update Strategy transformation."""
    return any(t.type in ("Update Strategy",) for t in mapping.transformations)


def _expression_ports(mapping: MappingDef) -> List[TransformationDef]:
    """Return all Expression-type transformations in the mapping."""
    return [t for t in mapping.transformations if t.type == "Expression"]


def _aggregator_ports(mapping: MappingDef) -> List[TransformationDef]:
    """Return all Aggregator-type transformations in the mapping."""
    return [t for t in mapping.transformations if t.type == "Aggregator"]


def _lookup_transforms(mapping: MappingDef) -> List[TransformationDef]:
    """Return all Lookup-type transformations in the mapping."""
    return [
        t
        for t in mapping.transformations
        if t.type in ("Lookup Procedure", "Lookup")
    ]


# ---------------------------------------------------------------------------
# COMMON BLOCKS (shared across layers)
# ---------------------------------------------------------------------------

_COMMON_IMPORTS = """\
# Databricks notebook source
# MAGIC %md
# MAGIC ## {nb_name}
# MAGIC Auto-generated by dbx_migration_tool from Informatica mapping: **{mapping_name}**

# COMMAND ----------
# Standard library
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# PySpark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Delta Lake
from delta.tables import DeltaTable

# COMMAND ----------
# Structured logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("{nb_name}")
"""

_WIDGET_SETUP_BRONZE = """\
# COMMAND ----------
# Widget / parameter definitions
dbutils.widgets.removeAll()
dbutils.widgets.text("p_load_date",    datetime.now(timezone.utc).strftime("%Y-%m-%d"), "Load Date (YYYY-MM-DD)")
dbutils.widgets.text("p_mapping_name", "{mapping_name}",    "Mapping Name")
dbutils.widgets.text("p_catalog",      "{catalog}",         "Unity Catalog name")
dbutils.widgets.text("p_schema",       "{bronze_schema}",   "Bronze schema name")
dbutils.widgets.text("p_source_table", "{source_table}",    "Oracle source table (OWNER.TABLE)")
dbutils.widgets.text("p_batch_size",   "10000",             "JDBC fetch size")

p_load_date    = dbutils.widgets.get("p_load_date")
p_mapping_name = dbutils.widgets.get("p_mapping_name")
p_catalog      = dbutils.widgets.get("p_catalog")
p_schema       = dbutils.widgets.get("p_schema")
p_source_table = dbutils.widgets.get("p_source_table")
p_batch_size   = int(dbutils.widgets.get("p_batch_size"))

logger.info("Parameters: load_date=%s  mapping=%s  catalog=%s  schema=%s  source=%s",
            p_load_date, p_mapping_name, p_catalog, p_schema, p_source_table)
"""

_WIDGET_SETUP_SILVER = """\
# COMMAND ----------
# Widget / parameter definitions
dbutils.widgets.removeAll()
dbutils.widgets.text("p_load_date",     datetime.now(timezone.utc).strftime("%Y-%m-%d"), "Load Date (YYYY-MM-DD)")
dbutils.widgets.text("p_mapping_name",  "{mapping_name}",   "Mapping Name")
dbutils.widgets.text("p_catalog",       "{catalog}",        "Unity Catalog name")
dbutils.widgets.text("p_bronze_schema", "{bronze_schema}",  "Bronze schema name")
dbutils.widgets.text("p_silver_schema", "{silver_schema}",  "Silver schema name")
dbutils.widgets.text("p_subject",       "{subject}",        "Subject / entity name")

p_load_date     = dbutils.widgets.get("p_load_date")
p_mapping_name  = dbutils.widgets.get("p_mapping_name")
p_catalog       = dbutils.widgets.get("p_catalog")
p_bronze_schema = dbutils.widgets.get("p_bronze_schema")
p_silver_schema = dbutils.widgets.get("p_silver_schema")
p_subject       = dbutils.widgets.get("p_subject")

logger.info("Parameters: load_date=%s  mapping=%s  catalog=%s  subject=%s",
            p_load_date, p_mapping_name, p_catalog, p_subject)
"""

_WIDGET_SETUP_GOLD = """\
# COMMAND ----------
# Widget / parameter definitions
dbutils.widgets.removeAll()
dbutils.widgets.text("p_load_date",     datetime.now(timezone.utc).strftime("%Y-%m-%d"), "Load Date (YYYY-MM-DD)")
dbutils.widgets.text("p_mapping_name",  "{mapping_name}",   "Mapping Name")
dbutils.widgets.text("p_catalog",       "{catalog}",        "Unity Catalog name")
dbutils.widgets.text("p_silver_schema", "{silver_schema}",  "Silver schema name")
dbutils.widgets.text("p_gold_schema",   "{gold_schema}",    "Gold schema name")
dbutils.widgets.text("p_subject",       "{subject}",        "Subject / entity name")

p_load_date     = dbutils.widgets.get("p_load_date")
p_mapping_name  = dbutils.widgets.get("p_mapping_name")
p_catalog       = dbutils.widgets.get("p_catalog")
p_silver_schema = dbutils.widgets.get("p_silver_schema")
p_gold_schema   = dbutils.widgets.get("p_gold_schema")
p_subject       = dbutils.widgets.get("p_subject")

logger.info("Parameters: load_date=%s  mapping=%s  catalog=%s  subject=%s",
            p_load_date, p_mapping_name, p_catalog, p_subject)
"""

_SECRETS_BLOCK = """\
# COMMAND ----------
# Fetch Oracle credentials from Databricks Secrets
_scope = "{secret_scope}"
try:
    jdbc_url = dbutils.secrets.get(scope=_scope, key="{key_url}")
    db_user  = dbutils.secrets.get(scope=_scope, key="{key_user}")
    db_pass  = dbutils.secrets.get(scope=_scope, key="{key_pass}")
    logger.info("Secrets retrieved from scope '%s'", _scope)
except Exception as _e:
    logger.error("Failed to retrieve secrets: %s", _e)
    dbutils.notebook.exit("FAILED|secrets_error")
    raise
"""


# ---------------------------------------------------------------------------
# Bronze notebook generator
# ---------------------------------------------------------------------------


def _build_bronze(mapping: MappingDef, cfg: CodegenConfig) -> str:
    """Generate a complete Bronze layer notebook string."""
    subj = _subject(mapping)
    nb_name = _nb_name("bronze", subj, "ingest")
    source_table = _source_table_name(mapping)
    target_table = f"{cfg.catalog}.{cfg.bronze_schema}.{subj}_raw"

    lines: List[str] = []

    # --- Header & imports ---
    lines.append(
        _COMMON_IMPORTS.format(nb_name=nb_name, mapping_name=mapping.name)
    )

    # --- Widgets ---
    lines.append(
        _WIDGET_SETUP_BRONZE.format(
            mapping_name=mapping.name,
            catalog=cfg.catalog,
            bronze_schema=cfg.bronze_schema,
            source_table=source_table,
        )
    )

    # --- Secrets ---
    lines.append(
        _SECRETS_BLOCK.format(
            secret_scope=cfg.secret_scope,
            key_url=SECRET_JDBC_URL,
            key_user=SECRET_JDBC_USER,
            key_pass=SECRET_JDBC_PASSWORD,
        )
    )

    # --- JDBC read ---
    pk_cols = _pk_columns(mapping)
    pk_col = pk_cols[0] if pk_cols else "ROWID"
    lines.append(f"""\
# COMMAND ----------
# Bronze ingestion: Oracle JDBC → Delta
TARGET_TABLE = "{target_table}"
JDBC_DRIVER  = "{cfg.oracle_driver}"

try:
    logger.info("Starting JDBC read from '%s'", p_source_table)

    df_raw: DataFrame = (
        spark.read.format("jdbc")
        .option("url",            jdbc_url)
        .option("dbtable",        p_source_table)
        .option("user",           db_user)
        .option("password",       db_pass)
        .option("driver",         JDBC_DRIVER)
        .option("fetchsize",      {cfg.jdbc_fetchsize})
        .option("numPartitions",  {cfg.jdbc_num_partitions})
        .option("partitionColumn", "{pk_col}")
        .option("lowerBound",     "1")
        .option("upperBound",     "9999999")
        .load()
    )

    row_count_raw: int = df_raw.count()
    logger.info("Rows read from Oracle: %d", row_count_raw)

except Exception as exc:
    logger.error("JDBC read failed: %s", exc, exc_info=True)
    dbutils.notebook.exit(f"FAILED|jdbc_read_error|{{exc}}")
    raise
""")

    # --- Add metadata columns ---
    lines.append(f"""\
# COMMAND ----------
# Add ETL metadata columns
df_bronze: DataFrame = (
    df_raw
    .withColumn("_etl_load_date",    F.lit(p_load_date).cast(DateType()))
    .withColumn("_etl_load_ts",      F.current_timestamp())
    .withColumn("_etl_mapping_name", F.lit(p_mapping_name))
    .withColumn("_etl_source_table", F.lit(p_source_table))
)
""")

    # --- Write to Delta ---
    lines.append(f"""\
# COMMAND ----------
# Write to Bronze Delta table (append with schema evolution)
try:
    logger.info("Writing to Bronze table: %s", TARGET_TABLE)

    (
        df_bronze
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .option("overwriteSchema", "false")
        .partitionBy("_etl_load_date")
        .saveAsTable(TARGET_TABLE)
    )

    row_count_written: int = (
        spark.table(TARGET_TABLE)
        .filter(F.col("_etl_load_date") == F.lit(p_load_date))
        .count()
    )
    logger.info("Bronze write complete — rows in partition: %d", row_count_written)

except Exception as exc:
    logger.error("Bronze write failed: %s", exc, exc_info=True)
    dbutils.notebook.exit(f"FAILED|bronze_write_error|{{exc}}")
    raise
""")

    # --- Exit ---
    lines.append("""\
# COMMAND ----------
# Notebook exit — signal SUCCESS with row count to parent job
logger.info("Notebook completed successfully.")
dbutils.notebook.exit(f"SUCCESS|{row_count_written}")
""")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Silver notebook generator
# ---------------------------------------------------------------------------


def _build_silver(mapping: MappingDef, cfg: CodegenConfig) -> str:
    """Generate a complete Silver layer notebook string."""
    subj = _subject(mapping)
    nb_name = _nb_name("silver", subj, "transform")
    source_table = f"{cfg.catalog}.{cfg.bronze_schema}.{subj}_raw"
    target_table = f"{cfg.catalog}.{cfg.silver_schema}.{subj}"
    quarantine_table = f"{cfg.catalog}.{cfg.quarantine_schema}.quarantine_{subj}"
    pk_cols = _pk_columns(mapping)
    pk_join_cond = " AND ".join([f"tgt.{c} = src.{c}" for c in pk_cols])

    # Collect expression transforms
    expr_transforms = _expression_ports(mapping)
    agg_transforms = _aggregator_ports(mapping)
    lookup_transforms = _lookup_transforms(mapping)
    scd2_enabled = cfg.enable_scd2 and _has_scd2(mapping)

    lines: List[str] = []

    # --- Header & imports ---
    lines.append(
        _COMMON_IMPORTS.format(nb_name=nb_name, mapping_name=mapping.name)
    )

    # --- Widgets ---
    lines.append(
        _WIDGET_SETUP_SILVER.format(
            mapping_name=mapping.name,
            catalog=cfg.catalog,
            bronze_schema=cfg.bronze_schema,
            silver_schema=cfg.silver_schema,
            subject=subj,
        )
    )

    # --- Read Bronze ---
    lines.append(f"""\
# COMMAND ----------
# Read from Bronze layer (incremental by load date)
SOURCE_TABLE     = "{source_table}"
TARGET_TABLE     = "{target_table}"
QUARANTINE_TABLE = "{quarantine_table}"

try:
    logger.info("Reading Bronze table: %s  (load_date=%s)", SOURCE_TABLE, p_load_date)
    df_bronze: DataFrame = (
        spark.table(SOURCE_TABLE)
        .filter(F.col("_etl_load_date") == F.lit(p_load_date))
    )
    row_count_bronze: int = df_bronze.count()
    logger.info("Bronze rows for this load date: %d", row_count_bronze)

except Exception as exc:
    logger.error("Bronze read failed: %s", exc, exc_info=True)
    dbutils.notebook.exit(f"FAILED|bronze_read_error|{{exc}}")
    raise
""")

    # --- Expression transformations ---
    if expr_transforms:
        expr_block_lines = [
            "# COMMAND ----------",
            "# Expression transformations (auto-generated from Informatica Expression ports)",
            "df_expr: DataFrame = df_bronze",
        ]
        for t in expr_transforms:
            expr_block_lines.append(f"# Transformation: {t.name} ({t.type})")
            for port in t.ports:
                if port.expression:
                    safe_expr = port.expression.replace('"', '\\"')
                    expr_block_lines.append(
                        f'df_expr = df_expr.withColumn("{port.name.lower()}", '
                        f'F.expr("{safe_expr}"))'
                    )
        expr_block_lines.append("")
        lines.append("\n".join(expr_block_lines))

    # --- Aggregations ---
    if agg_transforms:
        agg_block_lines = [
            "# COMMAND ----------",
            "# Aggregator transformations",
            "df_agg: DataFrame = df_expr if 'df_expr' in dir() else df_bronze",
        ]
        for t in agg_transforms:
            agg_block_lines.append(f"# Aggregation: {t.name}")
            group_ports = [p.name.lower() for p in t.ports if p.porttype == "INPUT"]
            agg_ports = [p for p in t.ports if p.porttype == "OUTPUT" and p.expression]
            if group_ports:
                agg_block_lines.append(f"# Group-by: {group_ports}")
            if agg_ports:
                agg_block_lines.append(f"# Aggregate output ports: {[p.name for p in agg_ports]}")
            agg_block_lines.append("# TODO: implement groupBy().agg() for this aggregator")
        agg_block_lines.append("")
        lines.append("\n".join(agg_block_lines))

    # --- Lookup joins ---
    if lookup_transforms:
        lu_block_lines = [
            "# COMMAND ----------",
            "# Lookup transformations (broadcast join for tables < {cfg.broadcast_threshold_mb} MB)",
        ]
        for t in lookup_transforms:
            lu_block_lines.append(f"# Lookup: {t.name}  table: {t.table_name or 'UNKNOWN'}")
            if t.table_name:
                lu_block_lines.append(
                    f'# df_lookup_{_snake(t.name)} = F.broadcast(spark.table("{t.table_name}"))'
                )
            lu_block_lines.append("# TODO: add join condition and select output ports")
        lu_block_lines.append("")
        lines.append("\n".join(lu_block_lines))

    # --- Data quality / quarantine ---
    lines.append(f"""\
# COMMAND ----------
# Data quality — quarantine records failing NOT NULL or business rules
df_source: DataFrame = df_expr if 'df_expr' in dir() else df_bronze

# Define quality check — extend with domain-specific rules
null_check_cols = {pk_cols}
dq_filter = F.lit(True)
for _c in null_check_cols:
    if _c in df_source.columns:
        dq_filter = dq_filter & F.col(_c).isNotNull()

df_good: DataFrame   = df_source.filter(dq_filter)
df_bad:  DataFrame   = df_source.filter(~dq_filter).withColumn(
    "_dq_reason", F.lit("NULL in key column(s): " + str(null_check_cols))
)

bad_count: int = df_bad.count()
if bad_count > 0:
    logger.warning("Quarantining %d bad records to %s", bad_count, QUARANTINE_TABLE)
    try:
        (
            df_bad
            .withColumn("_etl_quarantine_ts", F.current_timestamp())
            .write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(QUARANTINE_TABLE)
        )
    except Exception as _qe:
        logger.error("Quarantine write failed: %s", _qe, exc_info=True)
        # Quarantine failure is non-fatal — log and continue
else:
    logger.info("All records passed data quality checks.")
""")

    # --- SCD Type 2 block ---
    if scd2_enabled:
        lines.append(_build_scd2_block(subj, pk_cols, cfg))
    else:
        # Standard Delta write with mergeSchema
        lines.append(f"""\
# COMMAND ----------
# Write Silver table (Delta, mergeSchema=true)
try:
    logger.info("Writing Silver table: %s", TARGET_TABLE)
    (
        df_good
        .withColumn("_etl_silver_ts", F.current_timestamp())
        .withColumn("_etl_load_date", F.lit(p_load_date).cast(DateType()))
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("_etl_load_date")
        .saveAsTable(TARGET_TABLE)
    )
    row_count_written: int = df_good.count()
    logger.info("Silver write complete. Rows written: %d", row_count_written)

except Exception as exc:
    logger.error("Silver write failed: %s", exc, exc_info=True)
    dbutils.notebook.exit(f"FAILED|silver_write_error|{{exc}}")
    raise
""")

    # --- Exit ---
    lines.append("""\
# COMMAND ----------
logger.info("Silver notebook completed successfully.")
dbutils.notebook.exit(f"SUCCESS|{row_count_written}")
""")

    return "\n".join(lines)


def _build_scd2_block(subj: str, pk_cols: List[str], cfg: CodegenConfig) -> str:
    """Return SCD Type 2 MERGE block as a string."""
    target_table = f"{cfg.catalog}.{cfg.silver_schema}.{subj}"
    pk_join_cond = " AND ".join([f"tgt.{c} = src.{c}" for c in pk_cols])
    return f"""\
# COMMAND ----------
# SCD Type 2 — expire old records and insert new versions
# Requires columns: is_current (BOOLEAN), eff_start_date (DATE), eff_end_date (DATE)
TARGET_TABLE = "{target_table}"

try:
    if not spark.catalog.tableExists(TARGET_TABLE):
        logger.info("Target table does not exist — performing initial load.")
        (
            df_good
            .withColumn("is_current",     F.lit(True))
            .withColumn("eff_start_date", F.lit(p_load_date).cast(DateType()))
            .withColumn("eff_end_date",   F.lit("9999-12-31").cast(DateType()))
            .withColumn("_etl_load_date", F.lit(p_load_date).cast(DateType()))
            .write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .saveAsTable(TARGET_TABLE)
        )
        row_count_written = df_good.count()
    else:
        delta_tbl = DeltaTable.forName(spark, TARGET_TABLE)

        # Step 1: expire current records that have changed
        (
            delta_tbl.alias("tgt")
            .merge(
                df_good.alias("src"),
                "{pk_join_cond}"
            )
            .whenMatchedUpdate(
                condition=(
                    "tgt.is_current = true AND ("
                    + " OR ".join([f"tgt.{{c}} <> src.{{c}}"
                                   for c in ["_etl_load_date"]])  # extend with business cols
                    + ")"
                ),
                set={{
                    "is_current":   "false",
                    "eff_end_date": f"cast('{{p_load_date}}' as date) - interval 1 day",
                }},
            )
            .execute()
        )

        # Step 2: insert new current versions
        df_new_versions: DataFrame = (
            df_good
            .withColumn("is_current",     F.lit(True))
            .withColumn("eff_start_date", F.lit(p_load_date).cast(DateType()))
            .withColumn("eff_end_date",   F.lit("9999-12-31").cast(DateType()))
            .withColumn("_etl_load_date", F.lit(p_load_date).cast(DateType()))
        )
        (
            df_new_versions
            .write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(TARGET_TABLE)
        )
        row_count_written = df_new_versions.count()

    logger.info("SCD2 write complete. New versions inserted: %d", row_count_written)

except Exception as exc:
    logger.error("SCD2 write failed: %s", exc, exc_info=True)
    dbutils.notebook.exit(f"FAILED|scd2_write_error|{{exc}}")
    raise
"""


# ---------------------------------------------------------------------------
# Gold notebook generator
# ---------------------------------------------------------------------------


def _build_gold(mapping: MappingDef, cfg: CodegenConfig) -> str:
    """Generate a complete Gold layer notebook string."""
    subj = _subject(mapping)
    nb_name = _nb_name("gold", subj, "merge")
    source_table = f"{cfg.catalog}.{cfg.silver_schema}.{subj}"
    target_table = f"{cfg.catalog}.{cfg.gold_schema}.{subj}"
    pk_cols = _pk_columns(mapping)
    pk_join_cond = " AND ".join([f"tgt.{c} = src.{c}" for c in pk_cols])
    zorder_cols = ", ".join(pk_cols)

    lines: List[str] = []

    # --- Header & imports ---
    lines.append(
        _COMMON_IMPORTS.format(nb_name=nb_name, mapping_name=mapping.name)
    )

    # --- Widgets ---
    lines.append(
        _WIDGET_SETUP_GOLD.format(
            mapping_name=mapping.name,
            catalog=cfg.catalog,
            silver_schema=cfg.silver_schema,
            gold_schema=cfg.gold_schema,
            subject=subj,
        )
    )

    # --- Read Silver ---
    lines.append(f"""\
# COMMAND ----------
# Read from Silver layer
SOURCE_TABLE = "{source_table}"
TARGET_TABLE = "{target_table}"

try:
    logger.info("Reading Silver table: %s", SOURCE_TABLE)
    df_silver: DataFrame = (
        spark.table(SOURCE_TABLE)
        .filter(F.col("_etl_load_date") == F.lit(p_load_date))
    )
    row_count_silver: int = df_silver.count()
    logger.info("Silver rows for this load date: %d", row_count_silver)

except Exception as exc:
    logger.error("Silver read failed: %s", exc, exc_info=True)
    dbutils.notebook.exit(f"FAILED|silver_read_error|{{exc}}")
    raise
""")

    # --- Business aggregations / final transforms ---
    lines.append(f"""\
# COMMAND ----------
# Gold-layer business transformations
# Extend this block with final aggregations, metric calculations, etc.
df_gold: DataFrame = (
    df_silver
    .withColumn("_etl_gold_ts",   F.current_timestamp())
    .withColumn("_etl_load_date", F.lit(p_load_date).cast(DateType()))
    # TODO: add Gold-layer derived columns and aggregations here
)
""")

    # --- Delta MERGE ---
    lines.append(f"""\
# COMMAND ----------
# Delta MERGE (upsert) into Gold table
try:
    if not spark.catalog.tableExists(TARGET_TABLE):
        logger.info("Gold table does not exist — creating via initial overwrite.")
        (
            df_gold
            .write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .saveAsTable(TARGET_TABLE)
        )
        row_count_merged: int = df_gold.count()
        logger.info("Initial Gold load complete. Rows written: %d", row_count_merged)
    else:
        delta_tbl = DeltaTable.forName(spark, TARGET_TABLE)
        (
            delta_tbl.alias("tgt")
            .merge(
                df_gold.alias("src"),
                "{pk_join_cond}",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        row_count_merged: int = df_gold.count()
        logger.info("Delta MERGE complete. Source rows processed: %d", row_count_merged)

except Exception as exc:
    logger.error("Gold MERGE failed: %s", exc, exc_info=True)
    dbutils.notebook.exit(f"FAILED|gold_merge_error|{{exc}}")
    raise
""")

    # --- OPTIMIZE + ZORDER ---
    lines.append(f"""\
# COMMAND ----------
# OPTIMIZE the Gold table with Z-ORDER on primary key column(s)
try:
    logger.info("Running OPTIMIZE ZORDER BY ({zorder_cols}) on %s", TARGET_TABLE)
    spark.sql(
        "OPTIMIZE {{table}} ZORDER BY ({zorder_cols})".format(table=TARGET_TABLE)
    )
    logger.info("OPTIMIZE complete.")

except Exception as exc:
    # OPTIMIZE failure is non-fatal; log and continue
    logger.warning("OPTIMIZE ZORDER failed (non-fatal): %s", exc)
""")

    # --- Exit ---
    lines.append("""\
# COMMAND ----------
logger.info("Gold notebook completed successfully.")
dbutils.notebook.exit(f"SUCCESS|{row_count_merged}")
""")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main code generator class
# ---------------------------------------------------------------------------


class NotebookCodeGenerator:
    """
    Generates production-quality PySpark notebook source code from
    parsed Informatica mapping metadata.

    Parameters
    ----------
    config:
        Optional :class:`CodegenConfig` instance. Uses defaults if omitted.
    converter:
        Optional :class:`TransformationConverter` instance.
    """

    def __init__(
        self,
        config: Optional[CodegenConfig] = None,
        converter: Optional[TransformationConverter] = None,
    ) -> None:
        self.config = config or CodegenConfig()
        self.converter = converter or TransformationConverter()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_bronze(self, mapping: MappingDef) -> str:
        """
        Generate a Bronze layer ingest notebook for ``mapping``.

        Parameters
        ----------
        mapping:
            Parsed Informatica mapping.

        Returns
        -------
        str
            Complete Python notebook source (``# Databricks notebook source`` format).
        """
        logger.info("Generating Bronze notebook for mapping: %s", mapping.name)
        nb = _build_bronze(mapping, self.config)
        logger.debug("Bronze notebook generated (%d chars)", len(nb))
        return nb

    def generate_silver(self, mapping: MappingDef) -> str:
        """
        Generate a Silver layer transformation notebook for ``mapping``.

        Parameters
        ----------
        mapping:
            Parsed Informatica mapping.

        Returns
        -------
        str
            Complete Python notebook source.
        """
        logger.info("Generating Silver notebook for mapping: %s", mapping.name)
        nb = _build_silver(mapping, self.config)
        logger.debug("Silver notebook generated (%d chars)", len(nb))
        return nb

    def generate_gold(self, mapping: MappingDef) -> str:
        """
        Generate a Gold layer MERGE+OPTIMIZE notebook for ``mapping``.

        Parameters
        ----------
        mapping:
            Parsed Informatica mapping.

        Returns
        -------
        str
            Complete Python notebook source.
        """
        logger.info("Generating Gold notebook for mapping: %s", mapping.name)
        nb = _build_gold(mapping, self.config)
        logger.debug("Gold notebook generated (%d chars)", len(nb))
        return nb

    def generate_all(self, mapping: MappingDef) -> Dict[str, str]:
        """
        Generate Bronze, Silver, and Gold notebooks for ``mapping``.

        Parameters
        ----------
        mapping:
            Parsed Informatica mapping.

        Returns
        -------
        dict
            Keys: ``"bronze"``, ``"silver"``, ``"gold"``.
            Values: notebook source strings.
        """
        return {
            "bronze": self.generate_bronze(mapping),
            "silver": self.generate_silver(mapping),
            "gold":   self.generate_gold(mapping),
        }

    def notebook_name(self, layer: str, mapping: MappingDef) -> str:
        """
        Return the canonical notebook file name for a given layer and mapping.

        Parameters
        ----------
        layer:
            One of ``"bronze"``, ``"silver"``, or ``"gold"``.
        mapping:
            Parsed Informatica mapping.

        Returns
        -------
        str
            E.g. ``"nb_bronze_orders_ingest"``.
        """
        action_map = {"bronze": "ingest", "silver": "transform", "gold": "merge"}
        return _nb_name(layer, _subject(mapping), action_map.get(layer, "run"))
