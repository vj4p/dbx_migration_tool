# Databricks notebook source
# MAGIC %md
# MAGIC ## nb_silver_template_transform
# MAGIC **Layer:** Silver — Cleansing, transformations, SCD Type 2, and quarantine
# MAGIC
# MAGIC **Purpose:** Read the Bronze raw table for the current load date, apply
# MAGIC business-rule transformations, quarantine records that fail data quality
# MAGIC checks, and write clean records to the Silver Delta table. Supports both
# MAGIC full-overwrite (SCD1-style) and SCD Type 2 history tracking.
# MAGIC
# MAGIC **Medallion position:** Bronze (`cder_prod.bronze.<subject>_raw`)
# MAGIC   → Silver (`cder_prod.silver.<subject>`)
# MAGIC   → Quarantine (`cder_prod.silver.quarantine_<subject>`)
# MAGIC
# MAGIC **Parameters (dbutils.widgets)**
# MAGIC | Widget | Default | Description |
# MAGIC |--------|---------|-------------|
# MAGIC | p_load_date | today | Batch date `YYYY-MM-DD` |
# MAGIC | p_mapping_name | — | Mapping name for lineage |
# MAGIC | p_catalog | cder_prod | Unity Catalog catalog |
# MAGIC | p_bronze_schema | bronze | Bronze schema |
# MAGIC | p_silver_schema | silver | Silver schema |
# MAGIC | p_subject | — | Subject / entity name |
# MAGIC | p_scd2_enabled | false | Enable SCD Type 2 history |
# MAGIC | p_pk_cols | id | Comma-separated primary key columns |
# MAGIC | p_business_cols | — | Comma-separated change-detection columns |

# COMMAND ----------
# =============================================================================
# Imports
# =============================================================================
import logging
import sys
from datetime import datetime, timezone
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, TimestampType
from pyspark.sql.window import Window

try:
    from delta.tables import DeltaTable
except ImportError:
    DeltaTable = None

# COMMAND ----------
# =============================================================================
# Logging setup
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("nb_silver_template_transform")

# COMMAND ----------
# =============================================================================
# Widget / parameter definitions
# =============================================================================
dbutils.widgets.removeAll()

dbutils.widgets.text(
    "p_load_date",
    datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    "Load Date (YYYY-MM-DD)",
)
dbutils.widgets.text("p_mapping_name",   "",        "Mapping Name")
dbutils.widgets.text("p_catalog",        "cder_prod", "Unity Catalog name")
dbutils.widgets.text("p_bronze_schema",  "bronze",   "Bronze schema name")
dbutils.widgets.text("p_silver_schema",  "silver",   "Silver schema name")
dbutils.widgets.text("p_subject",        "",         "Subject / entity name (snake_case)")
dbutils.widgets.dropdown("p_scd2_enabled", "false", ["true", "false"], "Enable SCD Type 2")
dbutils.widgets.text("p_pk_cols",        "id",       "Primary key columns (comma-separated)")
dbutils.widgets.text("p_business_cols",  "",         "Business columns for change detection (comma-separated)")

p_load_date       = dbutils.widgets.get("p_load_date")
p_mapping_name    = dbutils.widgets.get("p_mapping_name")
p_catalog         = dbutils.widgets.get("p_catalog")
p_bronze_schema   = dbutils.widgets.get("p_bronze_schema")
p_silver_schema   = dbutils.widgets.get("p_silver_schema")
p_subject         = dbutils.widgets.get("p_subject")
p_scd2_enabled    = dbutils.widgets.get("p_scd2_enabled").lower() == "true"
p_pk_cols_raw     = dbutils.widgets.get("p_pk_cols")
p_business_cols   = dbutils.widgets.get("p_business_cols")

# Parse list parameters
pk_cols: List[str] = [c.strip() for c in p_pk_cols_raw.split(",") if c.strip()]
business_cols: List[str] = (
    [c.strip() for c in p_business_cols.split(",") if c.strip()]
    if p_business_cols else []
)

SOURCE_TABLE     = f"{p_catalog}.{p_bronze_schema}.{p_subject}_raw"
TARGET_TABLE     = f"{p_catalog}.{p_silver_schema}.{p_subject}"
QUARANTINE_TABLE = f"{p_catalog}.{p_silver_schema}.quarantine_{p_subject}"

logger.info(
    "Parameters — load_date=%s  subject=%s  scd2=%s  pk_cols=%s",
    p_load_date, p_subject, p_scd2_enabled, pk_cols,
)
logger.info("Source: %s  Target: %s", SOURCE_TABLE, TARGET_TABLE)

# COMMAND ----------
# =============================================================================
# Read Bronze layer (incremental by load date)
# =============================================================================
try:
    logger.info("Reading Bronze table: %s  (load_date=%s)", SOURCE_TABLE, p_load_date)

    df_bronze: DataFrame = (
        spark.table(SOURCE_TABLE)
        .filter(F.col("_etl_load_date") == F.lit(p_load_date))
    )

    row_count_bronze: int = df_bronze.count()
    logger.info("Bronze rows for load_date=%s: %d", p_load_date, row_count_bronze)

    if row_count_bronze == 0:
        logger.warning("No Bronze rows for this load date. Exiting with zero count.")
        dbutils.notebook.exit(f"SUCCESS|0")

except Exception as exc:
    logger.error("Bronze read failed: %s", exc, exc_info=True)
    dbutils.notebook.exit(f"FAILED|bronze_read_error|{exc}")
    raise

# COMMAND ----------
# =============================================================================
# Business transformations
# =============================================================================
# ---------------------------------------------------------------------------
# Step 1: Standardise / cleanse
# ---------------------------------------------------------------------------
df_cleansed: DataFrame = (
    df_bronze
    # Trim string columns — extend with domain-specific cleansing
    .select(
        *[
            F.trim(F.col(c)).alias(c) if t == "string" else F.col(c)
            for c, t in df_bronze.dtypes
        ]
    )
    # Replace empty strings with NULL
    .select(
        *[
            F.when(F.col(c) == "", None).otherwise(F.col(c)).alias(c)
            if t == "string" else F.col(c)
            for c, t in df_bronze.dtypes
        ]
    )
)

# ---------------------------------------------------------------------------
# Step 2: Derived / expression columns
# TODO: Replace the placeholder transformations below with mapping-specific logic
# generated by NotebookCodeGenerator or implemented manually.
# ---------------------------------------------------------------------------
df_transformed: DataFrame = (
    df_cleansed
    # Example: derive a status flag
    # .withColumn("active_flag", F.when(F.col("status") == "A", True).otherwise(False))
    # Example: parse a date string
    # .withColumn("effective_date", F.to_date(F.col("eff_dt_str"), "YYYYMMDD"))
    .withColumn("_etl_silver_ts", F.current_timestamp())
)

logger.info("Transformations applied. Column count: %d", len(df_transformed.columns))

# COMMAND ----------
# =============================================================================
# Data quality checks — quarantine bad records
# =============================================================================
# Build NOT-NULL predicate over all primary key columns
dq_null_check = F.lit(True)
for _pk in pk_cols:
    if _pk in df_transformed.columns:
        dq_null_check = dq_null_check & F.col(_pk).isNotNull()
    else:
        logger.warning("PK column '%s' not found in DataFrame — skipping null check.", _pk)

# TODO: extend with additional domain-specific DQ rules
dq_combined_check = dq_null_check

df_good: DataFrame = df_transformed.filter(dq_combined_check)
df_bad:  DataFrame = df_transformed.filter(~dq_combined_check).withColumn(
    "_dq_reason",
    F.lit(f"NULL in key column(s): {pk_cols}"),
)

bad_count: int = df_bad.count()

if bad_count > 0:
    logger.warning(
        "Data quality: %d record(s) quarantined to %s", bad_count, QUARANTINE_TABLE
    )
    try:
        (
            df_bad
            .withColumn("_etl_quarantine_ts", F.current_timestamp())
            .withColumn("_etl_load_date",     F.lit(p_load_date).cast(DateType()))
            .write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(QUARANTINE_TABLE)
        )
        logger.info("Quarantine write complete.")
    except Exception as _qe:
        # Quarantine failure is non-fatal; log and continue
        logger.error("Quarantine write failed (non-fatal): %s", _qe, exc_info=True)
else:
    logger.info("All %d records passed data quality checks.", row_count_bronze)

good_count: int = df_good.count()
logger.info("Good records to write: %d", good_count)

# COMMAND ----------
# =============================================================================
# Write to Silver Delta table
# =============================================================================
# ---------------------------------------------------------------------------
# SCD Type 2 path
# ---------------------------------------------------------------------------
if p_scd2_enabled:
    logger.info("SCD Type 2 mode enabled for %s", TARGET_TABLE)
    try:
        df_scd2_new: DataFrame = (
            df_good
            .withColumn("is_current",     F.lit(True).cast(BooleanType()))
            .withColumn("eff_start_date", F.lit(p_load_date).cast(DateType()))
            .withColumn("eff_end_date",   F.lit("9999-12-31").cast(DateType()))
            .withColumn("_etl_load_date", F.lit(p_load_date).cast(DateType()))
        )

        if not spark.catalog.tableExists(TARGET_TABLE):
            logger.info("SCD2 target does not exist — initial full load.")
            (
                df_scd2_new
                .write
                .format("delta")
                .mode("overwrite")
                .option("mergeSchema", "true")
                .saveAsTable(TARGET_TABLE)
            )
            row_count_written: int = df_scd2_new.count()
        else:
            # ---------------------------------------------------------------
            # Step 1: expire current rows where business columns have changed
            # ---------------------------------------------------------------
            if business_cols:
                change_cond = " OR ".join(
                    [f"tgt.{c} <> src.{c}" for c in business_cols
                     if c in df_scd2_new.columns]
                )
            else:
                all_biz_cols = [
                    c for c in df_good.columns
                    if c not in pk_cols
                    and not c.startswith("_etl")
                    and c not in ("is_current", "eff_start_date", "eff_end_date")
                ]
                change_cond = (
                    " OR ".join([f"tgt.{c} <> src.{c}" for c in all_biz_cols])
                    if all_biz_cols else "1=1"
                )

            pk_join = " AND ".join([f"tgt.{c} = src.{c}" for c in pk_cols])
            expire_cond = f"tgt.is_current = true AND ({change_cond})"

            delta_tbl = DeltaTable.forName(spark, TARGET_TABLE)
            (
                delta_tbl.alias("tgt")
                .merge(df_scd2_new.alias("src"), pk_join)
                .whenMatchedUpdate(
                    condition=expire_cond,
                    set={
                        "is_current":   "false",
                        "eff_end_date": (
                            f"cast(date_sub(cast('{p_load_date}' as date), 1) as date)"
                        ),
                    },
                )
                .execute()
            )
            logger.info("SCD2 expiry step complete.")

            # ---------------------------------------------------------------
            # Step 2: insert new current versions for changed + net-new records
            # ---------------------------------------------------------------
            existing_keys = (
                spark.table(TARGET_TABLE)
                .filter(F.col("is_current") == True)  # noqa: E712
                .select(*pk_cols)
            )
            df_net_new = df_scd2_new.join(
                F.broadcast(existing_keys), on=pk_cols, how="left_anti"
            )
            df_changed_pks = (
                spark.table(TARGET_TABLE)
                .filter(
                    (F.col("is_current") == False)  # noqa: E712
                    & (F.col("eff_end_date") == F.expr(
                        f"date_sub(cast('{p_load_date}' as date), 1)"
                    ))
                )
                .select(*pk_cols)
            )
            df_reinsert = df_scd2_new.join(df_changed_pks, on=pk_cols, how="inner")
            df_final_insert = df_net_new.unionByName(df_reinsert, allowMissingColumns=True)

            (
                df_final_insert
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(TARGET_TABLE)
            )
            row_count_written = df_final_insert.count()
            logger.info("SCD2 insert step complete. New versions: %d", row_count_written)

    except Exception as exc:
        logger.error("SCD2 write failed: %s", exc, exc_info=True)
        dbutils.notebook.exit(f"FAILED|scd2_write_error|{exc}")
        raise

# ---------------------------------------------------------------------------
# Standard append path (SCD Type 1 / non-history)
# ---------------------------------------------------------------------------
else:
    try:
        logger.info("Standard append write to Silver table: %s", TARGET_TABLE)
        (
            df_good
            .withColumn("_etl_load_date", F.lit(p_load_date).cast(DateType()))
            .write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("_etl_load_date")
            .saveAsTable(TARGET_TABLE)
        )
        row_count_written = good_count
        logger.info("Silver write complete. Rows written: %d", row_count_written)

    except Exception as exc:
        logger.error("Silver write failed: %s", exc, exc_info=True)
        dbutils.notebook.exit(f"FAILED|silver_write_error|{exc}")
        raise

# COMMAND ----------
# =============================================================================
# Notebook exit
# =============================================================================
logger.info("nb_silver_template_transform completed successfully.")
dbutils.notebook.exit(f"SUCCESS|{row_count_written}")
