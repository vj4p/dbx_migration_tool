# Databricks notebook source
# MAGIC %md
# MAGIC ## nb_gold_template_merge
# MAGIC **Layer:** Gold — Delta MERGE upsert + OPTIMIZE ZORDER
# MAGIC
# MAGIC **Purpose:** Read from the Silver table for the current load date, apply
# MAGIC final Gold-layer business logic and aggregations, upsert into the Gold
# MAGIC Delta table via `MERGE`, then OPTIMIZE with Z-ORDER on the primary key
# MAGIC columns for maximum query performance.
# MAGIC
# MAGIC **Medallion position:** Silver (`cder_prod.silver.<subject>`)
# MAGIC   → Gold (`cder_prod.gold.<subject>`)
# MAGIC
# MAGIC **Parameters (dbutils.widgets)**
# MAGIC | Widget | Default | Description |
# MAGIC |--------|---------|-------------|
# MAGIC | p_load_date | today | Batch date `YYYY-MM-DD` |
# MAGIC | p_mapping_name | — | Mapping name for lineage |
# MAGIC | p_catalog | cder_prod | Unity Catalog catalog |
# MAGIC | p_silver_schema | silver | Silver schema |
# MAGIC | p_gold_schema | gold | Gold schema |
# MAGIC | p_subject | — | Subject / entity name |
# MAGIC | p_pk_cols | id | Comma-separated primary key columns |
# MAGIC | p_zorder_cols | — | Comma-separated Z-ORDER columns (defaults to pk_cols) |
# MAGIC | p_run_optimize | true | Run OPTIMIZE after MERGE |
# MAGIC | p_delete_indicator_col | — | Column signalling a delete in source |
# MAGIC | p_delete_indicator_val | D | Value that means "delete this row" |

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
from pyspark.sql.types import DateType, TimestampType

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
logger = logging.getLogger("nb_gold_template_merge")

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
dbutils.widgets.text("p_mapping_name",         "",          "Mapping Name")
dbutils.widgets.text("p_catalog",              "cder_prod", "Unity Catalog name")
dbutils.widgets.text("p_silver_schema",        "silver",    "Silver schema name")
dbutils.widgets.text("p_gold_schema",          "gold",      "Gold schema name")
dbutils.widgets.text("p_subject",              "",          "Subject / entity name (snake_case)")
dbutils.widgets.text("p_pk_cols",              "id",        "Primary key columns (comma-separated)")
dbutils.widgets.text("p_zorder_cols",          "",          "Z-ORDER columns (defaults to pk_cols)")
dbutils.widgets.dropdown("p_run_optimize",     "true", ["true", "false"], "Run OPTIMIZE after MERGE")
dbutils.widgets.text("p_delete_indicator_col", "",          "Column name for delete flag")
dbutils.widgets.text("p_delete_indicator_val", "D",         "Value that signals a delete")

p_load_date             = dbutils.widgets.get("p_load_date")
p_mapping_name          = dbutils.widgets.get("p_mapping_name")
p_catalog               = dbutils.widgets.get("p_catalog")
p_silver_schema         = dbutils.widgets.get("p_silver_schema")
p_gold_schema           = dbutils.widgets.get("p_gold_schema")
p_subject               = dbutils.widgets.get("p_subject")
p_pk_cols_raw           = dbutils.widgets.get("p_pk_cols")
p_zorder_cols_raw       = dbutils.widgets.get("p_zorder_cols")
p_run_optimize          = dbutils.widgets.get("p_run_optimize").lower() == "true"
p_delete_indicator_col  = dbutils.widgets.get("p_delete_indicator_col")
p_delete_indicator_val  = dbutils.widgets.get("p_delete_indicator_val")

# Parse list parameters
pk_cols: List[str] = [c.strip() for c in p_pk_cols_raw.split(",") if c.strip()]
zorder_cols: List[str] = (
    [c.strip() for c in p_zorder_cols_raw.split(",") if c.strip()]
    if p_zorder_cols_raw else pk_cols
)

SOURCE_TABLE = f"{p_catalog}.{p_silver_schema}.{p_subject}"
TARGET_TABLE = f"{p_catalog}.{p_gold_schema}.{p_subject}"
pk_join_cond = " AND ".join([f"tgt.{c} = src.{c}" for c in pk_cols])

logger.info(
    "Parameters — load_date=%s  subject=%s  pk_cols=%s  zorder=%s  optimize=%s",
    p_load_date, p_subject, pk_cols, zorder_cols, p_run_optimize,
)
logger.info("Source: %s  Target: %s", SOURCE_TABLE, TARGET_TABLE)

# COMMAND ----------
# =============================================================================
# Validate required parameters
# =============================================================================
if not p_subject:
    logger.error("p_subject widget is required.")
    dbutils.notebook.exit("FAILED|missing_parameter|p_subject")

if not pk_cols:
    logger.error("p_pk_cols widget is required.")
    dbutils.notebook.exit("FAILED|missing_parameter|p_pk_cols")

# COMMAND ----------
# =============================================================================
# Read from Silver layer
# =============================================================================
try:
    logger.info("Reading Silver table: %s  (load_date=%s)", SOURCE_TABLE, p_load_date)

    df_silver: DataFrame = (
        spark.table(SOURCE_TABLE)
        .filter(F.col("_etl_load_date") == F.lit(p_load_date))
    )

    row_count_silver: int = df_silver.count()
    logger.info("Silver rows for load_date=%s: %d", p_load_date, row_count_silver)

    if row_count_silver == 0:
        logger.warning("No Silver rows for this load date. Exiting with zero count.")
        dbutils.notebook.exit(f"SUCCESS|0")

except Exception as exc:
    logger.error("Silver read failed: %s", exc, exc_info=True)
    dbutils.notebook.exit(f"FAILED|silver_read_error|{exc}")
    raise

# COMMAND ----------
# =============================================================================
# Gold-layer business transformations
# TODO: Replace/extend with mapping-specific aggregations and derived columns.
# =============================================================================
df_gold: DataFrame = (
    df_silver
    # Drop SCD2 columns if promoted from Silver history table
    .drop(*[c for c in ["is_current", "eff_start_date", "eff_end_date"]
            if c in df_silver.columns])
    .withColumn("_etl_gold_ts",   F.current_timestamp())
    .withColumn("_etl_load_date", F.lit(p_load_date).cast(DateType()))
    # Example: aggregate or derive Gold-specific metrics
    # .withColumn("revenue_usd", F.col("quantity") * F.col("unit_price"))
)

logger.info(
    "Gold transformations applied. Rows: %d  Columns: %d",
    row_count_silver, len(df_gold.columns),
)

# COMMAND ----------
# =============================================================================
# Delta MERGE (upsert) into Gold table
# =============================================================================
try:
    if not spark.catalog.tableExists(TARGET_TABLE):
        # ----------------------------------------------------------------
        # Initial load — no existing table, just overwrite
        # ----------------------------------------------------------------
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
        # ----------------------------------------------------------------
        # Incremental MERGE — upsert with optional delete support
        # ----------------------------------------------------------------
        if DeltaTable is None:
            raise ImportError("delta-spark package is required for MERGE operations.")

        logger.info("Delta MERGE on %s  pk_join=%s", TARGET_TABLE, pk_join_cond)
        delta_tbl = DeltaTable.forName(spark, TARGET_TABLE)
        merge_builder = (
            delta_tbl.alias("tgt")
            .merge(df_gold.alias("src"), pk_join_cond)
        )

        # Optional: matched delete based on indicator column
        if p_delete_indicator_col and p_delete_indicator_col in df_gold.columns:
            merge_builder = merge_builder.whenMatchedDelete(
                condition=f"src.{p_delete_indicator_col} = '{p_delete_indicator_val}'"
            )
            logger.info(
                "Delete handling enabled: %s = '%s'",
                p_delete_indicator_col, p_delete_indicator_val,
            )

        # Matched update all non-key columns
        merge_builder = (
            merge_builder
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
        )
        merge_builder.execute()

        row_count_merged = df_gold.count()
        logger.info(
            "Delta MERGE complete on %s. Source rows processed: %d",
            TARGET_TABLE, row_count_merged,
        )

except Exception as exc:
    logger.error("Gold MERGE failed: %s", exc, exc_info=True)
    dbutils.notebook.exit(f"FAILED|gold_merge_error|{exc}")
    raise

# COMMAND ----------
# =============================================================================
# OPTIMIZE + Z-ORDER
# =============================================================================
if p_run_optimize:
    zorder_str = ", ".join(zorder_cols)
    optimize_sql = f"OPTIMIZE {TARGET_TABLE} ZORDER BY ({zorder_str})"
    try:
        logger.info("Running: %s", optimize_sql)
        spark.sql(optimize_sql)
        logger.info("OPTIMIZE ZORDER BY (%s) complete on %s", zorder_str, TARGET_TABLE)
    except Exception as exc:
        # OPTIMIZE failure is non-fatal — log and continue
        logger.warning(
            "OPTIMIZE ZORDER failed (non-fatal): %s. Job will still succeed.", exc
        )
else:
    logger.info("p_run_optimize=false — skipping OPTIMIZE.")

# COMMAND ----------
# =============================================================================
# Table statistics (post-merge)
# =============================================================================
try:
    detail = spark.sql(f"DESCRIBE DETAIL {TARGET_TABLE}").collect()[0]
    logger.info(
        "Gold table stats — numFiles: %s  sizeInBytes: %s  lastModified: %s",
        detail["numFiles"], detail["sizeInBytes"], detail["lastModified"],
    )
except Exception as _stats_err:
    logger.warning("Could not fetch table stats: %s", _stats_err)

# COMMAND ----------
# =============================================================================
# Notebook exit
# =============================================================================
logger.info("nb_gold_template_merge completed successfully.")
dbutils.notebook.exit(f"SUCCESS|{row_count_merged}")
