# Databricks notebook source
# MAGIC %md
# MAGIC ## nb_bronze_template_ingest
# MAGIC **Layer:** Bronze — Raw ingestion from Oracle JDBC to Delta Lake
# MAGIC
# MAGIC **Purpose:** Read a full or incremental extract from an Oracle source table via
# MAGIC JDBC, append ETL audit columns, write to the Bronze Delta table, and emit a
# MAGIC `SUCCESS|{row_count}` exit signal to the parent job.
# MAGIC
# MAGIC **Medallion position:** Oracle → Bronze (`cder_prod.bronze.<subject>_raw`)
# MAGIC
# MAGIC **Parameters (dbutils.widgets)**
# MAGIC | Widget | Default | Description |
# MAGIC |--------|---------|-------------|
# MAGIC | p_load_date | today | Batch date `YYYY-MM-DD` |
# MAGIC | p_mapping_name | — | Mapping name for lineage |
# MAGIC | p_catalog | cder_prod | Unity Catalog catalog |
# MAGIC | p_schema | bronze | Target schema |
# MAGIC | p_source_table | — | Oracle `OWNER.TABLE` |
# MAGIC | p_subject | — | Subject / entity name |
# MAGIC | p_incremental | false | true = watermark-based incremental |
# MAGIC | p_watermark_col | — | Column used for incremental filter |
# MAGIC | p_last_watermark | — | Exclusive lower bound for incremental |

# COMMAND ----------
# =============================================================================
# Imports
# =============================================================================
import logging
import sys
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType

# COMMAND ----------
# =============================================================================
# Logging setup
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("nb_bronze_template_ingest")

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
dbutils.widgets.text("p_mapping_name",  "",          "Mapping Name")
dbutils.widgets.text("p_catalog",       "cder_prod", "Unity Catalog name")
dbutils.widgets.text("p_schema",        "bronze",    "Target schema")
dbutils.widgets.text("p_source_table",  "",          "Oracle source table (OWNER.TABLE)")
dbutils.widgets.text("p_subject",       "",          "Subject / entity name (snake_case)")
dbutils.widgets.dropdown("p_incremental", "false", ["true", "false"], "Incremental mode")
dbutils.widgets.text("p_watermark_col",  "",         "Watermark column for incremental reads")
dbutils.widgets.text("p_last_watermark", "",         "Last watermark value (exclusive lower bound)")

p_load_date      = dbutils.widgets.get("p_load_date")
p_mapping_name   = dbutils.widgets.get("p_mapping_name")
p_catalog        = dbutils.widgets.get("p_catalog")
p_schema         = dbutils.widgets.get("p_schema")
p_source_table   = dbutils.widgets.get("p_source_table")
p_subject        = dbutils.widgets.get("p_subject") or p_source_table.split(".")[-1].lower()
p_incremental    = dbutils.widgets.get("p_incremental").lower() == "true"
p_watermark_col  = dbutils.widgets.get("p_watermark_col")
p_last_watermark = dbutils.widgets.get("p_last_watermark")

TARGET_TABLE: str = f"{p_catalog}.{p_schema}.{p_subject}_raw"

logger.info(
    "Parameters — load_date=%s  mapping=%s  source=%s  target=%s  incremental=%s",
    p_load_date, p_mapping_name, p_source_table, TARGET_TABLE, p_incremental,
)

# COMMAND ----------
# =============================================================================
# Fetch Oracle credentials from Databricks Secrets
# =============================================================================
_SECRET_SCOPE = "cder-secrets"
try:
    jdbc_url = dbutils.secrets.get(scope=_SECRET_SCOPE, key="oracle-jdbc-url")
    db_user  = dbutils.secrets.get(scope=_SECRET_SCOPE, key="oracle-user")
    db_pass  = dbutils.secrets.get(scope=_SECRET_SCOPE, key="oracle-password")
    logger.info("Credentials retrieved from secret scope '%s'.", _SECRET_SCOPE)
except Exception as _cred_err:
    logger.error("Secret retrieval failed: %s", _cred_err)
    dbutils.notebook.exit(f"FAILED|secrets_error|{_cred_err}")
    raise

# COMMAND ----------
# =============================================================================
# Build JDBC reader
# =============================================================================
ORACLE_DRIVER    = "oracle.jdbc.OracleDriver"
JDBC_FETCHSIZE   = 10_000
NUM_PARTITIONS   = 8

try:
    if p_incremental and p_watermark_col and p_last_watermark:
        # --- Incremental / watermark-based read ---
        logger.info(
            "Incremental read: %s  watermark_col=%s  since=%s",
            p_source_table, p_watermark_col, p_last_watermark,
        )
        pushdown_sql = (
            f"SELECT * FROM {p_source_table} "
            f"WHERE {p_watermark_col} > TIMESTAMP '{p_last_watermark}'"
        )
        dbtable_arg = f"({pushdown_sql}) incr_t"
    else:
        # --- Full extract ---
        logger.info("Full extract from: %s", p_source_table)
        dbtable_arg = p_source_table

    df_raw: DataFrame = (
        spark.read.format("jdbc")
        .option("url",            jdbc_url)
        .option("dbtable",        dbtable_arg)
        .option("user",           db_user)
        .option("password",       db_pass)
        .option("driver",         ORACLE_DRIVER)
        .option("fetchsize",      JDBC_FETCHSIZE)
        .option("numPartitions",  NUM_PARTITIONS)
        .option("queryTimeout",   3600)
        .load()
    )

    row_count_raw: int = df_raw.count()
    logger.info("Rows read from Oracle: %d", row_count_raw)

    if row_count_raw == 0:
        logger.warning("No rows returned from Oracle. Exiting with zero-count success.")
        dbutils.notebook.exit(f"SUCCESS|0")

except Exception as exc:
    logger.error("JDBC read failed: %s", exc, exc_info=True)
    dbutils.notebook.exit(f"FAILED|jdbc_read_error|{exc}")
    raise

# COMMAND ----------
# =============================================================================
# Add ETL audit / metadata columns
# =============================================================================
df_bronze: DataFrame = (
    df_raw
    .withColumn("_etl_load_date",     F.lit(p_load_date).cast(DateType()))
    .withColumn("_etl_load_ts",       F.current_timestamp())
    .withColumn("_etl_mapping_name",  F.lit(p_mapping_name))
    .withColumn("_etl_source_table",  F.lit(p_source_table))
    .withColumn("_etl_incremental",   F.lit(p_incremental))
)

logger.info("ETL audit columns added.  Schema cols: %d", len(df_bronze.columns))

# COMMAND ----------
# =============================================================================
# Schema for auditing — log column names and types
# =============================================================================
logger.info("Bronze DataFrame schema:")
for field_def in df_bronze.schema.fields:
    logger.info("  %-40s %s", field_def.name, field_def.dataType.simpleString())

# COMMAND ----------
# =============================================================================
# Write to Bronze Delta table
# =============================================================================
try:
    logger.info("Writing to Bronze table: %s  (mode=append)", TARGET_TABLE)

    (
        df_bronze
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema",       "true")
        .option("overwriteSchema",   "false")
        .partitionBy("_etl_load_date")
        .saveAsTable(TARGET_TABLE)
    )

    # Verify row count in the target partition
    row_count_written: int = (
        spark.table(TARGET_TABLE)
        .filter(F.col("_etl_load_date") == F.lit(p_load_date))
        .count()
    )
    logger.info(
        "Bronze write complete. Rows in partition [%s]: %d",
        p_load_date, row_count_written,
    )

except Exception as exc:
    logger.error("Bronze write failed: %s", exc, exc_info=True)
    dbutils.notebook.exit(f"FAILED|bronze_write_error|{exc}")
    raise

# COMMAND ----------
# =============================================================================
# Notebook exit — signal SUCCESS with row count to orchestrating job
# =============================================================================
logger.info("nb_bronze_template_ingest completed successfully.")
dbutils.notebook.exit(f"SUCCESS|{row_count_written}")
