"""
delta_utils.py — Delta Lake Helper Utilities
=============================================
Provides production-ready helpers for common Delta Lake operations used in the
Informatica-to-Databricks migration tool:

- ``optimize_table``       — Run OPTIMIZE with optional Z-ORDER BY
- ``zorder_table``         — Shorthand for OPTIMIZE ZORDER
- ``vacuum_table``         — VACUUM with configurable retention hours
- ``merge_into_delta``     — Generic MERGE (upsert / SCD1)
- ``scd2_merge``           — SCD Type 2 merge (expire + insert new version)
- ``get_table_stats``      — Return row count and file count for a Delta table
- ``create_if_not_exists`` — CREATE TABLE IF NOT EXISTS wrapper
- ``add_etl_metadata``     — Append standard ETL audit columns to a DataFrame

All functions accept a ``SparkSession`` as their first argument so they work
both inside Databricks notebooks and in unit-test environments.

Usage
-----
    from src.utils.delta_utils import optimize_table, scd2_merge

    optimize_table(spark, "cder_prod.gold.orders", zorder_cols=["order_id"])
    scd2_merge(spark, df_updates, "cder_prod.silver.customers",
               pk_cols=["customer_id"], load_date="2024-06-01")
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, TimestampType

try:
    from delta.tables import DeltaTable
except ImportError:  # pragma: no cover — available on Databricks runtime
    DeltaTable = None  # type: ignore[misc,assignment]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_VACUUM_HOURS: int = 168  # 7 days
_SCD2_END_DATE: str = "9999-12-31"
_ETL_CURRENT_FLAG_COL: str = "is_current"
_ETL_EFF_START_COL: str = "eff_start_date"
_ETL_EFF_END_COL: str = "eff_end_date"
_ETL_LOAD_TS_COL: str = "_etl_load_ts"
_ETL_LOAD_DATE_COL: str = "_etl_load_date"
_ETL_MAPPING_COL: str = "_etl_mapping_name"


# ---------------------------------------------------------------------------
# OPTIMIZE helpers
# ---------------------------------------------------------------------------


def optimize_table(
    spark: SparkSession,
    table_fqn: str,
    zorder_cols: Optional[List[str]] = None,
    *,
    where_clause: Optional[str] = None,
) -> None:
    """
    Run ``OPTIMIZE`` on a Delta table with an optional ``ZORDER BY`` clause.

    Parameters
    ----------
    spark:
        Active SparkSession.
    table_fqn:
        Fully-qualified Delta table name (e.g. ``cder_prod.gold.orders``).
    zorder_cols:
        Column names to Z-ORDER by. If ``None`` or empty, plain OPTIMIZE is run.
    where_clause:
        Optional ``WHERE`` predicate to limit the files compacted, e.g.
        ``"_etl_load_date >= '2024-01-01'"`` (file skipping).

    Raises
    ------
    RuntimeError
        If the underlying SQL command fails.
    """
    sql_parts = [f"OPTIMIZE {table_fqn}"]
    if where_clause:
        sql_parts.append(f"WHERE {where_clause}")
    if zorder_cols:
        zorder_str = ", ".join(zorder_cols)
        sql_parts.append(f"ZORDER BY ({zorder_str})")
    sql = " ".join(sql_parts)

    logger.info("Running: %s", sql)
    try:
        spark.sql(sql)
        logger.info("OPTIMIZE complete on %s", table_fqn)
    except Exception as exc:
        raise RuntimeError(f"OPTIMIZE failed on {table_fqn}: {exc}") from exc


def zorder_table(
    spark: SparkSession,
    table_fqn: str,
    zorder_cols: List[str],
) -> None:
    """
    Convenience wrapper: run OPTIMIZE ZORDER BY on ``table_fqn``.

    Parameters
    ----------
    spark:
        Active SparkSession.
    table_fqn:
        Fully-qualified Delta table name.
    zorder_cols:
        One or more column names to Z-ORDER by.
    """
    if not zorder_cols:
        raise ValueError("zorder_cols must be non-empty for zorder_table()")
    optimize_table(spark, table_fqn, zorder_cols=zorder_cols)


# ---------------------------------------------------------------------------
# VACUUM helper
# ---------------------------------------------------------------------------


def vacuum_table(
    spark: SparkSession,
    table_fqn: str,
    retention_hours: int = _DEFAULT_VACUUM_HOURS,
    *,
    dry_run: bool = False,
) -> None:
    """
    Run ``VACUUM`` on a Delta table.

    Parameters
    ----------
    spark:
        Active SparkSession.
    table_fqn:
        Fully-qualified Delta table name.
    retention_hours:
        How many hours of history to retain (default 168 = 7 days).
        Must be >= 168 unless ``spark.databricks.delta.retentionDurationCheck.enabled``
        is set to ``false``.
    dry_run:
        If True, runs ``VACUUM ... DRY RUN`` to list files that would be removed
        without actually deleting them.
    """
    dry = "DRY RUN" if dry_run else ""
    sql = f"VACUUM {table_fqn} RETAIN {retention_hours} HOURS {dry}".strip()
    logger.info("Running: %s", sql)
    try:
        result = spark.sql(sql)
        if dry_run:
            cnt = result.count()
            logger.info("VACUUM DRY RUN — %d file(s) would be removed.", cnt)
        else:
            logger.info("VACUUM complete on %s", table_fqn)
    except Exception as exc:
        raise RuntimeError(f"VACUUM failed on {table_fqn}: {exc}") from exc


# ---------------------------------------------------------------------------
# MERGE (SCD Type 1 upsert)
# ---------------------------------------------------------------------------


def merge_into_delta(
    spark: SparkSession,
    df_updates: DataFrame,
    target_table_fqn: str,
    pk_cols: List[str],
    *,
    update_cols: Optional[List[str]] = None,
    delete_indicator_col: Optional[str] = None,
    delete_indicator_value: str = "D",
) -> Dict[str, int]:
    """
    Perform a generic Delta MERGE (SCD Type 1 upsert) into ``target_table_fqn``.

    - ``WHEN MATCHED AND <delete condition>``   → DELETE
    - ``WHEN MATCHED``                          → UPDATE ALL (or specified cols)
    - ``WHEN NOT MATCHED BY TARGET``            → INSERT ALL

    Parameters
    ----------
    spark:
        Active SparkSession.
    df_updates:
        Source DataFrame containing new / changed records.
    target_table_fqn:
        Fully-qualified target Delta table name.
    pk_cols:
        Primary key column(s) used for the MERGE join condition.
    update_cols:
        Subset of columns to update on match. If ``None``, all columns are
        updated (``whenMatchedUpdateAll``).
    delete_indicator_col:
        Optional column in ``df_updates`` that signals a logical delete.
    delete_indicator_value:
        Value of ``delete_indicator_col`` that indicates a delete (default ``"D"``).

    Returns
    -------
    dict
        ``{"source_rows": N}`` — row count of the source DataFrame.

    Raises
    ------
    ImportError
        If the ``delta`` package is not available.
    RuntimeError
        If the MERGE operation fails.
    """
    if DeltaTable is None:
        raise ImportError(
            "delta-spark is not installed. Install it or run on a Databricks cluster."
        )

    join_cond = " AND ".join([f"tgt.{c} = src.{c}" for c in pk_cols])
    logger.info(
        "MERGE into %s on (%s). Source rows: %d",
        target_table_fqn,
        join_cond,
        df_updates.count(),
    )

    try:
        if not spark.catalog.tableExists(target_table_fqn):
            logger.info("Target table %s does not exist — initial insert.", target_table_fqn)
            df_updates.write.format("delta").mode("overwrite").saveAsTable(target_table_fqn)
            return {"source_rows": df_updates.count()}

        delta_tbl = DeltaTable.forName(spark, target_table_fqn)
        merge_builder = delta_tbl.alias("tgt").merge(df_updates.alias("src"), join_cond)

        # Optional: matched delete
        if delete_indicator_col:
            merge_builder = merge_builder.whenMatchedDelete(
                condition=f"src.{delete_indicator_col} = '{delete_indicator_value}'"
            )

        # Matched update
        if update_cols:
            update_set = {c: f"src.{c}" for c in update_cols}
            merge_builder = merge_builder.whenMatchedUpdate(set=update_set)
        else:
            merge_builder = merge_builder.whenMatchedUpdateAll()

        # Not matched insert
        merge_builder = merge_builder.whenNotMatchedInsertAll()
        merge_builder.execute()

        source_rows = df_updates.count()
        logger.info("MERGE complete on %s. Source rows processed: %d", target_table_fqn, source_rows)
        return {"source_rows": source_rows}

    except Exception as exc:
        raise RuntimeError(f"MERGE failed on {target_table_fqn}: {exc}") from exc


# ---------------------------------------------------------------------------
# SCD Type 2 MERGE
# ---------------------------------------------------------------------------


def scd2_merge(
    spark: SparkSession,
    df_updates: DataFrame,
    target_table_fqn: str,
    pk_cols: List[str],
    load_date: str,
    *,
    business_cols: Optional[List[str]] = None,
    current_flag_col: str = _ETL_CURRENT_FLAG_COL,
    eff_start_col: str = _ETL_EFF_START_COL,
    eff_end_col: str = _ETL_EFF_END_COL,
) -> Dict[str, int]:
    """
    Apply SCD Type 2 logic to a Delta table.

    For each arriving record:
    - If a matching current row exists AND at least one business column changed:
      → expire the existing row (set ``is_current=False``, ``eff_end_date=load_date - 1``)
      → insert a new current row
    - If no matching row exists → insert a new current row
    - Unchanged current rows are left as-is.

    Parameters
    ----------
    spark:
        Active SparkSession.
    df_updates:
        Incoming DataFrame with new/changed records.
    target_table_fqn:
        Fully-qualified target Delta table name.
    pk_cols:
        Natural key column(s) used for matching existing records.
    load_date:
        ISO-8601 date string for the current load (``YYYY-MM-DD``).
    business_cols:
        Columns that determine if a record has changed. If ``None``, all
        non-key, non-audit columns are compared.
    current_flag_col:
        Column name for the current-record Boolean flag.
    eff_start_col:
        Column name for effective start date.
    eff_end_col:
        Column name for effective end date.

    Returns
    -------
    dict
        ``{"expired_rows": N, "inserted_rows": M}``.

    Raises
    ------
    ImportError:
        If delta-spark is not available.
    RuntimeError:
        If the merge operation fails.
    """
    if DeltaTable is None:
        raise ImportError(
            "delta-spark is not installed. Install it or run on a Databricks cluster."
        )

    join_cond = " AND ".join([f"tgt.{c} = src.{c}" for c in pk_cols])

    # Prepare incoming frame with SCD2 columns
    df_new = (
        df_updates
        .withColumn(current_flag_col, F.lit(True).cast(BooleanType()))
        .withColumn(eff_start_col, F.lit(load_date).cast(DateType()))
        .withColumn(eff_end_col, F.lit(_SCD2_END_DATE).cast(DateType()))
        .withColumn(_ETL_LOAD_DATE_COL, F.lit(load_date).cast(DateType()))
        .withColumn(_ETL_LOAD_TS_COL, F.current_timestamp())
    )

    try:
        if not spark.catalog.tableExists(target_table_fqn):
            logger.info(
                "SCD2 target %s does not exist — creating via initial load.", target_table_fqn
            )
            df_new.write.format("delta").mode("overwrite").saveAsTable(target_table_fqn)
            inserted = df_new.count()
            logger.info("SCD2 initial load complete. Rows inserted: %d", inserted)
            return {"expired_rows": 0, "inserted_rows": inserted}

        delta_tbl = DeltaTable.forName(spark, target_table_fqn)

        # Derive change-detection condition
        if business_cols:
            change_cond = " OR ".join([f"tgt.{c} <> src.{c}" for c in business_cols])
        else:
            # Detect changes on all non-key, non-audit columns present in source
            all_cols = [
                c for c in df_updates.columns
                if c not in pk_cols
                and not c.startswith("_etl")
                and c not in (current_flag_col, eff_start_col, eff_end_col)
            ]
            change_cond = (
                " OR ".join([f"tgt.{c} <> src.{c}" for c in all_cols])
                if all_cols
                else "1=1"  # force update if no business cols detected
            )

        expire_condition = f"tgt.{current_flag_col} = true AND ({change_cond})"

        # Step 1: expire current rows that have changed
        (
            delta_tbl.alias("tgt")
            .merge(df_new.alias("src"), join_cond)
            .whenMatchedUpdate(
                condition=expire_condition,
                set={
                    current_flag_col: "false",
                    eff_end_col: f"cast(date_sub(cast('{load_date}' as date), 1) as date)",
                },
            )
            .execute()
        )

        # Step 2: count how many rows were expired (approx: rows about to be re-inserted)
        df_changed = (
            spark.table(target_table_fqn)
            .filter(
                (F.col(current_flag_col) == False)  # noqa: E712
                & (F.col(eff_end_col) == F.date_sub(F.lit(load_date).cast(DateType()), 1))
            )
        )
        expired_count = df_changed.count()

        # Step 3: insert new current versions for changed + net-new records
        existing_current_keys = (
            spark.table(target_table_fqn)
            .filter(F.col(current_flag_col) == True)  # noqa: E712
            .select(*pk_cols)
        )
        df_to_insert = df_new.join(
            F.broadcast(existing_current_keys) if True else existing_current_keys,
            on=pk_cols,
            how="left_anti",  # records NOT already current
        )
        # Also re-insert records that were just expired
        df_expired_pks = df_changed.select(*pk_cols)
        df_reinsert = df_new.join(df_expired_pks, on=pk_cols, how="inner")
        df_final_insert = df_to_insert.unionByName(df_reinsert, allowMissingColumns=True)

        df_final_insert.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(target_table_fqn)

        inserted_count = df_final_insert.count()
        logger.info(
            "SCD2 complete on %s. Expired: %d, Inserted: %d",
            target_table_fqn, expired_count, inserted_count,
        )
        return {"expired_rows": expired_count, "inserted_rows": inserted_count}

    except Exception as exc:
        raise RuntimeError(f"SCD2 merge failed on {target_table_fqn}: {exc}") from exc


# ---------------------------------------------------------------------------
# Table stats
# ---------------------------------------------------------------------------


def get_table_stats(spark: SparkSession, table_fqn: str) -> Dict[str, object]:
    """
    Return basic statistics for a Delta table.

    Parameters
    ----------
    spark:
        Active SparkSession.
    table_fqn:
        Fully-qualified Delta table name.

    Returns
    -------
    dict
        Keys: ``table``, ``row_count``, ``size_bytes``, ``num_files``,
        ``last_modified``.
    """
    try:
        row_count: int = spark.table(table_fqn).count()
        detail_row = spark.sql(f"DESCRIBE DETAIL {table_fqn}").collect()[0]
        return {
            "table": table_fqn,
            "row_count": row_count,
            "size_bytes": detail_row["sizeInBytes"],
            "num_files": detail_row["numFiles"],
            "last_modified": str(detail_row["lastModified"]),
        }
    except Exception as exc:
        logger.warning("Could not retrieve stats for %s: %s", table_fqn, exc)
        return {"table": table_fqn, "error": str(exc)}


# ---------------------------------------------------------------------------
# CREATE TABLE IF NOT EXISTS helper
# ---------------------------------------------------------------------------


def create_if_not_exists(
    spark: SparkSession,
    table_fqn: str,
    df_schema: DataFrame,
    *,
    partition_cols: Optional[List[str]] = None,
    tbl_properties: Optional[Dict[str, str]] = None,
) -> bool:
    """
    Create a Delta table from a DataFrame schema if it does not already exist.

    Parameters
    ----------
    spark:
        Active SparkSession.
    table_fqn:
        Fully-qualified Delta table name.
    df_schema:
        A DataFrame whose schema is used for the CREATE TABLE statement.
        No data is written.
    partition_cols:
        Optional list of partition column names.
    tbl_properties:
        Optional Delta table properties (e.g. ``{"delta.autoOptimize.optimizeWrite": "true"}``).

    Returns
    -------
    bool
        ``True`` if the table was created; ``False`` if it already existed.
    """
    if spark.catalog.tableExists(table_fqn):
        logger.debug("Table %s already exists — skipping creation.", table_fqn)
        return False

    logger.info("Creating Delta table: %s", table_fqn)
    writer = (
        df_schema.limit(0)
        .write
        .format("delta")
        .mode("ignore")  # no-op if table appears between check and write (race condition)
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    if tbl_properties:
        for k, v in tbl_properties.items():
            writer = writer.option(k, v)
    writer.saveAsTable(table_fqn)
    logger.info("Table created: %s", table_fqn)
    return True


# ---------------------------------------------------------------------------
# ETL metadata helper
# ---------------------------------------------------------------------------


def add_etl_metadata(
    df: DataFrame,
    load_date: str,
    mapping_name: str,
    source_table: str = "",
) -> DataFrame:
    """
    Append standard ETL audit columns to a DataFrame.

    Added columns
    -------------
    - ``_etl_load_date``    : DateType — the ETL batch date
    - ``_etl_load_ts``      : TimestampType — current execution timestamp
    - ``_etl_mapping_name`` : StringType — Informatica/PySpark mapping name
    - ``_etl_source_table`` : StringType — source table identifier (optional)

    Parameters
    ----------
    df:
        Input DataFrame.
    load_date:
        ISO-8601 date string (``YYYY-MM-DD``).
    mapping_name:
        Name of the migration mapping.
    source_table:
        Optional source table identifier for lineage.

    Returns
    -------
    DataFrame
        A new DataFrame with the audit columns appended.
    """
    result = (
        df
        .withColumn(_ETL_LOAD_DATE_COL, F.lit(load_date).cast(DateType()))
        .withColumn(_ETL_LOAD_TS_COL, F.current_timestamp())
        .withColumn(_ETL_MAPPING_COL, F.lit(mapping_name))
    )
    if source_table:
        result = result.withColumn("_etl_source_table", F.lit(source_table))
    return result
