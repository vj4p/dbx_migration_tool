"""
oracle_utils.py — Oracle JDBC Helper Utilities
===============================================
Provides production-ready helpers for connecting to Oracle databases via JDBC
from Databricks / PySpark, including:

- ``build_jdbc_reader``      — Construct a fully-configured JDBC DataFrameReader
- ``get_partition_bounds``   — Automatically query min/max for numeric partitioning
- ``build_pushdown_reader``  — Build a reader with a SQL pushdown query
- ``build_jdbc_url``         — Construct an Oracle JDBC URL from components
- ``jdbc_table_row_count``   — Quickly count rows via JDBC without full scan
- ``get_oracle_table_schema``— Read zero rows to materialise the Oracle schema
- ``read_oracle_incremental`` — Incremental read with a watermark column

All credential parameters are fetched from Databricks Secrets so that no
plain-text passwords appear in notebook code.

Usage
-----
    from src.utils.oracle_utils import build_jdbc_reader

    df = build_jdbc_reader(
        spark,
        scope="cder-secrets",
        table_or_query="SCHEMA.MY_TABLE",
    ).load()
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, DataFrameReader, SparkSession

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ORACLE_DRIVER: str = "oracle.jdbc.OracleDriver"
DEFAULT_FETCHSIZE: int = 10000
DEFAULT_NUM_PARTITIONS: int = 8
DEFAULT_QUERY_TIMEOUT: int = 3600  # seconds

# Databricks secret scope keys
_SECRET_KEY_URL: str = "oracle-jdbc-url"
_SECRET_KEY_USER: str = "oracle-user"
_SECRET_KEY_PASSWORD: str = "oracle-password"


# ---------------------------------------------------------------------------
# URL builder
# ---------------------------------------------------------------------------


def build_jdbc_url(
    host: str,
    port: int,
    service_name: str,
    *,
    use_service: bool = True,
) -> str:
    """
    Build an Oracle JDBC connection URL.

    Parameters
    ----------
    host:
        Oracle database hostname or IP address.
    port:
        Oracle listener port (typically 1521).
    service_name:
        Oracle service name (if ``use_service=True``) or SID
        (if ``use_service=False``).
    use_service:
        ``True`` → ``jdbc:oracle:thin:@//host:port/service_name`` (recommended).
        ``False`` → ``jdbc:oracle:thin:@host:port:sid`` (legacy).

    Returns
    -------
    str
        Complete JDBC URL.
    """
    if use_service:
        url = f"jdbc:oracle:thin:@//{host}:{port}/{service_name}"
    else:
        url = f"jdbc:oracle:thin:@{host}:{port}:{service_name}"
    logger.debug("Built JDBC URL: %s", url)
    return url


# ---------------------------------------------------------------------------
# Core JDBC reader builder
# ---------------------------------------------------------------------------


def build_jdbc_reader(
    spark: SparkSession,
    scope: str,
    table_or_query: str,
    *,
    partition_col: Optional[str] = None,
    lower_bound: Optional[int] = None,
    upper_bound: Optional[int] = None,
    num_partitions: int = DEFAULT_NUM_PARTITIONS,
    fetchsize: int = DEFAULT_FETCHSIZE,
    query_timeout: int = DEFAULT_QUERY_TIMEOUT,
    extra_options: Optional[Dict[str, str]] = None,
    secret_key_url: str = _SECRET_KEY_URL,
    secret_key_user: str = _SECRET_KEY_USER,
    secret_key_password: str = _SECRET_KEY_PASSWORD,
) -> DataFrameReader:
    """
    Build a fully-configured PySpark ``DataFrameReader`` for Oracle via JDBC.

    Credentials are fetched from the specified Databricks secret scope so that
    no plain-text passwords appear in notebook source code.

    Parameters
    ----------
    spark:
        Active SparkSession.
    scope:
        Databricks secret scope name (e.g. ``"cder-secrets"``).
    table_or_query:
        Oracle table name (``OWNER.TABLE``), a sub-select
        (``(SELECT ... FROM ...) t``), or a view name.
    partition_col:
        Numeric column to use for parallel partitioned reads.
        Required if ``lower_bound`` / ``upper_bound`` are provided.
    lower_bound:
        Minimum value of ``partition_col`` for partition stride calculation.
    upper_bound:
        Maximum value of ``partition_col`` for partition stride calculation.
    num_partitions:
        Number of parallel JDBC partitions (default 8).
    fetchsize:
        Number of rows fetched per JDBC round-trip (default 10 000).
    query_timeout:
        JDBC query timeout in seconds (default 3 600).
    extra_options:
        Additional ``DataFrameReader.option(key, value)`` pairs.
    secret_key_url:
        Secret key name for the JDBC URL.
    secret_key_user:
        Secret key name for the Oracle username.
    secret_key_password:
        Secret key name for the Oracle password.

    Returns
    -------
    DataFrameReader
        Configured reader; call ``.load()`` to execute the read.

    Raises
    ------
    RuntimeError
        If ``partition_col`` is provided without ``lower_bound``/``upper_bound``.
    """
    # --- Retrieve secrets ---
    try:
        jdbc_url = spark._jvm.com.databricks.dbutils.DBUtilsHolder.dbutils().secrets().get(
            scope, secret_key_url
        )
        db_user = spark._jvm.com.databricks.dbutils.DBUtilsHolder.dbutils().secrets().get(
            scope, secret_key_user
        )
        db_pass = spark._jvm.com.databricks.dbutils.DBUtilsHolder.dbutils().secrets().get(
            scope, secret_key_password
        )
    except Exception:
        # Fallback for unit tests / local mode where dbutils JVM is not available.
        # In this case callers must supply credentials via environment variables.
        import os
        jdbc_url = os.environ.get("ORACLE_JDBC_URL", "")
        db_user  = os.environ.get("ORACLE_USER", "")
        db_pass  = os.environ.get("ORACLE_PASSWORD", "")
        if not all([jdbc_url, db_user, db_pass]):
            logger.warning(
                "Could not retrieve secrets from scope '%s' and environment variables "
                "are not set. JDBC read will likely fail.", scope
            )

    # Validate partitioning args
    if partition_col and not (lower_bound is not None and upper_bound is not None):
        raise RuntimeError(
            "Both lower_bound and upper_bound must be provided when partition_col is set."
        )

    reader: DataFrameReader = (
        spark.read.format("jdbc")
        .option("url",          jdbc_url)
        .option("dbtable",      table_or_query)
        .option("user",         db_user)
        .option("password",     db_pass)
        .option("driver",       ORACLE_DRIVER)
        .option("fetchsize",    fetchsize)
        .option("queryTimeout", query_timeout)
    )

    # Parallel partitioning
    if partition_col and lower_bound is not None and upper_bound is not None:
        reader = (
            reader
            .option("partitionColumn", partition_col)
            .option("lowerBound",      str(lower_bound))
            .option("upperBound",      str(upper_bound))
            .option("numPartitions",   str(num_partitions))
        )

    # Additional options
    if extra_options:
        for k, v in extra_options.items():
            reader = reader.option(k, v)

    logger.info(
        "JDBC reader configured: table=%s  partitions=%d  fetchsize=%d",
        table_or_query, num_partitions, fetchsize,
    )
    return reader


# ---------------------------------------------------------------------------
# Partition bounds helper
# ---------------------------------------------------------------------------


def get_partition_bounds(
    spark: SparkSession,
    scope: str,
    table_name: str,
    partition_col: str,
    *,
    where_clause: Optional[str] = None,
    secret_key_url: str = _SECRET_KEY_URL,
    secret_key_user: str = _SECRET_KEY_USER,
    secret_key_password: str = _SECRET_KEY_PASSWORD,
) -> Tuple[int, int]:
    """
    Query the Oracle database to determine the min and max values of
    ``partition_col`` for use with parallel JDBC partitioning.

    Parameters
    ----------
    spark:
        Active SparkSession.
    scope:
        Databricks secret scope name.
    table_name:
        Oracle table/view name (``OWNER.TABLE``).
    partition_col:
        Numeric column to partition on.
    where_clause:
        Optional filter clause (e.g. ``"LOAD_DT >= DATE '2024-01-01'"``).
    secret_key_url, secret_key_user, secret_key_password:
        Secret key names (defaults match the project convention).

    Returns
    -------
    Tuple[int, int]
        ``(lower_bound, upper_bound)`` — safe to pass to :func:`build_jdbc_reader`.

    Raises
    ------
    RuntimeError
        If the bounds query fails or the table is empty.
    """
    where = f"WHERE {where_clause}" if where_clause else ""
    query = (
        f"(SELECT MIN({partition_col}) AS lb, MAX({partition_col}) AS ub "
        f"FROM {table_name} {where}) bounds_qry"
    )

    logger.info("Fetching partition bounds: %s", query)
    try:
        reader = build_jdbc_reader(
            spark,
            scope,
            query,
            num_partitions=1,
            secret_key_url=secret_key_url,
            secret_key_user=secret_key_user,
            secret_key_password=secret_key_password,
        )
        row = reader.load().collect()[0]
        if row["lb"] is None or row["ub"] is None:
            raise RuntimeError(
                f"Table '{table_name}' appears to be empty; "
                "cannot determine partition bounds."
            )
        lower_bound = int(row["lb"])
        upper_bound = int(row["ub"])
        logger.info("Partition bounds: [%d, %d]", lower_bound, upper_bound)
        return lower_bound, upper_bound

    except Exception as exc:
        raise RuntimeError(
            f"Failed to get partition bounds for {table_name}.{partition_col}: {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# Pushdown SQL reader
# ---------------------------------------------------------------------------


def build_pushdown_reader(
    spark: SparkSession,
    scope: str,
    sql_query: str,
    *,
    num_partitions: int = 1,
    fetchsize: int = DEFAULT_FETCHSIZE,
    extra_options: Optional[Dict[str, str]] = None,
    secret_key_url: str = _SECRET_KEY_URL,
    secret_key_user: str = _SECRET_KEY_USER,
    secret_key_password: str = _SECRET_KEY_PASSWORD,
) -> DataFrameReader:
    """
    Build a JDBC reader that executes a full SQL query as a pushdown to Oracle.

    Wraps the query in an Oracle-compatible sub-select alias so that Spark
    treats it as a table.

    Parameters
    ----------
    spark:
        Active SparkSession.
    scope:
        Databricks secret scope name.
    sql_query:
        Complete Oracle SQL statement to push down (no trailing semicolon).
    num_partitions:
        Number of JDBC partitions. For complex pushdown queries, 1 is safest;
        supply a partition column via ``extra_options`` for parallelism.
    fetchsize:
        JDBC fetch size (default 10 000).
    extra_options:
        Additional reader options (e.g. partition column/bounds).
    secret_key_url, secret_key_user, secret_key_password:
        Secret key names.

    Returns
    -------
    DataFrameReader
        Configured reader.
    """
    # Wrap in sub-select with table alias for Oracle JDBC compatibility
    wrapped_query = f"({sql_query}) pushdown_qry"
    logger.info("Building pushdown reader. Query (truncated): %.120s ...", sql_query)
    return build_jdbc_reader(
        spark,
        scope,
        wrapped_query,
        num_partitions=num_partitions,
        fetchsize=fetchsize,
        extra_options=extra_options,
        secret_key_url=secret_key_url,
        secret_key_user=secret_key_user,
        secret_key_password=secret_key_password,
    )


# ---------------------------------------------------------------------------
# Row count helper
# ---------------------------------------------------------------------------


def jdbc_table_row_count(
    spark: SparkSession,
    scope: str,
    table_name: str,
    *,
    where_clause: Optional[str] = None,
    secret_key_url: str = _SECRET_KEY_URL,
    secret_key_user: str = _SECRET_KEY_USER,
    secret_key_password: str = _SECRET_KEY_PASSWORD,
) -> int:
    """
    Return the approximate row count of an Oracle table by pushing a
    ``SELECT COUNT(*) FROM ...`` query to the database.

    Much faster than reading the full table and calling ``.count()``.

    Parameters
    ----------
    spark:
        Active SparkSession.
    scope:
        Databricks secret scope name.
    table_name:
        Oracle table/view name.
    where_clause:
        Optional filter (e.g. ``"STATUS = 'A'"``).
    secret_key_url, secret_key_user, secret_key_password:
        Secret key names.

    Returns
    -------
    int
        Row count.
    """
    where = f"WHERE {where_clause}" if where_clause else ""
    sql = f"SELECT COUNT(*) AS cnt FROM {table_name} {where}"
    reader = build_pushdown_reader(
        spark, scope, sql,
        secret_key_url=secret_key_url,
        secret_key_user=secret_key_user,
        secret_key_password=secret_key_password,
    )
    cnt: int = reader.load().collect()[0]["cnt"]
    logger.info("Row count for %s: %d", table_name, cnt)
    return cnt


# ---------------------------------------------------------------------------
# Schema introspection helper
# ---------------------------------------------------------------------------


def get_oracle_table_schema(
    spark: SparkSession,
    scope: str,
    table_name: str,
    *,
    secret_key_url: str = _SECRET_KEY_URL,
    secret_key_user: str = _SECRET_KEY_USER,
    secret_key_password: str = _SECRET_KEY_PASSWORD,
) -> DataFrame:
    """
    Read zero rows from an Oracle table to materialise the Spark schema.

    Useful for schema validation before a full JDBC ingest.

    Parameters
    ----------
    spark:
        Active SparkSession.
    scope:
        Databricks secret scope name.
    table_name:
        Oracle table/view name.

    Returns
    -------
    DataFrame
        Empty DataFrame with the inferred Spark schema.
    """
    sql = f"SELECT * FROM {table_name} WHERE ROWNUM < 1"
    reader = build_pushdown_reader(
        spark, scope, sql,
        secret_key_url=secret_key_url,
        secret_key_user=secret_key_user,
        secret_key_password=secret_key_password,
    )
    df_empty = reader.load()
    logger.info(
        "Schema for %s: %d column(s) — %s",
        table_name, len(df_empty.columns), df_empty.columns,
    )
    return df_empty


# ---------------------------------------------------------------------------
# Incremental read helper
# ---------------------------------------------------------------------------


def read_oracle_incremental(
    spark: SparkSession,
    scope: str,
    table_name: str,
    watermark_col: str,
    last_watermark: str,
    *,
    partition_col: Optional[str] = None,
    num_partitions: int = DEFAULT_NUM_PARTITIONS,
    fetchsize: int = DEFAULT_FETCHSIZE,
    extra_where: Optional[str] = None,
    secret_key_url: str = _SECRET_KEY_URL,
    secret_key_user: str = _SECRET_KEY_USER,
    secret_key_password: str = _SECRET_KEY_PASSWORD,
) -> DataFrame:
    """
    Read only new/changed rows from Oracle since ``last_watermark``.

    The incremental filter is pushed down to Oracle as a SQL predicate.

    Parameters
    ----------
    spark:
        Active SparkSession.
    scope:
        Databricks secret scope name.
    table_name:
        Oracle table/view name.
    watermark_col:
        Column used for incremental filtering (e.g. ``LAST_UPDATED_DT``).
        Should be an Oracle ``DATE`` or ``TIMESTAMP`` column.
    last_watermark:
        ISO-8601 string representing the exclusive lower bound
        (e.g. ``"2024-05-31 00:00:00"``).
    partition_col:
        Optional numeric column for parallel partitioning.
    num_partitions:
        Number of parallel JDBC partitions.
    fetchsize:
        JDBC fetch size.
    extra_where:
        Additional AND predicate (e.g. ``"STATUS = 'A'"``).
    secret_key_url, secret_key_user, secret_key_password:
        Secret key names.

    Returns
    -------
    DataFrame
        Rows where ``watermark_col > last_watermark``.
    """
    extra = f" AND {extra_where}" if extra_where else ""
    sql = (
        f"SELECT * FROM {table_name} "
        f"WHERE {watermark_col} > TIMESTAMP '{last_watermark}'{extra}"
    )

    logger.info(
        "Incremental read: %s  watermark_col=%s  since=%s",
        table_name, watermark_col, last_watermark,
    )

    if partition_col:
        try:
            lower, upper = get_partition_bounds(
                spark, scope, table_name, partition_col,
                where_clause=f"{watermark_col} > TIMESTAMP '{last_watermark}'{extra}",
                secret_key_url=secret_key_url,
                secret_key_user=secret_key_user,
                secret_key_password=secret_key_password,
            )
        except RuntimeError:
            logger.warning("Could not get bounds — falling back to non-partitioned read.")
            partition_col = None
            lower = upper = None

    if partition_col and lower is not None:
        reader = build_jdbc_reader(
            spark, scope, f"({sql}) incr_qry",
            partition_col=partition_col,
            lower_bound=lower,
            upper_bound=upper,
            num_partitions=num_partitions,
            fetchsize=fetchsize,
            secret_key_url=secret_key_url,
            secret_key_user=secret_key_user,
            secret_key_password=secret_key_password,
        )
    else:
        reader = build_pushdown_reader(
            spark, scope, sql,
            fetchsize=fetchsize,
            secret_key_url=secret_key_url,
            secret_key_user=secret_key_user,
            secret_key_password=secret_key_password,
        )

    df = reader.load()
    logger.info("Incremental read complete.")
    return df
