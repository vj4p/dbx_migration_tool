"""
converter.py — Informatica Transformation Type Converter
=========================================================
Maps every Informatica PowerCenter transformation type to its PySpark/Databricks
equivalent and assigns it to the appropriate medallion architecture layer
(Bronze, Silver, or Gold).

Usage
-----
    from src.migration.converter import TransformationConverter, ConversionResult

    converter = TransformationConverter()
    result = converter.convert("Aggregator")
    print(result.pyspark_pattern)   # "df.groupBy(...).agg(...)"
    print(result.medallion_layer)   # "silver"
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from src.migration.parser import MappingDef, TransformationDef

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class MedallionLayer(str, Enum):
    """Medallion architecture layers."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


# PySpark equivalents are keyed by their canonical Informatica type name.
# We use plain string constants rather than an Enum so they stay JSON-serialisable.
INFA_SOURCE_QUALIFIER = "Source Qualifier"
INFA_FILTER = "Filter"
INFA_EXPRESSION = "Expression"
INFA_JOINER = "Joiner"
INFA_LOOKUP = "Lookup Procedure"
INFA_LOOKUP_ALT = "Lookup"
INFA_AGGREGATOR = "Aggregator"
INFA_UPDATE_STRATEGY = "Update Strategy"
INFA_ROUTER = "Router"
INFA_UNION = "Union"
INFA_SORTER = "Sorter"
INFA_SEQUENCE_GENERATOR = "Sequence Generator"
INFA_NORMALIZER = "Normalizer"
INFA_RANK = "Rank"
INFA_STORED_PROCEDURE = "Stored Procedure"


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class ConversionResult:
    """
    Holds the PySpark equivalent and metadata for one Informatica transformation.

    Attributes
    ----------
    infa_type:
        Original Informatica transformation type string.
    pyspark_class:
        Primary PySpark API class or method (e.g. ``DataFrame.filter``).
    pyspark_pattern:
        A short representative code snippet showing typical usage.
    pyspark_imports:
        List of import statements needed in the generated notebook.
    medallion_layer:
        The recommended medallion layer for this transformation.
    notes:
        Human-readable migration notes / caveats.
    requires_broadcast:
        True when the transformation benefits from a broadcast join hint
        (e.g. Lookup against a small dimension).
    supported:
        False when the transformation has no direct PySpark equivalent and
        requires manual intervention.
    """

    infa_type: str
    pyspark_class: str
    pyspark_pattern: str
    pyspark_imports: List[str] = field(default_factory=list)
    medallion_layer: MedallionLayer = MedallionLayer.SILVER
    notes: str = ""
    requires_broadcast: bool = False
    supported: bool = True


# ---------------------------------------------------------------------------
# Conversion catalogue
# ---------------------------------------------------------------------------

# Full mapping: Informatica type → ConversionResult template
_CONVERSION_CATALOGUE: Dict[str, ConversionResult] = {
    # ------------------------------------------------------------------
    # Source Qualifier  →  Bronze JDBC read
    # ------------------------------------------------------------------
    INFA_SOURCE_QUALIFIER: ConversionResult(
        infa_type=INFA_SOURCE_QUALIFIER,
        pyspark_class="DataFrameReader.jdbc",
        pyspark_pattern=(
            'df = (spark.read.format("jdbc")\n'
            '        .option("url", jdbc_url)\n'
            '        .option("dbtable", f"({sql_override}) t")\n'
            '        .option("user", db_user)\n'
            '        .option("password", db_password)\n'
            '        .option("driver", "oracle.jdbc.OracleDriver")\n'
            '        .option("fetchsize", 10000)\n'
            '        .option("numPartitions", 8)\n'
            "        .load())"
        ),
        pyspark_imports=["from pyspark.sql import SparkSession"],
        medallion_layer=MedallionLayer.BRONZE,
        notes=(
            "Replace the SQL override with the table name or custom query. "
            "Set lowerBound/upperBound/partitionColumn for parallel reads."
        ),
        supported=True,
    ),
    # ------------------------------------------------------------------
    # Filter  →  DataFrame.filter / DataFrame.where
    # ------------------------------------------------------------------
    INFA_FILTER: ConversionResult(
        infa_type=INFA_FILTER,
        pyspark_class="DataFrame.filter",
        pyspark_pattern='df_filtered = df.filter("filter_condition")',
        pyspark_imports=["from pyspark.sql import functions as F"],
        medallion_layer=MedallionLayer.SILVER,
        notes=(
            "Translate the Informatica filter expression to a Spark SQL "
            "predicate string or Column expression."
        ),
        supported=True,
    ),
    # ------------------------------------------------------------------
    # Expression  →  DataFrame.withColumn / select with F.*
    # ------------------------------------------------------------------
    INFA_EXPRESSION: ConversionResult(
        infa_type=INFA_EXPRESSION,
        pyspark_class="DataFrame.withColumn",
        pyspark_pattern=(
            "df_expr = df.withColumn(\n"
            '    "derived_col",\n'
            "    F.expr(\"informatica_expression\")\n"
            ")"
        ),
        pyspark_imports=["from pyspark.sql import functions as F"],
        medallion_layer=MedallionLayer.SILVER,
        notes=(
            "Each output port with an expression becomes a withColumn call. "
            "Use expression_map.json to translate built-in functions. "
            "Pass-through ports can be handled with a select() statement."
        ),
        supported=True,
    ),
    # ------------------------------------------------------------------
    # Joiner  →  DataFrame.join
    # ------------------------------------------------------------------
    INFA_JOINER: ConversionResult(
        infa_type=INFA_JOINER,
        pyspark_class="DataFrame.join",
        pyspark_pattern=(
            "df_joined = df_master.join(\n"
            "    df_detail,\n"
            '    on=F.expr("join_condition"),\n'
            '    how="inner"  # inner|left|right|full\n'
            ")"
        ),
        pyspark_imports=["from pyspark.sql import functions as F"],
        medallion_layer=MedallionLayer.SILVER,
        notes=(
            "Map Informatica join types: Normal→inner, Master Outer→right, "
            "Detail Outer→left, Full Outer→full. "
            "Use F.broadcast() hint for the detail side when < 200 MB."
        ),
        supported=True,
    ),
    # ------------------------------------------------------------------
    # Lookup  →  broadcast join or DataFrame.join with cache
    # ------------------------------------------------------------------
    INFA_LOOKUP: ConversionResult(
        infa_type=INFA_LOOKUP,
        pyspark_class="DataFrame.join (broadcast)",
        pyspark_pattern=(
            "df_lookup = spark.table(lookup_table_name).cache()\n"
            "df_result = df.join(\n"
            "    F.broadcast(df_lookup),\n"
            '    on=F.expr("lookup_condition"),\n'
            '    how="left"\n'
            ")"
        ),
        pyspark_imports=["from pyspark.sql import functions as F"],
        medallion_layer=MedallionLayer.SILVER,
        notes=(
            "For lookup tables < 200 MB use F.broadcast(). "
            "For larger tables use a regular left join with caching. "
            "Connected lookups map to left join; unconnected to UDF or join."
        ),
        requires_broadcast=True,
        supported=True,
    ),
    INFA_LOOKUP_ALT: ConversionResult(
        infa_type=INFA_LOOKUP_ALT,
        pyspark_class="DataFrame.join (broadcast)",
        pyspark_pattern=(
            "df_lookup = spark.table(lookup_table_name).cache()\n"
            "df_result = df.join(\n"
            "    F.broadcast(df_lookup),\n"
            '    on=F.expr("lookup_condition"),\n'
            '    how="left"\n'
            ")"
        ),
        pyspark_imports=["from pyspark.sql import functions as F"],
        medallion_layer=MedallionLayer.SILVER,
        notes=(
            "Alias for 'Lookup Procedure'. "
            "Use F.broadcast() for tables < 200 MB."
        ),
        requires_broadcast=True,
        supported=True,
    ),
    # ------------------------------------------------------------------
    # Aggregator  →  DataFrame.groupBy().agg()
    # ------------------------------------------------------------------
    INFA_AGGREGATOR: ConversionResult(
        infa_type=INFA_AGGREGATOR,
        pyspark_class="DataFrame.groupBy",
        pyspark_pattern=(
            "df_agg = (\n"
            "    df\n"
            "    .groupBy(\"group_by_cols\")\n"
            "    .agg(\n"
            "        F.sum(\"amount\").alias(\"total_amount\"),\n"
            "        F.count(\"*\").alias(\"record_count\"),\n"
            "    )\n"
            ")"
        ),
        pyspark_imports=["from pyspark.sql import functions as F"],
        medallion_layer=MedallionLayer.SILVER,
        notes=(
            "Group-by ports map to groupBy(); output ports with aggregate "
            "functions map to agg(). Translate IIF/DECODE in aggregates "
            "using F.when(). FILTERROWS option maps to a pre-filter."
        ),
        supported=True,
    ),
    # ------------------------------------------------------------------
    # Update Strategy  →  Delta MERGE (foreachBatch or direct)
    # ------------------------------------------------------------------
    INFA_UPDATE_STRATEGY: ConversionResult(
        infa_type=INFA_UPDATE_STRATEGY,
        pyspark_class="DeltaTable.merge",
        pyspark_pattern=(
            "from delta.tables import DeltaTable\n\n"
            "delta_tbl = DeltaTable.forName(spark, target_table_fqn)\n"
            "(\n"
            "    delta_tbl.alias(\"tgt\")\n"
            "    .merge(\n"
            "        df_updates.alias(\"src\"),\n"
            '        "tgt.pk_col = src.pk_col"\n'
            "    )\n"
            '    .whenMatchedUpdate(condition="src.flag = \'DD_UPDATE\'",\n'
            "                       set={\"col\": \"src.col\"})\n"
            '    .whenNotMatchedInsertAll(condition="src.flag = \'DD_INSERT\'")\n'
            '    .whenMatchedDelete(condition="src.flag = \'DD_DELETE\'")\n'
            "    .execute()\n"
            ")"
        ),
        pyspark_imports=[
            "from delta.tables import DeltaTable",
            "from pyspark.sql import functions as F",
        ],
        medallion_layer=MedallionLayer.GOLD,
        notes=(
            "DD_INSERT=0, DD_UPDATE=1, DD_DELETE=2, DD_REJECT=3. "
            "Map the Update Strategy expression to a flag column, then "
            "drive MERGE conditions from that flag. "
            "For Streaming sources use foreachBatch."
        ),
        supported=True,
    ),
    # ------------------------------------------------------------------
    # Router  →  DataFrame.filter per group (split into multiple DataFrames)
    # ------------------------------------------------------------------
    INFA_ROUTER: ConversionResult(
        infa_type=INFA_ROUTER,
        pyspark_class="DataFrame.filter (multi-branch)",
        pyspark_pattern=(
            "# One filtered DataFrame per Router group\n"
            "df_group1 = df.filter(F.expr(\"group1_filter_condition\"))\n"
            "df_group2 = df.filter(F.expr(\"group2_filter_condition\"))\n"
            "df_default = df.filter(\n"
            "    ~F.expr(\"group1_filter_condition\") &\n"
            "    ~F.expr(\"group2_filter_condition\")\n"
            ")"
        ),
        pyspark_imports=["from pyspark.sql import functions as F"],
        medallion_layer=MedallionLayer.SILVER,
        notes=(
            "Each named group in the Router becomes a separate filter. "
            "The DEFAULT group catches records not matching any group condition. "
            "Cache the source DataFrame before branching to avoid re-computation."
        ),
        supported=True,
    ),
    # ------------------------------------------------------------------
    # Union  →  DataFrame.union / unionByName
    # ------------------------------------------------------------------
    INFA_UNION: ConversionResult(
        infa_type=INFA_UNION,
        pyspark_class="DataFrame.unionByName",
        pyspark_pattern=(
            "df_union = (\n"
            "    df1\n"
            "    .unionByName(df2, allowMissingColumns=True)\n"
            "    .unionByName(df3, allowMissingColumns=True)\n"
            ")"
        ),
        pyspark_imports=["from pyspark.sql import functions as F"],
        medallion_layer=MedallionLayer.SILVER,
        notes=(
            "Use unionByName(allowMissingColumns=True) to handle schema "
            "differences across input groups. All sources must be unioned "
            "in the order they appear in the transformation."
        ),
        supported=True,
    ),
    # ------------------------------------------------------------------
    # Sorter  →  DataFrame.orderBy / sort
    # ------------------------------------------------------------------
    INFA_SORTER: ConversionResult(
        infa_type=INFA_SORTER,
        pyspark_class="DataFrame.orderBy",
        pyspark_pattern=(
            "df_sorted = df.orderBy(\n"
            "    F.col(\"sort_key1\").asc(),\n"
            "    F.col(\"sort_key2\").desc(),\n"
            ")"
        ),
        pyspark_imports=["from pyspark.sql import functions as F"],
        medallion_layer=MedallionLayer.SILVER,
        notes=(
            "Spark sort is global (across all partitions). "
            "Consider whether a global sort is truly required; "
            "if ordering is only needed for a downstream window function "
            "use Window.orderBy() instead to avoid a full shuffle."
        ),
        supported=True,
    ),
    # ------------------------------------------------------------------
    # Sequence Generator  →  monotonically_increasing_id or row_number
    # ------------------------------------------------------------------
    INFA_SEQUENCE_GENERATOR: ConversionResult(
        infa_type=INFA_SEQUENCE_GENERATOR,
        pyspark_class="F.monotonically_increasing_id / Window.row_number",
        pyspark_pattern=(
            "from pyspark.sql.window import Window\n\n"
            "# Option A — surrogate key (non-consecutive but unique)\n"
            "df = df.withColumn(\"seq_id\", F.monotonically_increasing_id())\n\n"
            "# Option B — consecutive integer (requires sort, avoid on large data)\n"
            "w = Window.orderBy(F.lit(1))\n"
            "df = df.withColumn(\"seq_id\",\n"
            "                   F.row_number().over(w) + start_value - 1)"
        ),
        pyspark_imports=[
            "from pyspark.sql import functions as F",
            "from pyspark.sql.window import Window",
        ],
        medallion_layer=MedallionLayer.SILVER,
        notes=(
            "monotonically_increasing_id() is non-consecutive but scalable. "
            "row_number() over an orderBy(lit(1)) produces consecutive IDs "
            "but forces a single-partition sort — avoid for large datasets. "
            "For surrogate keys in Gold prefer IDENTITY columns on Delta tables."
        ),
        supported=True,
    ),
    # ------------------------------------------------------------------
    # Normalizer  →  DataFrame.select with explode / stack
    # ------------------------------------------------------------------
    INFA_NORMALIZER: ConversionResult(
        infa_type=INFA_NORMALIZER,
        pyspark_class="F.explode / F.stack",
        pyspark_pattern=(
            "# Pivot COBOL-style repeating groups into rows\n"
            "df_norm = df.select(\n"
            '    "key_col",\n'
            "    F.explode(\n"
            "        F.array(\n"
            '            F.struct(F.col("col_1").alias("col"), F.lit(1).alias("idx")),\n'
            '            F.struct(F.col("col_2").alias("col"), F.lit(2).alias("idx")),\n'
            "        )\n"
            "    ).alias(\"norm\")\n"
            ").select(\"key_col\", \"norm.idx\", \"norm.col\")"
        ),
        pyspark_imports=["from pyspark.sql import functions as F"],
        medallion_layer=MedallionLayer.SILVER,
        notes=(
            "Map COBOL repeating groups / OCCURS clauses to explode(). "
            "For wide pivots use F.stack(n, col1, col2, ...). "
            "The generated row index (GCID) maps to the occurrence number."
        ),
        supported=True,
    ),
    # ------------------------------------------------------------------
    # Rank  →  Window.rank / dense_rank / row_number
    # ------------------------------------------------------------------
    INFA_RANK: ConversionResult(
        infa_type=INFA_RANK,
        pyspark_class="Window.rank / dense_rank",
        pyspark_pattern=(
            "from pyspark.sql.window import Window\n\n"
            "w = (\n"
            "    Window\n"
            "    .partitionBy(\"group_by_cols\")\n"
            "    .orderBy(F.col(\"rank_col\").desc())\n"
            ")\n"
            "df_ranked = df.withColumn(\"rnk\", F.rank().over(w))\n"
            "# Keep only top-N rows\n"
            "df_top = df_ranked.filter(F.col(\"rnk\") <= top_n)"
        ),
        pyspark_imports=[
            "from pyspark.sql import functions as F",
            "from pyspark.sql.window import Window",
        ],
        medallion_layer=MedallionLayer.SILVER,
        notes=(
            "Informatica Rank TOP maps to orderBy DESC + rank <= N; "
            "BOTTOM maps to orderBy ASC + rank <= N. "
            "Use dense_rank() if duplicate ranks should not leave gaps."
        ),
        supported=True,
    ),
    # ------------------------------------------------------------------
    # Stored Procedure  →  Spark JDBC call / UDF / SQL CALL
    # ------------------------------------------------------------------
    INFA_STORED_PROCEDURE: ConversionResult(
        infa_type=INFA_STORED_PROCEDURE,
        pyspark_class="spark.sql / JDBC CallableStatement",
        pyspark_pattern=(
            "# Option A — execute via Spark SQL (Databricks supports CALL)\n"
            "spark.sql(\"CALL catalog.schema.procedure_name(arg1, arg2)\")\n\n"
            "# Option B — execute via JDBC CallableStatement\n"
            "import jaydebeapi\n"
            "conn = jaydebeapi.connect(\n"
            '    "oracle.jdbc.OracleDriver", jdbc_url, [user, password])\n'
            "curs = conn.cursor()\n"
            "curs.execute(\"{call schema.proc_name(?, ?)\", [arg1, arg2])\n"
            "conn.close()"
        ),
        pyspark_imports=["from pyspark.sql import SparkSession"],
        medallion_layer=MedallionLayer.SILVER,
        notes=(
            "Stored procedures cannot be directly replicated in PySpark. "
            "Preferred path: re-implement as PySpark transformations. "
            "If must-call, use JDBC CallableStatement or Databricks SQL CALL "
            "with an external connection. Flag for manual review."
        ),
        supported=False,
    ),
}


# ---------------------------------------------------------------------------
# Converter class
# ---------------------------------------------------------------------------


class TransformationConverter:
    """
    Converts Informatica transformation types to PySpark/Databricks equivalents.

    Parameters
    ----------
    extra_mappings:
        Optional dict of additional ``{infa_type: ConversionResult}`` entries
        that extend or override the built-in catalogue.
    """

    def __init__(
        self, extra_mappings: Optional[Dict[str, ConversionResult]] = None
    ) -> None:
        self._catalogue: Dict[str, ConversionResult] = dict(_CONVERSION_CATALOGUE)
        if extra_mappings:
            self._catalogue.update(extra_mappings)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def convert(self, infa_type: str) -> ConversionResult:
        """
        Return the :class:`ConversionResult` for the given Informatica type.

        Falls back to a generic unsupported result if the type is unknown.

        Parameters
        ----------
        infa_type:
            Informatica transformation type string (e.g. ``"Aggregator"``).

        Returns
        -------
        ConversionResult
        """
        result = self._catalogue.get(infa_type)
        if result is None:
            logger.warning(
                "No conversion mapping for Informatica type '%s' — "
                "marking as unsupported.",
                infa_type,
            )
            result = ConversionResult(
                infa_type=infa_type,
                pyspark_class="UNKNOWN",
                pyspark_pattern="# TODO: manual conversion required",
                medallion_layer=MedallionLayer.SILVER,
                notes=f"No automatic mapping for type '{infa_type}'. Manual review required.",
                supported=False,
            )
        return result

    def convert_mapping(self, mapping: MappingDef) -> List[ConversionResult]:
        """
        Convert every transformation in a :class:`MappingDef`.

        Returns a list of :class:`ConversionResult` in the same order as
        ``mapping.transformations``.

        Parameters
        ----------
        mapping:
            A parsed Informatica mapping.

        Returns
        -------
        list[ConversionResult]
        """
        results: List[ConversionResult] = []
        for t in mapping.transformations:
            result = self.convert(t.type)
            logger.debug(
                "Mapping '%s' | transformation '%s' (%s) → %s [%s]",
                mapping.name,
                t.name,
                t.type,
                result.pyspark_class,
                result.medallion_layer.value,
            )
            results.append(result)
        return results

    def get_layer_for_type(self, infa_type: str) -> MedallionLayer:
        """
        Return the recommended medallion layer for an Informatica type.

        Parameters
        ----------
        infa_type:
            Informatica transformation type string.

        Returns
        -------
        MedallionLayer
        """
        return self.convert(infa_type).medallion_layer

    def unsupported_types(self, mapping: MappingDef) -> List[str]:
        """
        Return the list of transformation type names in ``mapping`` that have
        no automatic PySpark equivalent.

        Parameters
        ----------
        mapping:
            A parsed Informatica mapping.

        Returns
        -------
        list[str]
        """
        return [
            t.type
            for t in mapping.transformations
            if not self.convert(t.type).supported
        ]

    def requires_broadcast(self, mapping: MappingDef) -> List[TransformationDef]:
        """
        Return transformations in ``mapping`` that should use broadcast joins.

        Parameters
        ----------
        mapping:
            A parsed Informatica mapping.

        Returns
        -------
        list[TransformationDef]
        """
        return [
            t
            for t in mapping.transformations
            if self.convert(t.type).requires_broadcast
        ]

    def all_imports(self, mapping: MappingDef) -> List[str]:
        """
        Collect the union of all PySpark import statements needed for ``mapping``.

        Parameters
        ----------
        mapping:
            A parsed Informatica mapping.

        Returns
        -------
        list[str]   Deduplicated, sorted import statements.
        """
        imports: set[str] = set()
        for result in self.convert_mapping(mapping):
            imports.update(result.pyspark_imports)
        return sorted(imports)

    def conversion_report(self, mapping: MappingDef) -> List[Dict[str, object]]:
        """
        Produce a structured conversion report for all transformations in ``mapping``.

        Useful for generating migration planning documentation.

        Parameters
        ----------
        mapping:
            A parsed Informatica mapping.

        Returns
        -------
        list[dict]
            Each dict has keys: name, infa_type, pyspark_class,
            medallion_layer, supported, requires_broadcast, notes.
        """
        report = []
        for t, result in zip(mapping.transformations, self.convert_mapping(mapping)):
            report.append(
                {
                    "name": t.name,
                    "infa_type": t.type,
                    "pyspark_class": result.pyspark_class,
                    "medallion_layer": result.medallion_layer.value,
                    "supported": result.supported,
                    "requires_broadcast": result.requires_broadcast,
                    "notes": result.notes,
                }
            )
        return report
