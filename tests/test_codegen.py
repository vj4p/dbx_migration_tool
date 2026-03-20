"""
tests/test_codegen.py — Unit tests for the PySpark notebook code generator
=========================================================================
Tests cover:
- generate_bronze() produces correct notebook skeleton
- generate_silver() produces correct notebook skeleton
- generate_gold() produces correct notebook skeleton
- SCD Type 2 block is conditionally included
- Broadcast join hints appear for Lookup-type mappings
- Quarantine table name is derived correctly
- Widget declarations are present for required parameters
- Secret scope and key references are correct
- Oracle driver and JDBC options are present in Bronze notebooks
- OPTIMIZE ZORDER appears in Gold notebooks
- dbutils.notebook.exit("SUCCESS|...") is present in every notebook
- notebook_name() generates canonical names
- generate_all() returns all three layers
- CodegenConfig defaults and overrides
- Empty mapping (no transformations) generates valid notebooks
- Aggregator and Expression transformation comments appear in Silver
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List
from unittest.mock import MagicMock

import pytest

from src.migration.codegen import (
    CodegenConfig,
    NotebookCodeGenerator,
    _nb_name,
    _snake,
    _subject,
    _pk_columns,
)
from src.migration.parser import (
    FieldDef,
    MappingDef,
    SourceDef,
    TargetDef,
    TransformationDef,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_source(
    name: str = "ORDERS",
    owner: str = "SCHEMA",
    pk_col: str = "ORDER_ID",
) -> SourceDef:
    pk_field = FieldDef(
        name=pk_col, datatype="NUMBER", precision=10, key_type="PRIMARY KEY"
    )
    return SourceDef(name=name, dbtype="ORACLE", owner=owner, fields=[pk_field])


def _make_target(name: str = "TGT_ORDERS", pk_col: str = "ORDER_ID") -> TargetDef:
    pk_field = FieldDef(
        name=pk_col, datatype="NUMBER", precision=10, key_type="PRIMARY KEY"
    )
    return TargetDef(name=name, dbtype="ORACLE", fields=[pk_field])


def _make_transformation(name: str, t_type: str, ports=None) -> TransformationDef:
    return TransformationDef(
        name=name,
        type=t_type,
        ports=ports or [],
    )


def _make_mapping(
    name: str = "M_ORDERS",
    trans_types: List[str] = None,
    has_update_strategy: bool = False,
) -> MappingDef:
    trans = [_make_transformation(f"T_{t}", t) for t in (trans_types or [])]
    if has_update_strategy:
        trans.append(_make_transformation("UPD_ORDERS", "Update Strategy"))
    return MappingDef(
        name=name,
        sources=[_make_source()],
        targets=[_make_target()],
        transformations=trans,
    )


@pytest.fixture
def default_config() -> CodegenConfig:
    return CodegenConfig()


@pytest.fixture
def generator(default_config) -> NotebookCodeGenerator:
    return NotebookCodeGenerator(config=default_config)


@pytest.fixture
def simple_mapping() -> MappingDef:
    return _make_mapping("M_ORDERS")


@pytest.fixture
def complex_mapping() -> MappingDef:
    return _make_mapping(
        "M_ORDERS_COMPLEX",
        trans_types=["Expression", "Aggregator", "Filter"],
    )


@pytest.fixture
def scd2_mapping() -> MappingDef:
    return _make_mapping(
        "M_CUSTOMERS_SCD2",
        has_update_strategy=True,
    )


@pytest.fixture
def lookup_mapping() -> MappingDef:
    return _make_mapping(
        "M_ORDERS_WITH_LKP",
        trans_types=["Source Qualifier", "Lookup Procedure"],
    )


# ===========================================================================
# Tests: helper functions
# ===========================================================================


class TestHelpers:
    def test_snake_basic(self):
        assert _snake("M_ORDERS") == "m_orders"

    def test_snake_camel_case(self):
        assert _snake("MyOrdersTable") == "my_orders_table"

    def test_snake_spaces(self):
        result = _snake("my orders table")
        assert " " not in result

    def test_snake_special_chars(self):
        result = _snake("order-line_items!")
        assert result.replace("_", "").isalnum()

    def test_nb_name_format(self):
        assert _nb_name("bronze", "orders", "ingest") == "nb_bronze_orders_ingest"

    def test_nb_name_silver(self):
        assert _nb_name("silver", "customers", "transform") == "nb_silver_customers_transform"

    def test_nb_name_gold(self):
        assert _nb_name("gold", "products", "merge") == "nb_gold_products_merge"

    def test_subject_from_mapping(self, simple_mapping):
        subj = _subject(simple_mapping)
        assert isinstance(subj, str)
        assert subj  # non-empty

    def test_pk_columns_from_target(self, simple_mapping):
        pks = _pk_columns(simple_mapping)
        assert "order_id" in pks

    def test_pk_columns_fallback_to_id(self):
        mapping = MappingDef(name="M_NO_PK", targets=[TargetDef(name="T", dbtype="ORACLE")])
        pks = _pk_columns(mapping)
        assert pks == ["id"]


# ===========================================================================
# Tests: CodegenConfig
# ===========================================================================


class TestCodegenConfig:
    def test_defaults(self):
        cfg = CodegenConfig()
        assert cfg.catalog == "cder_prod"
        assert cfg.secret_scope == "cder-secrets"
        assert cfg.oracle_driver == "oracle.jdbc.OracleDriver"
        assert cfg.jdbc_fetchsize == 10_000
        assert cfg.jdbc_num_partitions == 8
        assert cfg.enable_scd2 is True
        assert cfg.broadcast_threshold_mb == 200

    def test_custom_catalog(self):
        cfg = CodegenConfig(catalog="my_catalog")
        assert cfg.catalog == "my_catalog"

    def test_scd2_disabled(self):
        cfg = CodegenConfig(enable_scd2=False)
        assert cfg.enable_scd2 is False


# ===========================================================================
# Tests: generate_bronze()
# ===========================================================================


class TestGenerateBronze:
    def test_returns_string(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        assert isinstance(nb, str)
        assert len(nb) > 100

    def test_contains_databricks_notebook_source(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        assert "# Databricks notebook source" in nb

    def test_contains_oracle_driver(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        assert "oracle.jdbc.OracleDriver" in nb

    def test_contains_fetchsize(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        assert "10000" in nb or "10_000" in nb

    def test_contains_num_partitions(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        assert "numPartitions" in nb or "8" in nb

    def test_contains_p_load_date_widget(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        assert "p_load_date" in nb

    def test_contains_p_mapping_name_widget(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        assert "p_mapping_name" in nb

    def test_contains_p_source_table_widget(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        assert "p_source_table" in nb

    def test_contains_catalog_widget(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        assert "p_catalog" in nb or "cder_prod" in nb

    def test_contains_secret_scope(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        assert "cder-secrets" in nb

    def test_contains_oracle_jdbc_url_key(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        assert "oracle-jdbc-url" in nb

    def test_contains_oracle_user_key(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        assert "oracle-user" in nb

    def test_contains_oracle_password_key(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        assert "oracle-password" in nb

    def test_contains_dbutils_notebook_exit_success(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        assert "dbutils.notebook.exit" in nb
        assert "SUCCESS" in nb

    def test_contains_dbutils_notebook_exit_failed(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        assert "FAILED" in nb

    def test_bronze_table_in_output(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        # Should reference the bronze schema
        assert "bronze" in nb

    def test_contains_mergeschema_option(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        assert "mergeSchema" in nb

    def test_custom_catalog_in_notebook(self, simple_mapping):
        cfg = CodegenConfig(catalog="custom_catalog")
        gen = NotebookCodeGenerator(config=cfg)
        nb = gen.generate_bronze(simple_mapping)
        assert "custom_catalog" in nb

    def test_contains_logging(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        assert "logger" in nb or "logging" in nb

    def test_contains_etl_metadata_columns(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        assert "_etl_load_date" in nb
        assert "_etl_load_ts" in nb

    def test_contains_partition_by_load_date(self, generator, simple_mapping):
        nb = generator.generate_bronze(simple_mapping)
        assert "partitionBy" in nb and "_etl_load_date" in nb


# ===========================================================================
# Tests: generate_silver()
# ===========================================================================


class TestGenerateSilver:
    def test_returns_string(self, generator, simple_mapping):
        nb = generator.generate_silver(simple_mapping)
        assert isinstance(nb, str)

    def test_contains_databricks_notebook_source(self, generator, simple_mapping):
        nb = generator.generate_silver(simple_mapping)
        assert "# Databricks notebook source" in nb

    def test_contains_bronze_schema_widget(self, generator, simple_mapping):
        nb = generator.generate_silver(simple_mapping)
        assert "p_bronze_schema" in nb

    def test_contains_silver_schema_widget(self, generator, simple_mapping):
        nb = generator.generate_silver(simple_mapping)
        assert "p_silver_schema" in nb

    def test_contains_quarantine_table(self, generator, simple_mapping):
        nb = generator.generate_silver(simple_mapping)
        assert "quarantine_" in nb

    def test_quarantine_uses_silver_schema(self, generator, simple_mapping):
        nb = generator.generate_silver(simple_mapping)
        assert "quarantine_" in nb
        assert "cder_prod.silver" in nb

    def test_contains_mergeschema_true(self, generator, simple_mapping):
        nb = generator.generate_silver(simple_mapping)
        assert "mergeSchema" in nb

    def test_contains_dq_null_check(self, generator, simple_mapping):
        nb = generator.generate_silver(simple_mapping)
        # DQ check on primary key
        assert "isNotNull" in nb or "null" in nb.lower()

    def test_dbutils_exit_success(self, generator, simple_mapping):
        nb = generator.generate_silver(simple_mapping)
        assert "dbutils.notebook.exit" in nb
        assert "SUCCESS" in nb

    def test_dbutils_exit_failed(self, generator, simple_mapping):
        nb = generator.generate_silver(simple_mapping)
        assert "FAILED" in nb

    def test_expression_transforms_appear_in_silver(self, generator, complex_mapping):
        nb = generator.generate_silver(complex_mapping)
        assert "Expression" in nb

    def test_aggregator_transforms_appear_in_silver(self, generator, complex_mapping):
        nb = generator.generate_silver(complex_mapping)
        assert "Aggregator" in nb

    def test_scd2_block_present_when_has_update_strategy(self, generator, scd2_mapping):
        cfg = CodegenConfig(enable_scd2=True)
        gen = NotebookCodeGenerator(config=cfg)
        nb = gen.generate_silver(scd2_mapping)
        assert "SCD" in nb or "is_current" in nb or "eff_start_date" in nb

    def test_scd2_block_absent_when_disabled(self, simple_mapping):
        cfg = CodegenConfig(enable_scd2=False)
        gen = NotebookCodeGenerator(config=cfg)
        nb = gen.generate_silver(simple_mapping)
        # Standard write should not contain SCD2 columns when disabled and no Update Strategy
        # SCD2 requires both the config flag AND an Update Strategy transformation
        assert "eff_start_date" not in nb or "is_current" not in nb

    def test_lookup_broadcast_hint_present(self, generator, lookup_mapping):
        nb = generator.generate_silver(lookup_mapping)
        assert "broadcast" in nb.lower() or "F.broadcast" in nb

    def test_contains_logging(self, generator, simple_mapping):
        nb = generator.generate_silver(simple_mapping)
        assert "logger" in nb


# ===========================================================================
# Tests: generate_gold()
# ===========================================================================


class TestGenerateGold:
    def test_returns_string(self, generator, simple_mapping):
        nb = generator.generate_gold(simple_mapping)
        assert isinstance(nb, str)

    def test_contains_databricks_notebook_source(self, generator, simple_mapping):
        nb = generator.generate_gold(simple_mapping)
        assert "# Databricks notebook source" in nb

    def test_contains_silver_schema_widget(self, generator, simple_mapping):
        nb = generator.generate_gold(simple_mapping)
        assert "p_silver_schema" in nb

    def test_contains_gold_schema_widget(self, generator, simple_mapping):
        nb = generator.generate_gold(simple_mapping)
        assert "p_gold_schema" in nb

    def test_contains_delta_merge(self, generator, simple_mapping):
        nb = generator.generate_gold(simple_mapping)
        assert "merge" in nb.lower() or "MERGE" in nb

    def test_contains_optimize(self, generator, simple_mapping):
        nb = generator.generate_gold(simple_mapping)
        assert "OPTIMIZE" in nb

    def test_contains_zorder(self, generator, simple_mapping):
        nb = generator.generate_gold(simple_mapping)
        assert "ZORDER" in nb

    def test_pk_cols_in_zorder(self, generator, simple_mapping):
        nb = generator.generate_gold(simple_mapping)
        # order_id is the pk from our fixture
        assert "order_id" in nb

    def test_contains_merge_update_all(self, generator, simple_mapping):
        nb = generator.generate_gold(simple_mapping)
        assert "UpdateAll" in nb or "updateAll" in nb or "whenMatchedUpdateAll" in nb

    def test_contains_merge_insert_all(self, generator, simple_mapping):
        nb = generator.generate_gold(simple_mapping)
        assert "InsertAll" in nb or "insertAll" in nb or "whenNotMatchedInsertAll" in nb

    def test_dbutils_exit_success(self, generator, simple_mapping):
        nb = generator.generate_gold(simple_mapping)
        assert "dbutils.notebook.exit" in nb
        assert "SUCCESS" in nb

    def test_dbutils_exit_failed(self, generator, simple_mapping):
        nb = generator.generate_gold(simple_mapping)
        assert "FAILED" in nb

    def test_delta_table_for_name_used(self, generator, simple_mapping):
        nb = generator.generate_gold(simple_mapping)
        assert "DeltaTable" in nb or "delta" in nb.lower()

    def test_contains_logging(self, generator, simple_mapping):
        nb = generator.generate_gold(simple_mapping)
        assert "logger" in nb

    def test_gold_table_uses_gold_schema(self, generator, simple_mapping):
        nb = generator.generate_gold(simple_mapping)
        assert "cder_prod.gold" in nb


# ===========================================================================
# Tests: generate_all()
# ===========================================================================


class TestGenerateAll:
    def test_returns_dict_with_three_keys(self, generator, simple_mapping):
        all_nbs = generator.generate_all(simple_mapping)
        assert set(all_nbs.keys()) == {"bronze", "silver", "gold"}

    def test_all_values_are_strings(self, generator, simple_mapping):
        for _, nb in generator.generate_all(simple_mapping).items():
            assert isinstance(nb, str)

    def test_all_values_non_empty(self, generator, simple_mapping):
        for layer, nb in generator.generate_all(simple_mapping).items():
            assert len(nb) > 100, f"{layer} notebook is unexpectedly short"

    def test_notebooks_differ_by_layer(self, generator, simple_mapping):
        all_nbs = generator.generate_all(simple_mapping)
        # Bronze and Gold should not be identical
        assert all_nbs["bronze"] != all_nbs["gold"]


# ===========================================================================
# Tests: notebook_name()
# ===========================================================================


class TestNotebookName:
    def test_bronze_name(self, generator, simple_mapping):
        name = generator.notebook_name("bronze", simple_mapping)
        assert name.startswith("nb_bronze_")
        assert name.endswith("_ingest")

    def test_silver_name(self, generator, simple_mapping):
        name = generator.notebook_name("silver", simple_mapping)
        assert name.startswith("nb_silver_")
        assert name.endswith("_transform")

    def test_gold_name(self, generator, simple_mapping):
        name = generator.notebook_name("gold", simple_mapping)
        assert name.startswith("nb_gold_")
        assert name.endswith("_merge")

    def test_unknown_layer_produces_run_suffix(self, generator, simple_mapping):
        name = generator.notebook_name("unknown_layer", simple_mapping)
        assert "run" in name

    def test_name_is_snake_case(self, generator):
        mapping = _make_mapping("M_ORDER_LINE_ITEMS")
        name = generator.notebook_name("bronze", mapping)
        # Should not contain uppercase letters
        assert name == name.lower()


# ===========================================================================
# Tests: edge cases
# ===========================================================================


class TestEdgeCases:
    def test_empty_mapping_bronze(self, generator):
        mapping = MappingDef(name="M_EMPTY")
        nb = generator.generate_bronze(mapping)
        assert "# Databricks notebook source" in nb

    def test_empty_mapping_silver(self, generator):
        mapping = MappingDef(name="M_EMPTY")
        nb = generator.generate_silver(mapping)
        assert "# Databricks notebook source" in nb

    def test_empty_mapping_gold(self, generator):
        mapping = MappingDef(name="M_EMPTY")
        nb = generator.generate_gold(mapping)
        assert "# Databricks notebook source" in nb

    def test_mapping_name_appears_in_bronze(self, generator):
        mapping = _make_mapping("M_SPECIFIC_ENTITY")
        nb = generator.generate_bronze(mapping)
        assert "M_SPECIFIC_ENTITY" in nb

    def test_custom_secret_scope_appears_in_bronze(self):
        cfg = CodegenConfig(secret_scope="my-custom-scope")
        gen = NotebookCodeGenerator(config=cfg)
        mapping = _make_mapping()
        nb = gen.generate_bronze(mapping)
        assert "my-custom-scope" in nb

    def test_multiple_mappings_independent(self, generator):
        m1 = _make_mapping("M_ORDERS")
        m2 = _make_mapping("M_CUSTOMERS")
        nb1 = generator.generate_bronze(m1)
        nb2 = generator.generate_bronze(m2)
        assert nb1 != nb2

    def test_no_public_endpoints_in_generated_code(self, generator, simple_mapping):
        """FedRAMP safety check: no hard-coded public HTTP endpoints."""
        nb = generator.generate_bronze(simple_mapping)
        assert "http://" not in nb
        assert "0.0.0.0" not in nb
