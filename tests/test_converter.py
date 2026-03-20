"""
tests/test_converter.py — Unit tests for the transformation type converter
=========================================================================
Tests cover:
- convert() returns correct ConversionResult for every supported Informatica type
- Medallion layer assignment for each type
- PySpark class name and import correctness
- Unsupported type handling (Stored Procedure + unknown types)
- requires_broadcast flag
- convert_mapping() processes all transformations in a mapping
- unsupported_types() filter
- requires_broadcast() filter
- all_imports() deduplication
- conversion_report() structure
- Extra-mappings override in constructor
"""

from __future__ import annotations

from unittest.mock import MagicMock
from typing import List

import pytest

from src.migration.converter import (
    INFA_AGGREGATOR,
    INFA_EXPRESSION,
    INFA_FILTER,
    INFA_JOINER,
    INFA_LOOKUP,
    INFA_LOOKUP_ALT,
    INFA_NORMALIZER,
    INFA_RANK,
    INFA_ROUTER,
    INFA_SEQUENCE_GENERATOR,
    INFA_SORTER,
    INFA_SOURCE_QUALIFIER,
    INFA_STORED_PROCEDURE,
    INFA_UNION,
    INFA_UPDATE_STRATEGY,
    ConversionResult,
    MedallionLayer,
    TransformationConverter,
)
from src.migration.parser import MappingDef, TransformationDef


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mapping(trans_types: List[str]) -> MappingDef:
    """Build a minimal MappingDef with the specified transformation types."""
    transformations = [
        TransformationDef(name=f"T_{t_type.replace(' ', '_')}", type=t_type)
        for t_type in trans_types
    ]
    return MappingDef(name="M_TEST", transformations=transformations)


# ===========================================================================
# Tests: convert() for each supported type
# ===========================================================================


class TestConvertSupportedTypes:
    @pytest.fixture
    def converter(self) -> TransformationConverter:
        return TransformationConverter()

    @pytest.mark.parametrize("infa_type,expected_class", [
        (INFA_SOURCE_QUALIFIER,   "DataFrameReader.jdbc"),
        (INFA_FILTER,             "DataFrame.filter"),
        (INFA_EXPRESSION,         "DataFrame.withColumn"),
        (INFA_JOINER,             "DataFrame.join"),
        (INFA_LOOKUP,             "DataFrame.join (broadcast)"),
        (INFA_LOOKUP_ALT,         "DataFrame.join (broadcast)"),
        (INFA_AGGREGATOR,         "DataFrame.groupBy"),
        (INFA_UPDATE_STRATEGY,    "DeltaTable.merge"),
        (INFA_ROUTER,             "DataFrame.filter (multi-branch)"),
        (INFA_UNION,              "DataFrame.unionByName"),
        (INFA_SORTER,             "DataFrame.orderBy"),
        (INFA_SEQUENCE_GENERATOR, "F.monotonically_increasing_id / Window.row_number"),
        (INFA_NORMALIZER,         "F.explode / F.stack"),
        (INFA_RANK,               "Window.rank / dense_rank"),
        (INFA_STORED_PROCEDURE,   "spark.sql / JDBC CallableStatement"),
    ])
    def test_pyspark_class(self, converter, infa_type, expected_class):
        result = converter.convert(infa_type)
        assert result.pyspark_class == expected_class, (
            f"Type '{infa_type}': expected pyspark_class='{expected_class}', "
            f"got '{result.pyspark_class}'"
        )

    @pytest.mark.parametrize("infa_type,expected_layer", [
        (INFA_SOURCE_QUALIFIER,   MedallionLayer.BRONZE),
        (INFA_FILTER,             MedallionLayer.SILVER),
        (INFA_EXPRESSION,         MedallionLayer.SILVER),
        (INFA_JOINER,             MedallionLayer.SILVER),
        (INFA_LOOKUP,             MedallionLayer.SILVER),
        (INFA_LOOKUP_ALT,         MedallionLayer.SILVER),
        (INFA_AGGREGATOR,         MedallionLayer.SILVER),
        (INFA_UPDATE_STRATEGY,    MedallionLayer.GOLD),
        (INFA_ROUTER,             MedallionLayer.SILVER),
        (INFA_UNION,              MedallionLayer.SILVER),
        (INFA_SORTER,             MedallionLayer.SILVER),
        (INFA_SEQUENCE_GENERATOR, MedallionLayer.SILVER),
        (INFA_NORMALIZER,         MedallionLayer.SILVER),
        (INFA_RANK,               MedallionLayer.SILVER),
        (INFA_STORED_PROCEDURE,   MedallionLayer.SILVER),
    ])
    def test_medallion_layer(self, converter, infa_type, expected_layer):
        result = converter.convert(infa_type)
        assert result.medallion_layer == expected_layer, (
            f"Type '{infa_type}': expected layer={expected_layer}, "
            f"got {result.medallion_layer}"
        )

    @pytest.mark.parametrize("infa_type,expected_supported", [
        (INFA_SOURCE_QUALIFIER,  True),
        (INFA_FILTER,            True),
        (INFA_EXPRESSION,        True),
        (INFA_JOINER,            True),
        (INFA_LOOKUP,            True),
        (INFA_AGGREGATOR,        True),
        (INFA_UPDATE_STRATEGY,   True),
        (INFA_ROUTER,            True),
        (INFA_UNION,             True),
        (INFA_SORTER,            True),
        (INFA_SEQUENCE_GENERATOR,True),
        (INFA_NORMALIZER,        True),
        (INFA_RANK,              True),
        (INFA_STORED_PROCEDURE,  False),  # explicitly unsupported
    ])
    def test_supported_flag(self, converter, infa_type, expected_supported):
        result = converter.convert(infa_type)
        assert result.supported == expected_supported

    def test_infa_type_field_matches_key(self, converter):
        """The infa_type field on the result must match the key used to look it up."""
        for t in [INFA_FILTER, INFA_AGGREGATOR, INFA_UPDATE_STRATEGY]:
            assert converter.convert(t).infa_type == t

    def test_pyspark_pattern_not_empty(self, converter):
        for t in [INFA_SOURCE_QUALIFIER, INFA_EXPRESSION, INFA_JOINER]:
            result = converter.convert(t)
            assert result.pyspark_pattern.strip(), f"Empty pattern for '{t}'"

    def test_notes_not_empty_for_complex_types(self, converter):
        """Transformation types with caveats must have non-empty notes."""
        for t in [INFA_LOOKUP, INFA_SEQUENCE_GENERATOR, INFA_STORED_PROCEDURE]:
            assert converter.convert(t).notes.strip(), f"Empty notes for '{t}'"


# ===========================================================================
# Tests: requires_broadcast
# ===========================================================================


class TestRequiresBroadcast:
    @pytest.fixture
    def converter(self) -> TransformationConverter:
        return TransformationConverter()

    def test_lookup_requires_broadcast(self, converter):
        assert converter.convert(INFA_LOOKUP).requires_broadcast is True

    def test_lookup_alt_requires_broadcast(self, converter):
        assert converter.convert(INFA_LOOKUP_ALT).requires_broadcast is True

    def test_filter_does_not_require_broadcast(self, converter):
        assert converter.convert(INFA_FILTER).requires_broadcast is False

    def test_joiner_does_not_require_broadcast(self, converter):
        assert converter.convert(INFA_JOINER).requires_broadcast is False


# ===========================================================================
# Tests: unknown type handling
# ===========================================================================


class TestUnknownTypeHandling:
    @pytest.fixture
    def converter(self) -> TransformationConverter:
        return TransformationConverter()

    def test_unknown_type_returns_unsupported(self, converter):
        result = converter.convert("MySuperCustomTransform")
        assert result.supported is False

    def test_unknown_type_pyspark_class_unknown(self, converter):
        result = converter.convert("NonExistentType")
        assert result.pyspark_class == "UNKNOWN"

    def test_unknown_type_includes_type_in_result(self, converter):
        result = converter.convert("WeirdType")
        assert result.infa_type == "WeirdType"

    def test_unknown_type_logs_warning(self, converter, caplog):
        import logging
        with caplog.at_level(logging.WARNING, logger="src.migration.converter"):
            converter.convert("GhostTransform")
        assert any("GhostTransform" in r.message for r in caplog.records)

    def test_unknown_type_has_todo_pattern(self, converter):
        result = converter.convert("NoSuchTransform")
        assert "TODO" in result.pyspark_pattern or "manual" in result.pyspark_pattern.lower()


# ===========================================================================
# Tests: convert_mapping()
# ===========================================================================


class TestConvertMapping:
    @pytest.fixture
    def converter(self) -> TransformationConverter:
        return TransformationConverter()

    def test_returns_list_same_length_as_transformations(self, converter):
        mapping = _make_mapping([INFA_FILTER, INFA_EXPRESSION, INFA_AGGREGATOR])
        results = converter.convert_mapping(mapping)
        assert len(results) == 3

    def test_order_preserved(self, converter):
        types = [INFA_SOURCE_QUALIFIER, INFA_FILTER, INFA_EXPRESSION, INFA_UPDATE_STRATEGY]
        mapping = _make_mapping(types)
        results = converter.convert_mapping(mapping)
        for i, t in enumerate(types):
            assert results[i].infa_type == t

    def test_empty_mapping_returns_empty_list(self, converter):
        mapping = _make_mapping([])
        assert converter.convert_mapping(mapping) == []

    def test_mixed_supported_unsupported(self, converter):
        mapping = _make_mapping([INFA_FILTER, INFA_STORED_PROCEDURE, "Unknown"])
        results = converter.convert_mapping(mapping)
        assert results[0].supported is True
        assert results[1].supported is False
        assert results[2].supported is False


# ===========================================================================
# Tests: get_layer_for_type()
# ===========================================================================


class TestGetLayerForType:
    @pytest.fixture
    def converter(self) -> TransformationConverter:
        return TransformationConverter()

    def test_source_qualifier_is_bronze(self, converter):
        assert converter.get_layer_for_type(INFA_SOURCE_QUALIFIER) == MedallionLayer.BRONZE

    def test_update_strategy_is_gold(self, converter):
        assert converter.get_layer_for_type(INFA_UPDATE_STRATEGY) == MedallionLayer.GOLD

    def test_filter_is_silver(self, converter):
        assert converter.get_layer_for_type(INFA_FILTER) == MedallionLayer.SILVER

    def test_unknown_type_defaults_to_silver(self, converter):
        assert converter.get_layer_for_type("RandomType") == MedallionLayer.SILVER


# ===========================================================================
# Tests: unsupported_types()
# ===========================================================================


class TestUnsupportedTypes:
    @pytest.fixture
    def converter(self) -> TransformationConverter:
        return TransformationConverter()

    def test_no_unsupported(self, converter):
        mapping = _make_mapping([INFA_FILTER, INFA_EXPRESSION])
        assert converter.unsupported_types(mapping) == []

    def test_stored_procedure_is_unsupported(self, converter):
        mapping = _make_mapping([INFA_STORED_PROCEDURE])
        unsupported = converter.unsupported_types(mapping)
        assert INFA_STORED_PROCEDURE in unsupported

    def test_unknown_type_is_unsupported(self, converter):
        mapping = _make_mapping([INFA_FILTER, "WeirdType"])
        unsupported = converter.unsupported_types(mapping)
        assert "WeirdType" in unsupported
        assert INFA_FILTER not in unsupported

    def test_returns_list_not_set(self, converter):
        mapping = _make_mapping([INFA_STORED_PROCEDURE])
        result = converter.unsupported_types(mapping)
        assert isinstance(result, list)


# ===========================================================================
# Tests: requires_broadcast() on mapping
# ===========================================================================


class TestRequiresBroadcastMapping:
    @pytest.fixture
    def converter(self) -> TransformationConverter:
        return TransformationConverter()

    def test_lookup_flagged_for_broadcast(self, converter):
        mapping = _make_mapping([INFA_FILTER, INFA_LOOKUP])
        broadcast_transforms = converter.requires_broadcast(mapping)
        assert len(broadcast_transforms) == 1
        assert broadcast_transforms[0].type == INFA_LOOKUP

    def test_no_broadcast_transforms(self, converter):
        mapping = _make_mapping([INFA_FILTER, INFA_EXPRESSION])
        broadcast_transforms = converter.requires_broadcast(mapping)
        assert broadcast_transforms == []


# ===========================================================================
# Tests: all_imports()
# ===========================================================================


class TestAllImports:
    @pytest.fixture
    def converter(self) -> TransformationConverter:
        return TransformationConverter()

    def test_returns_list(self, converter):
        mapping = _make_mapping([INFA_FILTER])
        imports = converter.all_imports(mapping)
        assert isinstance(imports, list)

    def test_imports_deduplicated(self, converter):
        # Both FILTER and EXPRESSION need F imports — should not duplicate
        mapping = _make_mapping([INFA_FILTER, INFA_EXPRESSION, INFA_AGGREGATOR])
        imports = converter.all_imports(mapping)
        assert len(imports) == len(set(imports))

    def test_imports_sorted(self, converter):
        mapping = _make_mapping([INFA_SEQUENCE_GENERATOR, INFA_FILTER])
        imports = converter.all_imports(mapping)
        assert imports == sorted(imports)

    def test_source_qualifier_has_spark_import(self, converter):
        mapping = _make_mapping([INFA_SOURCE_QUALIFIER])
        imports = converter.all_imports(mapping)
        assert any("SparkSession" in imp or "pyspark" in imp for imp in imports)

    def test_update_strategy_has_delta_import(self, converter):
        mapping = _make_mapping([INFA_UPDATE_STRATEGY])
        imports = converter.all_imports(mapping)
        assert any("delta" in imp.lower() or "DeltaTable" in imp for imp in imports)


# ===========================================================================
# Tests: conversion_report()
# ===========================================================================


class TestConversionReport:
    @pytest.fixture
    def converter(self) -> TransformationConverter:
        return TransformationConverter()

    def test_report_length_matches_transformations(self, converter):
        mapping = _make_mapping([INFA_FILTER, INFA_AGGREGATOR, INFA_UPDATE_STRATEGY])
        report = converter.conversion_report(mapping)
        assert len(report) == 3

    def test_report_entry_has_required_keys(self, converter):
        mapping = _make_mapping([INFA_FILTER])
        entry = converter.conversion_report(mapping)[0]
        required_keys = {
            "name", "infa_type", "pyspark_class",
            "medallion_layer", "supported", "requires_broadcast", "notes",
        }
        assert required_keys <= set(entry.keys())

    def test_report_entry_medallion_layer_is_string(self, converter):
        mapping = _make_mapping([INFA_UPDATE_STRATEGY])
        entry = converter.conversion_report(mapping)[0]
        assert isinstance(entry["medallion_layer"], str)
        assert entry["medallion_layer"] == "gold"

    def test_report_entry_supported_is_bool(self, converter):
        mapping = _make_mapping([INFA_FILTER])
        entry = converter.conversion_report(mapping)[0]
        assert isinstance(entry["supported"], bool)

    def test_empty_mapping_returns_empty_report(self, converter):
        mapping = _make_mapping([])
        assert converter.conversion_report(mapping) == []


# ===========================================================================
# Tests: extra_mappings constructor override
# ===========================================================================


class TestExtraMappings:
    def test_extra_mapping_overrides_built_in(self):
        custom_result = ConversionResult(
            infa_type=INFA_FILTER,
            pyspark_class="CustomFilter",
            pyspark_pattern="custom_pattern",
            medallion_layer=MedallionLayer.GOLD,
        )
        converter = TransformationConverter(extra_mappings={INFA_FILTER: custom_result})
        result = converter.convert(INFA_FILTER)
        assert result.pyspark_class == "CustomFilter"
        assert result.medallion_layer == MedallionLayer.GOLD

    def test_extra_mapping_adds_new_type(self):
        new_result = ConversionResult(
            infa_type="CustomXMLTransform",
            pyspark_class="F.from_xml",
            pyspark_pattern="df.select(F.from_xml(...))",
            medallion_layer=MedallionLayer.SILVER,
            supported=True,
        )
        converter = TransformationConverter(
            extra_mappings={"CustomXMLTransform": new_result}
        )
        result = converter.convert("CustomXMLTransform")
        assert result.supported is True
        assert result.pyspark_class == "F.from_xml"

    def test_built_in_types_still_work_after_extra_mappings(self):
        converter = TransformationConverter(
            extra_mappings={"NewType": ConversionResult(
                infa_type="NewType",
                pyspark_class="F.something",
                pyspark_pattern="",
            )}
        )
        # Built-in should still work
        assert converter.convert(INFA_AGGREGATOR).pyspark_class == "DataFrame.groupBy"


# ===========================================================================
# Tests: MedallionLayer enum
# ===========================================================================


class TestMedallionLayerEnum:
    def test_bronze_value(self):
        assert MedallionLayer.BRONZE.value == "bronze"

    def test_silver_value(self):
        assert MedallionLayer.SILVER.value == "silver"

    def test_gold_value(self):
        assert MedallionLayer.GOLD.value == "gold"

    def test_enum_string_comparison(self):
        assert MedallionLayer.BRONZE == "bronze"
