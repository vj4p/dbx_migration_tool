"""
tests/test_parser.py — Unit tests for the PowerMart XML parser
=============================================================
Tests cover:
- Repository-level parsing (name, version, folders)
- Folder-level parsing (sources, targets, reusable transformations, mappings, workflows)
- Mapping parsing (sources, targets, transformations, connectors)
- All supported transformation types and their attributes
- Workflow, worklet, task, and session parsing
- Edge cases: empty XML, missing attributes, unknown transformation types
- Utility methods: get_mapping, get_workflow, get_transformations_by_type, summarise
"""

from __future__ import annotations

import textwrap
import xml.etree.ElementTree as ET
from pathlib import Path
from unittest.mock import patch
import pytest
import tempfile
import os

from src.migration.parser import (
    ConnectorDef,
    FieldDef,
    FolderDef,
    MappingDef,
    PowerMartParser,
    RepositoryDef,
    SessionDef,
    SourceDef,
    TargetDef,
    TaskDef,
    TransformationDef,
    TransformationPort,
    WorkflowDef,
    WorkletDef,
    KNOWN_TRANSFORMATION_TYPES,
)


# ---------------------------------------------------------------------------
# Fixtures — helpers for building test XML
# ---------------------------------------------------------------------------

def _write_xml(xml_content: str) -> str:
    """Write XML to a temp file and return the path."""
    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=".xml", delete=False, encoding="utf-8"
    )
    f.write(xml_content)
    f.flush()
    f.close()
    return f.name


def _minimal_xml(folder_content: str = "") -> str:
    """Wrap folder_content in a minimal PowerMart/REPOSITORY envelope."""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        "<POWERMART>\n"
        '  <REPOSITORY NAME="TEST_REPO" VERSION="182" CODEPAGE="UTF-8">\n'
        '    <FOLDER NAME="TEST_FOLDER" OWNER="admin" DESCRIPTION="Test folder">\n'
        f"{folder_content}\n"
        "    </FOLDER>\n"
        "  </REPOSITORY>\n"
        "</POWERMART>\n"
    )


def _source_xml(name: str = "SRC_TABLE", dbtype: str = "ORACLE") -> str:
    return textwrap.dedent(f"""\
        <SOURCE NAME="{name}" DBTYPE="{dbtype}" OWNERNAME="SCHEMA" DESCRIPTION="Source table">
          <SOURCEFIELD NAME="ID"   DATATYPE="NUMBER"  PRECISION="10" SCALE="0" NULLABLE="NO" KEYTYPE="PRIMARY KEY"/>
          <SOURCEFIELD NAME="NAME" DATATYPE="VARCHAR" PRECISION="50" SCALE="0" NULLABLE="YES" KEYTYPE="NOT A KEY"/>
        </SOURCE>
    """)


def _target_xml(name: str = "TGT_TABLE") -> str:
    return textwrap.dedent(f"""\
        <TARGET NAME="{name}" DBTYPE="ORACLE" OWNERNAME="SCHEMA" DESCRIPTION="Target table">
          <TARGETFIELD NAME="ID"   DATATYPE="NUMBER"  PRECISION="10" SCALE="0" NULLABLE="NO"  KEYTYPE="PRIMARY KEY"/>
          <TARGETFIELD NAME="NAME" DATATYPE="VARCHAR" PRECISION="50" SCALE="0" NULLABLE="YES" KEYTYPE="NOT A KEY"/>
        </TARGET>
    """)


def _transformation_xml(
    name: str,
    t_type: str,
    table_attrs: str = "",
    ports: str = "",
    reusable: str = "NO",
) -> str:
    return textwrap.dedent(f"""\
        <TRANSFORMATION NAME="{name}" TYPE="{t_type}" REUSABLE="{reusable}" DESCRIPTION="{t_type} test">
          {table_attrs}
          {ports}
        </TRANSFORMATION>
    """)


def _connector_xml(
    from_instance: str = "SQ_SRC",
    from_field: str = "ID",
    to_instance: str = "EXP_TRANSFORM",
    to_field: str = "ID",
) -> str:
    return (
        f'<CONNECTOR FROMINSTANCE="{from_instance}" FROMFIELD="{from_field}" '
        f'TOINSTANCE="{to_instance}" TOFIELD="{to_field}" '
        f'FROMINSTANCETYPE="Source Qualifier" TOINSTANCETYPE="Expression"/>'
    )


def _mapping_xml(name: str = "M_TEST", body: str = "") -> str:
    return textwrap.dedent(f"""\
        <MAPPING NAME="{name}" DESCRIPTION="Test mapping">
          {_source_xml()}
          {_target_xml()}
          {body}
          {_connector_xml()}
        </MAPPING>
    """)


def _workflow_xml(name: str = "WF_TEST", body: str = "") -> str:
    return textwrap.dedent(f"""\
        <WORKFLOW NAME="{name}" DESCRIPTION="Test workflow" ISVALID="YES">
          {body}
        </WORKFLOW>
    """)


def _session_task_xml(name: str = "s_M_TEST", mapping_name: str = "M_TEST") -> str:
    return textwrap.dedent(f"""\
        <TASK NAME="{name}" TYPE="SESSION" DESCRIPTION="Session task">
          <ATTRIBUTE NAME="Mapping name" VALUE="{mapping_name}"/>
        </TASK>
    """)


# ===========================================================================
# Tests: file handling
# ===========================================================================


class TestFileHandling:
    def test_file_not_found_raises(self):
        with pytest.raises(FileNotFoundError, match="PowerMart XML not found"):
            PowerMartParser("/nonexistent/path/export.xml")

    def test_accepts_string_path(self, tmp_path):
        xml = _minimal_xml()
        p = tmp_path / "test.xml"
        p.write_text(xml)
        parser = PowerMartParser(str(p))
        repo = parser.parse()
        assert repo.name == "TEST_REPO"

    def test_accepts_pathlib_path(self, tmp_path):
        xml = _minimal_xml()
        p = tmp_path / "test.xml"
        p.write_text(xml)
        parser = PowerMartParser(p)
        repo = parser.parse()
        assert isinstance(repo, RepositoryDef)


# ===========================================================================
# Tests: repository
# ===========================================================================


class TestRepositoryParsing:
    @pytest.fixture
    def repo(self, tmp_path) -> RepositoryDef:
        p = tmp_path / "repo.xml"
        p.write_text(_minimal_xml())
        return PowerMartParser(p).parse()

    def test_repository_name(self, repo):
        assert repo.name == "TEST_REPO"

    def test_repository_version(self, repo):
        assert repo.version == "182"

    def test_repository_codepage(self, repo):
        assert repo.codepage == "UTF-8"

    def test_repository_has_one_folder(self, repo):
        assert len(repo.folders) == 1

    def test_flat_mappings_empty_when_no_mappings(self, repo):
        assert repo.mappings == []

    def test_flat_workflows_empty_when_no_workflows(self, repo):
        assert repo.workflows == []


# ===========================================================================
# Tests: folder
# ===========================================================================


class TestFolderParsing:
    @pytest.fixture
    def folder(self, tmp_path) -> FolderDef:
        xml = _minimal_xml(
            _source_xml("FOLDER_SRC") + _target_xml("FOLDER_TGT")
        )
        p = tmp_path / "f.xml"
        p.write_text(xml)
        repo = PowerMartParser(p).parse()
        return repo.folders[0]

    def test_folder_name(self, folder):
        assert folder.name == "TEST_FOLDER"

    def test_folder_owner(self, folder):
        assert folder.owner == "admin"

    def test_folder_sources(self, folder):
        assert len(folder.sources) == 1
        assert folder.sources[0].name == "FOLDER_SRC"

    def test_folder_targets(self, folder):
        assert len(folder.targets) == 1
        assert folder.targets[0].name == "FOLDER_TGT"


# ===========================================================================
# Tests: source
# ===========================================================================


class TestSourceParsing:
    @pytest.fixture
    def source(self, tmp_path) -> SourceDef:
        xml = _minimal_xml(_mapping_xml())
        p = tmp_path / "s.xml"
        p.write_text(xml)
        repo = PowerMartParser(p).parse()
        return repo.mappings[0].sources[0]

    def test_source_name(self, source):
        assert source.name == "SRC_TABLE"

    def test_source_dbtype(self, source):
        assert source.dbtype == "ORACLE"

    def test_source_owner(self, source):
        assert source.owner == "SCHEMA"

    def test_source_field_count(self, source):
        assert len(source.fields) == 2

    def test_source_primary_key_field(self, source):
        pk_fields = [f for f in source.fields if "PRIMARY" in f.key_type.upper()]
        assert len(pk_fields) == 1
        assert pk_fields[0].name == "ID"

    def test_source_field_nullable(self, source):
        id_field = next(f for f in source.fields if f.name == "ID")
        assert id_field.nullable is False

    def test_source_field_precision(self, source):
        id_field = next(f for f in source.fields if f.name == "ID")
        assert id_field.precision == 10


# ===========================================================================
# Tests: target
# ===========================================================================


class TestTargetParsing:
    @pytest.fixture
    def target(self, tmp_path) -> TargetDef:
        xml = _minimal_xml(_mapping_xml())
        p = tmp_path / "t.xml"
        p.write_text(xml)
        repo = PowerMartParser(p).parse()
        return repo.mappings[0].targets[0]

    def test_target_name(self, target):
        assert target.name == "TGT_TABLE"

    def test_target_field_count(self, target):
        assert len(target.fields) == 2


# ===========================================================================
# Tests: transformation types
# ===========================================================================


class TestTransformationParsing:
    def _parse_with_transform(self, tmp_path, t_xml: str) -> TransformationDef:
        xml = _minimal_xml(_mapping_xml(body=t_xml))
        p = tmp_path / "tx.xml"
        p.write_text(xml)
        repo = PowerMartParser(p).parse()
        return repo.mappings[0].transformations[0]

    def test_source_qualifier(self, tmp_path):
        t_xml = _transformation_xml(
            "SQ_ORDERS", "Source Qualifier",
            table_attrs='<TABLEATTRIBUTE NAME="Sql Query" VALUE="SELECT * FROM ORDERS"/>',
        )
        t = self._parse_with_transform(tmp_path, t_xml)
        assert t.type == "Source Qualifier"
        assert "SELECT * FROM ORDERS" in t.sql_override

    def test_filter(self, tmp_path):
        t_xml = _transformation_xml(
            "FIL_ACTIVE", "Filter",
            table_attrs='<TABLEATTRIBUTE NAME="Filter Condition" VALUE="STATUS = \'A\'"/>',
        )
        t = self._parse_with_transform(tmp_path, t_xml)
        assert t.type == "Filter"
        assert "STATUS" in t.filter_condition

    def test_expression(self, tmp_path):
        ports = textwrap.dedent("""\
            <TRANSFORMFIELD NAME="OUT_NAME" DATATYPE="VARCHAR" PORTTYPE="OUTPUT"
              EXPRESSION="UPPER(NAME)" PRECISION="50" SCALE="0"/>
        """)
        t_xml = _transformation_xml("EXP_CLEANSE", "Expression", ports=ports)
        t = self._parse_with_transform(tmp_path, t_xml)
        assert t.type == "Expression"
        assert len(t.ports) == 1
        assert t.ports[0].expression == "UPPER(NAME)"

    def test_joiner(self, tmp_path):
        t_xml = _transformation_xml(
            "JNR_ORDERS_CUST", "Joiner",
            table_attrs='<TABLEATTRIBUTE NAME="Join Condition" VALUE="ORDER.CUST_ID = CUST.ID"/>',
        )
        t = self._parse_with_transform(tmp_path, t_xml)
        assert t.type == "Joiner"
        assert "CUST_ID" in t.join_condition

    def test_lookup(self, tmp_path):
        t_xml = _transformation_xml(
            "LKP_CUSTOMERS", "Lookup Procedure",
            table_attrs=(
                '<TABLEATTRIBUTE NAME="Lookup table name" VALUE="DIM_CUSTOMER"/>'
                '<TABLEATTRIBUTE NAME="Lookup Condition" VALUE="LKP.CUST_ID = CUST_ID"/>'
            ),
        )
        t = self._parse_with_transform(tmp_path, t_xml)
        assert t.type == "Lookup Procedure"
        assert t.table_name == "DIM_CUSTOMER"
        assert "CUST_ID" in t.lookup_condition

    def test_aggregator(self, tmp_path):
        t_xml = _transformation_xml("AGG_SALES", "Aggregator")
        t = self._parse_with_transform(tmp_path, t_xml)
        assert t.type == "Aggregator"

    def test_update_strategy(self, tmp_path):
        t_xml = _transformation_xml(
            "UPD_ORDERS", "Update Strategy",
            table_attrs='<TABLEATTRIBUTE NAME="Update Strategy Expression" VALUE="DD_UPDATE"/>',
        )
        t = self._parse_with_transform(tmp_path, t_xml)
        assert t.type == "Update Strategy"
        assert t.update_strategy_expr == "DD_UPDATE"

    def test_router(self, tmp_path):
        t_xml = textwrap.dedent("""\
            <TRANSFORMATION NAME="RTR_STATUS" TYPE="Router" REUSABLE="NO">
              <TRANSFORMGROUP NAME="ACTIVE">
                <TABLEATTRIBUTE NAME="Filter Condition" VALUE="STATUS = 'A'"/>
              </TRANSFORMGROUP>
              <TRANSFORMGROUP NAME="INACTIVE">
                <TABLEATTRIBUTE NAME="Filter Condition" VALUE="STATUS = 'I'"/>
              </TRANSFORMGROUP>
            </TRANSFORMATION>
        """)
        t = self._parse_with_transform(tmp_path, t_xml)
        assert t.type == "Router"
        assert "ACTIVE" in t.group_filter_conditions
        assert "STATUS = 'A'" in t.group_filter_conditions["ACTIVE"]

    def test_union(self, tmp_path):
        t_xml = _transformation_xml("UNI_ALL_ORDERS", "Union")
        t = self._parse_with_transform(tmp_path, t_xml)
        assert t.type == "Union"

    def test_sorter(self, tmp_path):
        t_xml = _transformation_xml(
            "SRT_DATE", "Sorter",
            table_attrs='<TABLEATTRIBUTE NAME="Sort Key" VALUE="ORDER_DATE, CUST_ID"/>',
        )
        t = self._parse_with_transform(tmp_path, t_xml)
        assert t.type == "Sorter"
        assert "ORDER_DATE" in t.sort_keys

    def test_sequence_generator(self, tmp_path):
        t_xml = _transformation_xml(
            "SEQ_ID", "Sequence Generator",
            table_attrs=(
                '<TABLEATTRIBUTE NAME="Start Value" VALUE="1"/>'
                '<TABLEATTRIBUTE NAME="Increment By" VALUE="1"/>'
            ),
        )
        t = self._parse_with_transform(tmp_path, t_xml)
        assert t.type == "Sequence Generator"
        assert t.sequence_start == 1
        assert t.sequence_increment == 1

    def test_normalizer(self, tmp_path):
        t_xml = _transformation_xml("NRM_ITEMS", "Normalizer")
        t = self._parse_with_transform(tmp_path, t_xml)
        assert t.type == "Normalizer"

    def test_rank(self, tmp_path):
        t_xml = _transformation_xml(
            "RNK_TOP_SALES", "Rank",
            table_attrs=(
                '<TABLEATTRIBUTE NAME="Top/Bottom" VALUE="TOP"/>'
                '<TABLEATTRIBUTE NAME="Rank Port Name" VALUE="SALES_AMOUNT"/>'
            ),
        )
        t = self._parse_with_transform(tmp_path, t_xml)
        assert t.type == "Rank"
        assert t.rank_order == "TOP"
        assert t.rank_port == "SALES_AMOUNT"

    def test_stored_procedure(self, tmp_path):
        t_xml = _transformation_xml("SP_CALC_TOTALS", "Stored Procedure")
        t = self._parse_with_transform(tmp_path, t_xml)
        assert t.type == "Stored Procedure"

    def test_reusable_flag(self, tmp_path):
        t_xml = _transformation_xml("EXP_REUSABLE", "Expression", reusable="YES")
        t = self._parse_with_transform(tmp_path, t_xml)
        assert t.reusable is True

    def test_non_reusable_flag(self, tmp_path):
        t_xml = _transformation_xml("EXP_LOCAL", "Expression", reusable="NO")
        t = self._parse_with_transform(tmp_path, t_xml)
        assert t.reusable is False

    def test_unknown_transformation_type_logged(self, tmp_path, caplog):
        import logging
        t_xml = _transformation_xml("TX_UNKNOWN", "CustomType123")
        xml = _minimal_xml(_mapping_xml(body=t_xml))
        p = tmp_path / "unk.xml"
        p.write_text(xml)
        with caplog.at_level(logging.WARNING, logger="src.migration.parser"):
            PowerMartParser(p).parse()
        assert any("CustomType123" in r.message for r in caplog.records)

    def test_port_expression_captured(self, tmp_path):
        ports = (
            '<TRANSFORMFIELD NAME="FULL_NAME" DATATYPE="VARCHAR" PORTTYPE="OUTPUT" '
            'EXPRESSION="LTRIM(RTRIM(FIRST_NAME)) || \' \' || LTRIM(RTRIM(LAST_NAME))" '
            'PRECISION="100" SCALE="0" GROUP=""/>'
        )
        t_xml = _transformation_xml("EXP_NAME", "Expression", ports=ports)
        t = self._parse_with_transform(tmp_path, t_xml)
        assert len(t.ports) == 1
        assert "FIRST_NAME" in t.ports[0].expression


# ===========================================================================
# Tests: connector
# ===========================================================================


class TestConnectorParsing:
    @pytest.fixture
    def connectors(self, tmp_path):
        xml = _minimal_xml(_mapping_xml())
        p = tmp_path / "c.xml"
        p.write_text(xml)
        return PowerMartParser(p).parse().mappings[0].connectors

    def test_connector_from_instance(self, connectors):
        assert connectors[0].from_instance == "SQ_SRC"

    def test_connector_to_instance(self, connectors):
        assert connectors[0].to_instance == "EXP_TRANSFORM"

    def test_connector_from_field(self, connectors):
        assert connectors[0].from_field == "ID"

    def test_connector_to_field(self, connectors):
        assert connectors[0].to_field == "ID"

    def test_connector_from_instance_type(self, connectors):
        assert connectors[0].from_instance_type == "Source Qualifier"


# ===========================================================================
# Tests: workflow / worklet / session / task
# ===========================================================================


class TestWorkflowParsing:
    @pytest.fixture
    def workflow(self, tmp_path) -> WorkflowDef:
        xml = _minimal_xml(
            _workflow_xml(body=_session_task_xml())
        )
        p = tmp_path / "wf.xml"
        p.write_text(xml)
        repo = PowerMartParser(p).parse()
        return repo.workflows[0]

    def test_workflow_name(self, workflow):
        assert workflow.name == "WF_TEST"

    def test_workflow_is_valid(self, workflow):
        assert workflow.is_valid is True

    def test_workflow_folder(self, workflow):
        assert workflow.folder == "TEST_FOLDER"

    def test_workflow_has_task(self, workflow):
        assert len(workflow.tasks) == 1

    def test_task_type(self, workflow):
        assert workflow.tasks[0].task_type == "SESSION"

    def test_task_mapping_name(self, workflow):
        assert workflow.tasks[0].mapping_name == "M_TEST"

    def test_session_created_from_task(self, workflow):
        assert len(workflow.sessions) == 1
        assert workflow.sessions[0].mapping_name == "M_TEST"

    def test_invalid_workflow(self, tmp_path):
        xml = _minimal_xml(
            '<WORKFLOW NAME="WF_INVALID" ISVALID="NO"/>'
        )
        p = tmp_path / "inv.xml"
        p.write_text(xml)
        repo = PowerMartParser(p).parse()
        assert repo.workflows[0].is_valid is False


class TestWorkletParsing:
    def test_worklet_within_workflow(self, tmp_path):
        worklet_xml = textwrap.dedent("""\
            <WORKLET NAME="WL_SUB_FLOW" DESCRIPTION="Sub-workflow">
              <TASK NAME="s_inner" TYPE="SESSION">
                <ATTRIBUTE NAME="Mapping name" VALUE="M_INNER"/>
              </TASK>
            </WORKLET>
        """)
        xml = _minimal_xml(_workflow_xml(body=worklet_xml))
        p = tmp_path / "wl.xml"
        p.write_text(xml)
        repo = PowerMartParser(p).parse()
        wf = repo.workflows[0]
        assert len(wf.worklets) == 1
        assert wf.worklets[0].name == "WL_SUB_FLOW"
        assert len(wf.worklets[0].sessions) == 1
        assert wf.worklets[0].sessions[0].mapping_name == "M_INNER"


# ===========================================================================
# Tests: mapping-level flat lists
# ===========================================================================


class TestFlatLists:
    @pytest.fixture
    def repo(self, tmp_path) -> RepositoryDef:
        xml = _minimal_xml(
            _mapping_xml("M_ORDERS") + _workflow_xml("WF_DAILY")
        )
        p = tmp_path / "flat.xml"
        p.write_text(xml)
        return PowerMartParser(p).parse()

    def test_flat_mappings_populated(self, repo):
        assert len(repo.mappings) == 1
        assert repo.mappings[0].name == "M_ORDERS"

    def test_flat_workflows_populated(self, repo):
        assert len(repo.workflows) == 1
        assert repo.workflows[0].name == "WF_DAILY"

    def test_mapping_folder_set(self, repo):
        assert repo.mappings[0].folder == "TEST_FOLDER"


# ===========================================================================
# Tests: utility accessors
# ===========================================================================


class TestUtilityAccessors:
    @pytest.fixture
    def parser_and_repo(self, tmp_path):
        xml = _minimal_xml(
            _mapping_xml("M_ORDERS")
            + _transformation_xml("EXP_1", "Expression", reusable="YES")
            + _workflow_xml("WF_DAILY")
        )
        p = tmp_path / "util.xml"
        p.write_text(xml)
        parser = PowerMartParser(p)
        repo = parser.parse()
        return parser, repo

    def test_get_mapping_by_name(self, parser_and_repo):
        parser, repo = parser_and_repo
        m = parser.get_mapping("M_ORDERS", repo)
        assert m is not None
        assert m.name == "M_ORDERS"

    def test_get_mapping_case_insensitive(self, parser_and_repo):
        parser, repo = parser_and_repo
        m = parser.get_mapping("m_orders", repo)
        assert m is not None

    def test_get_mapping_not_found_returns_none(self, parser_and_repo):
        parser, repo = parser_and_repo
        m = parser.get_mapping("NONEXISTENT", repo)
        assert m is None

    def test_get_workflow_by_name(self, parser_and_repo):
        parser, repo = parser_and_repo
        wf = parser.get_workflow("WF_DAILY", repo)
        assert wf is not None
        assert wf.name == "WF_DAILY"

    def test_get_transformations_by_type(self, parser_and_repo):
        parser, repo = parser_and_repo
        mapping = repo.mappings[0]
        # The mapping XML includes the Expression transformation via the body arg
        exprs = parser.get_transformations_by_type(mapping, "Expression")
        # No Expression was in the body passed to _mapping_xml() in this fixture
        assert isinstance(exprs, list)

    def test_summarise_structure(self, parser_and_repo):
        parser, repo = parser_and_repo
        summary = parser.summarise(repo)
        assert summary["repository"] == "TEST_REPO"
        assert summary["folders"] == 1
        assert isinstance(summary["transformation_type_counts"], dict)

    def test_summarise_counts_transformations(self, tmp_path):
        t_xml = (
            _transformation_xml("EXP_A", "Expression")
            + _transformation_xml("EXP_B", "Expression")
            + _transformation_xml("AGG_C", "Aggregator")
        )
        xml = _minimal_xml(_mapping_xml(body=t_xml))
        p = tmp_path / "summ.xml"
        p.write_text(xml)
        parser = PowerMartParser(p)
        repo = parser.parse()
        summary = parser.summarise(repo)
        counts = summary["transformation_type_counts"]
        assert counts.get("Expression", 0) == 2
        assert counts.get("Aggregator", 0) == 1


# ===========================================================================
# Tests: edge cases
# ===========================================================================


class TestEdgeCases:
    def test_no_folders(self, tmp_path):
        xml = '<?xml version="1.0"?><REPOSITORY NAME="EMPTY" VERSION="1"/>'
        p = tmp_path / "empty.xml"
        p.write_text(xml)
        repo = PowerMartParser(p).parse()
        assert repo.folders == []
        assert repo.mappings == []

    def test_missing_optional_attrs(self, tmp_path):
        # SOURCE with no OWNERNAME, DESCRIPTION
        xml = textwrap.dedent("""\
            <?xml version="1.0"?>
            <REPOSITORY NAME="R">
              <FOLDER NAME="F">
                <MAPPING NAME="M_MINIMAL">
                  <SOURCE NAME="BARE_SRC" DBTYPE="ORACLE"/>
                  <TARGET NAME="BARE_TGT" DBTYPE="ORACLE"/>
                </MAPPING>
              </FOLDER>
            </REPOSITORY>
        """)
        p = tmp_path / "minimal.xml"
        p.write_text(xml)
        repo = PowerMartParser(p).parse()
        src = repo.mappings[0].sources[0]
        assert src.owner == ""
        assert src.fields == []

    def test_source_without_fields(self, tmp_path):
        xml = _minimal_xml("<MAPPING NAME='M'><SOURCE NAME='S' DBTYPE='ORACLE'/></MAPPING>")
        p = tmp_path / "nf.xml"
        p.write_text(xml)
        repo = PowerMartParser(p).parse()
        assert repo.mappings[0].sources[0].fields == []

    def test_multiple_folders(self, tmp_path):
        xml = textwrap.dedent("""\
            <?xml version="1.0"?>
            <REPOSITORY NAME="MULTI">
              <FOLDER NAME="F1"><MAPPING NAME="M1"><SOURCE NAME="S1" DBTYPE="ORACLE"/></MAPPING></FOLDER>
              <FOLDER NAME="F2"><MAPPING NAME="M2"><SOURCE NAME="S2" DBTYPE="ORACLE"/></MAPPING></FOLDER>
            </REPOSITORY>
        """)
        p = tmp_path / "multi.xml"
        p.write_text(xml)
        repo = PowerMartParser(p).parse()
        assert len(repo.folders) == 2
        assert len(repo.mappings) == 2

    def test_known_transformation_types_constant(self):
        assert "Source Qualifier" in KNOWN_TRANSFORMATION_TYPES
        assert "Expression" in KNOWN_TRANSFORMATION_TYPES
        assert "Aggregator" in KNOWN_TRANSFORMATION_TYPES
        assert "Lookup Procedure" in KNOWN_TRANSFORMATION_TYPES
        assert "Update Strategy" in KNOWN_TRANSFORMATION_TYPES

    def test_sequence_generator_nonnumeric_start(self, tmp_path):
        t_xml = _transformation_xml(
            "SEQ_BAD", "Sequence Generator",
            table_attrs='<TABLEATTRIBUTE NAME="Start Value" VALUE="N/A"/>',
        )
        xml = _minimal_xml(_mapping_xml(body=t_xml))
        p = tmp_path / "seq_bad.xml"
        p.write_text(xml)
        repo = PowerMartParser(p).parse()
        t = repo.mappings[0].transformations[0]
        assert t.sequence_start == 0  # fallback to 0
