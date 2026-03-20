"""
parser.py — PowerMart XML Parser
=================================
Parses Informatica PowerCenter repository XML exports (PowerMart format)
and extracts structured metadata for mappings, sources, targets,
transformations, connectors, workflows, worklets, sessions, and tasks.

Supported XML elements
-----------------------
FOLDER, MAPPING, SOURCE, TARGET, TRANSFORMATION (all subtypes),
CONNECTOR, WORKFLOW, WORKLET, SESSION, TASK

Usage
-----
    from src.migration.parser import PowerMartParser

    parser = PowerMartParser("path/to/export.xml")
    repo   = parser.parse()
    for mapping in repo.mappings:
        print(mapping.name, [t.type for t in mapping.transformations])
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Informatica transformation subtypes recognised by the parser
# ---------------------------------------------------------------------------
KNOWN_TRANSFORMATION_TYPES: frozenset[str] = frozenset(
    [
        "Source Qualifier",
        "Filter",
        "Expression",
        "Joiner",
        "Lookup Procedure",
        "Aggregator",
        "Update Strategy",
        "Router",
        "Union",
        "Sorter",
        "Sequence Generator",
        "Normalizer",
        "Rank",
        "Stored Procedure",
        # Aliases used in some PowerCenter versions
        "Lookup",
        "Source",
        "Target",
    ]
)


# ===========================================================================
# Data-class hierarchy
# ===========================================================================


@dataclass
class FieldDef:
    """Represents a single field (column) definition on a source, target, or transformation port."""

    name: str
    datatype: str
    precision: int = 0
    scale: int = 0
    nullable: bool = True
    description: str = ""
    key_type: str = ""          # PRIMARY KEY / FOREIGN KEY / NOT A KEY
    porttype: str = "INPUT/OUTPUT"  # INPUT, OUTPUT, INPUT/OUTPUT, LOOKUP


@dataclass
class SourceDef:
    """Informatica SOURCE element — represents a relational or flat-file source."""

    name: str
    dbtype: str                  # ORACLE, SQLSERVER, FLAT FILE, etc.
    owner: str = ""
    database: str = ""
    fields: List[FieldDef] = field(default_factory=list)
    description: str = ""
    raw_attrs: Dict[str, str] = field(default_factory=dict)


@dataclass
class TargetDef:
    """Informatica TARGET element — represents a relational or flat-file target."""

    name: str
    dbtype: str
    owner: str = ""
    database: str = ""
    fields: List[FieldDef] = field(default_factory=list)
    description: str = ""
    raw_attrs: Dict[str, str] = field(default_factory=dict)


@dataclass
class TransformationPort:
    """A single port (field) on a TRANSFORMATION."""

    name: str
    datatype: str
    porttype: str = "INPUT/OUTPUT"
    expression: str = ""
    default_value: str = ""
    precision: int = 0
    scale: int = 0
    group: str = ""             # Used by Router, Union, Joiner


@dataclass
class TransformationDef:
    """
    An Informatica TRANSFORMATION element with its subtype and ports.

    The ``subtype`` maps to the TRANSFORMTYPE attribute in the XML, e.g.
    'Source Qualifier', 'Expression', 'Aggregator', etc.
    """

    name: str
    type: str                    # Raw TRANSFORMTYPE value
    reusable: bool = False
    description: str = ""
    ports: List[TransformationPort] = field(default_factory=list)
    table_name: str = ""         # For Source Qualifier / Lookup
    sql_override: str = ""       # SQL override query (Source Qualifier)
    join_condition: str = ""     # Joiner condition
    filter_condition: str = ""   # Filter condition
    lookup_condition: str = ""   # Lookup condition
    update_strategy_expr: str = "" # DD_INSERT / DD_UPDATE / DD_DELETE expression
    group_filter_conditions: Dict[str, str] = field(default_factory=dict)  # Router
    sort_keys: List[str] = field(default_factory=list)   # Sorter
    rank_port: str = ""          # Rank transformation rank port
    rank_order: str = "TOP"      # TOP / BOTTOM
    sequence_start: int = 0
    sequence_increment: int = 1
    raw_attrs: Dict[str, str] = field(default_factory=dict)


@dataclass
class ConnectorDef:
    """
    An Informatica CONNECTOR element — a data flow link between two objects
    within a mapping.
    """

    from_instance: str
    from_field: str
    to_instance: str
    to_field: str
    from_instance_type: str = ""
    to_instance_type: str = ""


@dataclass
class MappingDef:
    """An Informatica MAPPING element with all its child objects."""

    name: str
    folder: str = ""
    description: str = ""
    sources: List[SourceDef] = field(default_factory=list)
    targets: List[TargetDef] = field(default_factory=list)
    transformations: List[TransformationDef] = field(default_factory=list)
    connectors: List[ConnectorDef] = field(default_factory=list)
    raw_attrs: Dict[str, str] = field(default_factory=dict)


@dataclass
class TaskDef:
    """An Informatica TASK element (Command, Email, Decision, etc.)."""

    name: str
    task_type: str               # SESSION, COMMAND, EMAIL, DECISION, etc.
    mapping_name: str = ""       # For SESSION tasks
    description: str = ""
    raw_attrs: Dict[str, str] = field(default_factory=dict)


@dataclass
class SessionDef:
    """An Informatica SESSION task — links a workflow task to a mapping."""

    name: str
    mapping_name: str
    source_connections: Dict[str, str] = field(default_factory=dict)
    target_connections: Dict[str, str] = field(default_factory=dict)
    description: str = ""
    raw_attrs: Dict[str, str] = field(default_factory=dict)


@dataclass
class WorkletDef:
    """An Informatica WORKLET — a reusable sub-workflow."""

    name: str
    tasks: List[TaskDef] = field(default_factory=list)
    sessions: List[SessionDef] = field(default_factory=list)
    description: str = ""


@dataclass
class WorkflowDef:
    """An Informatica WORKFLOW element."""

    name: str
    folder: str = ""
    description: str = ""
    tasks: List[TaskDef] = field(default_factory=list)
    sessions: List[SessionDef] = field(default_factory=list)
    worklets: List[WorkletDef] = field(default_factory=list)
    is_valid: bool = True
    raw_attrs: Dict[str, str] = field(default_factory=dict)


@dataclass
class FolderDef:
    """An Informatica FOLDER — the top-level organisational container."""

    name: str
    owner: str = ""
    description: str = ""
    mappings: List[MappingDef] = field(default_factory=list)
    workflows: List[WorkflowDef] = field(default_factory=list)
    sources: List[SourceDef] = field(default_factory=list)
    targets: List[TargetDef] = field(default_factory=list)
    reusable_transformations: List[TransformationDef] = field(default_factory=list)


@dataclass
class RepositoryDef:
    """
    Top-level result object returned by :class:`PowerMartParser`.

    Aggregates all folders, mappings, workflows, sources, and targets
    extracted from the PowerMart XML export.
    """

    name: str
    version: str = ""
    codepage: str = ""
    folders: List[FolderDef] = field(default_factory=list)

    # Convenience flat lists (populated after parse)
    mappings: List[MappingDef] = field(default_factory=list)
    workflows: List[WorkflowDef] = field(default_factory=list)
    sources: List[SourceDef] = field(default_factory=list)
    targets: List[TargetDef] = field(default_factory=list)


# ===========================================================================
# Parser implementation
# ===========================================================================


class PowerMartParser:
    """
    Parses an Informatica PowerCenter PowerMart XML export file.

    The PowerMart format is the standard XML export produced by the
    Informatica Repository Manager or ``pmrep ObjectExport`` command.

    Parameters
    ----------
    xml_path:
        Path to the ``.xml`` export file.

    Raises
    ------
    FileNotFoundError
        If ``xml_path`` does not exist.
    ET.ParseError
        If the XML is malformed.
    """

    def __init__(self, xml_path: str | Path) -> None:
        self.xml_path = Path(xml_path)
        if not self.xml_path.exists():
            raise FileNotFoundError(f"PowerMart XML not found: {self.xml_path}")
        self._tree: Optional[ET.ElementTree] = None
        self._root: Optional[ET.Element] = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def parse(self) -> RepositoryDef:
        """
        Parse the XML file and return a fully-populated :class:`RepositoryDef`.

        Returns
        -------
        RepositoryDef
        """
        logger.info("Parsing PowerMart XML: %s", self.xml_path)
        self._tree = ET.parse(self.xml_path)
        self._root = self._tree.getroot()

        repo = self._parse_repository()
        self._flatten(repo)

        logger.info(
            "Parse complete — %d folder(s), %d mapping(s), %d workflow(s)",
            len(repo.folders),
            len(repo.mappings),
            len(repo.workflows),
        )
        return repo

    # ------------------------------------------------------------------
    # Repository
    # ------------------------------------------------------------------

    def _parse_repository(self) -> RepositoryDef:
        """Extract the top-level REPOSITORY element."""
        root = self._root
        # PowerMart root is <POWERMART> containing <REPOSITORY>
        repo_el = root if root.tag == "REPOSITORY" else root.find("REPOSITORY")
        if repo_el is None:
            # Some exports omit the REPOSITORY wrapper
            repo_el = root

        repo = RepositoryDef(
            name=repo_el.get("NAME", "UNKNOWN"),
            version=repo_el.get("VERSION", ""),
            codepage=repo_el.get("CODEPAGE", ""),
        )

        for folder_el in repo_el.findall("FOLDER"):
            repo.folders.append(self._parse_folder(folder_el))

        return repo

    # ------------------------------------------------------------------
    # Folder
    # ------------------------------------------------------------------

    def _parse_folder(self, el: ET.Element) -> FolderDef:
        folder = FolderDef(
            name=el.get("NAME", ""),
            owner=el.get("OWNER", ""),
            description=el.get("DESCRIPTION", ""),
        )
        logger.debug("Parsing folder: %s", folder.name)

        # Reusable sources & targets at folder level
        for src_el in el.findall("SOURCE"):
            folder.sources.append(self._parse_source(src_el))

        for tgt_el in el.findall("TARGET"):
            folder.targets.append(self._parse_target(tgt_el))

        # Reusable transformations
        for trans_el in el.findall("TRANSFORMATION"):
            t = self._parse_transformation(trans_el)
            t.reusable = True
            folder.reusable_transformations.append(t)

        # Mappings
        for mapping_el in el.findall("MAPPING"):
            m = self._parse_mapping(mapping_el)
            m.folder = folder.name
            folder.mappings.append(m)

        # Workflows
        for wf_el in el.findall("WORKFLOW"):
            wf = self._parse_workflow(wf_el)
            wf.folder = folder.name
            folder.workflows.append(wf)

        return folder

    # ------------------------------------------------------------------
    # Mapping
    # ------------------------------------------------------------------

    def _parse_mapping(self, el: ET.Element) -> MappingDef:
        mapping = MappingDef(
            name=el.get("NAME", ""),
            description=el.get("DESCRIPTION", ""),
            raw_attrs=dict(el.attrib),
        )
        logger.debug("  Parsing mapping: %s", mapping.name)

        for src_el in el.findall("SOURCE"):
            mapping.sources.append(self._parse_source(src_el))

        for tgt_el in el.findall("TARGET"):
            mapping.targets.append(self._parse_target(tgt_el))

        for trans_el in el.findall("TRANSFORMATION"):
            mapping.transformations.append(self._parse_transformation(trans_el))

        # INSTANCE elements reference transformations within the mapping;
        # we capture them implicitly through connectors.
        for conn_el in el.findall("CONNECTOR"):
            mapping.connectors.append(self._parse_connector(conn_el))

        return mapping

    # ------------------------------------------------------------------
    # Source
    # ------------------------------------------------------------------

    def _parse_source(self, el: ET.Element) -> SourceDef:
        src = SourceDef(
            name=el.get("NAME", ""),
            dbtype=el.get("DBTYPE", "ORACLE"),
            owner=el.get("OWNERNAME", ""),
            database=el.get("DATABASETYPE", ""),
            description=el.get("DESCRIPTION", ""),
            raw_attrs=dict(el.attrib),
        )
        for field_el in el.findall("SOURCEFIELD"):
            src.fields.append(self._parse_source_field(field_el))
        return src

    def _parse_source_field(self, el: ET.Element) -> FieldDef:
        return FieldDef(
            name=el.get("NAME", ""),
            datatype=el.get("DATATYPE", "VARCHAR"),
            precision=int(el.get("PRECISION", 0) or 0),
            scale=int(el.get("SCALE", 0) or 0),
            nullable=el.get("NULLABLE", "YES").upper() != "NO",
            description=el.get("DESCRIPTION", ""),
            key_type=el.get("KEYTYPE", "NOT A KEY"),
        )

    # ------------------------------------------------------------------
    # Target
    # ------------------------------------------------------------------

    def _parse_target(self, el: ET.Element) -> TargetDef:
        tgt = TargetDef(
            name=el.get("NAME", ""),
            dbtype=el.get("DBTYPE", "ORACLE"),
            owner=el.get("OWNERNAME", ""),
            database=el.get("DATABASETYPE", ""),
            description=el.get("DESCRIPTION", ""),
            raw_attrs=dict(el.attrib),
        )
        for field_el in el.findall("TARGETFIELD"):
            tgt.fields.append(self._parse_target_field(field_el))
        return tgt

    def _parse_target_field(self, el: ET.Element) -> FieldDef:
        return FieldDef(
            name=el.get("NAME", ""),
            datatype=el.get("DATATYPE", "VARCHAR"),
            precision=int(el.get("PRECISION", 0) or 0),
            scale=int(el.get("SCALE", 0) or 0),
            nullable=el.get("NULLABLE", "YES").upper() != "NO",
            description=el.get("DESCRIPTION", ""),
            key_type=el.get("KEYTYPE", "NOT A KEY"),
        )

    # ------------------------------------------------------------------
    # Transformation
    # ------------------------------------------------------------------

    def _parse_transformation(self, el: ET.Element) -> TransformationDef:
        t_type = el.get("TYPE", "")
        t = TransformationDef(
            name=el.get("NAME", ""),
            type=t_type,
            reusable=el.get("REUSABLE", "NO").upper() == "YES",
            description=el.get("DESCRIPTION", ""),
            raw_attrs=dict(el.attrib),
        )

        if t_type not in KNOWN_TRANSFORMATION_TYPES:
            logger.warning("Unknown transformation type '%s' on '%s'", t_type, t.name)

        # Parse ports (TRANSFORMFIELD elements)
        for port_el in el.findall("TRANSFORMFIELD"):
            t.ports.append(self._parse_transformation_port(port_el))

        # Type-specific attributes extracted from TABLEATTRIBUTE elements
        attrs: Dict[str, str] = {
            ta.get("NAME", ""): ta.get("VALUE", "")
            for ta in el.findall("TABLEATTRIBUTE")
        }

        t.table_name = attrs.get("Source Table", attrs.get("Lookup table name", ""))
        t.sql_override = attrs.get("Sql Query", "")
        t.join_condition = attrs.get("Join Condition", "")
        t.filter_condition = attrs.get("Filter Condition", "")
        t.lookup_condition = attrs.get("Lookup Condition", "")
        t.update_strategy_expr = attrs.get("Update Strategy Expression", "")
        t.rank_order = attrs.get("Top/Bottom", "TOP")
        t.rank_port = attrs.get("Rank Port Name", "")
        t.sort_keys = [
            v.strip()
            for v in attrs.get("Sort Key", "").split(",")
            if v.strip()
        ]
        seq_start = attrs.get("Start Value", "0")
        seq_incr = attrs.get("Increment By", "1")
        t.sequence_start = int(seq_start) if seq_start.lstrip("-").isdigit() else 0
        t.sequence_increment = int(seq_incr) if seq_incr.lstrip("-").isdigit() else 1

        # Router group filter conditions
        for grp_el in el.findall("TRANSFORMGROUP"):
            grp_name = grp_el.get("NAME", "")
            for ta in grp_el.findall("TABLEATTRIBUTE"):
                if ta.get("NAME", "") == "Filter Condition":
                    t.group_filter_conditions[grp_name] = ta.get("VALUE", "")

        return t

    def _parse_transformation_port(self, el: ET.Element) -> TransformationPort:
        return TransformationPort(
            name=el.get("NAME", ""),
            datatype=el.get("DATATYPE", "VARCHAR"),
            porttype=el.get("PORTTYPE", "INPUT/OUTPUT"),
            expression=el.get("EXPRESSION", ""),
            default_value=el.get("DEFAULTVALUE", ""),
            precision=int(el.get("PRECISION", 0) or 0),
            scale=int(el.get("SCALE", 0) or 0),
            group=el.get("GROUP", ""),
        )

    # ------------------------------------------------------------------
    # Connector
    # ------------------------------------------------------------------

    def _parse_connector(self, el: ET.Element) -> ConnectorDef:
        return ConnectorDef(
            from_instance=el.get("FROMINSTANCE", ""),
            from_field=el.get("FROMFIELD", ""),
            to_instance=el.get("TOINSTANCE", ""),
            to_field=el.get("TOFIELD", ""),
            from_instance_type=el.get("FROMINSTANCETYPE", ""),
            to_instance_type=el.get("TOINSTANCETYPE", ""),
        )

    # ------------------------------------------------------------------
    # Workflow
    # ------------------------------------------------------------------

    def _parse_workflow(self, el: ET.Element) -> WorkflowDef:
        wf = WorkflowDef(
            name=el.get("NAME", ""),
            description=el.get("DESCRIPTION", ""),
            is_valid=el.get("ISVALID", "YES").upper() == "YES",
            raw_attrs=dict(el.attrib),
        )
        logger.debug("  Parsing workflow: %s", wf.name)

        for task_el in el.findall("TASK"):
            task = self._parse_task(task_el)
            wf.tasks.append(task)
            if task.task_type == "SESSION":
                wf.sessions.append(
                    SessionDef(
                        name=task.name,
                        mapping_name=task.mapping_name,
                        description=task.description,
                        raw_attrs=task.raw_attrs,
                    )
                )

        for worklet_el in el.findall("WORKLET"):
            wf.worklets.append(self._parse_worklet(worklet_el))

        return wf

    # ------------------------------------------------------------------
    # Worklet
    # ------------------------------------------------------------------

    def _parse_worklet(self, el: ET.Element) -> WorkletDef:
        wl = WorkletDef(
            name=el.get("NAME", ""),
            description=el.get("DESCRIPTION", ""),
        )
        for task_el in el.findall("TASK"):
            task = self._parse_task(task_el)
            wl.tasks.append(task)
            if task.task_type == "SESSION":
                wl.sessions.append(
                    SessionDef(
                        name=task.name,
                        mapping_name=task.mapping_name,
                        description=task.description,
                        raw_attrs=task.raw_attrs,
                    )
                )
        return wl

    # ------------------------------------------------------------------
    # Task / Session
    # ------------------------------------------------------------------

    def _parse_task(self, el: ET.Element) -> TaskDef:
        task_type = el.get("TYPE", "")
        mapping_name = ""

        # SESSION tasks reference a mapping via the $PMRootDir session attribute
        if task_type == "SESSION":
            # Mapping name is stored in a CONFIG_VALUE or ATTRIBUTE sub-element
            for attr_el in el.findall(".//ATTRIBUTE"):
                if attr_el.get("NAME", "") in ("Mapping name", "MappingName"):
                    mapping_name = attr_el.get("VALUE", "")
                    break
            # Fallback: SESSTRANSFORMATION sub-element
            if not mapping_name:
                sess_trans = el.find(".//SESSTRANSFORMATION")
                if sess_trans is not None:
                    mapping_name = sess_trans.get("MAPPINGNAME", "")

        return TaskDef(
            name=el.get("NAME", ""),
            task_type=task_type,
            mapping_name=mapping_name,
            description=el.get("DESCRIPTION", ""),
            raw_attrs=dict(el.attrib),
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _flatten(self, repo: RepositoryDef) -> None:
        """Populate the flat convenience lists on the RepositoryDef."""
        for folder in repo.folders:
            repo.mappings.extend(folder.mappings)
            repo.workflows.extend(folder.workflows)
            repo.sources.extend(folder.sources)
            repo.targets.extend(folder.targets)

    # ------------------------------------------------------------------
    # Utility accessors
    # ------------------------------------------------------------------

    def get_mapping(self, name: str, repo: RepositoryDef) -> Optional[MappingDef]:
        """Return a mapping by name (case-insensitive)."""
        name_upper = name.upper()
        for m in repo.mappings:
            if m.name.upper() == name_upper:
                return m
        return None

    def get_workflow(self, name: str, repo: RepositoryDef) -> Optional[WorkflowDef]:
        """Return a workflow by name (case-insensitive)."""
        name_upper = name.upper()
        for wf in repo.workflows:
            if wf.name.upper() == name_upper:
                return wf
        return None

    def get_transformations_by_type(
        self, mapping: MappingDef, t_type: str
    ) -> List[TransformationDef]:
        """Return all transformations of a given type within a mapping."""
        return [t for t in mapping.transformations if t.type == t_type]

    def summarise(self, repo: RepositoryDef) -> Dict[str, object]:
        """
        Return a human-readable summary dictionary of the parsed repository.

        Useful for logging and debugging during migration planning.
        """
        type_counts: Dict[str, int] = {}
        for m in repo.mappings:
            for t in m.transformations:
                type_counts[t.type] = type_counts.get(t.type, 0) + 1

        return {
            "repository": repo.name,
            "version": repo.version,
            "folders": len(repo.folders),
            "mappings": len(repo.mappings),
            "workflows": len(repo.workflows),
            "sources": len(repo.sources),
            "targets": len(repo.targets),
            "transformation_type_counts": type_counts,
        }
