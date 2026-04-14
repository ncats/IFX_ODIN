import csv
import os
import re
from datetime import datetime
from typing import Generator, List, Optional, Union

from src.constants import DataSourceName, Prefix
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo, parse_to_date
from src.models.node import EquivalentId, Node, Relationship
from src.models.panther_class import (
    PantherClass,
    PantherClassParentEdge,
    PantherFamily,
    PantherFamilyParentEdge,
    ProteinPantherClassEdge,
    ProteinPantherFamilyEdge,
)
from src.models.protein import Protein

_PCID_RE = re.compile(r"#(PC\d{5})")


def _panther_class_node_id(pcid: str) -> str:
    return EquivalentId(id=pcid, type=Prefix.PANTHER_CLASS).id_str()


def _panther_family_node_id(panther_family_id: str) -> str:
    return EquivalentId(id=panther_family_id, type=Prefix.PANTHER_FAMILY).id_str()


class PantherClassesAdapter(InputAdapter):
    def __init__(
        self,
        class_file_path: str,
        relationship_file_path: str,
        sequence_classification_file_path: str,
        version_file_path: Optional[str] = None,
        max_rows: Optional[int] = None,
    ):
        self.class_file_path = class_file_path
        self.relationship_file_path = relationship_file_path
        self.sequence_classification_file_path = sequence_classification_file_path
        self.version_file_path = version_file_path
        self.max_rows = max_rows

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.PANTHERClasses

    def get_version(self) -> DatasourceVersionInfo:
        version = None
        version_date = None
        download_date = None
        if self.version_file_path and os.path.exists(self.version_file_path):
            with open(self.version_file_path, "r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle, delimiter="\t")
                row = next(reader, None)
                if row:
                    version = row.get("version") or None
                    version_date = parse_to_date(row.get("version_date"))
                    download_date = parse_to_date(row.get("download_date"))

        if download_date is None:
            timestamps = []
            for path in (
                self.class_file_path,
                self.relationship_file_path,
                self.sequence_classification_file_path,
            ):
                if os.path.exists(path):
                    timestamps.append(os.path.getmtime(path))
            if timestamps:
                download_date = datetime.fromtimestamp(max(timestamps)).date()

        return DatasourceVersionInfo(
            version=version,
            version_date=version_date,
            download_date=download_date,
        )

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        family_nodes = self._load_family_nodes()
        if family_nodes:
            yield list(family_nodes.values())

        class_nodes = self._load_class_nodes()
        yield list(class_nodes.values())

        family_parent_edges = self._load_family_parent_edges(family_nodes)
        if family_parent_edges:
            yield family_parent_edges

        parent_edges = self._load_parent_edges()
        if parent_edges:
            yield parent_edges

        family_edges = self._load_family_membership_edges(family_nodes)
        for i in range(0, len(family_edges), self.batch_size):
            yield family_edges[i:i + self.batch_size]

        protein_edges = self._load_protein_edges(class_nodes)
        for i in range(0, len(protein_edges), self.batch_size):
            yield protein_edges[i:i + self.batch_size]

    def _load_family_nodes(self) -> dict:
        nodes = {}
        with open(self.sequence_classification_file_path, "r", encoding="utf-8", errors="replace") as handle:
            reader = csv.reader(handle, delimiter="\t")
            kept_rows = 0
            for row in reader:
                if len(row) < 5:
                    continue
                family_id = (row[3] or "").strip()
                family_name = (row[4] or "").strip() or None
                if not family_id:
                    continue

                top_level_id = family_id.split(":", 1)[0]
                if top_level_id not in nodes:
                    nodes[top_level_id] = PantherFamily(
                        id=_panther_family_node_id(top_level_id),
                        source_id=top_level_id,
                        level="family",
                        name=family_name,
                        source="PANTHER",
                    )

                if family_id not in nodes:
                    nodes[family_id] = PantherFamily(
                        id=_panther_family_node_id(family_id),
                        source_id=family_id,
                        level="subfamily" if ":" in family_id else "family",
                        source="PANTHER",
                    )

                if self.max_rows is not None:
                    kept_rows += 1
                    if kept_rows >= self.max_rows:
                        break
        return nodes

    def _load_family_parent_edges(self, family_nodes: dict) -> List[PantherFamilyParentEdge]:
        edges: List[PantherFamilyParentEdge] = []
        seen = set()
        for source_id in family_nodes.keys():
            if ":" not in source_id:
                continue
            parent_id = source_id.split(":", 1)[0]
            key = (source_id, parent_id)
            if key in seen:
                continue
            seen.add(key)
            edges.append(
                PantherFamilyParentEdge(
                    start_node=PantherFamily(id=_panther_family_node_id(source_id)),
                    end_node=PantherFamily(id=_panther_family_node_id(parent_id)),
                )
            )
        return edges

    def _load_class_nodes(self) -> dict:
        nodes = {}
        for row in self._iter_tsv_rows(self.class_file_path):
            if len(row) < 3:
                continue
            pcid = row[0].strip()
            if pcid.startswith("#"):
                continue
            if not pcid or pcid in nodes:
                continue
            hierarchy_code = row[1].strip() if len(row) > 1 else None
            name = row[2].strip() if len(row) > 2 else None
            description = row[3].strip() if len(row) > 3 else None
            nodes[pcid] = PantherClass(
                id=_panther_class_node_id(pcid),
                source_id=pcid,
                name=name or None,
                description=description or None,
                hierarchy_code=hierarchy_code or None,
            )
        return nodes

    def _load_parent_edges(self) -> List[PantherClassParentEdge]:
        edges = []
        seen = set()
        for row in self._iter_tsv_rows(self.relationship_file_path):
            if len(row) < 3:
                continue
            child_pcid = row[0].strip()
            parent_pcid = row[2].strip()
            if not child_pcid or not parent_pcid:
                continue
            key = (child_pcid, parent_pcid)
            if key in seen:
                continue
            seen.add(key)
            edges.append(
                PantherClassParentEdge(
                    start_node=PantherClass(id=_panther_class_node_id(child_pcid)),
                    end_node=PantherClass(id=_panther_class_node_id(parent_pcid)),
                )
            )
        return edges

    def _load_protein_edges(self, class_nodes: dict) -> List[ProteinPantherClassEdge]:
        edges: List[ProteinPantherClassEdge] = []
        seen = set()
        kept_rows = 0
        with open(self.sequence_classification_file_path, "r", encoding="utf-8", errors="replace") as handle:
            reader = csv.reader(handle, delimiter="\t")
            for row in reader:
                if len(row) < 10:
                    continue
                protein_classes = row[9].strip()
                if not protein_classes:
                    continue
                uniprot_id = self._extract_uniprot_id(row)
                if not uniprot_id:
                    continue
                pcids = _PCID_RE.findall(protein_classes)
                if not pcids:
                    continue
                kept_rows += 1
                protein_node = Protein(id=EquivalentId(id=uniprot_id, type=Prefix.UniProtKB).id_str())
                for pcid in pcids:
                    if pcid not in class_nodes:
                        continue
                    key = (protein_node.id, pcid)
                    if key in seen:
                        continue
                    seen.add(key)
                    edges.append(
                        ProteinPantherClassEdge(
                            start_node=protein_node,
                            end_node=PantherClass(id=_panther_class_node_id(pcid)),
                            source="PANTHER",
                        )
                    )
                if self.max_rows is not None and kept_rows >= self.max_rows:
                    break
        return edges

    def _load_family_membership_edges(self, family_nodes: dict) -> List[ProteinPantherFamilyEdge]:
        edges: List[ProteinPantherFamilyEdge] = []
        seen = set()
        kept_rows = 0
        with open(self.sequence_classification_file_path, "r", encoding="utf-8", errors="replace") as handle:
            reader = csv.reader(handle, delimiter="\t")
            for row in reader:
                if len(row) < 4:
                    continue
                family_id = (row[3] or "").strip()
                if not family_id or family_id not in family_nodes:
                    continue
                uniprot_id = self._extract_uniprot_id(row)
                if not uniprot_id:
                    continue
                kept_rows += 1
                protein_node = Protein(id=EquivalentId(id=uniprot_id, type=Prefix.UniProtKB).id_str())
                key = (protein_node.id, family_id)
                if key in seen:
                    continue
                seen.add(key)
                edges.append(
                    ProteinPantherFamilyEdge(
                        start_node=protein_node,
                        end_node=PantherFamily(id=_panther_family_node_id(family_id)),
                        source="PANTHER",
                    )
                )
                if self.max_rows is not None and kept_rows >= self.max_rows:
                    break
        return edges

    @staticmethod
    def _extract_uniprot_id(row: list) -> Optional[str]:
        if len(row) > 1 and row[1].strip():
            return row[1].strip()
        compound_field = row[0].strip() if row else ""
        parts = compound_field.split("|")
        for part in parts:
            if part.startswith("UniProtKB="):
                return part.split("=", 1)[1].strip() or None
        return None

    def _iter_tsv_rows(self, file_path: str):
        with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
            reader = csv.reader(handle, delimiter="\t")
            for row in reader:
                if not row:
                    continue
                first = (row[0] or "").strip()
                if not first or first.startswith("!"):
                    continue
                if file_path == self.class_file_path and first.startswith("#") and not first.startswith("#PC"):
                    continue
                yield row
