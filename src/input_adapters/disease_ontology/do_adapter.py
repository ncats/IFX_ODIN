import re
from typing import Generator, List, Optional

from src.constants import DataSourceName
from src.input_adapters.mondo.mondo_adapter import MondoBaseAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.disease import Disease, DODiseaseParentEdge


class DOBaseAdapter(MondoBaseAdapter):
    """Base adapter for Disease Ontology (DO) JSON file.

    Reuses MondoBaseAdapter infrastructure (JSON loading, version extraction,
    date parsing) since DO ships in the same OBO JSON format.
    """

    version_info: Optional[DatasourceVersionInfo] = None

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.DiseaseOntology

    @staticmethod
    def _normalize_doid(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        if value.startswith("DOID:"):
            return value
        match = re.search(r"/obo/DOID_(\d+)$", value)
        if match:
            return f"DOID:{match.group(1)}"
        return None

    def _iter_included_do_nodes(self):
        for node in self._get_graph().get("nodes") or []:
            if not self._is_class_node(node):
                continue
            if self._is_deprecated(node):
                continue
            doid = self._normalize_doid(node.get("id"))
            if doid is None:
                continue
            yield node

    def _included_do_ids(self) -> set:
        return {
            self._normalize_doid(node.get("id"))
            for node in self._iter_included_do_nodes()
        }


class DODiseaseAdapter(DOBaseAdapter):

    def _to_disease(self, node: dict) -> Disease:
        meta = node.get("meta") or {}
        definition = meta.get("definition") or {}
        return Disease(
            id=self._normalize_doid(node.get("id")),
            name=node.get("lbl"),
            type=node.get("type"),
            do_description=definition.get("val") if isinstance(definition, dict) else None,
            synonyms=self._extract_values(meta.get("synonyms")),
            comments=self._extract_values(meta.get("comments")),
        )

    def get_all(self) -> Generator[List[Disease], None, None]:
        diseases = [self._to_disease(node) for node in self._iter_included_do_nodes()]
        yield diseases


class DODiseaseParentEdgeAdapter(DOBaseAdapter):

    def _edge_do_ids(self, edge: dict) -> tuple[Optional[str], Optional[str]]:
        child_id = self._normalize_doid(edge.get("sub"))
        parent_id = self._normalize_doid(edge.get("obj"))
        return child_id, parent_id

    @staticmethod
    def _is_parent_edge(edge: dict) -> bool:
        return edge.get("pred") == "is_a"

    @staticmethod
    def _to_parent_edge(child_id: str, parent_id: str) -> DODiseaseParentEdge:
        return DODiseaseParentEdge(
            start_node=Disease(id=child_id),
            end_node=Disease(id=parent_id),
            source="DO",
        )

    def get_all(self) -> Generator[List[DODiseaseParentEdge], None, None]:
        included_do_ids = self._included_do_ids()
        edges: List[DODiseaseParentEdge] = []

        for edge in self._get_graph().get("edges") or []:
            if not self._is_parent_edge(edge):
                continue

            child_id, parent_id = self._edge_do_ids(edge)
            if child_id is None or parent_id is None:
                continue
            if child_id not in included_do_ids or parent_id not in included_do_ids:
                continue

            edges.append(self._to_parent_edge(child_id, parent_id))

        yield edges
