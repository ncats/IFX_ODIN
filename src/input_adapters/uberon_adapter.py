import re
from typing import Generator, List, Union

import obonet

from src.constants import DataSourceName
from src.input_adapters.flat_file_adapter import FlatFileAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import Node, Relationship
from src.models.tissue import Tissue, TissueParentEdge

UBERON_PREFIX = "UBERON:"
SYNONYM_FIELDS = ["exact_synonym", "related_synonym", "broad_synonym", "narrow_synonym"]
DEFINITION_PATTERN = re.compile(r'"(.+)" \[')


class UberonBaseAdapter(FlatFileAdapter):
    def __init__(self, data_source):
        self.version_info = data_source.version_info()
        file_path = str(data_source.file("uberon.obo"))
        super().__init__(file_path)
        self._graph = obonet.read_obo(self.file_path)

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.UBERON

    @staticmethod
    def _is_uberon_id(term_id: str) -> bool:
        return isinstance(term_id, str) and term_id.startswith(UBERON_PREFIX)

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    @staticmethod
    def _extract_definition(definition: str | None) -> str | None:
        if not definition:
            return None
        match = DEFINITION_PATTERN.findall(definition)
        if match:
            return match[0]
        return definition

    @staticmethod
    def _extract_synonyms(term_data: dict) -> List[str]:
        synonyms = []
        for synonym_field in SYNONYM_FIELDS:
            synonyms.extend(term_data.get(synonym_field, []))
        return synonyms


class UberonAdapter(UberonBaseAdapter):
    def _build_tissue(self, term_id: str, term_data: dict) -> Tissue:
        return Tissue(
            id=term_id,
            name=term_data.get("name", term_id),
            definition=self._extract_definition(term_data.get("def")),
            synonyms=self._extract_synonyms(term_data),
        )

    @staticmethod
    def _build_parent_edge(child_id: str, parent_id: str) -> TissueParentEdge:
        return TissueParentEdge(
            start_node=Tissue(id=child_id),
            end_node=Tissue(id=parent_id),
        )

    def _iter_parent_edges(self, term_id: str, term_data: dict):
        for parent_id in term_data.get("is_a", []):
            if self._is_uberon_id(parent_id):
                yield self._build_parent_edge(term_id, parent_id)

    def _iter_uberon_terms(self):
        for term_id, term_data in self._graph.nodes(data=True):
            if self._is_uberon_id(term_id):
                yield term_id, term_data

    def _yield_tissue_batches(self) -> Generator[List[Tissue], None, None]:
        tissue_batch = []
        for term_id, term_data in self._iter_uberon_terms():
            tissue_batch.append(self._build_tissue(term_id, term_data))
            if len(tissue_batch) >= self.batch_size:
                yield tissue_batch
                tissue_batch = []
        if tissue_batch:
            yield tissue_batch

    def _yield_edge_batches(self) -> Generator[List[TissueParentEdge], None, None]:
        edge_batch = []
        for term_id, term_data in self._iter_uberon_terms():
            for edge in self._iter_parent_edges(term_id, term_data):
                edge_batch.append(edge)
                if len(edge_batch) >= self.batch_size:
                    yield edge_batch
                    edge_batch = []
        if edge_batch:
            yield edge_batch

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        for node_batch in self._yield_tissue_batches():
            yield node_batch

        for edge_batch in self._yield_edge_batches():
            yield edge_batch
