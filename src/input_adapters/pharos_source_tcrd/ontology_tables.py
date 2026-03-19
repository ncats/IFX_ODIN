from typing import Generator, List

from src.input_adapters.disease_ontology.do_adapter import DOBaseAdapter, DODiseaseAdapter
from src.input_adapters.mondo.mondo_adapter import MondoBaseAdapter, MondoDiseaseAdapter
from src.models.tcrd_disease_ontology import MondoTerm, MondoTermParentEdge, DOTerm, DOTermParentEdge


class MondoTableAdapter(MondoDiseaseAdapter):

    def _to_term(self, node: dict, subset_label_map: dict) -> MondoTerm:
        disease = self._to_disease(node, subset_label_map)
        comments = disease.comments or []
        return MondoTerm(
            id=disease.id,
            name=disease.name,
            mondo_description=disease.mondo_description,
            comment=comments[0] if comments else None,
        )

    def get_all(self) -> Generator[List[MondoTerm], None, None]:
        subset_label_map = self._build_subset_label_map()
        yield [self._to_term(node, subset_label_map) for node in self._iter_included_disease_nodes()]


class MondoTableParentEdgeAdapter(MondoBaseAdapter):

    def get_all(self) -> Generator[List[MondoTermParentEdge], None, None]:
        included_disease_ids = self._included_disease_ids()
        edges: List[MondoTermParentEdge] = []

        for edge in self._get_graph().get("edges") or []:
            if edge.get("pred") != "is_a":
                continue

            child_id = self._normalize_mondo_id(edge.get("sub"))
            parent_id = self._normalize_mondo_id(edge.get("obj"))
            if child_id is None or parent_id is None:
                continue
            if child_id not in included_disease_ids or parent_id not in included_disease_ids:
                continue

            edges.append(MondoTermParentEdge(
                start_node=MondoTerm(id=child_id),
                end_node=MondoTerm(id=parent_id),
            ))

        yield edges


class DOTableAdapter(DODiseaseAdapter):

    def _to_term(self, node: dict) -> DOTerm:
        disease = self._to_disease(node)
        return DOTerm(
            id=disease.id,
            name=disease.name,
            do_description=disease.do_description,
        )

    def get_all(self) -> Generator[List[DOTerm], None, None]:
        yield [self._to_term(node) for node in self._iter_included_do_nodes()]


class DOTableParentEdgeAdapter(DOBaseAdapter):

    def get_all(self) -> Generator[List[DOTermParentEdge], None, None]:
        included_do_ids = self._included_do_ids()
        edges: List[DOTermParentEdge] = []

        for edge in self._get_graph().get("edges") or []:
            if edge.get("pred") != "is_a":
                continue

            child_id = self._normalize_doid(edge.get("sub"))
            parent_id = self._normalize_doid(edge.get("obj"))
            if child_id is None or parent_id is None:
                continue
            if child_id not in included_do_ids or parent_id not in included_do_ids:
                continue

            edges.append(DOTermParentEdge(
                start_node=DOTerm(id=child_id),
                end_node=DOTerm(id=parent_id),
            ))

        yield edges
