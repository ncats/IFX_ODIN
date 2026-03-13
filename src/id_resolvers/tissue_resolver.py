from typing import Dict, List, Set

import obonet

from src.interfaces.id_resolver import IdMatch, IdResolver
from src.models.node import Node

class TissueResolver(IdResolver):
    name = "Tissue Resolver"

    def __init__(self, file_path: str, valid_ontologies: List[str], **kwargs):
        super().__init__(**kwargs)
        self._graph = obonet.read_obo(file_path)
        self._valid_ontologies = self._normalize_valid_ontologies(valid_ontologies)
        self._xref_map = self._build_xref_map()
        self._xref_reverse_map = self._build_xref_reverse_map()
        self._obo_ids: Set[str] = set(self._xref_map.keys())

    @staticmethod
    def _normalize_valid_ontologies(valid_ontologies: List[str]) -> Set[str]:
        return {ontology.lower() for ontology in valid_ontologies}

    @staticmethod
    def _clean_xref(raw_xref: str) -> str | None:
        if not isinstance(raw_xref, str):
            return None
        candidate = raw_xref.strip()
        if not candidate:
            return None
        if " " in candidate:
            candidate = candidate.split(" ", 1)[0]
        return candidate if ":" in candidate else None

    def _normalize_xref_if_allowed(self, xref: str) -> str | None:
        prefix, local_id = xref.split(":", 1)
        normalized_prefix = prefix.strip().lower()
        normalized_local_id = local_id.strip()

        if normalized_prefix not in self._valid_ontologies:
            return None
        if not normalized_local_id:
            return None

        return f"{normalized_prefix}:{normalized_local_id}"

    def _get_filtered_xrefs(self, node_data: dict) -> List[str]:
        filtered_xrefs = set()
        for raw_xref in node_data.get("xref", []):
            cleaned_xref = self._clean_xref(raw_xref)
            if cleaned_xref is None:
                continue

            normalized_xref = self._normalize_xref_if_allowed(cleaned_xref)
            if normalized_xref is not None:
                filtered_xrefs.add(normalized_xref)

        return sorted(filtered_xrefs)

    def _build_xref_map(self) -> Dict[str, List[str]]:
        """Index every term in the OBO as a canonical output node.

        valid_ontologies controls which xref prefixes are indexed as inputs.
        The output namespace is whatever terms are actually in the OBO file.
        """
        xref_map = {}
        for node_id, node_data in self._graph.nodes(data=True):
            xref_map[node_id] = self._get_filtered_xrefs(node_data)
        return xref_map

    def _build_xref_reverse_map(self) -> Dict[str, str]:
        """xref ID (normalized: lowercase prefix) -> canonical UBERON/CL ID.

        For every UBERON/CL node, maps each of its valid xrefs back to that
        node. This lets any input ID (BTO, MESH, etc.) that appears in uberon.obo
        resolve to its canonical equivalent without needing adapter-level mapping.
        """
        reverse_map: Dict[str, str] = {}
        for node_id, xrefs in self._xref_map.items():
            for xref in xrefs:
                reverse_map[xref] = node_id
        return reverse_map

    @staticmethod
    def _normalize_input_id(node_id: str) -> str:
        """Normalize an input ID to lowercase prefix so it can be looked up in the reverse map."""
        if ":" not in node_id:
            return node_id
        prefix, local_id = node_id.split(":", 1)
        return f"{prefix.lower()}:{local_id}"

    def _build_equivalent_ids(self, node_id: str) -> List[str]:
        equivalent_ids = [node_id]
        equivalent_ids.extend(self._xref_map.get(node_id, []))
        return sorted(set(equivalent_ids))

    def _build_match(self, node_id: str) -> IdMatch:
        return IdMatch(
            input=node_id,
            match=node_id,
            equivalent_ids=self._build_equivalent_ids(node_id),
            context=["exact"],
        )

    def resolve_internal(self, input_nodes: List[Node]) -> Dict[str, List[IdMatch]]:
        resolution_map = {}
        for node in input_nodes:
            node_id = node.id
            normalized = self._normalize_input_id(node_id)
            if node_id not in self._obo_ids and normalized in self._xref_reverse_map:
                canonical_id = self._xref_reverse_map[normalized]
                resolution_map[node_id] = [
                    IdMatch(
                        input=node_id,
                        match=canonical_id,
                        equivalent_ids=self._build_equivalent_ids(canonical_id),
                        context=["xref"],
                    )
                ]
            else:
                resolution_map[node_id] = [self._build_match(node_id)]
        return resolution_map
