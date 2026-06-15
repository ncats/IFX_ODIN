import csv
from collections import defaultdict
from typing import Dict, List, Optional, Union

from src.id_resolvers.resolver_snapshot import resolver_input, resolver_options
from src.interfaces.id_resolver import IdMatch, IdResolver
from src.models.node import Node


class CureIdLabelResolver(IdResolver):
    name = "CURE ID Label Resolver"

    DEFAULT_NODE_TYPE_TO_SOURCE_TYPES = {
        "Condition": {"Disease"},
        "Drug": {"Drug"},
        "Phenotype": {"PhenotypicFeature", "AdverseEvent"},
        "Gene": {"Gene"},
    }

    def __init__(
        self,
        resolver_snapshot,
        **kwargs,
    ):
        options = resolver_options(resolver_snapshot)
        label_field_by_type = options.get("label_field_by_type")
        node_type_to_source_types = options.get("node_type_to_source_types")
        manual_label_map = options.get("manual_label_map")
        super().__init__(**kwargs)
        self.resolver_snapshot = resolver_snapshot
        self.tsv_file = str(resolver_input(resolver_snapshot, "data_source").file())
        self.label_field_by_type = label_field_by_type or {}
        self.manual_label_map = manual_label_map or {}
        raw_mapping = node_type_to_source_types or self.DEFAULT_NODE_TYPE_TO_SOURCE_TYPES
        self.node_type_to_source_types = {
            node_type: {value.strip() for value in values}
            for node_type, values in raw_mapping.items()
        }
        self.label_map: Dict[str, Dict[str, List[str]]] = self._load_label_map()
        self.curie_map: Dict[str, set[str]] = self._load_curie_map()
        self._apply_manual_label_map()

    @staticmethod
    def _normalize_label(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        if not isinstance(value, str):
            value = str(value)
        normalized = " ".join(value.split()).strip()
        return normalized or None

    def _get_label_for_node(self, node: Node) -> Optional[str]:
        node_type = node.__class__.__name__
        field_name = self.label_field_by_type.get(node_type, "name")
        value = getattr(node, field_name, None)
        normalized = self._normalize_label(value)
        if normalized is not None:
            return normalized
        if field_name != "text":
            text_value = getattr(node, "text", None)
            normalized = self._normalize_label(text_value)
            if normalized is not None:
                return normalized
        return None

    def _load_label_map(self) -> Dict[str, Dict[str, List[str]]]:
        label_map: Dict[str, Dict[str, set[str]]] = {
            node_type: defaultdict(set) for node_type in self.types
        }
        with open(self.tsv_file, newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row in reader:
                for side in ("subject", "object"):
                    source_type = self._normalize_label(row.get(f"{side}_type"))
                    label = self._normalize_label(row.get(f"{side}_label_original"))
                    curie = self._normalize_label(row.get(f"{side}_final_curie"))
                    if label is None or curie is None or source_type is None:
                        continue
                    for node_type in self.types:
                        allowed_source_types = self.node_type_to_source_types.get(node_type, set())
                        if source_type in allowed_source_types:
                            label_map.setdefault(node_type, defaultdict(set))[label].add(curie)
        return {
            node_type: {
                label: sorted(curies)
                for label, curies in labels.items()
            }
            for node_type, labels in label_map.items()
        }

    def _load_curie_map(self) -> Dict[str, set[str]]:
        curie_map: Dict[str, set[str]] = {
            node_type: set() for node_type in self.types
        }
        with open(self.tsv_file, newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row in reader:
                for side in ("subject", "object"):
                    source_type = self._normalize_label(row.get(f"{side}_type"))
                    curie = self._normalize_label(row.get(f"{side}_final_curie"))
                    if curie is None or source_type is None:
                        continue
                    for node_type in self.types:
                        allowed_source_types = self.node_type_to_source_types.get(node_type, set())
                        if source_type in allowed_source_types:
                            curie_map.setdefault(node_type, set()).add(curie)
        return curie_map

    def _apply_manual_label_map(self) -> None:
        for node_type, label_to_curies in self.manual_label_map.items():
            if node_type not in self.types:
                continue
            for raw_label, raw_curies in label_to_curies.items():
                label = self._normalize_label(raw_label)
                if label is None:
                    continue
                if isinstance(raw_curies, str):
                    curies = [raw_curies]
                else:
                    curies = list(raw_curies or [])
                normalized_curies = [
                    curie
                    for curie in (self._normalize_label(curie) for curie in curies)
                    if curie is not None
                ]
                if not normalized_curies:
                    continue
                existing_curies = set(self.label_map.setdefault(node_type, {}).get(label, []))
                existing_curies.update(normalized_curies)
                self.label_map[node_type][label] = sorted(existing_curies)
                self.curie_map.setdefault(node_type, set()).update(normalized_curies)

    def resolve_internal(self, input_nodes: List[Node]) -> Dict[str, List[IdMatch]]:
        result: Dict[str, List[IdMatch]] = {}
        for node in input_nodes:
            node_type = node.__class__.__name__
            if node.id in self.curie_map.get(node_type, set()):
                result[node.id] = [
                    IdMatch(
                        input=node.id,
                        match=node.id,
                        equivalent_ids=[node.id],
                        context=[f"canonical_id:{node.id}"],
                    )
                ]
                continue
            label = self._get_label_for_node(node)
            if label is None:
                result[node.id] = []
                continue
            matches = self.label_map.get(node_type, {}).get(label, [])
            result[node.id] = [
                IdMatch(
                    input=node.id,
                    match=curie,
                    equivalent_ids=matches,
                    context=[f"label:{label}"],
                )
                for curie in matches
            ]
        return result
