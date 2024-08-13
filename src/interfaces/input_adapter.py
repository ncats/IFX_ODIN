import copy
from abc import ABC, abstractmethod
from typing import List
from src.interfaces.id_normalizer import IdNormalizer
from src.models.node import Node, Relationship


class InputAdapter(ABC):
    name: str

    @abstractmethod
    def get_audit_trail_entries(self, obj) -> List[str]:
        raise NotImplementedError("derived classes must implement get_version_info")

    @abstractmethod
    def get_normalized_and_provenanced_list(self) -> List:
        raise NotImplementedError("derived classes must implement get_normalized_list")


class NodeInputAdapter(InputAdapter, ABC):
    name = "Unnamed Node Adapter"
    id_normalizer: IdNormalizer = None

    def set_id_normalizer(self, id_normalizer: IdNormalizer):
        self.id_normalizer = id_normalizer
        return self

    @abstractmethod
    def get_all(self) -> List[Node]:
        raise NotImplementedError("derived classes must implement get_all")

    def get_normalized_and_provenanced_list(self) -> List:
        entries = self.get_all()
        for entry in entries:
            version_info = self.get_audit_trail_entries(entry)
            entry.provenance.extend(version_info)
        if self.id_normalizer is not None:
            normalization_map = self.id_normalizer.normalize_nodes(entries)
            entries = self.id_normalizer.parse_flat_node_list_from_map(normalization_map)
        return entries


class RelationshipInputAdapter(InputAdapter, ABC):
    start_id_normalizer: IdNormalizer = None
    end_id_normalizer: IdNormalizer = None

    @abstractmethod
    def get_all(self) -> List[Relationship]:
        raise NotImplementedError("derived classes must implement get_all")

    def set_start_normalizer(self, id_normalizer: IdNormalizer):
        self.start_id_normalizer = id_normalizer
        return self

    def set_end_normalizer(self, id_normalizer: IdNormalizer):
        self.end_id_normalizer = id_normalizer
        return self

    def get_normalized_and_provenanced_list(self):
        entries = self.get_all()
        for entry in entries:
            version_info = self.get_audit_trail_entries(entry)
            entry.provenance.extend(version_info)

        if self.start_id_normalizer is None and self.end_id_normalizer is None:
            return entries

        start_node_map, end_node_map = {}, {}

        if self.start_id_normalizer is not None:
            start_nodes = [entry.start_node for entry in entries]
            start_node_map = self.start_id_normalizer.normalize_nodes(start_nodes)
            start_node_map = self.start_id_normalizer.parse_node_map(start_node_map)

        if self.end_id_normalizer is not None:
            end_nodes = [entry.end_node for entry in entries]
            end_node_map = self.end_id_normalizer.normalize_nodes(end_nodes)
            end_node_map = self.end_id_normalizer.parse_node_map(end_node_map)

        return_entries = []
        for entry in entries:
            start_id = entry.start_node.id
            start_nodes = [entry.start_node]

            end_id = entry.end_node.id
            end_nodes = [entry.end_node]

            if self.start_id_normalizer is not None:
                if start_id in start_node_map:
                    start_nodes = start_node_map[start_id]

            if self.end_id_normalizer is not None:
                if end_id in end_node_map:
                    end_nodes = end_node_map[end_id]

            for start_node in start_nodes:
                count = 0
                for end_node in end_nodes:
                    count += 1
                    if count > 1:
                        rel_copy = copy.deepcopy(entry)
                    else:
                        rel_copy = entry
                    rel_copy.start_node.id = start_node.id
                    rel_copy.end_node.id = end_node.id
                    return_entries.append(rel_copy)
        print(f"prepared {len(return_entries)} relationship records to merge")
        return return_entries
