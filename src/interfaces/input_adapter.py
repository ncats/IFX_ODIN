import copy
from abc import ABC, abstractmethod
from typing import List, Union
from src.interfaces.id_resolver import IdResolver
from src.models.node import Node, Relationship


class InputAdapter(ABC):
    name: str

    @abstractmethod
    def get_all(self) -> List[Union[Node, Relationship]]:
        raise NotImplementedError("derived classes must implement get_all")

    @abstractmethod
    def get_audit_trail_entries(self, obj) -> List[str]:
        raise NotImplementedError("derived classes must implement get_audit_trail_entries")

    def get_resolved_and_provenanced_list(self) -> List:
        entries = self.get_all()

        for entry in entries:
            version_info = self.get_audit_trail_entries(entry)
            entry.provided_by.extend(version_info)

        nodes = [e for e in entries if isinstance(e, Node)]
        relationships = [e for e in entries if isinstance(e, Relationship)]

        if isinstance(self, NodeInputAdapter):
            if self.id_resolver is not None:
                entity_map = self.id_resolver.resolve_nodes(nodes)
                nodes = self.id_resolver.parse_flat_node_list_from_map(entity_map)

        if isinstance(self, RelationshipInputAdapter):
            if self.start_id_resolver is None and self.end_id_resolver is None:
                return [*nodes, *relationships]

            start_node_map, end_node_map = {}, {}

            if self.start_id_resolver is not None:
                start_nodes = [entry.start_node for entry in relationships]
                start_node_map = self.start_id_resolver.resolve_nodes(start_nodes)
                start_node_map = self.start_id_resolver.parse_entity_map(start_node_map)

            if self.end_id_resolver is not None:
                end_nodes = [entry.end_node for entry in relationships]
                end_node_map = self.end_id_resolver.resolve_nodes(end_nodes)
                end_node_map = self.end_id_resolver.parse_entity_map(end_node_map)

            return_relationships = []
            for entry in relationships:
                start_id = entry.start_node.id
                start_nodes = [entry.start_node]

                end_id = entry.end_node.id
                end_nodes = [entry.end_node]

                if self.start_id_resolver is not None:
                    if start_id in start_node_map:
                        start_nodes = start_node_map[start_id]

                if self.end_id_resolver is not None:
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
                        return_relationships.append(rel_copy)
            relationships = return_relationships
            print(f"prepared {len(relationships)} relationship records to merge")
        return [*nodes, *relationships]

class NodeInputAdapter(InputAdapter, ABC):
    name = "Unnamed Node Adapter"
    id_resolver: IdResolver = None

    def set_id_resolver(self, id_resolver: IdResolver):
        self.id_resolver = id_resolver
        return self


class RelationshipInputAdapter(InputAdapter, ABC):
    start_id_resolver: IdResolver = None
    end_id_resolver: IdResolver = None

    def set_start_resolver(self, id_resolver: IdResolver):
        self.start_id_resolver = id_resolver
        return self

    def set_end_resolver(self, id_resolver: IdResolver):
        self.end_id_resolver = id_resolver
        return self
