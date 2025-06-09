import copy
from abc import ABC, abstractmethod
from typing import List, Union, Dict, Generator, Any

from src.constants import DataSourceName
from src.interfaces.id_resolver import IdResolver
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import Node, Relationship


class InputAdapter(ABC):
    batch_size = 50000

    def get_name(self) -> str:
        return f"{self.__class__.__name__} ({self.get_datasource_name().value})"

    @abstractmethod
    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        raise NotImplementedError("derived classes must implement get_all")

    @abstractmethod
    def get_datasource_name(self) -> DataSourceName:
        raise NotImplementedError("derived classes must implement get_datasource_name")

    @abstractmethod
    def get_version(self) -> DatasourceVersionInfo:
        raise NotImplementedError("derived classes must implement get_version")

    def get_resolved_and_provenanced_list(self, resolver_map: Dict[str, IdResolver]) -> Generator[list[Any], Any, None]:
        def get_and_delete_old_id(node):
            source_id = node.id
            if hasattr(node, 'old_id'):
                source_id = node.old_id
                delattr(node, 'old_id')
            return source_id

        for entries in self.get_all():

            for entry in entries:
                version_info = self.get_version()
                version_data = [self.get_datasource_name(), version_info.version, version_info.version_date, version_info.download_date]
                version_string = '\t'.join([str(e) for e in version_data])
                entry.provenance = version_string
                entry.sources = [version_string]

            nodes = [e for e in entries if isinstance(e, Node)]
            relationships = [e for e in entries if isinstance(e, Relationship)]

            if len(nodes) > 0:
                type_map = {}
                for node in nodes:
                    type = node.__class__.__name__
                    if type not in type_map:
                        type_map[type] = []
                    type_map[type].append(node)

                resolved_nodes = []
                for type, node_list in type_map.items():
                    if type in resolver_map:
                        resolver = resolver_map[type]
                        entity_map = resolver.resolve_nodes(node_list)
                        resolved_nodes.extend(resolver.parse_flat_node_list_from_map(entity_map))
                    else:
                        resolved_nodes.extend(node_list)
                nodes = resolved_nodes

            for node in nodes:
                source_id = get_and_delete_old_id(node)
                node.entity_resolution = f"{self.get_datasource_name()}\t{self.__class__.__name__}\t{ source_id }"

            for i in range(0, len(nodes), self.batch_size):
                yield nodes[i:i + self.batch_size]

            for rel in relationships:
                start_source_id = get_and_delete_old_id(rel.start_node)
                end_source_id = get_and_delete_old_id(rel.end_node)
                rel.entity_resolution = f"{self.get_datasource_name()}\t{self.__class__.__name__}\t{ start_source_id }\t{ end_source_id }"

            if len(relationships) > 0:
                temp_nodes = [entry.start_node for entry in relationships] + [entry.end_node for entry in relationships]
                type_map = {}
                node_map = {}

                for node in temp_nodes:
                    type = node.__class__.__name__
                    if type not in type_map:
                        type_map[type] = []
                    type_map[type].append(node)

                for type in type_map:
                    if type in resolver_map:
                        resolver = resolver_map[type]
                        temp_node_map = resolver.resolve_nodes(type_map[type])
                        node_map.update(resolver.parse_entity_map(temp_node_map))

                return_relationships = []
                has_returned_batches = False
                for entry in relationships:
                    if entry.start_node.__class__.__name__ in resolver_map and entry.start_node.id not in node_map:
                        continue
                    if entry.end_node.__class__.__name__ in resolver_map and entry.end_node.id not in node_map:
                        continue

                    start_id = entry.start_node.id
                    start_nodes = [entry.start_node]

                    end_id = entry.end_node.id
                    end_nodes = [entry.end_node]

                    if start_id in node_map:
                        start_nodes = node_map[start_id]
                    if end_id in node_map:
                        end_nodes = node_map[end_id]

                    for start_node in start_nodes:
                        for end_node in end_nodes:
                            rel_copy = copy.deepcopy(entry)
                            rel_copy.start_node.id = start_node.id
                            rel_copy.end_node.id = end_node.id
                            return_relationships.append(rel_copy)

                    if len(return_relationships) >= self.batch_size:
                        has_returned_batches = True
                        print(f"prepared a batch of {len(return_relationships)} relationship records")
                        yield return_relationships
                        return_relationships = []

                if has_returned_batches:
                    print(f"final batch: {len(return_relationships)} relationship records")
                yield return_relationships

