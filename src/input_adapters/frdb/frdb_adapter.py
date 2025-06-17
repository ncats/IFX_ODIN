from datetime import date
from typing import Generator, List, Union
import ijson
from src.constants import DataSourceName
from src.input_adapters.frdb.frdb_parser import FRDBParser
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import Node, Relationship


def stream_large_json_array(filepath):
    with open(filepath, 'r') as f:
        for item in ijson.items(f, 'item'):
            yield item

class FRDBAdapter(InputAdapter):
    file_path: str
    parser: FRDBParser = FRDBParser()

    def __init__(self, file_path: str):
        self.file_path = file_path

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        count = 0
        ligands = []
        source_map = {}
        source_edges = []
        condition_nodes = []
        condition_edges = []

        for item in stream_large_json_array(self.file_path):
            count += 1

            # if count > 1000:
            #     continue
            new_lig = self.parser.parse_ligands(item)
            ligands.append(new_lig)

            source_nodes, source_rels = self.parser.parse_sources(item, new_lig)
            for node in source_nodes:
                if node.id not in source_map:
                    source_map[node.id] = node
            source_edges.extend(source_rels)

            temp_condition_nodes, temp_condition_edges = self.parser.parse_conditions(item, new_lig)
            condition_nodes.extend(temp_condition_nodes)
            condition_edges.extend(temp_condition_edges)


        yield ligands
        yield list(source_map.values())
        yield source_edges
        yield condition_nodes
        yield condition_edges

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.FRDB

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version_date=date(2024, 12,31)
        )