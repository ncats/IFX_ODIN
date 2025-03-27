import csv
import re
from typing import Generator, List, Union
import yaml
from src.constants import DataSourceName
from src.core.etl import ETL
import os

from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import Node, Relationship
from src.output_adapters.neo4j_output_adapter import MemgraphOutputAdapter
from src.shared.db_credentials import DBCredentials

credentials_file = "./secrets/ifxdev_pounce_dev.yaml"

with open(credentials_file, "r") as file:
    credentials = yaml.safe_load(file)

output_adapter = MemgraphOutputAdapter(credentials=DBCredentials(
    url = credentials['url'], user = credentials['user'], password=credentials['password']
))



class DumpAdapter(InputAdapter):

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        # Read directory, compile all node and relationship CSVs
        csv_directory = "/Users/kelleherkj/IdeaProjects/ramp-backend-ncats/src/problem_mets_with_no_known_source"

        node_files = [f for f in os.listdir(csv_directory) if f.endswith('_nodes.tsv')]
        relationship_files = [f for f in os.listdir(csv_directory) if f.endswith('_relationships.tsv')]

        nodes = []

        relationships = []

        for rel_file in relationship_files:
            ramp_id = re.search(r'RAMP_C_\d+', rel_file).group(0)
            print('reading: ', rel_file)
            with open(os.path.join(csv_directory, rel_file), 'r') as f:
                reader = csv.reader(f, delimiter='\t')
                for row in reader:
                    start_node = Node(id=row[0])
                    end_node = Node(id=row[1])
                    rel = Relationship(
                        start_node=start_node, end_node=end_node
                    )
                    setattr(rel, 'rampIDs', [ramp_id])
                    relationships.append(rel)

        for node_file in node_files:

            ramp_id = re.search(r'RAMP_C_\d+', node_file).group(0)
            ramp_node = Node(id=ramp_id, labels=['RAMP_ID'])
            nodes.append(ramp_node)

            print('reading: ', node_file)
            with open(os.path.join(csv_directory, node_file), 'r') as f:
                reader = csv.reader(f, delimiter='\t')
                for row in reader:
                    id_node = Node(
                        id=row[0]
                    )
                    setattr(id_node, 'rampIDs', [ramp_id])
                    setattr(id_node, 'mws', row[1])
                    setattr(id_node, 'synonyms', row[2])
                    nodes.append(id_node)

                    relationships.append(Relationship(
                        start_node=ramp_node, end_node=id_node
                    ))
        print(len(nodes))
        yield [*nodes, *relationships]

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.Dummy

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(version="1")

dump_adapter = DumpAdapter()

builder = ETL(input_adapters=[dump_adapter], output_adapters=[output_adapter])
builder.create_or_truncate_datastores()
builder.do_etl()
