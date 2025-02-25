from typing import List, Generator

from src.constants import DataSourceName
from src.input_adapters.neo4j_adapter import Neo4jAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.go_term import GoTerm


class SetGoTermLeafFlagAdapter(InputAdapter, Neo4jAdapter):

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.PostProcessing

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo()

    def get_all(self) -> Generator[List[GoTerm], None, None]:
        leaf_nodes = self.runQuery(is_leaf_query)
        yield [GoTerm(id=go_id[0], is_leaf=True) for go_id in leaf_nodes]


is_leaf_query = """MATCH (n:GoTerm)
WHERE NOT (n)<-[:GoTermHasParent]-()
RETURN distinct n.id"""
