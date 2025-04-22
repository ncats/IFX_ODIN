from typing import List, Generator

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.go_term import GoTerm
from src.shared.arango_adapter import ArangoAdapter


class SetGoTermLeafFlagAdapter(InputAdapter, ArangoAdapter):

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.PostProcessing

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo()

    def get_all(self) -> Generator[List[GoTerm], None, None]:
        leaf_nodes = self.runQuery(is_leaf_query)
        yield [GoTerm(id=go_id, is_leaf=True) for go_id in leaf_nodes]


is_leaf_query = """FOR n IN `GoTerm`
  FILTER LENGTH(FOR e IN INBOUND n `GoTermHasParent` RETURN e) == 0
  RETURN DISTINCT n.id
"""
