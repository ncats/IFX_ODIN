from abc import ABC, abstractmethod
from typing import Generator, List

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo, DataSourceDetails
from src.models.node import Node
from src.models.protein import Protein
from src.shared.arango_adapter import ArangoAdapter
from src.shared.db_credentials import DBCredentials

def protein_query(reviewed_only: bool) -> str:
    if not reviewed_only:
        return f"""FOR pro IN `biolink:Protein`
        RETURN pro
        """
    return f"""FOR pro IN `biolink:Protein`
    FILTER pro.uniprot_reviewed == {reviewed_only}
    RETURN pro
    """

def protein_version_query():

    return f"""FOR pro IN `biolink:Protein`
        limit 1
        RETURN pro.creation
        """

class PharosArangoAdapter(InputAdapter, ArangoAdapter, ABC):
    reviewed_only: bool
    name: DataSourceName
    version: DatasourceVersionInfo

    def __init__(self, credentials: DBCredentials, database_name: str, reviewed_only = True):
        self.reviewed_only = reviewed_only
        ArangoAdapter.__init__(self, credentials, database_name)
        InputAdapter.__init__(self)
        dsd = self.get_version_info_query()
        self.name = DataSourceName(dsd.name)
        self.version = DatasourceVersionInfo(
            version=dsd.version,
            version_date=dsd.version_date,
            download_date=dsd.download_date
        )

    def get_datasource_name(self) -> DataSourceName:
        return self.name

    def get_version(self) -> DatasourceVersionInfo:
        return self.version

    @abstractmethod
    def get_version_info_query(self):
        pass


class ProteinAdapter(PharosArangoAdapter):

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(protein_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)

    def get_all(self) -> Generator[List[Node], None, None]:
        proteins = self.runQuery(protein_query(self.reviewed_only))
        symbol_count = {}
        for p in proteins:
            symbol = p.get('symbol', None)
            if symbol is not None:
                if symbol not in symbol_count:
                    symbol_count[symbol] = 0
                symbol_count[symbol] += 1

        for p in proteins:
            symbol = p.get('symbol', None)
            if symbol is not None and symbol_count[symbol] == 1:
                p['preferred_symbol'] = symbol
            else:
                p['preferred_symbol'] = p['uniprot_id']

        yield [Protein.from_dict(row) for row in proteins]
