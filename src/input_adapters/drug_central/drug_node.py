from typing import List, Generator
from src.constants import Prefix, DataSourceName
from src.input_adapters.drug_central.tables import Structures, DBVersion
from src.input_adapters.sql_adapter import PostgreSqlAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.ligand import Ligand
from src.models.node import EquivalentId
from src.shared.db_credentials import DBCredentials


class DrugCentralAdapter(PostgreSqlAdapter):
    version_info: DatasourceVersionInfo

    def __init__(self, credentials: DBCredentials):
        PostgreSqlAdapter.__init__(self, credentials)
        self.initialize_version()

    def initialize_version(self):
        results = self.get_session().query(
            DBVersion.version,
            DBVersion.dtime
        ).first()
        self.version_info = DatasourceVersionInfo(version=results.version, version_date=results.dtime)


class DrugNodeAdapter(InputAdapter, DrugCentralAdapter):

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.DrugCentral

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List[Ligand], None, None]:
        query_results = self.get_session().query(
            Structures.id,
            Structures.name,
            Structures.smiles
        ).all()

        drug_list = [
            Ligand(
                id=EquivalentId(id=row.id, type=Prefix.DrugCentral).id_str(),
                name=row.name,
                smiles=row.smiles,
                isDrug=True
            )
            for row in query_results
        ]
        yield drug_list
