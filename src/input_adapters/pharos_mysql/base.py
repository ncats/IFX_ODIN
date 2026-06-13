from src.constants import DataSourceName
from src.input_adapters.sql_adapter import MySqlAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.shared.db_credentials import DBCredentials


class Pharos319Adapter(InputAdapter, MySqlAdapter):
    def __init__(self, credentials: DBCredentials, data_source):
        super().__init__(credentials)
        self.version_info = data_source.version_info()

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.OldPharos

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info
