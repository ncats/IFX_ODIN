from datetime import date

from src.constants import DataSourceName
from src.input_adapters.sql_adapter import MySqlAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.shared.db_credentials import DBCredentials


class Pharos319Adapter(InputAdapter, MySqlAdapter):
    version = DatasourceVersionInfo(
        version="3.19",
        version_date=date.fromisoformat("2024-02-15"),
    )

    def __init__(self, credentials: DBCredentials):
        super().__init__(credentials)

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.OldPharos

    def get_version(self) -> DatasourceVersionInfo:
        return self.version
