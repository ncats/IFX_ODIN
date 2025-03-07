from typing import List, Generator

from src.constants import DataSourceName
from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.metabolite_class import MetaboliteClass
from src.input_adapters.sqlite_ramp.tables import MetaboliteClass as SqliteMetaboliteClass


class MetaboliteClassAdapter(InputAdapter, RaMPSqliteAdapter):

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.RaMP

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version=self.ramp_version_info.db_version.id,
            version_date=self.ramp_version_info.db_version.timestamp
        )

    name = "RaMP Metabolite Class Adapter"

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self) -> Generator[List[MetaboliteClass], None, None]:
        results = self.get_session().query(
            SqliteMetaboliteClass.class_level_name,
            SqliteMetaboliteClass.class_name,
            SqliteMetaboliteClass.source
        ).distinct().all()

        metabolite_classes: [MetaboliteClass] = [
            MetaboliteClass(
                id=MetaboliteClass.compiled_name(row[0], row[1]),
                level=row[0],
                name=row[1],
                source=row[2]) for row in
            results
        ]
        yield metabolite_classes
