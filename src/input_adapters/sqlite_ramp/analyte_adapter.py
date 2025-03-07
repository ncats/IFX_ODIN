from abc import ABC, abstractmethod
from typing import List

from src.constants import DataSourceName
from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.input_adapters.sqlite_ramp.tables import (
    Source as SqliteSource)
from src.interfaces.input_adapter import InputAdapter
from src.models.analyte import Analyte
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import EquivalentId


class AnalyteAdapter(InputAdapter, RaMPSqliteAdapter, ABC):

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.RaMP

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version=self.ramp_version_info.db_version.id,
            version_date=self.ramp_version_info.db_version.timestamp
        )

    @abstractmethod
    def get_source_prefix(self):
        raise NotImplementedError("derived classes must implement get_source_prefix")

    def __init__(self, sqlite_file: str):
        InputAdapter.__init__(self)
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def add_equivalent_ids(self, analytes: List[Analyte]):
        id_results = self.get_session().query(
            SqliteSource.rampId,
            SqliteSource.sourceId,
            SqliteSource.IDtype,
            SqliteSource.priorityHMDBStatus,
            SqliteSource.dataSource
        ).filter(SqliteSource.rampId.startswith(self.get_source_prefix())).distinct()

        analyte_dict = {}
        for row in id_results:
            ramp_id = row[0]
            if ramp_id in analyte_dict:
                analyte_dict[ramp_id].append(row)
            else:
                analyte_dict[ramp_id] = [row]

        for analyte in analytes:
            if analyte.id in analyte_dict:
                equiv_ids = []
                for row in analyte_dict[analyte.id]:
                    equiv_id = EquivalentId.parse(row[1])
                    equiv_id.source = row[4]
                    equiv_id.status = row[3]
                    equiv_ids.append(equiv_id)
                analyte.xref = equiv_ids
