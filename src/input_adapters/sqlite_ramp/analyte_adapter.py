from abc import ABC, abstractmethod
from typing import List
from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.input_adapters.sqlite_ramp.tables import (
    Source as SqliteSource)
from src.interfaces.input_adapter import NodeInputAdapter
from src.models.analyte import Analyte
from src.models.node import EquivalentId


class AnalyteAdapter(NodeInputAdapter, RaMPSqliteAdapter, ABC):

    @abstractmethod
    def get_source_prefix(self):
        raise NotImplementedError("derived classes must implement get_source_prefix")

    def __init__(self, sqlite_file: str):
        NodeInputAdapter.__init__(self)
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
                equiv_ids = [EquivalentId(
                    id=row[1],
                    type=row[2],
                    status=row[3],
                    source=row[4]
                ) for row in analyte_dict[analyte.id]]
                analyte.xref = equiv_ids
    def get_audit_trail_entries(self, obj) -> List[str]:
        return [
            f"Node created based on RaMP version: {self.get_database_version().id}"
        ]