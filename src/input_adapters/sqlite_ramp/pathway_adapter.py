from typing import Generator, List

from src.constants import DataSourceName
from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.input_adapters.sqlite_ramp.tables import Pathway as SqlitePathway
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.pathway import Pathway


class PathwayAdapter(InputAdapter, RaMPSqliteAdapter):

    name = "RaMP Pathway Adapter"

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self) -> Generator[List[Pathway], None, None]:
        results = self.get_session().query(
            SqlitePathway.pathwayRampId,
            SqlitePathway.sourceId,
            SqlitePathway.type,
            SqlitePathway.pathwayCategory,
            SqlitePathway.pathwayName
        ).all()

        pathways: [Pathway] = [
            Pathway(
                id=row[0],
                source_id=row[1],
                type=row[2],
                category=row[3],
                name=row[4]
            ) for row in results
        ]
        yield pathways

