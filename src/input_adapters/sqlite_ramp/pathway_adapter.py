from typing import List

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.interfaces.input_adapter import NodeInputAdapter
from src.input_adapters.sqlite_ramp.tables import Pathway as SqlitePathway
from src.models.pathway import Pathway
from src.output_adapters.generic_labels import NodeLabel


class PathwayAdapter(NodeInputAdapter, RaMPSqliteAdapter):
    def get_audit_trail_entries(self, obj: Pathway) -> List[str]:
        data_version = self.get_data_version(obj.type)
        return [f"Pathway from {data_version.name} ({data_version.version})"]

    name = "RaMP Pathway Adapter"

    def __init__(self, sqlite_file):
        NodeInputAdapter.__init__(self)
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
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
                name=row[4],
                labels=[NodeLabel.Pathway]
            ) for row in results
        ]
        return pathways

