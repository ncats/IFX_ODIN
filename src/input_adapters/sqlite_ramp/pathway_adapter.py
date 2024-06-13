from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import NodeInputAdapter
from src.input_adapters.sqlite_ramp.tables import Pathway as SqlitePathway
from src.models.pathway import Pathway


class PathwayAdapter(NodeInputAdapter, SqliteAdapter):
    name = "RaMP Pathway Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        NodeInputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

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
                name=row[4]
            ) for row in results
        ]
        return pathways

