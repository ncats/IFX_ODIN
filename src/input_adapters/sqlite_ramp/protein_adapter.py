from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import NodeInputAdapter
from src.input_adapters.sqlite_ramp.tables import (
    Analyte as SqliteAnalyte,
    Catalyzed as SqliteCatalyzed)
from src.models.protein import Protein


class ProteinAdapter(NodeInputAdapter, SqliteAdapter):
    name = "RaMP Protein Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        NodeInputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = (self.get_session().query(
            SqliteAnalyte.rampId,
            SqliteCatalyzed.proteinType
        ).outerjoin(SqliteCatalyzed, SqliteAnalyte.rampId == SqliteCatalyzed.rampGeneId)
                   .filter(SqliteAnalyte.type == "gene").distinct())

        nodes: [Protein] = [
            Protein(id=row[0], protein_type=row[1]) for row in results
        ]
        return nodes
