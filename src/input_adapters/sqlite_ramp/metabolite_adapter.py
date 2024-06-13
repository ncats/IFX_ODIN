from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import NodeInputAdapter
from src.models.metabolite import Metabolite
from src.input_adapters.sqlite_ramp.tables import Analyte as SqliteAnalyte


class MetaboliteAdapter(NodeInputAdapter, SqliteAdapter):
    name = "RaMP Metabolite Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        NodeInputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteAnalyte.rampId
        ).filter(SqliteAnalyte.type == "compound").all()

        metabolites: [Metabolite] = [
            Metabolite(
                id=row[0],
            ) for row in results
        ]
        return metabolites

