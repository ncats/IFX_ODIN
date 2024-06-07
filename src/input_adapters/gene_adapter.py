from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.util.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.input_adapters.sqlite_ramp.tables import Analyte as SqliteAnalyte, Catalyzed as SqliteCatalyzed
from src.models.gene import Gene


class GeneAdapter(InputAdapter, SqliteAdapter):
    name = "RaMP Gene Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all_genes(self):
        results = (self.get_session().query(
            SqliteAnalyte.rampId, SqliteCatalyzed.proteinType
        ).outerjoin(SqliteCatalyzed, SqliteAnalyte.rampId == SqliteCatalyzed.rampGeneId)
                   .filter(SqliteAnalyte.type == "gene").distinct())

        genes: [Gene] = [
            Gene(id=row[0], protein_type=row[1]) for row in results
        ]
        return genes

    def next(self):
        genes = self.get_all_genes()
        for gene in genes:
            yield gene
