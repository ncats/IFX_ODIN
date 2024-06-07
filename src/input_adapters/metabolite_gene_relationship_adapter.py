from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.util.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.input_adapters.sqlite_ramp.tables import Catalyzed as SqliteCatalyzed
from src.models.gene import Gene
from src.models.metabolite import MetaboliteGeneRelationship, Metabolite


class MetaboliteGeneRelationshipAdapter(InputAdapter, SqliteAdapter):
    name = "RaMP Metabolite Gene Relationship Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all_metabolite_gene_relationships(self):
        results = self.get_session().query(
            SqliteCatalyzed.rampCompoundId,
            SqliteCatalyzed.rampGeneId
        ).all()

        met_gene_relationships: [MetaboliteGeneRelationship] = [
            MetaboliteGeneRelationship(
                metabolite=Metabolite(id=row[0]),
                gene=Gene(id=row[1])
            ) for row in results
        ]
        return met_gene_relationships

    def next(self):
        metabolite_gene_relationships = self.get_all_metabolite_gene_relationships()
        for met_rel in metabolite_gene_relationships:
            yield met_rel
