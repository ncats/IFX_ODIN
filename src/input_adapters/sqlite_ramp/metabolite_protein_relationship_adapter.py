from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import RelationshipInputAdapter
from src.input_adapters.sqlite_ramp.tables import Catalyzed as SqliteCatalyzed
from src.models.protein import Protein
from src.models.metabolite import MetaboliteProteinRelationship, Metabolite


class MetaboliteProteinRelationshipAdapter(RelationshipInputAdapter, SqliteAdapter):
    name = "RaMP Metabolite Protein Relationship Adapter"
    start_id_normalizer = PassthroughNormalizer()
    end_id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        RelationshipInputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteCatalyzed.rampCompoundId,
            SqliteCatalyzed.rampGeneId
        ).all()

        relationships: [MetaboliteProteinRelationship] = [
            MetaboliteProteinRelationship(
                metabolite=Metabolite(id=row[0]),
                protein=Protein(id=row[1])
            ) for row in results
        ]
        return relationships

