from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import RelationshipInputAdapter
from src.input_adapters.sqlite_ramp.tables import ReactionToProtein as SqliteReactionToProtein
from src.models.protein import Protein, ProteinReactionRelationship
from src.models.reaction import Reaction

class ProteinReactionRelationshipAdapter(RelationshipInputAdapter, SqliteAdapter):
    name = "RaMP Protein Reaction Relationship Adapter"
    start_id_normalizer = PassthroughNormalizer()
    end_id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        RelationshipInputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteReactionToProtein.ramp_gene_id,
            SqliteReactionToProtein.ramp_rxn_id,
            SqliteReactionToProtein.is_reviewed
        ).all()

        relationships: [ProteinReactionRelationship] = [
            ProteinReactionRelationship(
                protein=Protein(id=row[0]),
                reaction=Reaction(id=row[1]),
                is_reviewed=row[2] == 1
            ) for row in results
        ]
        return relationships

