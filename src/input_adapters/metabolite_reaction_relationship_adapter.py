from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.util.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.input_adapters.sqlite_ramp.tables import ReactionToMetabolite as SqliteReactionToMetabolite
from src.models.metabolite import Metabolite, MetaboliteReactionRelationship
from src.models.reaction import Reaction


class MetaboliteReactionRelationshipAdapter(InputAdapter, SqliteAdapter):
    name = "RaMP Metabolite Reaction Relationship Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteReactionToMetabolite.ramp_cmpd_id,
            SqliteReactionToMetabolite.ramp_rxn_id,
            SqliteReactionToMetabolite.substrate_product,
            SqliteReactionToMetabolite.is_cofactor
        ).all()

        relationships: [MetaboliteReactionRelationship] = [
            MetaboliteReactionRelationship(
                metabolite=Metabolite(id=row[0]),
                reaction=Reaction(id=row[1]),
                substrate_product=row[2],
                is_cofactor=row[3] == 1
            ) for row in results
        ]
        return relationships

    def next(self):
        relationships = self.get_all()
        for rel in relationships:
            yield rel
