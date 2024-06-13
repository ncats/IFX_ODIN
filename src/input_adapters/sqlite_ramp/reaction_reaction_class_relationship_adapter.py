from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import RelationshipInputAdapter
from src.input_adapters.sqlite_ramp.tables import Reaction as SqliteReaction
from src.models.reaction import Reaction, ReactionClass, ReactionReactionClassRelationship


class ReactionReactionClassRelationshipAdapter(RelationshipInputAdapter, SqliteAdapter):
    name = "RaMP Reaction Reaction Class Relationship Adapter"
    start_id_normalizer = PassthroughNormalizer()
    end_id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        RelationshipInputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteReaction.ramp_rxn_id,
            SqliteReaction.ec_num
        ).filter(SqliteReaction.ec_num).all()

        relationships: [ReactionReactionClassRelationship] = [
            ReactionReactionClassRelationship(
                reaction=Reaction(id=row[0]),
                reaction_class=ReactionClass(id=row[1])
            ) for row in results
        ]
        return relationships

