from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.sqlite_ramp.tables import ReactionClass as SqliteReactionClass
from src.input_adapters.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import NodeInputAdapter
from src.models.reaction import ReactionClass


class ReactionClassAdapter(NodeInputAdapter, SqliteAdapter):
    name = "RaMP Reaction Class Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        NodeInputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteReactionClass.rxn_class_ec,
            SqliteReactionClass.ec_level,
            SqliteReactionClass.rxn_class
        ).distinct()

        reaction_classes: [ReactionClass] = [
            ReactionClass(
                id=row[0],
                level=row[1],
                name=row[2]
            ) for row in results
        ]
        return reaction_classes

