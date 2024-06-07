from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.sqlite_ramp.tables import ReactionClass as SqliteReactionClass
from src.input_adapters.util.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.reaction import ReactionClass


class ReactionClassAdapter(InputAdapter, SqliteAdapter):
    name = "RaMP Reaction Class Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_reaction_classes(self):
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

    def next(self):
        reaction_classes = self.get_reaction_classes()
        for reaction_class in reaction_classes:
            yield reaction_class
