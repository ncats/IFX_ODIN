from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.sqlite_ramp.tables import Reaction as SqliteReaction
from src.input_adapters.util.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.reaction import Reaction, ReactionDirection


class ReactionAdapter(InputAdapter, SqliteAdapter):
    name = "RaMP Reaction Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_reactions(self):
        results = self.get_session().query(
            SqliteReaction.ramp_rxn_id,
            SqliteReaction.rxn_source_id,
            SqliteReaction.is_transport,
            SqliteReaction.direction,
            SqliteReaction.label,
            SqliteReaction.equation,
            SqliteReaction.html_equation
        ).all()

        reactions: [Reaction] = [
            Reaction(
                id=row[0],
                source_id=row[1],
                is_transport=row[2],
                direction=ReactionDirection.parse(row[3]),
                label=row[4],
                equation=row[5],
                html_equation=row[6]
            ) for row in results
        ]
        return reactions

    def next(self):
        reactions = self.get_reactions()
        for reaction in reactions:
            yield reaction
