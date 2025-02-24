from typing import List

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.input_adapters.sqlite_ramp.tables import Reaction as SqliteReaction
from src.interfaces.input_adapter import InputAdapter
from src.models.reaction import Reaction, ReactionDirection


class ReactionAdapter(InputAdapter, RaMPSqliteAdapter):
    def get_audit_trail_entries(self, obj) -> List[str]:
        data_version = self.get_data_version('rhea')
        return [f"Reaction from {data_version.name} ({data_version.version})"]

    name = "RaMP Reaction Adapter"

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
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

