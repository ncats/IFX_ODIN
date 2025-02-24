from typing import List

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.input_adapters.sqlite_ramp.tables import Reaction as SqliteReaction
from src.models.reaction import Reaction, ReactionClass, ReactionReactionClassRelationship


class ReactionReactionClassRelationshipAdapter(RelationshipInputAdapter, RaMPSqliteAdapter):
    name = "RaMP Reaction Reaction Class Relationship Adapter"

    def get_audit_trail_entries(self, obj) -> List[str]:
        data_version = self.get_data_version('rhea')
        return [f"Reaction to Reaction Class Association from {data_version.name} ({data_version.version})"]

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteReaction.ramp_rxn_id,
            SqliteReaction.ec_num
        ).filter(SqliteReaction.ec_num).all()

        relationships: [ReactionReactionClassRelationship] = [
            ReactionReactionClassRelationship(
                start_node=Reaction(id=row[0]),
                end_node=ReactionClass(id=row[1])
            ) for row in results
        ]
        return relationships

