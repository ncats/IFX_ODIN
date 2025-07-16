from typing import List, Generator

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.input_adapters.sqlite_ramp.tables import Reaction as SqliteReaction
from src.models.reaction import Reaction, ReactionClass, ReactionReactionClassRelationship


class ReactionReactionClassRelationshipAdapter(RaMPSqliteAdapter):

    def __init__(self, sqlite_file):
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self) -> Generator[List[ReactionReactionClassRelationship], None, None]:
        results = self.get_session().query(
            SqliteReaction.ramp_rxn_id,
            SqliteReaction.ec_num
        ).filter(SqliteReaction.ec_num).all()

        relationships: List[ReactionReactionClassRelationship] = [
            ReactionReactionClassRelationship(
                start_node=Reaction(id=row[0]),
                end_node=ReactionClass(id=row[1])
            ) for row in results
        ]
        yield relationships

