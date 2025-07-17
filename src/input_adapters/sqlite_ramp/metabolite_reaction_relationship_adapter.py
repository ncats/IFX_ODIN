from typing import List, Generator

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.input_adapters.sqlite_ramp.tables import ReactionToMetabolite as SqliteReactionToMetabolite
from src.models.metabolite import Metabolite, MetaboliteReactionRelationship
from src.models.reaction import Reaction


class MetaboliteReactionRelationshipAdapter(RaMPSqliteAdapter):
    def __init__(self, sqlite_file):
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self) -> Generator[List[MetaboliteReactionRelationship], None, None]:
        results = self.get_session().query(
            SqliteReactionToMetabolite.ramp_cmpd_id,
            SqliteReactionToMetabolite.ramp_rxn_id,
            SqliteReactionToMetabolite.substrate_product,
            SqliteReactionToMetabolite.is_cofactor
        ).all()

        relationships: List[MetaboliteReactionRelationship] = [
            MetaboliteReactionRelationship(
                start_node=Metabolite(id=row[0]),
                end_node=Reaction(id=row[1]),
                substrate_product=row[2],
                is_cofactor=row[3] == 1
            ) for row in results
        ]
        yield relationships

