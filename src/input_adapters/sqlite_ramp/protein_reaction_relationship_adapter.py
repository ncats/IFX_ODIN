from typing import List, Generator

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.input_adapters.sqlite_ramp.tables import ReactionToProtein as SqliteReactionToProtein
from src.models.protein import Protein, ProteinReactionRelationship
from src.models.reaction import Reaction


class ProteinReactionRelationshipAdapter(RaMPSqliteAdapter):

    def __init__(self, sqlite_file):
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self) -> Generator[List[ProteinReactionRelationship], None, None]:
        results = self.get_session().query(
            SqliteReactionToProtein.ramp_gene_id,
            SqliteReactionToProtein.ramp_rxn_id,
            SqliteReactionToProtein.is_reviewed
        ).all()

        relationships: List[ProteinReactionRelationship] = [
            ProteinReactionRelationship(
                start_node=Protein(id=row[0]),
                end_node=Reaction(id=row[1]),
                is_reviewed=row[2] == 1
            ) for row in results
        ]
        yield relationships

