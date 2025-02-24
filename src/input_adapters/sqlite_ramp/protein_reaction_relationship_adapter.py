from typing import List

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.input_adapters.sqlite_ramp.tables import ReactionToProtein as SqliteReactionToProtein
from src.models.protein import Protein, ProteinReactionRelationship
from src.models.reaction import Reaction


class ProteinReactionRelationshipAdapter(RelationshipInputAdapter, RaMPSqliteAdapter):
    name = "RaMP Protein Reaction Relationship Adapter"

    def get_audit_trail_entries(self, obj) -> List[str]:
        data_version = self.get_data_version('rhea')
        return [f"Protein to Reaction Association from {data_version.name} ({data_version.version})"]

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteReactionToProtein.ramp_gene_id,
            SqliteReactionToProtein.ramp_rxn_id,
            SqliteReactionToProtein.is_reviewed
        ).all()

        relationships: [ProteinReactionRelationship] = [
            ProteinReactionRelationship(
                start_node=Protein(id=row[0]),
                end_node=Reaction(id=row[1]),
                is_reviewed=row[2] == 1
            ) for row in results
        ]
        return relationships

