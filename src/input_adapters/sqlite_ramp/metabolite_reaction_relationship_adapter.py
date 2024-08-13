from typing import List

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.interfaces.input_adapter import RelationshipInputAdapter
from src.input_adapters.sqlite_ramp.tables import ReactionToMetabolite as SqliteReactionToMetabolite
from src.models.metabolite import Metabolite, MetaboliteReactionRelationship
from src.models.reaction import Reaction


class MetaboliteReactionRelationshipAdapter(RelationshipInputAdapter, RaMPSqliteAdapter):
    name = "RaMP Metabolite Reaction Relationship Adapter"

    def get_audit_trail_entries(self, obj) -> List[str]:
        data_version = self.get_data_version('rhea')
        return [f"Metabolite to Reaction Association from {data_version.name} ({data_version.version})"]
    def __init__(self, sqlite_file):
        RelationshipInputAdapter.__init__(self)
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteReactionToMetabolite.ramp_cmpd_id,
            SqliteReactionToMetabolite.ramp_rxn_id,
            SqliteReactionToMetabolite.substrate_product,
            SqliteReactionToMetabolite.is_cofactor
        ).all()

        relationships: [MetaboliteReactionRelationship] = [
            MetaboliteReactionRelationship(
                start_node=Metabolite(id=row[0]),
                end_node=Reaction(id=row[1]),
                substrate_product=row[2],
                is_cofactor=row[3] == 1
            ) for row in results
        ]
        return relationships

