from typing import List

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.interfaces.input_adapter import RelationshipInputAdapter
from src.input_adapters.sqlite_ramp.tables import Catalyzed as SqliteCatalyzed
from src.models.protein import Protein
from src.models.metabolite import MetaboliteProteinRelationship, Metabolite
from src.output_adapters.generic_labels import NodeLabel, RelationshipLabel


class MetaboliteProteinRelationshipAdapter(RelationshipInputAdapter, RaMPSqliteAdapter):
    cached_audit_trail_info = None
    name = "RaMP Metabolite Protein Relationship Adapter"

    def get_audit_trail_entries(self, obj) -> List[str]:
        return [
            f"catalyzed relationship based on HMDB version: {self.get_data_version('hmdb').version}"
        ]

    def __init__(self, sqlite_file):
        RelationshipInputAdapter.__init__(self)
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteCatalyzed.rampCompoundId,
            SqliteCatalyzed.rampGeneId
        ).all()

        relationships: [MetaboliteProteinRelationship] = [
            MetaboliteProteinRelationship(
                start_node=Metabolite(id=row[0], labels=[NodeLabel.Metabolite]),
                end_node=Protein(id=row[1], labels=[NodeLabel.Protein]),
                labels=[RelationshipLabel.Catalyzes]
            ) for row in results
        ]
        return relationships
