from abc import ABC, abstractmethod
from typing import List

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.interfaces.input_adapter import RelationshipInputAdapter
from src.input_adapters.sqlite_ramp.tables import AnalytePathwayRelationship as SqliteAnalytePathwayRelationship
from src.models.analyte import Analyte
from src.models.pathway import AnalytePathwayRelationship, Pathway
from src.output_adapters.generic_labels import NodeLabel, RelationshipLabel


class AnalytePathwayRelationshipAdapter(RelationshipInputAdapter, RaMPSqliteAdapter, ABC):
    name = "RaMP Analyte Pathway Relationship Adapter"

    def get_audit_trail_entries(self, obj: AnalytePathwayRelationship) -> List[str]:
        data_version = self.get_data_version(obj.source)
        return [f"Analyte Pathway Relationship from {data_version.name} ({data_version.version})"]

    def __init__(self, sqlite_file):
        RelationshipInputAdapter.__init__(self)
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    @abstractmethod
    def get_id_prefix(self) -> str:
        pass

    def get_all(self):
        results = self.get_session().query(
            SqliteAnalytePathwayRelationship.rampId,
            SqliteAnalytePathwayRelationship.pathwayRampId,
            SqliteAnalytePathwayRelationship.pathwaySource
        ).filter(SqliteAnalytePathwayRelationship.rampId.startswith(self.get_id_prefix())).all()

        analyte_pathway_relationships: [AnalytePathwayRelationship] = [
            AnalytePathwayRelationship(
                start_node=Analyte(id=row[0], labels=[NodeLabel.Analyte]),
                end_node=Pathway(id=row[1], labels=[NodeLabel.Pathway]),
                labels=[RelationshipLabel.Analyte_Has_Pathway],
                source=row[2]
            ) for row in results
        ]
        return analyte_pathway_relationships


class MetabolitePathwayRelationshipAdapter(AnalytePathwayRelationshipAdapter):
    name = "RaMP Metabolite Pathway Relationship Adapter"

    def get_id_prefix(self) -> str:
        return "RAMP_C"


class ProteinPathwayRelationshipAdapter(AnalytePathwayRelationshipAdapter):
    name = "RaMP Protein Pathway Relationship Adapter"

    def get_id_prefix(self) -> str:
        return "RAMP_G"
