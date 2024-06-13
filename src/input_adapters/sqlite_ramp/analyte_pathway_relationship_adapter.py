from abc import ABC, abstractmethod

from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import RelationshipInputAdapter
from src.input_adapters.sqlite_ramp.tables import AnalytePathwayRelationship as SqliteAnalytePathwayRelationship
from src.models.analyte import Analyte
from src.models.pathway import AnalytePathwayRelationship, Pathway


class AnalytePathwayRelationshipAdapter(RelationshipInputAdapter, SqliteAdapter, ABC):
    name = "RaMP Analyte Pathway Relationship Adapter"
    end_id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        RelationshipInputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    @abstractmethod
    def get_id_prefix(self) -> str:
        pass

    def get_all(self):
        results = self.get_session().query(
            SqliteAnalytePathwayRelationship.rampId,
            SqliteAnalytePathwayRelationship.pathwayRampId
        ).filter(SqliteAnalytePathwayRelationship.rampId.startswith(self.get_id_prefix())).all()

        analyte_pathway_relationships: [AnalytePathwayRelationship] = [
            AnalytePathwayRelationship(
                analyte=Analyte(id=row[0]),
                pathway=Pathway(id=row[1])
            ) for row in results
        ]
        return analyte_pathway_relationships


class MetabolitePathwayRelationshipAdapter(AnalytePathwayRelationshipAdapter):
    name = "RaMP Metabolite Pathway Relationship Adapter"
    start_id_normalizer = PassthroughNormalizer()

    def get_id_prefix(self) -> str:
        return "RAMP_C"


class ProteinPathwayRelationshipAdapter(AnalytePathwayRelationshipAdapter):
    name = "RaMP Protein Pathway Relationship Adapter"
    start_id_normalizer = PassthroughNormalizer()  # this should be node-norm

    def get_id_prefix(self) -> str:
        return "RAMP_G"
