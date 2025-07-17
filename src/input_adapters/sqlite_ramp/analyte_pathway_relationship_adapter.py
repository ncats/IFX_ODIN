from abc import ABC, abstractmethod
from typing import List, Generator

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.input_adapters.sqlite_ramp.tables import AnalytePathwayRelationship as SqliteAnalytePathwayRelationship
from src.models.metabolite import Metabolite
from src.models.pathway import AnalytePathwayRelationship, Pathway
from src.models.protein import Protein


class AnalytePathwayRelationshipAdapter(RaMPSqliteAdapter, ABC):

    def __init__(self, sqlite_file):
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    @abstractmethod
    def get_id_prefix(self) -> str:
        pass

    def get_all(self) -> Generator[List[AnalytePathwayRelationship], None, None]:
        results = self.get_session().query(
            SqliteAnalytePathwayRelationship.rampId,
            SqliteAnalytePathwayRelationship.pathwayRampId,
            SqliteAnalytePathwayRelationship.pathwaySource
        ).filter(SqliteAnalytePathwayRelationship.rampId.startswith(self.get_id_prefix())).all()

        analyte_pathway_relationships: List[AnalytePathwayRelationship] = [
            AnalytePathwayRelationship(
                start_node=Metabolite(id=row[0]) if row[0].startswith("RAMP_C") else Protein(id=row[0]),
                end_node=Pathway(id=row[1]),
                source=row[2]
            ) for row in results
        ]
        yield analyte_pathway_relationships


class MetabolitePathwayRelationshipAdapter(AnalytePathwayRelationshipAdapter):
    def get_id_prefix(self) -> str:
        return "RAMP_C"


class ProteinPathwayRelationshipAdapter(AnalytePathwayRelationshipAdapter):
    def get_id_prefix(self) -> str:
        return "RAMP_G"
