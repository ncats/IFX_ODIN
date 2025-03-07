from abc import ABC, abstractmethod
from typing import List, Generator

from src.constants import DataSourceName
from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.input_adapters.sqlite_ramp.tables import AnalytePathwayRelationship as SqliteAnalytePathwayRelationship
from src.models.analyte import Analyte
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.pathway import AnalytePathwayRelationship, Pathway


class AnalytePathwayRelationshipAdapter(RelationshipInputAdapter, RaMPSqliteAdapter, ABC):
    name = "RaMP Analyte Pathway Relationship Adapter"

    def get_audit_trail_entries(self, obj: AnalytePathwayRelationship) -> List[str]:
        data_version = self.get_data_version(obj.source)
        return [f"Analyte Pathway Relationship from {data_version.name} ({data_version.version})"]

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
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

        analyte_pathway_relationships: [AnalytePathwayRelationship] = [
            AnalytePathwayRelationship(
                start_node=Analyte(id=row[0]),
                end_node=Pathway(id=row[1]),
                source=row[2]
            ) for row in results
        ]
        yield analyte_pathway_relationships


class MetabolitePathwayRelationshipAdapter(AnalytePathwayRelationshipAdapter):
    name = "RaMP Metabolite Pathway Relationship Adapter"

    def get_id_prefix(self) -> str:
        return "RAMP_C"


class ProteinPathwayRelationshipAdapter(AnalytePathwayRelationshipAdapter):
    name = "RaMP Protein Pathway Relationship Adapter"

    def get_id_prefix(self) -> str:
        return "RAMP_G"
