from typing import List

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.input_adapters.sqlite_ramp.tables import AnalyteHasOntology as SqliteAnalyteHasOntology
from src.models.analyte import Analyte
from src.models.ontology import AnalyteOntologyRelationship, Ontology


class MetaboliteOntologyRelationshipAdapter(RelationshipInputAdapter, RaMPSqliteAdapter):
    name = "RaMP Metabolite Ontology Relationship Adapter"

    def get_audit_trail_entries(self, obj) -> List[str]:
        data_version = self.get_data_version('hmdb')
        return [f"Ontological Association from {data_version.name} ({data_version.version})"]

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteAnalyteHasOntology.rampCompoundId,
            SqliteAnalyteHasOntology.rampOntologyId
        ).all()

        analyte_ontology_relationships: [AnalyteOntologyRelationship] = [
            AnalyteOntologyRelationship(
                start_node=Analyte(id=row[0]),
                end_node=Ontology(id=row[1])
            ) for row in results
        ]
        return analyte_ontology_relationships

