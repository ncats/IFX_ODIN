from typing import List, Generator

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.input_adapters.sqlite_ramp.tables import AnalyteHasOntology as SqliteAnalyteHasOntology
from src.models.analyte import Analyte
from src.models.ontology import AnalyteOntologyRelationship, Ontology


class MetaboliteOntologyRelationshipAdapter(RaMPSqliteAdapter):

    def __init__(self, sqlite_file):
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self) -> Generator[List[AnalyteOntologyRelationship], None, None]:
        results = self.get_session().query(
            SqliteAnalyteHasOntology.rampCompoundId,
            SqliteAnalyteHasOntology.rampOntologyId
        ).all()

        analyte_ontology_relationships: List[AnalyteOntologyRelationship] = [
            AnalyteOntologyRelationship(
                start_node=Analyte(id=row[0]),
                end_node=Ontology(id=row[1])
            ) for row in results
        ]
        yield analyte_ontology_relationships

