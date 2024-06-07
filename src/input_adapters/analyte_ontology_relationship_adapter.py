from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.util.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.input_adapters.sqlite_ramp.tables import AnalyteHasOntology as SqliteAnalyteHasOntology
from src.models.analyte import Analyte
from src.models.ontology import AnalyteOntologyRelationship, Ontology


class AnalyteOntologyRelationshipAdapter(InputAdapter, SqliteAdapter):
    name = "RaMP Analyte Ontology Relationship Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all_ontology_relationships(self):
        results = self.get_session().query(
            SqliteAnalyteHasOntology.rampCompoundId,
            SqliteAnalyteHasOntology.rampOntologyId
        ).all()

        analyte_ontology_relationships: [AnalyteOntologyRelationship] = [
            AnalyteOntologyRelationship(
                analyte=Analyte(id=row[0]),
                ontology=Ontology(id=row[1])
            ) for row in results
        ]
        return analyte_ontology_relationships

    def next(self):
        ontology_relationships = self.get_all_ontology_relationships()
        for ontology_rel in ontology_relationships:
            yield ontology_rel
